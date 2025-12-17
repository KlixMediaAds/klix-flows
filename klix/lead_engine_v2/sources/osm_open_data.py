"""
OSM / Overpass source for Lead Engine v2.

Reliability guardrails:
- Overpass instances can rate-limit / 504. We use retries + exponential backoff.
- We also support endpoint fallback across multiple public Overpass instances.

Output:
- Returns a list of NormalizedLead objects with source='osm_scraper_v1'
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError

from ..models import NormalizedLead


OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]


def _http_post_json(url: str, form: Dict[str, str], timeout_s: int) -> Dict[str, Any]:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": "KlixOS-LeadEngineV2/1.0 (contact: ops@klixmedia.ca)",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _http_post_json_resilient(
    endpoints: List[str],
    form: Dict[str, str],
    timeout_s: int,
    attempts_per_endpoint: int = 3,
    backoff_base_s: float = 1.5,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for endpoint in endpoints:
        for i in range(attempts_per_endpoint):
            try:
                return _http_post_json(endpoint, form, timeout_s=timeout_s)
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
                last_err = e
                # exponential backoff
                sleep_s = backoff_base_s * (2 ** i)
                time.sleep(sleep_s)
                continue
    # If all endpoints fail, raise a clean exception with context
    raise RuntimeError(f"Overpass request failed across endpoints after retries: {last_err!r}") from last_err


def _extract_addr(tags: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    street = None
    city = None
    region = None
    country = None
    postal = None

    housenumber = tags.get("addr:housenumber")
    road = tags.get("addr:street")
    if housenumber and road:
        street = f"{housenumber} {road}".strip()
    elif road:
        street = str(road).strip()

    city = tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village")
    region = tags.get("addr:state") or tags.get("addr:province") or tags.get("addr:region")
    country = tags.get("addr:country")
    postal = tags.get("addr:postcode")

    return (
        street.strip() if isinstance(street, str) else None,
        city.strip() if isinstance(city, str) else None,
        region.strip() if isinstance(region, str) else None,
        country.strip() if isinstance(country, str) else None,
        postal.strip() if isinstance(postal, str) else None,
    )


def _pick_website(tags: Dict[str, Any]) -> Optional[str]:
    for k in ("contact:website", "website", "url"):
        v = tags.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pick_phone(tags: Dict[str, Any]) -> Optional[str]:
    for k in ("contact:phone", "phone", "contact:mobile", "mobile"):
        v = tags.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _category_from_tags(tags: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    amenity = tags.get("amenity")
    shop = tags.get("shop")
    healthcare = tags.get("healthcare")
    beauty = tags.get("beauty")

    if isinstance(healthcare, str) and healthcare.strip():
        return ("healthcare", healthcare.strip())
    if isinstance(amenity, str) and amenity.strip():
        return ("amenity", amenity.strip())
    if isinstance(shop, str) and shop.strip():
        return ("shop", shop.strip())
    if isinstance(beauty, str) and beauty.strip():
        return ("beauty", beauty.strip())

    return (None, None)


def _make_source_ref(osm_type: str, osm_id: int) -> str:
    return f"https://www.openstreetmap.org/{osm_type}/{osm_id}"


def query_overpass(
    *,
    area_name: str,
    country_code: str,
    tags_any: List[Tuple[str, str]],
    timeout_s: int = 60,
    maxsize: int = 1073741824,
    sleep_s: float = 1.0,
) -> List[NormalizedLead]:
    """
    Query Overpass for an administrative area by name, returning normalized leads.

    NOTE:
    - `country_code` is retained for config compatibility / forensics.
    - Name-only area selection is used because ISO filters can produce 0 results.

    tags_any: list of (k,v) pairs; we OR them together.
    """
    if not area_name.strip():
        raise ValueError("area_name required")
    if not tags_any:
        raise ValueError("tags_any required")

    tag_filters = "\n".join([f'  nwr["{k}"="{v}"](area.searchArea);' for k, v in tags_any])

    query = f"""
[out:json][timeout:{timeout_s}][maxsize:{maxsize}];
area["name"="{area_name}"]->.searchArea;
(
{tag_filters}
);
out center tags;
""".strip()

    payload = {"data": query}

    if sleep_s > 0:
        time.sleep(sleep_s)

    data = _http_post_json_resilient(
        OVERPASS_ENDPOINTS,
        payload,
        timeout_s=timeout_s,
        attempts_per_endpoint=3,
        backoff_base_s=1.25,
    )
    elements = data.get("elements", []) if isinstance(data, dict) else []

    out: List[NormalizedLead] = []
    for el in elements:
        if not isinstance(el, dict):
            continue

        osm_type = el.get("type")
        osm_id = el.get("id")
        tags = el.get("tags") or {}
        if not isinstance(tags, dict):
            tags = {}

        name = tags.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")

        street, city, region, country, postal = _extract_addr(tags)
        website = _pick_website(tags)
        phone = _pick_phone(tags)
        category, subcategory = _category_from_tags(tags)

        source_ref = _make_source_ref(str(osm_type), int(osm_id)) if osm_type and osm_id else "unknown"

        lead = NormalizedLead(
            name=name.strip(),
            website=website,
            email=None,
            phone=phone,
            street=street,
            city=city,
            region=region,
            country=country,
            postal_code=postal,
            latitude=float(lat) if isinstance(lat, (int, float)) else None,
            longitude=float(lon) if isinstance(lon, (int, float)) else None,
            category=category,
            subcategory=subcategory,
            source="osm_scraper_v1",
            source_ref=source_ref,
            source_details={
                "overpass_area_name": area_name,
                "overpass_country_code": country_code,
                "osm_type": osm_type,
                "osm_id": osm_id,
                "tags": tags,
            },
        )
        out.append(lead)

    return out
