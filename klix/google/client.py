import os
import time
import logging
from typing import Dict, Generator, Iterable, Optional, Tuple

import requests


logger = logging.getLogger(__name__)


class GoogleApiError(Exception):
    """Raised when Google API calls fail in a non-recoverable way."""
    pass


# === Environment / constants ===

PLACES_KEY = (os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()

# When this flag is true, ALL Google Places endpoints are disabled at runtime.
# This is to prevent unexpected billing (e.g. surprise $350 months).
PLACES_DEPRECATED = os.getenv("KLIX_PLACES_DEPRECATED", "1") == "1"

HTTP_TIMEOUT = 12
PAGE_TOKEN_WAIT = 2.0
MAX_RETRIES_DEFAULT = 3
RETRY_BACKOFF_DEFAULT = 1.5


def _require_key() -> str:
    """
    Ensure GOOGLE_PLACES_API_KEY is present and non-empty, or raise GoogleApiError.
    """
    if not PLACES_KEY:
        raise GoogleApiError(
            "GOOGLE_PLACES_API_KEY is not set in the environment. "
            "Ensure /etc/klix/secret.env contains GOOGLE_PLACES_API_KEY and "
            "that the process is started with 'set -a; source /etc/klix/secret.env; set +a'."
        )
    return PLACES_KEY


def _assert_places_enabled() -> None:
    """
    Guardrail: prevent any Places API calls when PLACES_DEPRECATED is enabled.

    This ensures that even if old code paths are accidentally hit,
    they will fail loudly instead of silently burning money.
    """
    if PLACES_DEPRECATED:
        msg = (
            "Google Places is deprecated for cost reasons; "
            "use Lead Engine v2 (OSM + crawler) instead."
        )
        logger.error(msg)
        # Use GoogleApiError so existing callers that catch this type still work.
        raise GoogleApiError(msg)


def _http_get_json(
    url: str,
    params: Dict[str, str],
    *,
    kind: str,
    max_retries: int = MAX_RETRIES_DEFAULT,
    retry_backoff: float = RETRY_BACKOFF_DEFAULT,
    timeout: int = HTTP_TIMEOUT,
) -> Dict:
    """
    Thin wrapper around requests.get with basic retry + Google status handling.

    Returns the parsed JSON dict on success, or raises GoogleApiError on hard failure.
    """
    key = _require_key()
    params = dict(params or {})
    params.setdefault("key", key)

    last_status: Optional[str] = None
    data: Optional[Dict] = None

    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            if attempt >= max_retries:
                raise GoogleApiError(
                    f"[{kind}] HTTP error after {max_retries} retries: {e}"
                )
            sleep = retry_backoff * (2 ** attempt)
            time.sleep(sleep)
            continue

        status = (data or {}).get("status", "OK")
        last_status = status

        if status == "OK":
            return data

        if status == "OVER_QUERY_LIMIT":
            cooldown = min(60.0, retry_backoff * (2 ** attempt))
            time.sleep(cooldown)
            if attempt >= max_retries:
                raise GoogleApiError(
                    f"[{kind}] OVER_QUERY_LIMIT after {max_retries} retries."
                )
            continue

        if status in ("INVALID_REQUEST", "ZERO_RESULTS", "NOT_FOUND"):
            # "Soft" failures; caller can decide how to handle.
            return data

        # Unknown status: retry a few times, then raise.
        if attempt >= max_retries:
            raise GoogleApiError(
                f"[{kind}] Unexpected Google status='{status}' after {max_retries} retries."
            )
        sleep = retry_backoff * (2 ** attempt)
        time.sleep(sleep)

    raise GoogleApiError(f"[{kind}] Failed with status={last_status!r}")


# === Public API ===

def geocode_city(city: str, country_code: str) -> Tuple[float, float]:
    """
    Geocode "City, CountryCode" to (lat, lng) using the Geocoding API.

    Example:
        geocode_city("Toronto", "CA") -> (43.65..., -79.38...)
    """
    if not city or not country_code:
        raise GoogleApiError("geocode_city requires both city and country_code.")

    address = f"{city}, {country_code}"
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address}

    data = _http_get_json(url, params, kind="geocode")
    results = (data or {}).get("results") or []
    if not results:
        raise GoogleApiError(f"[geocode] No results for address={address!r}")

    loc = results[0].get("geometry", {}).get("location") or {}
    try:
        lat = float(loc["lat"])
        lng = float(loc["lng"])
    except Exception:
        raise GoogleApiError(f"[geocode] Malformed location for address={address!r}")

    return lat, lng


def places_text_search(
    query: str,
    lat_lng: Optional[Tuple[float, float]],
    radius_m: int,
    type_filter: Optional[str] = None,
    *,
    max_retries: int = MAX_RETRIES_DEFAULT,
    retry_backoff: float = RETRY_BACKOFF_DEFAULT,
) -> Generator[Dict, None, None]:
    """
    Generator over Google Places Text Search results.

    NOTE:
        This endpoint is now guarded by PLACES_DEPRECATED / _assert_places_enabled().
        In production, PLACES_DEPRECATED should remain enabled (true) to prevent
        accidental billing. Use Lead Engine v2 instead of Places.

    - query: free-text search query (e.g., "coffee shop")
    - lat_lng: optional (lat, lng) for location bias; if None, no location is sent.
    - radius_m: search radius in meters (used when lat_lng is provided).
    - type_filter: optional Places 'type' (e.g., 'cafe', 'restaurant').

    Yields individual Place result dicts (items inside 'results').
    """
    _assert_places_enabled()

    if not query:
        raise GoogleApiError("places_text_search requires a non-empty query.")

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params: Dict[str, str] = {"query": query}

    if lat_lng is not None:
        lat, lng = lat_lng
        params["location"] = f"{lat},{lng}"
        params["radius"] = str(int(radius_m))

    if type_filter:
        params["type"] = type_filter

    pagetoken: Optional[str] = None

    while True:
        if pagetoken:
            params["pagetoken"] = pagetoken
        else:
            params.pop("pagetoken", None)

        data = _http_get_json(
            url,
            params,
            kind="textsearch",
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )
        results: Iterable[Dict] = (data or {}).get("results") or []
        for r in results:
            yield r

        pagetoken = (data or {}).get("next_page_token")
        if not pagetoken:
            break

        time.sleep(PAGE_TOKEN_WAIT)


def places_details(
    place_id: str,
    *,
    max_retries: int = MAX_RETRIES_DEFAULT,
    retry_backoff: float = RETRY_BACKOFF_DEFAULT,
) -> Dict:
    """
    Fetch detailed information for a single Place using its place_id.

    NOTE:
        This endpoint is now guarded by PLACES_DEPRECATED / _assert_places_enabled().
        In production, PLACES_DEPRECATED should remain enabled (true) to prevent
        accidental billing. Use Lead Engine v2 instead of Places.

    Returns the 'result' dict on success, or {} if ZERO_RESULTS / NOT_FOUND.
    Raises GoogleApiError on hard HTTP/OVER_QUERY_LIMIT failures.
    """
    _assert_places_enabled()

    if not place_id:
        raise GoogleApiError("places_details requires a non-empty place_id.")

    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join(
        [
            "name",
            "website",
            "formatted_phone_number",
            "formatted_address",
            "rating",
            "user_ratings_total",
        ]
    )
    params = {"place_id": place_id, "fields": fields}

    data = _http_get_json(
        url,
        params,
        kind="details",
        max_retries=max_retries,
        retry_backoff=retry_backoff,
    )

    status = (data or {}).get("status", "OK")
    if status in ("ZERO_RESULTS", "NOT_FOUND"):
        return {}

    return (data or {}).get("result") or {}
