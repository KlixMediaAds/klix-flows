from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timezone
import random, time

# Reuse your existing US finder implementation
from klix.lead_finder import lead_finder_us as LF

def _company_from_row(row: Dict[str, Any]) -> str:
    name = (row.get("name") or "").strip()
    if name:
        return name
    # fallback: domain as company if name absent
    try:
        ext = LF.tldextract.extract(row.get("website",""))
        dom = ".".join(p for p in [ext.domain, ext.suffix] if p)
        return dom or "Unknown"
    except Exception:
        return "Unknown"

def _to_neon_item(row: Dict[str, Any]) -> Dict[str, Any]:
    # minimal Neon schema fields + a rich meta payload
    meta = {
        "website": row.get("website"),
                "website_domain": row.get("website_domain"),
"city": row.get("city"),
        "niche": row.get("niche_search"),
        "rating": row.get("rating"),
        "ratings_total": row.get("ratings_total"),
        "instagram": row.get("instagram"),
        "tiktok": row.get("tiktok"),
        "site_title": row.get("site_title"),
        "site_desc": row.get("site_desc"),
        "tagline": row.get("tagline"),
        "unique_service": row.get("unique_service"),
        "vibe": row.get("vibe"),
                "tone_words": row.get("tone_words"),
        "services": row.get("services"),
        "about_names": row.get("about_names"),
        "hero_product": row.get("hero_product"),
"owner_name": row.get("owner_name"),
        "tech_stack": row.get("tech_stack"),
        "emails_all": row.get("emails_all"),
        "platform_primary": row.get("platform_primary"),
    }
    return {
        "email": (row.get("email") or "").lower(),
        "company": _company_from_row(row),
        "first_name": None,
        "last_name": None,
        "source": "google_places",
        "status": "NEW",
        "meta": {k: v for k, v in meta.items() if v not in (None, "", [])},
        "discovered_at": datetime.now(timezone.utc),
    }

def _gather_details_for_query(query: str, cfg: Dict[str, Any]) -> list[dict]:
    """Use Places Text Search + Details to build a details list for one query."""
    results = LF.run_text_search(query, cfg)
    time.sleep(cfg["sleep"])
    return LF.collect_details_for_query(results, cfg)

def _enrich_and_filter(details_list: list[dict], niche: str, city: str, sleep_sec: float) -> list[Dict[str, Any]]:
    leads = []
    for d in details_list:
        out = LF.process_one(d, niche, city, sleep_sec)
        if not out:
            continue
        row, _ = out
        email = (row.get("email") or "").lower()
        if not email or LF._is_blacklisted(email):
            continue
        # carry niche/city through for meta
        row["niche_search"] = niche
        row["city"] = city
        leads.append(row)
    return leads

def find_leads(count: int = 25) -> List[Dict[str, Any]]:
    """
    Return up to `count` REAL leads using Google Places + site parsing from lead_finder_us.py.
    Only leads with a usable email are returned.
    """
    # Small, polite config (reuses LF.http_get_json retry/backoff)
    cfg = {
        "max_per_query": max(5, min(40, count*3)),  # over-fetch a bit to ensure we end with `count` emails
        "sleep": 0.8,
        "max_retries": 2,
        "retry_backoff": 1.5,
        "city_timeout": 300,
        "api_calls": 0,
        "over_limit_hits": 0,
        "api_budget": 1000,
        "warn_at": 0.9,
    }

    # Use your defaults from the module (niches + cities)
    niches = LF.load_niches()[:4]  # first few niches
    cities = LF.load_cities()[:10] # first 10 cities

    items: List[Dict[str, Any]] = []
    # Try a couple of country-scope queries first (they return lots of candidates)
    for niche, mode in niches:
        if len(items) >= count:
            break
        # country-wide sweep
        details_list = _gather_details_for_query(f"{niche} United States", cfg)
        rows = _enrich_and_filter(details_list, niche, "", cfg["sleep"])
        for r in rows:
            items.append(_to_neon_item(r))
            if len(items) >= count:
                break

        # then a few city-targeted queries to diversify
        random.shuffle(cities)
        for city in cities[:5]:
            if len(items) >= count:
                break
            details_list = _gather_details_for_query(f"{niche} in {city}, United States", cfg)
            rows = _enrich_and_filter(details_list, niche, city, cfg["sleep"])
            for r in rows:
                items.append(_to_neon_item(r))
                if len(items) >= count:
                    break

    # De-dupe by email+company before returning (extra safety before DB upsert)
    seen = set()
    unique_items = []
    for it in items:
        key = (it["email"], it["company"])
        if key in seen:
            continue
        seen.add(key)
        unique_items.append(it)

    return unique_items[:count]
