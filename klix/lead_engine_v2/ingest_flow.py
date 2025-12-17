"""
Core ingestion orchestration for Lead Engine v2.

Prefect-free on purpose; Prefect wrapper lives in flows/lead_engine_v2_flow.py.

Current live sources:
- osm_scraper_v1 (Overpass) -> INSERT (idempotent)
- website_crawler_v1 (regex email extraction) -> UPDATE enrichment into existing lead row (same dedupe_key)

Design goals:
- Nightly stability: source failures do not crash the whole run.
- Cost-bound: OSM freshness guard prevents pointless re-pulls.
- No duplicates: crawler enriches existing rows instead of inserting new ones.
- Operational discipline (ERA 1.9.15):
    - per-source yield counters
    - mechanical low-yield disablement for OSM based on consecutive zero-insert normal runs
"""

from __future__ import annotations

import json
import os
import random
import urllib.parse
from typing import Dict, List, Optional, Tuple

from sqlalchemy import bindparam, text
from sqlalchemy.types import JSON

from klix.db import get_engine, engine  # keep engine for existing repo expectations

from .config import OSM_AREAS, OSM_TAGS_ANY, SOURCES_ENABLED
from .dedupe import make_dedupe_key
from .models import NormalizedLead
from .sources.osm_open_data import query_overpass
from .crawler.website_crawler import crawl_domain, normalize_root_url
from .crawler.extractor import extract_emails_from_html


# -----------------------------
# Env helpers
# -----------------------------
def _env_int(name: str, default: int) -> int:
    try:
        v = int(os.environ.get(name, str(default)) or str(default))
        return v
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(os.environ.get(name, str(default)) or str(default))
        return v
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_debug() -> bool:
    return _env_bool("LEAD_ENGINE_V2_DEBUG", False)


def _is_normal_run() -> bool:
    """
    A normal run is defined as:
      - no forced diagnostic flags
      - no hard-coded queries
      - all enabled sources active

    We can only enforce what this code controls; the key guardrail here is
    DIAGNOSTIC-only modes being OFF.
    """
    if _env_bool("LEAD_ENGINE_V2_DIAGNOSTIC_DEDUPE_INCLUDE_SOURCE_REF", False):
        return False
    if _env_bool("LEAD_ENGINE_V2_DIAGNOSTIC_MODE", False):
        return False
    return True


# -----------------------------
# ingest_state (k/v) helpers
# -----------------------------
_STATE_PREFIX = "lead_engine_v2"
_OSM_DISABLED_K = f"{_STATE_PREFIX}.source_disabled.osm_scraper_v1"
_OSM_ZERO_STREAK_K = f"{_STATE_PREFIX}.osm_zero_insert_streak"
_LAST_INSERT_TS_K = f"{_STATE_PREFIX}.last_successful_insert_ts"


def _get_state(k: str) -> Optional[str]:
    eng = get_engine()
    with eng.begin() as conn:
        v = conn.execute(text("SELECT v FROM ingest_state WHERE k = :k"), {"k": k}).scalar()
    return v if v is not None else None


def _set_state(k: str, v: str) -> None:
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ingest_state (k, v, ts)
                VALUES (:k, :v, now())
                ON CONFLICT (k) DO UPDATE
                SET v = EXCLUDED.v, ts = now()
                """
            ),
            {"k": k, "v": v},
        )


def _del_state(k: str) -> None:
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM ingest_state WHERE k = :k"), {"k": k})


def _is_osm_disabled() -> bool:
    v = (_get_state(_OSM_DISABLED_K) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _get_osm_zero_streak() -> int:
    try:
        return int((_get_state(_OSM_ZERO_STREAK_K) or "0").strip())
    except Exception:
        return 0


def _set_osm_zero_streak(n: int) -> None:
    _set_state(_OSM_ZERO_STREAK_K, str(int(max(0, n))))


def _note_last_successful_insert_ts() -> None:
    eng = get_engine()
    with eng.begin() as conn:
        ts = conn.execute(
            text(
                """
                SELECT MAX(created_at)
                FROM leads
                WHERE source = 'osm_scraper_v1'
                """
            )
        ).scalar()
    if ts is not None:
        _set_state(_LAST_INSERT_TS_K, ts.isoformat())


def _last_successful_insert_ts() -> Optional[str]:
    return _get_state(_LAST_INSERT_TS_K)


# -----------------------------
# Small helpers
# -----------------------------
def _host_for_site(site: str) -> str:
    try:
        s = (site or "").strip()
        if not s:
            return ""
        if "://" not in s:
            s = "https://" + s
        u = urllib.parse.urlparse(s)
        host = (u.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _pick_best_email(emails: List[str], website: str) -> Optional[str]:
    if not emails:
        return None
    host = _host_for_site(website)

    # Prefer same-domain emails first
    if host:
        for e in emails:
            dom = (e.split("@", 1)[1].lower().strip() if "@" in e else "")
            if dom == host or dom.endswith("." + host):
                return e

    # Otherwise just take the first (already filtered by extractor)
    return emails[0]


# -----------------------------
# SQL (INSERT / ENRICH)
# -----------------------------
_INSERT_SQL = text(
    """
    INSERT INTO leads (
        company,
        email,
        website,
        source,
        created_at,
        status,
        meta,
        dedupe_key,
        email_verification_status,
        source_details,
        source_ref,
        category,
        subcategory
    )
    VALUES (
        :company,
        :email,
        :website,
        :source,
        now(),
        :status,
        :meta,
        :dedupe_key,
        :email_verification_status,
        :source_details,
        :source_ref,
        :category,
        :subcategory
    )
    ON CONFLICT (dedupe_key) DO NOTHING
    """
).bindparams(
    bindparam("meta", type_=JSON()),
    bindparam("source_details", type_=JSON()),
)

_ENRICH_UPDATE_SQL = text(
    """
    UPDATE leads
    SET
        email = COALESCE(NULLIF(trim(leads.email), ''), :email),
        website = COALESCE(NULLIF(trim(leads.website), ''), :website),
        status = CASE
            WHEN :email IS NOT NULL AND length(trim(:email)) > 0 THEN 'enriched'
            ELSE leads.status
        END,
        meta = COALESCE(leads.meta, '{}'::jsonb) || COALESCE((:meta)::jsonb, '{}'::jsonb),
        source_details = COALESCE(leads.source_details, '{}'::jsonb) || COALESCE((:source_details)::jsonb, '{}'::jsonb)
    WHERE dedupe_key = :dedupe_key
    """
).bindparams(
    bindparam("meta", type_=JSON()),
    bindparam("source_details", type_=JSON()),
)


def _lead_to_params(lead: NormalizedLead, *, status: str) -> Dict:
    meta = {
        "name": lead.name,
        "phone": lead.phone,
        "street": lead.street,
        "city": lead.city,
        "region": lead.region,
        "country": lead.country,
        "postal_code": lead.postal_code,
        "latitude": lead.latitude,
        "longitude": lead.longitude,
    }

    return {
        "company": lead.name,
        "email": lead.email,
        "website": lead.website,
        "source": lead.source,
        "status": status,
        "meta": meta,
        "dedupe_key": lead.dedupe_key,
        "email_verification_status": "",
        "source_details": lead.source_details,
        "source_ref": lead.source_ref,
        "category": lead.category,
        "subcategory": lead.subcategory,
    }


def _insert_base_leads(leads: List[NormalizedLead]) -> Dict[str, int]:
    if not leads:
        return {"attempted": 0, "inserted": 0, "deduped": 0}

    eng = get_engine()
    inserted = 0

    with eng.begin() as conn:
        for lead in leads:
            params = _lead_to_params(lead, status="new")
            res = conn.execute(_INSERT_SQL, params)
            inserted += int(res.rowcount or 0)

    attempted = len(leads)
    deduped = attempted - inserted
    return {"attempted": attempted, "inserted": inserted, "deduped": deduped}


def _should_skip_osm_freshness() -> Optional[str]:
    # Enforce a minimum interval between OSM insert attempts (NORMAL runs only)
    if not _env_bool("LEAD_ENGINE_OSM_ENFORCE_FRESHNESS", False):
        return None

    min_seconds = _env_int("LEAD_ENGINE_OSM_MIN_INTERVAL_SECONDS", 1800)
    last_ts = _last_successful_insert_ts()
    if not last_ts:
        return None

    try:
        import datetime as dt
        from dateutil import parser as dtparser  # type: ignore
        last = dtparser.isoparse(last_ts)
        now = dt.datetime.now(dt.timezone.utc)
        delta = (now - last).total_seconds()
        if delta < float(min_seconds):
            return f"osm inserted recently (min interval gate: {int(delta)}s < {int(min_seconds)}s)"
    except Exception:
        # If parsing fails, do not block OSM
        return None

    return None


def _fetch_osm_base() -> Tuple[int, int, int, int]:
    fetched_total = 0
    all_leads: List[NormalizedLead] = []

    for area_name, country_code in OSM_AREAS:
        leads = query_overpass(
            area_name=area_name,
            country_code=country_code,
            tags_any=OSM_TAGS_ANY,
        )
        fetched_total += len(leads)
        all_leads.extend(leads)

    normalized_total = len(all_leads)

    for lead in all_leads:
        lead.dedupe_key = make_dedupe_key(lead)

    ins = _insert_base_leads(all_leads)
    inserted_total = ins["inserted"]
    deduped_total = ins["deduped"]

    return fetched_total, normalized_total, inserted_total, deduped_total


def _select_crawl_candidates(limit: int) -> List[Dict]:
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    dedupe_key,
                    company,
                    website,
                    category,
                    subcategory,
                    meta
                FROM leads
                WHERE website IS NOT NULL
                    AND length(trim(website)) > 0
                    AND (email IS NULL OR length(trim(email)) = 0)
                    AND source = 'osm_scraper_v1'
                    AND NOT (COALESCE(source_details, '{}'::jsonb) ? 'crawler')
                ORDER BY created_at DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).mappings().all()
    return [dict(r) for r in rows]


def _run_website_crawler() -> Dict[str, int]:
    limit = _env_int("LEAD_ENGINE_V2_CRAWL_LIMIT", 25)
    max_pages = _env_int("LEAD_ENGINE_V2_CRAWL_MAX_PAGES", 8)
    max_depth = _env_int("LEAD_ENGINE_V2_CRAWL_MAX_DEPTH", 2)
    timeout_s = _env_int("LEAD_ENGINE_V2_CRAWL_TIMEOUT_S", 15)
    delay_s = _env_float("LEAD_ENGINE_V2_CRAWL_DELAY_S", 0.2)
    shuffle = _env_bool("LEAD_ENGINE_V2_CRAWL_SHUFFLE", True)
    debug = _env_debug()

    candidates = _select_crawl_candidates(limit=limit)
    if shuffle:
        random.shuffle(candidates)

    fetched = len(candidates)
    normalized = len(candidates)
    inserted = 0
    deduped = 0

    eng = get_engine()

    for row in candidates:
        dedupe_key = (row.get("dedupe_key") or "").strip()
        company = (row.get("company") or "").strip()
        website_raw = (row.get("website") or "").strip()
        category = row.get("category")
        subcategory = row.get("subcategory")
        meta = row.get("meta") or {}

        if not dedupe_key:
            deduped += 1
            continue

        website = normalize_root_url(website_raw) or website_raw

        city = None
        region = None
        country = None
        if isinstance(meta, dict):
            city = meta.get("city")
            region = meta.get("region")
            country = meta.get("country")

        try:
            pages = crawl_domain(
                website,
                max_pages=max_pages,
                max_depth=max_depth,
                timeout_s=timeout_s,
                delay_s=delay_s,
            )

            # If the crawler returns zero pages (blocked/timeout/DNS), mark attempt so it won't re-queue forever
            if not pages:
                try:
                    from datetime import datetime
                    now = datetime.utcnow().isoformat() + "Z"
                    meta_patch = {
                        "crawler_last_seen_website": website,
                        "crawler_last_attempt_ts": now,
                        "crawler_last_error": "no_pages",
                    }
                    source_details = {
                        "crawler": {
                            "status": "no_pages",
                            "email_count": 0,
                            "pages_crawled": 0,
                            "seed_company": company or website,
                            "seed_city": city,
                            "seed_region": region,
                            "seed_country": country,
                            "category": category,
                            "subcategory": subcategory,
                        }
                    }
                    with eng.begin() as conn:
                        conn.execute(
                            text(
                                """
                                UPDATE leads
                                SET
                                  meta = COALESCE(meta, '{}'::jsonb) || CAST(:meta AS jsonb),
                                  source_details = jsonb_set(
                                      CASE WHEN jsonb_typeof(COALESCE(source_details, '{}'::jsonb))='object'
                                           THEN COALESCE(source_details, '{}'::jsonb) ELSE '{}'::jsonb END,
                                      '{crawler}', CAST(:crawler AS jsonb), true
                                  )
                                WHERE dedupe_key = :dedupe_key
                                """
                            ),
                            {
                                "meta": json.dumps(meta_patch),
                                "crawler": json.dumps(source_details["crawler"]),
                                "dedupe_key": dedupe_key,
                            },
                        )
                except Exception as mark_no_pages_err:
                    if debug:
                        print(
                            "WARN: failed to mark no_pages for",
                            website,
                            type(mark_no_pages_err).__name__,
                            str(mark_no_pages_err)[:220],
                        )
                deduped += 1
                continue

            all_emails: List[str] = []
            crawled_urls: List[str] = []
            for p in pages:
                crawled_urls.append(p.url)
                all_emails.extend(extract_emails_from_html(p.body))

            seen = set()
            emails: List[str] = []
            for e in all_emails:
                if e in seen:
                    continue
                seen.add(e)
                emails.append(e)

            email_primary = _pick_best_email(emails, website)

            source_details = {
                "crawler": {
                    "emails": emails[:25],
                    "crawled_urls": crawled_urls[:25],
                    "email_count": len(emails),
                    "pages_crawled": len(pages),
                    "seed_company": company or website,
                    "seed_city": city,
                    "seed_region": region,
                    "seed_country": country,
                    "category": category,
                    "subcategory": subcategory,
                }
            }

            meta_patch = {"crawler_last_seen_website": website}

            with eng.begin() as conn:
                res = conn.execute(
                    _ENRICH_UPDATE_SQL,
                    {
                        "source_details": source_details,
                        "dedupe_key": dedupe_key,
                        "email": email_primary,
                        "website": website,
                        "meta": json.dumps(meta_patch),
                    },
                )
                updated = int(res.rowcount or 0)

            if updated != 1:
                if debug:
                    print("WARN: enrich update rowcount != 1 for", website, "dk", dedupe_key[:12], "rc", updated)
                deduped += 1
                continue

            if email_primary:
                inserted += 1
            else:
                deduped += 1

        except Exception as e:
            if debug:
                print("ERROR: crawl/enrich failed for", website, type(e).__name__, str(e)[:220])

            # Mark attempt as error so dead domains don't re-queue forever
            try:
                from datetime import datetime

                err_type = type(e).__name__
                err_msg = (str(e) or "")[:500]
                now = datetime.utcnow().isoformat() + "Z"

                meta_patch = {
                    "crawler_last_seen_website": website,
                    "crawler_last_error": f"{err_type}: {err_msg}",
                    "crawler_last_attempt_ts": now,
                }

                source_details = {
                    "crawler": {
                        "status": "error",
                        "error": f"{err_type}: {err_msg}",
                        "seed_company": company or website,
                        "seed_city": city,
                        "seed_region": region,
                        "seed_country": country,
                        "category": category,
                        "subcategory": subcategory,
                    }
                }

                with eng.begin() as conn:
                    conn.execute(
                        text(
                            """
                            UPDATE leads
                            SET
                              meta = COALESCE(meta, '{}'::jsonb) || CAST(:meta AS jsonb),
                              source_details = jsonb_set(
                                  CASE WHEN jsonb_typeof(COALESCE(source_details, '{}'::jsonb))='object'
                                       THEN COALESCE(source_details, '{}'::jsonb) ELSE '{}'::jsonb END,
                                  '{crawler}', CAST(:crawler AS jsonb), true
                              )
                            WHERE dedupe_key = :dedupe_key
                            """
                        ),
                        {
                            "meta": json.dumps(meta_patch),
                            "crawler": json.dumps(source_details["crawler"]),
                            "dedupe_key": dedupe_key,
                        },
                    )

            except Exception as mark_err:
                if debug:
                    print("WARN: failed to mark crawler error for", website, type(mark_err).__name__, str(mark_err)[:220])

            deduped += 1
            continue

    return {"fetched": fetched, "normalized": normalized, "inserted": inserted, "deduped": deduped}


def run_ingest() -> Dict[str, Dict[str, object]]:
    """
    Returns per-source counters and (optionally) per-source error messages.
    NEVER raises; caller must treat errors as INTAKE_BROKEN.
    """
    out: Dict[str, Dict[str, object]] = {}

    # ---- OSM (INSERT) ----
    if not SOURCES_ENABLED.get("osm_scraper_v1", False):
        out["osm_scraper_v1"] = {"fetched": 0, "normalized": 0, "inserted": 0, "deduped": 0}
    elif _is_osm_disabled():
        out["osm_scraper_v1"] = {
            "fetched": 0,
            "normalized": 0,
            "inserted": 0,
            "deduped": 0,
            "error": "SOURCE_DISABLED_LOW_YIELD",
            "consecutive_zero_insert_runs": _get_osm_zero_streak(),
        }

    else:
        try:
            skip_reason = _should_skip_osm_freshness()
            if skip_reason:
                out["osm_scraper_v1"] = {
                    "fetched": 0,
                    "normalized": 0,
                    "inserted": 0,
                    "deduped": 0,
                    "skip_reason": skip_reason,
                }
            else:
                fetched, normalized, inserted, deduped = _fetch_osm_base()
                out["osm_scraper_v1"] = {
                    "fetched": int(fetched),
                    "normalized": int(normalized),
                    "inserted": int(inserted),
                    "deduped": int(deduped),
                }
        except Exception as e:
            out["osm_scraper_v1"] = {
                "fetched": 0,
                "normalized": 0,
                "inserted": 0,
                "deduped": 0,
                "error": f"{type(e).__name__}: {str(e)[:500]}",
            }

    # ---- Website crawler (ENRICH) ----
    if SOURCES_ENABLED.get("website_crawler_v1", False):
        try:
            out["website_crawler_v1"] = _run_website_crawler()
        except Exception as e:
            out["website_crawler_v1"] = {
                "fetched": 0,
                "normalized": 0,
                "inserted": 0,
                "deduped": 0,
                "error": f"{type(e).__name__}: {str(e)[:500]}",
            }
    else:
        out["website_crawler_v1"] = {"fetched": 0, "normalized": 0, "inserted": 0, "deduped": 0}

    # stubs for later eras
    out["open_data_registry_v1"] = {"fetched": 0, "normalized": 0, "inserted": 0, "deduped": 0}
    out["search_discovery_v1"] = {"fetched": 0, "normalized": 0, "inserted": 0, "deduped": 0}

    # ---- ERA 1.9.15: OSM mechanical disablement ----
    # We only update the streak on *normal runs*.
    # Condition: OSM inserted == 0 for N consecutive normal runs -> disable OSM.
    if _is_normal_run() and SOURCES_ENABLED.get("osm_scraper_v1", False) and not _is_osm_disabled():
        threshold = _env_int("OSM_ZERO_INSERT_THRESHOLD", 5)
        osm_inserted = int((out.get("osm_scraper_v1", {}) or {}).get("inserted", 0) or 0)

        if osm_inserted == 0:
            streak = _get_osm_zero_streak() + 1
            _set_osm_zero_streak(streak)

            if streak >= threshold:
                _set_state(_OSM_DISABLED_K, "1")
        else:
            _set_osm_zero_streak(0)
            _note_last_successful_insert_ts()

    return out


if __name__ == "__main__":
    from pprint import pprint
    pprint(run_ingest())
