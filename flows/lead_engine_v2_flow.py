from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, Tuple

from prefect import flow, get_run_logger
from prefect.runtime import flow_run  # type: ignore

from klix.lead_engine_v2.ingest_flow import run_ingest
from klix.db import get_engine
from sqlalchemy import text


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _classify_intake(*, totals: Dict[str, int], counts_by_source: Dict[str, Dict[str, Any]]) -> str:
    """
    Mutually exclusive, ordered classification (ERA 1.9.15):
      1) INTAKE_BROKEN
      2) INTAKE_DISCOVERY_LIMITED
      3) INTAKE_ZERO_YIELD
      4) INTAKE_SUCCESS
    """
    # 1) broken if any source has an error (including disabled marker errors)
    for _, c in counts_by_source.items():
        if c.get("error"):
            # NOTE: disabled marker is still a "broken-ish" intake surface for ops
            # but classification precedence says broken outranks everything.
            return "INTAKE_BROKEN"

    fetched = int(totals.get("total_fetched", 0) or 0)
    inserted = int(totals.get("total_inserted", 0) or 0)

    # 4) success
    if inserted > 0:
        return "INTAKE_SUCCESS"

    # 2) discovery limited (fetched > 0 but inserted == 0)
    if fetched > 0 and inserted == 0:
        return "INTAKE_DISCOVERY_LIMITED"

    # 3) zero yield (fetched == 0 and inserted == 0)
    return "INTAKE_ZERO_YIELD"


def _last_successful_insert_ts() -> Optional[str]:
    """
    Best-effort: determine timestamp of last successful insert from OSM.
    (Crawler enrich doesn't create new rows.)
    """
    try:
        eng = get_engine()
        with eng.begin() as conn:
            ts = conn.execute(text("""
                SELECT MAX(created_at)
                FROM leads
                WHERE source = 'osm_scraper_v1'
                """)).scalar()
        if ts is None:
            return None
        return ts.isoformat()
    except Exception:
        return None


@flow(name="lead-engine-v2", persist_result=False)
def lead_engine_v2() -> Dict[str, Any]:
    """
    Prefect flow wrapper for Lead Engine v2.

    Delegates to `run_ingest()` and emits JSON log lines suitable for runbook checks,
    plus simple per-source yield lines for quick operator scanning.
    """
    logger = get_run_logger()
    logger.info("Lead Engine v2 flow started.")

    # diagnostic framing (explicit)
    diag_dedupe = _env_bool("LEAD_ENGINE_V2_DIAGNOSTIC_DEDUPE_INCLUDE_SOURCE_REF", False)
    if diag_dedupe:
        logger.warning(
            json.dumps(
                {
                    "event": "lead_engine_v2_diagnostic_mode_enabled",
                    "mode": "DIAGNOSTIC_ONLY",
                    "flag": "LEAD_ENGINE_V2_DIAGNOSTIC_DEDUPE_INCLUDE_SOURCE_REF",
                },
                sort_keys=True,
            )
        )

    counts_by_source = run_ingest()

    totals = {
        "total_sources": len(counts_by_source),
        "total_fetched": 0,
        "total_normalized": 0,
        "total_inserted": 0,
        "total_deduped": 0,
    }

    # per-source yield logging
    for source, counts in counts_by_source.items():
        fetched = int(counts.get("fetched", 0) or 0)
        normalized = int(counts.get("normalized", 0) or 0)
        inserted = int(counts.get("inserted", 0) or 0)
        deduped = int(counts.get("deduped", 0) or 0)
        err = counts.get("error")
        skip_reason = counts.get("skip_reason")

        totals["total_fetched"] += fetched
        totals["total_normalized"] += normalized
        totals["total_inserted"] += inserted
        totals["total_deduped"] += deduped

        # Requirement: logs-only quick scan line
        logger.info(f"[{source}] fetched={fetched} normalized={normalized} deduped={deduped} inserted={inserted}")

        payload = {
            "event": "lead_engine_v2_source_done",
            "source": source,
            "fetched": fetched,
            "normalized": normalized,
            "inserted": inserted,
            "deduped": deduped,
        }
        if err:
            payload["error"] = err
        if skip_reason:
            payload["skip_reason"] = skip_reason

        logger.info(json.dumps(payload, sort_keys=True))

        # If OSM got mechanically disabled, emit the structured disablement log
        if source == "osm_scraper_v1" and err == "SOURCE_DISABLED_LOW_YIELD":
            logger.warning(
                json.dumps(
                    {
                        "event": "SOURCE_DISABLED_LOW_YIELD",
                        "source": "osm_scraper_v1",
                        "consecutive_zero_insert_runs": counts_by_source.get("osm_scraper_v1", {}).get("consecutive_zero_insert_runs"),
                    },
                    sort_keys=True,
                )
            )

    classification = _classify_intake(totals=totals, counts_by_source=counts_by_source)
    run_id = getattr(flow_run, "id", None)
    last_ok = _last_successful_insert_ts()

    run_complete_payload = {
        "event": "lead_engine_v2_run_complete",
        "run_id": str(run_id) if run_id else None,
        "intake_classification": classification,
        "last_successful_insert_ts": last_ok,
        **totals,
    }
    logger.info(json.dumps(run_complete_payload, sort_keys=True))

    # Return summary so supervisor can treat intake results as state
    return {
        "run_id": str(run_id) if run_id else None,
        "intake_classification": classification,
        "last_successful_insert_ts": last_ok,
        "totals": totals,
        "counts_by_source": counts_by_source,
        "diagnostic_only_enabled": bool(diag_dedupe),
    }


if __name__ == "__main__":
    lead_engine_v2()
