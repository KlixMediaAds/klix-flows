from __future__ import annotations
import os, importlib
from typing import List, Dict, Any
from prefect import flow, get_run_logger

# DB upsert
from klix.lead_finder.main import upsert_leads_into_neon

def _import_find_leads():
    """
    Try to import an existing find_leads() from a few likely places.
    Must return List[Dict[str,Any]] with keys like email, company, etc.
    """
    candidates = [
        ("klix.lead_finder.finders", "find_leads"),
        ("klix.lead_finder.main", "find_leads"),
        ("flows.lead_finder", "find_leads"),
    ]
    for mod, fn in candidates:
        try:
            m = importlib.import_module(mod)
            if hasattr(m, fn):
                return getattr(m, fn)
        except Exception:
            pass

    # Fallback seed for sanity
    def seed(count: int = 1) -> List[Dict[str, Any]]:
        return [{
            "email": "owner@example.com",
            "company": "TestCo",
            "source": "seed",
            "meta": {"seed": True},
        }][:count]
    return seed

@flow(name="lead_finder")
def run_lead_finder(count: int = 25) -> int:
    """
    Find leads and write them to Neon.leads with ON CONFLICT(dedupe_key) upsert.
    Sheets writes are disabled in Phase 1 (DB-first).
    """
    logger = get_run_logger()
    sink = os.getenv("LEAD_FINDER_SINK", "neon").lower()
    find_leads = _import_find_leads()

    # Discover leads
    items = find_leads(count=count) if "count" in getattr(find_leads, "__code__", lambda:None).__code__.co_varnames else find_leads()
    if not isinstance(items, list):
        logger.warning("find_leads() did not return a list; coercing to empty list.")
        items = []
    logger.info(f"discovered={len(items)} sink={sink}")

    # Phase 1: only Neon writes
    written = 0
    if sink in ("neon", "both"):
        written = upsert_leads_into_neon(items)
        logger.info(f"upserted_to_neon={written}")
    else:
        logger.info("LEAD_FINDER_SINK != neon; skipping Neon write by config")

    # (Sheets intentionally disabled in Phase 1)
    return written

if __name__ == "__main__":
    run_lead_finder()
