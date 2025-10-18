from __future__ import annotations
import os
from prefect import flow, get_run_logger

# Real leads from your Google Places finder adapter
from klix.lead_finder.finders import find_leads
# DB writer (dedupe_key + ON CONFLICT)
from klix.lead_finder.main import upsert_leads_into_neon

@flow(name="lead_finder")
def lead_finder(count: int = 25) -> int:
    """
    Discover leads with klix.lead_finder.finders.find_leads and write to Neon via upsert.
    Idempotent via dedupe_key (sha1(email + "\\0" + company)).
    """
    log = get_run_logger()
    sink = os.getenv("LEAD_FINDER_SINK", "neon").lower()

    items = find_leads(count=count)
    log.info(f"found {len(items)} items from adapter")

    written = 0
    if sink in ("neon", "both"):
        written = upsert_leads_into_neon(items)
        log.info(f"upserted {written} items into Neon")

    # Sheets intentionally disabled for Phase 1
    if sink == "sheets":
        log.warning("LEAD_FINDER_SINK=sheets is ignored in Phase 1 (DB-first)")

    return written

if __name__ == "__main__":
    # local smoke-run
    print(lead_finder(5))
