"""
Core ingestion orchestration for Lead Engine v2.

For now this is a structured no-op that returns zero counts.
Later, this module will:

- Call source modules (e.g. osm_open_data, open_data_registry, website_crawler).
- Normalize raw leads into `NormalizedLead`.
- Generate dedupe keys.
- Insert into the `leads` table with ON CONFLICT(dedupe_key) DO NOTHING.

This file is kept Prefect-free on purpose; the Prefect flow wrapper lives in
`flows/lead_engine_v2_flow.py` and just calls `run_ingest()` from here.
"""

from typing import Dict


def run_ingest() -> Dict[str, Dict[str, int]]:
    """
    Run a single ingestion cycle.

    Returns a nested dict with per-source counters, e.g.:

        {
            "osm_scraper_v1": {
                "fetched": 120,
                "normalized": 110,
                "inserted": 95,
                "deduped": 15,
            },
            "website_crawler_v1": {
                "fetched": 80,
                "normalized": 70,
                "inserted": 50,
                "deduped": 20,
            },
        }

    For now this is a no-op placeholder.
    """
    return {
        "osm_scraper_v1": {
            "fetched": 0,
            "normalized": 0,
            "inserted": 0,
            "deduped": 0,
        },
        "open_data_registry_v1": {
            "fetched": 0,
            "normalized": 0,
            "inserted": 0,
            "deduped": 0,
        },
        "website_crawler_v1": {
            "fetched": 0,
            "normalized": 0,
            "inserted": 0,
            "deduped": 0,
        },
    }


if __name__ == "__main__":
    # Manual quick test: print the counts to stdout.
    from pprint import pprint

    pprint(run_ingest())
