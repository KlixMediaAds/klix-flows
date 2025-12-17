"""
Configuration for Lead Engine v2.

This module controls:
- Which sources are enabled.
- Which regions / areas we ingest first.
- Which OSM tag filters we use for the first implementation.

NOTE:
We keep knobs here minimal and safe. Operational knobs that might change often
(limits/timeouts) are primarily env-driven in ingest_flow.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

SOURCES_ENABLED: Dict[str, bool] = {
    # Base lead ingestion (OSM -> INSERT)
    "osm_scraper_v1": True,

    # Not implemented yet
    "open_data_registry_v1": False,

    # Website enrichment (crawl existing OSM rows with website -> UPSERT enrich)
    "website_crawler_v1": True,

    # Optional later (paid)
    "search_discovery_v1": False,
}

# Starter regions (expand later).
# area_name must match an OSM administrative area name; country_code is ISO3166-1 (e.g. CA, US).
OSM_AREAS: List[Tuple[str, str]] = [
    ("Toronto", "CA"),
]

# OR-tag filters for OSM queries.
# Keep narrow initially; add more verticals once insertion + dedupe is proven.
OSM_TAGS_ANY: List[Tuple[str, str]] = [
    ("amenity", "clinic"),
    ("shop", "beauty"),
    ("healthcare", "clinic"),
]
