"""
Configuration for Lead Engine v2.

This module controls:
- Which sources are enabled.
- Global knobs for ingestion behavior.
"""

SOURCES_ENABLED = {
    "osm_scraper_v1": True,
    "open_data_registry_v1": True,
    "website_crawler_v1": True,
    # Only enable once you're ready to pay for a search API, if needed.
    "search_discovery_v1": False,
}

# In the future you can add per-region configs, timeouts, etc. here.
