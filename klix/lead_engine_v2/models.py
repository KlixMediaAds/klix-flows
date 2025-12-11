from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class NormalizedLead:
    """
    Canonical lead shape used internally by Lead Engine v2.

    This is the unified representation BEFORE it is written into the `leads` table.
    """
    name: str
    website: Optional[str]
    email: Optional[str]
    phone: Optional[str]

    street: Optional[str]
    city: Optional[str]
    region: Optional[str]      # state / province
    country: Optional[str]
    postal_code: Optional[str]

    latitude: Optional[float]
    longitude: Optional[float]

    category: Optional[str]
    subcategory: Optional[str]

    # Lineage / source data
    source: str                # e.g. 'osm_scraper_v1', 'website_crawler_v1'
    source_ref: str            # canonical URL or ID for tracing
    source_details: Dict[str, Any]  # raw snapshot / metadata for forensics

    # Optional dedupe key; set by dedupe.make_dedupe_key()
    dedupe_key: Optional[str] = None
