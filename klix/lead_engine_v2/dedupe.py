import hashlib
import os

from .models import NormalizedLead


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def make_dedupe_key(lead: NormalizedLead) -> str:
    """
    Generate a stable dedupe key for a lead.

    Default (normal mode):
    - lowercased name
    - lowercased city
    - lowercased phone OR website (whichever is present)

    Diagnostic-only mode (explicitly enabled):
    - includes source_ref to help debug over-deduping
    - MUST NOT be left on for normal operations

    Enable via:
      LEAD_ENGINE_V2_DIAGNOSTIC_DEDUPE_INCLUDE_SOURCE_REF=true
    """
    name = (lead.name or "").strip().lower()
    city = (lead.city or "").strip().lower()
    contact = (lead.phone or lead.website or "").strip().lower()

    core = f"{name}|{city}|{contact}"

    if _env_bool("LEAD_ENGINE_V2_DIAGNOSTIC_DEDUPE_INCLUDE_SOURCE_REF", False):
        source_ref = (lead.source_ref or "").strip().lower()
        core = f"{core}|{source_ref}"

    return hashlib.sha256(core.encode("utf-8")).hexdigest()
