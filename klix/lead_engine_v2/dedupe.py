import hashlib

from .models import NormalizedLead


def make_dedupe_key(lead: NormalizedLead) -> str:
    """
    Generate a stable dedupe key for a lead.

    We keep it simple and human-predictable:
    - lowercased name
    - lowercased city
    - lowercased phone OR website (whichever is present)

    This string is then hashed to a fixed-length SHA-256.

    The resulting key is meant to be stored in `leads.dedupe_key` and used with:
        ON CONFLICT (dedupe_key) DO NOTHING
    """
    name = (lead.name or "").strip().lower()
    city = (lead.city or "").strip().lower()
    contact = (lead.phone or lead.website or "").strip().lower()

    core = f"{name}|{city}|{contact}"
    return hashlib.sha256(core.encode("utf-8")).hexdigest()
