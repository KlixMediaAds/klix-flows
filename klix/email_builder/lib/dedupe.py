# lib/dedupe.py
from __future__ import annotations
from typing import Dict, List, Set, Iterable
import re, hashlib

# ----------------------------
# Body-level dedupe utilities
# ----------------------------

def ngram_fingerprint(text: str, n: int = 3) -> str:
    """
    Build a SHA1 fingerprint of whitespace-normalized, lowercased n-grams.
    n=3 is a good default to catch near-duplicates without overblocking.
    """
    toks = re.findall(r"[a-z0-9']+", (text or "").lower())
    grams = [" ".join(toks[i:i+n]) for i in range(max(0, len(toks)-n+1))]
    return hashlib.sha1(("|".join(grams)).encode("utf-8")).hexdigest()

def compute_body_hash(body: str) -> str:
    """Convenience wrapper for 3-gram fingerprint of an email body."""
    return ngram_fingerprint(body, n=3)

def history_hash_set(
    all_emails: List[Dict],
    body_key: str = "BodyMD",
    limit: int = 250
) -> Set[str]:
    """
    Build a set of body hashes from the last `limit` emails (or all if limit<=0).
    Uses BodyMD by default; pass another key if your sheet differs.
    """
    hashes: Set[str] = set()
    if not all_emails:
        return hashes
    if limit and limit > 0:
        emails = all_emails[-min(len(all_emails), limit):]
    else:
        emails = all_emails

    for row in emails:
        body = (row.get(body_key) or "").strip()
        if body:
            hashes.add(compute_body_hash(body))
    return hashes

def is_body_duplicate(body: str, history_hashes: Iterable[str]) -> bool:
    """
    Return True if `body` collides with any hash in `history_hashes`.
    """
    try:
        h = compute_body_hash(body)
        return h in set(history_hashes or [])
    except Exception:
        return False

# ---------------------------------------
# Prospect-level dedupe (original logic)
# ---------------------------------------

def dedupe_hit_for_prospect(prospect: Dict, all_emails: List[Dict], all_prospects: List[Dict]) -> bool:
    """Skip if:
       - any Prospect with the same DedupeKey is not NEW, OR
       - any Emails row exists for this ProspectID with Status in {DRAFTED, APPROVED, SENT}
    """
    pid = str(prospect.get("ID", "")).strip()
    dkey = str(prospect.get("DedupeKey", "")).strip().lower()

    # If any prospect with same dedupe key has moved beyond NEW, skip
    if dkey:
        for p in (all_prospects or []):
            p_key = str(p.get("DedupeKey", "")).strip().lower()
            status = str(p.get("Status", "")).strip().upper()
            if p_key == dkey and status and status != "NEW":
                return True

    # If any email already exists for this ProspectID in terminal/active drafting states, skip
    terminal = {"DRAFTED", "APPROVED", "SENT"}
    for e in (all_emails or []):
        if str(e.get("ProspectID", "")).strip() == pid:
            if str(e.get("Status", "")).strip().upper() in terminal:
                return True

    return False
