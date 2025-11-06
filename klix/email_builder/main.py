from __future__ import annotations
import os, html, random
from typing import Tuple, Dict, Any, Optional, List

from sqlalchemy import create_engine, text

# Low-level AI email generator
from klix.email_builder.lib import prompt as pr

FRIENDLY_DOMAINS = {"gmail.com", "klixads.org"}


def _domain(email: str) -> str:
    email = email or ""
    return email.split("@", 1)[1].lower() if "@" in email else ""


def _to_html(md: str) -> str:
    """Very small MD→HTML: paragraphs + line breaks (no markdown library)."""
    if not md:
        return ""
    safe = html.escape(md).replace("\r\n", "\n").replace("\r", "\n")
    parts: List[str] = []
    for chunk in safe.split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.append("<p>" + chunk.replace("\n", "<br>") + "</p>")
    return "".join(parts) or ("<p>" + safe + "</p>")


def _engine():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        return None
    try:
        return create_engine(url, pool_pre_ping=True)
    except Exception:
        return None


def _oldest_queued_for_lead(lead_id: int | str) -> Optional[Dict[str, Any]]:
    """Return {'subject':..., 'body':...} for the oldest queued email for this lead, else None."""
    if not lead_id:
        return None
    eng = _engine()
    if not eng:
        return None
    try:
        with eng.begin() as c:
            row = (
                c.execute(
                    text(
                        """
                        select subject, body
                          from email_sends
                         where status = 'queued' and lead_id = :lid
                         order by id asc
                         limit 1
                        """
                    ),
                    {"lid": lead_id},
                )
                .mappings()
                .first()
            )
            return dict(row) if row else None
    except Exception:
        return None


def _pick_prompt_profile() -> Optional[Dict[str, Any]]:
    """
    Pick an active prompt profile using its weight for selection.
    Returns: {id, name, angle_id, system_text, model_name, weight} or None.
    """
    eng = _engine()
    if not eng:
        return None

    try:
        with eng.begin() as c:
            rows = (
                c.execute(
                    text(
                        """
                        select id, name, angle_id, system_text, model_name, weight
                          from email_prompt_profiles
                         where is_active = true
                           and weight > 0
                        """
                    )
                )
                .mappings()
                .all()
            )
    except Exception:
        return None

    if not rows:
        return None

    weighted: List[Dict[str, Any]] = []
    for row in rows:
        w = float(row.get("weight") or 0.0)
        if w <= 0:
            continue
        copies = max(1, int(round(w * 10)))  # scale weights a bit
        weighted.extend([dict(row)] * copies)

    if not weighted:
        # Fallback: just pick the first active row
        return dict(rows[0])

    return random.choice(weighted)


def build_email_for_lead(
    lead: Dict[str, Any],
    prompt_profile: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, str, str, Optional[str]]:
    """
    Returns (subject, body_text, body_html, send_type, prompt_profile_id).

    Behavior:
    - If there's an existing queued row for this lead, return that subject/body (authoritative)
      and prompt_profile_id=None (because the content is already stored in DB).
    - Else, generate via AI prompt lib, using a DB-backed prompt profile if available.
    """
    # derive lead_id + email for send_type
    lead_id = lead.get("lead_id") or lead.get("id") or lead.get("ID")
    email = (
        lead.get("email")
        or lead.get("Email")
        or lead.get("to")
        or lead.get("To")
        or ""
    )

    # 1) Prefer an already-queued row (so sender uses it instead of regenerating)
    queued = _oldest_queued_for_lead(lead_id)
    if queued and queued.get("subject") and queued.get("body"):
        subj = queued["subject"]
        body_text = queued["body"]
        body_html = _to_html(body_text)
        send_type = "friendly" if _domain(email) in FRIENDLY_DOMAINS else "cold"
        return subj, body_text, body_html, send_type, None

    # 2) Build the business dict for prompt logic
    biz = {
        "BusinessName": lead.get("business_name") or lead.get("BusinessName") or "",
        "City":         lead.get("city") or lead.get("City") or "",
        "Niche":        lead.get("niche") or lead.get("Niche") or "",
        "Website":      lead.get("website") or lead.get("Website") or "",
        "SiteTitle":    lead.get("site_title") or lead.get("SiteTitle") or "",
        "Tagline":      lead.get("tagline") or lead.get("Tagline") or "",
        "Products":     lead.get("products") or lead.get("Products") or [],
    }

    # 2a) Choose prompt profile
    profile = prompt_profile if prompt_profile is not None else _pick_prompt_profile() or {}
    profile_id: Optional[str] = None
    if profile.get("id"):
        profile_id = str(profile["id"])

    angle_id = profile.get("angle_id") or "site-copy-hook"
    model_name = profile.get("model_name") or os.getenv("MODEL_NAME", "gpt-4o-mini")
    system_text = profile.get("system_text")

    # Override the global SYSTEM_TEXT for this call if we have one.
    if system_text:
        pr.SYSTEM_TEXT = system_text

    draft = pr.draft_email(biz, angle_id=angle_id, model_name=model_name)
    subj = draft.get("subject") or "Tiny video thought"
    body_text = draft.get("body_md") or ""
    body_html = _to_html(body_text)
    send_type = "friendly" if _domain(email) in FRIENDLY_DOMAINS else "cold"

    return subj[:120], body_text, body_html, send_type, profile_id
