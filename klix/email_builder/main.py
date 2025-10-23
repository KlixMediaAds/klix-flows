from __future__ import annotations
import os, html
from typing import Tuple, Dict, Any

from sqlalchemy import create_engine, text

# your AI generator
from klix.email_builder.lib import prompt as pr

FRIENDLY_DOMAINS = {"gmail.com","klixads.org"}

def _domain(email: str) -> str:
    e = (email or "").strip()
    return e.split("@",1)[1].lower() if "@" in e else ""

def _to_html(md: str) -> str:
    """Very small MD→HTML: paragraphs + line breaks (no f-string backslashes)."""
    if not md:
        return ""
    safe = html.escape(md).replace("\r\n","\n").replace("\r","\n")
    parts = []
    for chunk in safe.split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.append("<p>" + chunk.replace("\n", "<br>") + "</p>")
    return "".join(parts) or ("<p>" + safe + "</p>")

def _engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    try:
        return create_engine(url, pool_pre_ping=True)
    except Exception:
        return None

def _oldest_queued_for_lead(lead_id: int | str) -> dict | None:
    """Return {'subject':..., 'body':...} for the oldest queued email for this lead, else None."""
    if not lead_id:
        return None
    eng = _engine()
    if not eng:
        return None
    try:
        with eng.begin() as c:
            row = c.execute(
                text("""
                    select subject, body
                      from email_sends
                     where status='queued' and lead_id=:lid
                     order by id asc
                     limit 1
                """),
                {"lid": lead_id},
            ).mappings().first()
            return dict(row) if row else None
    except Exception:
        return None

def build_email_for_lead(lead: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Returns (subject, body_text, body_html, send_type).

    Behavior:
    - If there's an existing queued row for this lead, return that subject/body (authoritative).
    - Else, generate via AI prompt lib.
    """
    # derive lead_id + email for send_type
    lead_id = lead.get("lead_id") or lead.get("id") or lead.get("ID")
    email   = (lead.get("email") or lead.get("Email") or lead.get("to") or lead.get("To") or "").strip()

    # 1) Prefer the already-queued row (so sender uses it instead of template)
    q = _oldest_queued_for_lead(lead_id)
    if q and (q.get("subject") and q.get("body")):
        subj = q["subject"].strip()
        body_text = q["body"].strip()
        body_html = _to_html(body_text)
        send_type = "friendly" if _domain(email) in FRIENDLY_DOMAINS else "cold"
        return subj, body_text, body_html, send_type

    # 2) Fallback: generate new (should rarely happen for the sender path)
    biz = {
        "BusinessName": lead.get("business_name") or lead.get("BusinessName") or "",
        "City":         lead.get("city") or lead.get("City") or "",
        "Niche":        lead.get("niche") or lead.get("Niche") or "",
        "Website":      lead.get("website") or lead.get("Website") or "",
        "SiteTitle":    lead.get("site_title") or lead.get("SiteTitle") or "",
        "Tagline":      lead.get("tagline") or lead.get("Tagline") or "",
        "Products":     lead.get("products") or lead.get("Products") or [],
    }
    draft = pr.draft_email(biz, angle_id="site-copy-hook", model_name=os.getenv("MODEL_NAME","gpt-4o-mini"))
    subj = (draft.get("subject") or "").strip() or "Tiny video thought"
    body_text = (draft.get("body_md") or "").strip()
    body_html = _to_html(body_text)
    send_type = "friendly" if _domain(email) in FRIENDLY_DOMAINS else "cold"
    return subj[:120], body_text, body_html, send_type
