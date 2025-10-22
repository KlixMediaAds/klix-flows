from __future__ import annotations
import os, html
from typing import Tuple, Dict, Any
from klix.email_builder.lib import prompt as pr

FRIENDLY_DOMAINS = {"gmail.com","klixads.org"}

def _domain(email: str) -> str:
    e = (email or "").strip()
    return e.split("@",1)[1].lower() if "@" in e else ""

def _to_html(md: str) -> str:
    if not md: return ""
    safe = html.escape(md).replace("\r\n","\n").replace("\r","\n")
    parts = [f"<p>{p.replace('\n','<br>')}</p>" for p in safe.split("\n\n") if p.strip()]
    return "".join(parts) or f"<p>{safe}</p>"

def build_email_for_lead(lead: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Returns (subject, body_text, body_html, send_type) using your AI prompt-based generator.
    """
    email = (lead.get("email") or "").strip()
    company = (lead.get("company") or lead.get("name") or "your brand").strip()
    website = (lead.get("website") or "").strip()
    city    = (lead.get("city") or "").strip()
    niche   = (lead.get("niche") or "").strip()
    notes   = (lead.get("notes") or "").strip()

    biz = {
        "BusinessName": company,
        "Website": website,
        "City": city,
        "Niche": niche,
        "Notes": notes,
        "SiteTitle": "", "Tagline": "", "Products": [], "Instagram": "", "LastPostDate": ""
    }

    angle_id = "site-copy-hook"
    model = os.getenv("MODEL_NAME","gpt-4o-mini")

    try:
        draft = pr.draft_email(biz, angle_id, model_name=model) or {}
    except Exception:
        draft = {}

    subject   = (draft.get("subject") or f"Idea for {company}")[:120]
    body_text = (draft.get("body_md") or "Quick thought for your brand.")[:2000]
    body_html = _to_html(body_text)
    send_type = "friendly" if _domain(email) in FRIENDLY_DOMAINS else "cold"
    return subject, body_text, body_html, send_type
