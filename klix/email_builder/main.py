from __future__ import annotations
from typing import List, Tuple
from sqlalchemy import text
from klix.db import engine

SUBJECT_FRIENDLY = "Quick idea for {company}"
BODY_FRIENDLY = """Hi there,

I’m Alex from Klix. I had a simple idea that could help {company} get more inquiries from local search + socials.
If you’re open to it, I can share a 2-minute Loom walking through the idea.

Best,
Alex
"""

def _pick_subject_body(company: str, mode: str) -> Tuple[str, str, str]:
    mode = (mode or "friendly").lower()
    if mode in ("friendly", "friendly_then_cold"):
        return ("friendly",
                SUBJECT_FRIENDLY.format(company=company or "your team"),
                BODY_FRIENDLY.format(company=company or "your team"))
    # fallback "cold"
    return ("cold",
            f"{company}: quick idea to boost inquiries" if company else "Quick idea to boost inquiries",
            f"Hi,\n\nWe help brands like {company or 'yours'} turn local search + social into booked appointments.\nIf helpful, I can share a 2-minute Loom with a tailored suggestion.\n\n– Alex")

def build_for_new_leads(limit: int = 25, mode: str = "friendly") -> int:
    """
    Select eligible leads from Neon and queue emails into email_sends with status='queued'.
    Idempotent via unique(lead_id, send_type).
    """
    queued = 0
    with engine.begin() as c:
        leads = c.execute(text("""
          SELECT l.id, l.company, l.email
          FROM public.leads l
          WHERE COALESCE(l.status,'NEW') IN ('NEW')
            AND l.email IS NOT NULL AND l.email <> ''
            AND NOT EXISTS (
              SELECT 1 FROM public.email_sends es
              WHERE es.lead_id = l.id AND es.send_type IN ('friendly','cold')
            )
          ORDER BY l.discovered_at DESC NULLS LAST, l.id DESC
          LIMIT :limit
        """), {"limit": limit}).all()

        for lid, company, email in leads:
            send_type, subject, body = _pick_subject_body(company or "", mode)
            res = c.execute(text("""
              INSERT INTO public.email_sends (lead_id, send_type, subject, body, status)
              VALUES (:lid, :stype, :subj, :body, 'queued')
              ON CONFLICT (lead_id, send_type) DO NOTHING
              RETURNING id
            """), {"lid": lid, "stype": send_type, "subj": subject, "body": body}).first()
            if res:
                queued += 1
    return queued
