from typing import List, Tuple
from sqlalchemy import select, insert
from flows.db import SessionLocal
from flows.schema import leads, email_sends

# EDIT THIS IMPORT to your actual builder function if needed:
try:
    from klix.email_builder.builder import build_email_for_lead as _build_email_for_lead
except Exception:
    _build_email_for_lead = None

def _fallback_builder(**kw):
    first = (kw.get("first_name") or "there").strip()
    company = kw.get("company") or "your store"
    subject = f"{first} — quick idea for your ads"
    body = (
        f"Hi {first},\n\n"
        f"We make short-form ads designed to convert on TikTok/Shop. "
        f"Happy to share 2–3 ideas tailored to {company}.\n\n— Klix"
    )
    return subject, body

def _pick_next_leads(s, limit:int):
    sent = select(email_sends.c.lead_id).subquery()
    q = (
        select(
            leads.c.id, leads.c.email, leads.c.first_name,
            leads.c.last_name, leads.c.company, leads.c.meta
        )
        .where(~leads.c.id.in_(select(sent.c.lead_id)))
        .limit(limit)
    )
    return s.execute(q).fetchall()

def build_for_new_leads(limit: int = 100, mode: str = "friendly_then_cold") -> int:
    with SessionLocal() as s:
        batch = _pick_next_leads(s, limit)
        queued = 0
        for lead_id, email, first_name, last_name, company, meta in batch:
            builder = _build_email_for_lead or (lambda **k: _fallback_builder(**k))
            subject, body = builder(
                email=email, first_name=first_name, last_name=last_name,
                company=company, meta=meta, mode=mode
            )
            s.execute(
                insert(email_sends).values(
                    lead_id=lead_id,
                    send_type="friendly" if mode.startswith("friendly") else "cold",
                    subject=subject,
                    body=body,
                    status="queued",
                )
            )
            queued += 1
        s.commit()
        return queued
