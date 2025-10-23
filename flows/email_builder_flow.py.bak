from __future__ import annotations
import os, itertools
from typing import List, Dict, Any
from prefect import flow, get_run_logger
from sqlalchemy import create_engine, text
from klix.email_builder.main import build_email_for_lead

def _engine():
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

def _fetch_candidates(limit: int) -> List[Dict[str, Any]]:
    """
    Grab a generous slab of the freshest NEW/READY leads that do not
    already have any email_sends row (any send_type). Filter by domain later.
    """
    e = _engine()
    with e.begin() as c:
        rows = list(c.execute(text("""
            select id, email, company, source, status, discovered_at
              from leads l
             where status in ('NEW','READY')
               and not exists (
                   select 1 from email_sends s where s.lead_id = l.id
               )
             order by discovered_at desc
             limit :lim
        """), {"lim": max(limit * 5, 50)}).mappings())
    return [dict(r) for r in rows]

def _domain(addr: str) -> str:
    if not addr or "@" not in addr: return ""
    return addr.rsplit("@", 1)[1].lower().strip()

@flow(name="email_builder")
def email_builder(limit: int = 25,
                  friendlies_domains: List[str] | None = None):
    """
    Queues emails into email_sends based on leads + builder.
    - Respects idempotency via (lead_id, send_type) unique key.
    - Inserts into columns: (lead_id, send_type, status='queued', subject, body).
    """
    logger = get_run_logger()
    friendlies_domains = friendlies_domains or ["klixads.org", "gmail.com"]
    domains_set = {d.lower() for d in friendlies_domains}

    candidates = _fetch_candidates(limit)
    logger.info(f"Fetched {len(candidates)} candidate leads")

    queued = 0
    e = _engine()
    with e.begin() as c:
        for lead in candidates:
            dom = _domain(lead.get("email", ""))
            # Only queue friendlies for this deployment’s default path
            if dom not in domains_set:
                continue
            try:
                subject, body_text, body_html, send_type = build_email_for_lead(lead)
            except Exception as ex:
                logger.exception(f"Builder failed for lead {lead.get('id')}: {ex}")
                continue

            # prefer plain text column 'body' (your schema enforces NOT NULL on it)
            c.execute(text("""
                insert into email_sends (lead_id, send_type, status, subject, body)
                values (:lid, :stype, 'queued', :subj, :body)
                on conflict (lead_id, send_type) do nothing
            """), {
                "lid": lead["id"],
                "stype": send_type,
                "subj": subject,
                "body": body_text or "",
            })
            # Check if insert actually happened
            inserted = c.execute(text("""
                select 1 from email_sends where lead_id=:lid and send_type=:stype
            """), {"lid": lead["id"], "stype": send_type}).scalar()
            if inserted:
                queued += 1
            if queued >= limit:
                break

    logger.info(f"Queued {queued} emails.")
