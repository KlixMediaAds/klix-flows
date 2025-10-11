from __future__ import annotations
from typing import List, Dict
from datetime import datetime
from prefect import flow, task, get_run_logger
from sqlalchemy.orm import Session

from flows.db import SessionLocal
from flows.schema import Lead, EmailTemplate, EmailSend, Event
from toolkit.validate.emails import validate
from toolkit.personalize.subject import build_subject
from toolkit.personalize.body import build_body_html

def _db() -> Session:
    return SessionLocal()

@task
def load_raw_leads(leads: List[Dict]) -> int:
    db = _db(); n = 0
    try:
        for L in leads:
            email = (L.get("email") or "").strip().lower()
            name = (L.get("name") or "").strip() or None
            company = (L.get("company") or "").strip() or None
            if not email: continue
            if db.query(Lead).filter(Lead.email == email).first(): continue
            db.add(Lead(email=email, name=name, company=company,
                        status="new", created_at=datetime.utcnow()))
            n += 1
        db.commit(); return n
    finally:
        db.close()

@task
def validate_leads() -> Dict[str, int]:
    db = _db(); logger = get_run_logger()
    try:
        rows = db.query(Lead).filter(Lead.status == "new").all()
        ok = dnc = 0
        for lead in rows:
            v, reason = validate(lead.email)
            if v:
                lead.status = "validated"; ok += 1
            else:
                lead.status = "dnc"; lead.note = f"validation:{reason}"; dnc += 1
        db.commit(); logger.info(f"validated={ok} dnc={dnc}")
        return {"validated": ok, "dnc": dnc}
    finally:
        db.close()

@task
def personalize_leads() -> int:
    db = _db(); n = 0
    try:
        rows = db.query(Lead).filter(Lead.status == "validated").all()
        for L in rows:
            subj = build_subject(L.name, L.company)
            body = build_body_html(L.name, L.company, L.email)
            db.add(EmailTemplate(lead_id=L.id, subject=subj,
                                 body_html=body, created_at=datetime.utcnow()))
            L.status = "personalized"; n += 1
        db.commit(); return n
    finally:
        db.close()

@task
def queue_email_sends() -> int:
    db = _db(); n = 0
    try:
        rows = (db.query(Lead, EmailTemplate)
                  .join(EmailTemplate, EmailTemplate.lead_id == Lead.id)
                  .filter(Lead.status == "personalized")
                  .all())
        for lead, tmpl in rows:
            db.add(EmailSend(lead_id=lead.id, template_id=tmpl.id,
                             status="queued", created_at=datetime.utcnow()))
            lead.status = "queued"; n += 1
        db.commit(); return n
    finally:
        db.close()

@task
def record_flow_event(kind: str, payload: Dict) -> int:
    db = _db()
    try:
        ev = Event(kind=kind, payload=payload, created_at=datetime.utcnow())
        db.add(ev); db.commit(); return ev.id
    finally:
        db.close()

@flow
def real_ingest(leads: List[Dict]):
    logger = get_run_logger()
    inserted = load_raw_leads(leads)
    stats_v = validate_leads()
    personalized = personalize_leads()
    queued = queue_email_sends()
    ev_id = record_flow_event("flow.finished", {
        "inserted": inserted,
        "validated": stats_v.get("validated", 0),
        "dnc": stats_v.get("dnc", 0),
        "personalized": personalized,
        "queued": queued
    })
    logger.info(f"finished ev_id={ev_id} inserted={inserted} queued={queued}")
    return {"event_id": ev_id, "queued": queued}
