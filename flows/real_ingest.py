# flows/real_ingest.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple
from datetime import datetime
import json
import re

from prefect import flow, task, get_run_logger
from sqlalchemy import MetaData, Table, select, insert, update
from sqlalchemy.orm import Session

from flows.db import get_engine, ensure_tables  # keep for compatibility

EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _engine():
    return get_engine()

def _tbl(name: str) -> Table:
    md = MetaData()
    return Table(name, md, autoload_with=_engine())

# ---------- tasks ----------

@task
def load_raw_leads(leads: List[Dict[str, Any]]) -> List[int]:
    """
    Insert incoming leads as status='new'. Returns inserted lead IDs.
    """
    logger = get_run_logger()
    t = _tbl("leads")
    ids: List[int] = []
    with Session(_engine()) as s:
        for ld in leads:
            row = {
                "company": ld.get("company"),
                "email": ld.get("email"),
                "website": ld.get("website"),
                "source": ld.get("source", "json"),
                "status": "new",
                "created_at": datetime.utcnow(),
            }
            rid = s.execute(insert(t).values(**row).returning(t.c.id)).scalar()
            ids.append(int(rid))
        s.commit()
    logger.info(f"Loaded {len(ids)} leads as status='new'")
    return ids

@task
def validate_leads(lead_ids: List[int]) -> Tuple[List[int], List[int]]:
    """
    Regex email validation. Marks validated / dnc. Returns (good_ids, bad_ids).
    """
    logger = get_run_logger()
    t = _tbl("leads")
    good, bad = [], []
    with Session(_engine()) as s:
        rows = s.execute(select(t.c.id, t.c.email).where(t.c.id.in_(lead_ids))).all()
        for lid, email in rows:
            ok = bool(EMAIL_RX.match(email or ""))
            s.execute(
                update(t)
                .where(t.c.id == lid)
                .values(status="validated" if ok else "dnc")
            )
            (good if ok else bad).append(lid)
        s.commit()
    logger.info(f"Validated {len(good)} ok, {len(bad)} dnc")
    return good, bad

@task
def personalize_leads(valid_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Minimal templating (can be swapped for your builder later).
    Returns list of dicts with lead_id, subject, body.
    """
    logger = get_run_logger()
    t = _tbl("leads")
    drafts: List[Dict[str, Any]] = []
    with Session(_engine()) as s:
        rows = s.execute(
            select(t.c.id, t.c.company, t.c.website).where(t.c.id.in_(valid_ids))
        ).all()
        for lid, company, website in rows:
            subj = f"{company}: quick idea to boost inquiries"
            body = (
                f"Hi {company},\n\n"
                f"We build short-form ad creatives tailored to {company}.\n"
                f"Thoughts on testing 2–3 concepts against your audience?\n\n"
                f"Site noted: {website or 'n/a'}\n"
                f"— Klix Media"
            )
            drafts.append({"lead_id": lid, "subject": subj, "body": body})
    logger.info(f"Drafted {len(drafts)} messages")
    return drafts

@task
def queue_email_sends(drafts: List[Dict[str, Any]]) -> int:
    """
    Insert into email_sends (status='queued'). Skip if a send already exists for that lead.
    """
    logger = get_run_logger()
    sends = _tbl("email_sends")
    count = 0
    with Session(_engine()) as s:
        for d in drafts:
            exists = s.execute(
                select(sends.c.id).where(sends.c.lead_id == d["lead_id"]).limit(1)
            ).first()
            if exists:
                continue
            s.execute(insert(sends).values(
                lead_id=d["lead_id"],
                subject=d["subject"],
                body=d["body"],
                status="queued",
                created_at=datetime.utcnow(),
            ))
            count += 1
        s.commit()
    logger.info(f"Queued {count} email_sends")
    return count

@task
def record_flow_event(name: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort event row; logs if the table is missing or any error occurs.
    """
    logger = get_run_logger()
    try:
        events = _tbl("events")
        with Session(_engine()) as s:
            s.execute(
                insert(events).values(
                    type=name,
                    payload=json.dumps(payload),
                    created_at=datetime.utcnow(),
                )
            )
            s.commit()
        logger.info(f"Recorded event '{name}'")
    except Exception as e:
        logger.warning(f"Event record skipped ({e}); payload={payload}")

# ---------- flow ----------

@flow(name="real_ingest")
def real_ingest(leads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    End-to-end ingest pipeline:
      load_raw_leads -> validate -> personalize -> queue -> record event
    """
    # Safe if present; bootstrap_db flow now owns schema creation.
    try:
        ensure_tables()
    except Exception:
        pass

    logger = get_run_logger()
    n = len(leads or [])
    if not n:
        logger.info("No leads provided.")
        return {"received": 0, "inserted": 0, "validated_ok": 0, "validated_dnc": 0, "queued": 0}

    logger.info(f"Received {n} leads")
    inserted_ids = load_raw_leads(leads)
    good_ids, bad_ids = validate_leads(inserted_ids)
    drafts = personalize_leads(good_ids)
    queued = queue_email_sends(drafts)

    summary = {
        "received": n,
        "inserted": len(inserted_ids),
        "validated_ok": len(good_ids),
        "validated_dnc": len(bad_ids),
        "queued": queued,
    }
    record_flow_event("real_ingest", summary)
    logger.info(f"Summary: {summary}")
    return summary

if __name__ == "__main__":
    sample = [
        {"company": "Blue Spa", "email": "info@bluespa.example", "website": "https://bluespa.example", "source": "json"},
        {"company": "Rose Florist", "email": "hello@roseflorist.example", "website": "https://roseflorist.example", "source": "json"},
    ]
    real_ingest(sample)
