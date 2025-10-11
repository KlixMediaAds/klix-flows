from __future__ import annotations
from typing import Optional, List, Dict
from datetime import datetime

from prefect import flow, task, get_run_logger
from sqlalchemy import MetaData, Table, select, update
from sqlalchemy.orm import Session
from flows.db import get_engine  # absolute import; we use get_engine()

def _engine():
    return get_engine()

metadata = MetaData()

def _tbl(name: str) -> Table:
    # autoload table definitions from the current engine
    return Table(name, metadata, autoload_with=_engine())

# ---- TEMP: stub sender (replace with your real sender later) ----
def send_email_stub(to: str, subject: str, body: str, meta: Optional[Dict] = None) -> str:
    return f"mock-{int(datetime.utcnow().timestamp())}"

@task
def fetch_queued(batch_size: int) -> List[Dict]:
    logger = get_run_logger()
    sends = _tbl("email_sends")
    leads = _tbl("leads")
    out: List[Dict] = []
    with Session(_engine()) as s:
        rows = s.execute(
            select(
                sends.c.id, sends.c.lead_id, sends.c.subject, sends.c.body,
                leads.c.email, leads.c.company
            )
            .join(leads, leads.c.id == sends.c.lead_id)
            .where(sends.c.status == "queued")
            .order_by(sends.c.id.asc())
            .limit(batch_size)
        ).all()
        for rid, lead_id, subject, body, email, company in rows:
            out.append({
                "send_id": rid, "lead_id": lead_id, "to": email,
                "company": company, "subject": subject, "body": body
            })
    logger.info(f"Fetched {len(out)} queued emails")
    return out

@task
def deliver(batch: List[Dict]) -> List[Dict]:
    logger = get_run_logger()
    results = []
    for item in batch:
        try:
            msg_id = send_email_stub(item["to"], item["subject"], item["body"], meta={"lead_id": item["lead_id"]})
            results.append({"send_id": item["send_id"], "status": "sent", "message_id": msg_id})
        except Exception as e:
            logger.warning(f"Send failed for send_id={item['send_id']}: {e}")
            results.append({"send_id": item["send_id"], "status": "failed", "message_id": None})
    logger.info(f"Delivered {sum(1 for r in results if r['status']=='sent')} sent, {sum(1 for r in results if r['status']=='failed')} failed")
    return results

@task
def persist_results(results: List[Dict]) -> int:
    sends = _tbl("email_sends")
    count = 0
    with Session(_engine()) as s:
        for r in results:
            s.execute(
                update(sends)
                .where(sends.c.id == r["send_id"])
                .values(status=r["status"], provider_message_id=r["message_id"])
            )
            count += 1
        s.commit()
    return count

@flow(name="send_queue")
def send_queue(batch_size: int = 25) -> int:
    queued = fetch_queued(batch_size)
    if not queued:
        get_run_logger().info("No queued emails found.")
        return 0
    results = deliver(queued)
    updated = persist_results(results)
    get_run_logger().info(f"Updated {updated} email_sends rows")
    return updated
