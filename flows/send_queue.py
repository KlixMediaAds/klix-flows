# flows/send_queue.py
from __future__ import annotations
from typing import Optional, List, Dict
from datetime import datetime
import zoneinfo

from prefect import flow, task, get_run_logger
from sqlalchemy import MetaData, Table, select, update
from sqlalchemy.orm import Session

from flows.db import get_engine  # uses your existing engine factory


# ---------- DB helpers ----------
def _engine():
    return get_engine()

metadata = MetaData()

def _tbl(name: str) -> Table:
    # reflect the table from the current engine
    return Table(name, metadata, autoload_with=_engine())


# ---------- TEMP mailer (swap later for a real provider) ----------
def send_email_stub(to: str, subject: str, body: str, meta: Optional[Dict] = None) -> str:
    # return a mock provider message id
    return f"mock-{int(datetime.utcnow().timestamp())}"


# ---------- Tasks ----------
@task(name="fetch_queued")
def fetch_queued(batch_size: int) -> List[Dict]:
    log = get_run_logger()
    sends = _tbl("email_sends")
    leads = _tbl("leads")
    out: List[Dict] = []

    with Session(_engine()) as s:
        rows = s.execute(
            select(
                sends.c.id,
                sends.c.lead_id,
                sends.c.subject,
                sends.c.body,
                leads.c.email,
                leads.c.company,
            )
            .join(leads, leads.c.id == sends.c.lead_id)
            .where(sends.c.status == "queued")
            .order_by(sends.c.id.asc())
            .limit(batch_size)
        ).all()

        for rid, lead_id, subject, body, email, company in rows:
            out.append(
                {
                    "send_id": rid,
                    "lead_id": lead_id,
                    "to": email,
                    "company": company,
                    "subject": subject,
                    "body": body,
                }
            )

    log.info(f"Fetched {len(out)} queued emails (batch_size={batch_size})")
    return out


@task(name="deliver_batch")
def deliver(batch: List[Dict]) -> List[Dict]:
    log = get_run_logger()
    results: List[Dict] = []

    for item in batch:
        try:
            msg_id = send_email_stub(
                item["to"], item["subject"], item["body"], meta={"lead_id": item["lead_id"]}
            )
            results.append({"send_id": item["send_id"], "status": "sent", "message_id": msg_id})
        except Exception as e:
            log.warning(f"Send failed for send_id={item['send_id']}: {e}")
            results.append({"send_id": item["send_id"], "status": "failed", "message_id": None})

    sent = sum(1 for r in results if r["status"] == "sent")
    failed = len(results) - sent
    log.info(f"Deliver summary → sent={sent}, failed={failed}")
    return results


@task(name="persist_results")
def persist_results(results: List[Dict]) -> int:
    sends = _tbl("email_sends")
    updated = 0

    with Session(_engine()) as s:
        for r in results:
            s.execute(
                update(sends)
                .where(sends.c.id == r["send_id"])
                .values(status=r["status"], provider_message_id=r["message_id"])
            )
            updated += 1
        s.commit()

    return updated


# ---------- Flow ----------
@flow(name="send_queue")
def send_queue(
    batch_size: int = 25,
    allow_weekend: bool = False,
    tz_name: str = "America/Toronto",
) -> int:
    """
    Drains queued emails and writes provider_message_id.
    Weekend safety: by default, does nothing on Sat/Sun unless allow_weekend=True.
    """
    log = get_run_logger()
    tz = zoneinfo.ZoneInfo(tz_name)
    is_weekend = datetime.now(tz).weekday() >= 5  # 5=Sat, 6=Sun

    if is_weekend and not allow_weekend:
        log.info("Weekend guard active → skipping run (set allow_weekend=True to override).")
        return 0

    queued = fetch_queued(batch_size)
    if not queued:
        log.info("No queued emails found.")
        return 0

    results = deliver(queued)
    updated = persist_results(results)
    log.info(f"Updated {updated} rows in email_sends.")
    return updated
