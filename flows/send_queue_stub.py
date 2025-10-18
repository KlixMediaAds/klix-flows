from __future__ import annotations
from typing import List, Dict
from datetime import datetime
import os

from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)

@task
def fetch_queued(limit: int = 50) -> List[Dict]:
    sql = """
    SELECT s.id AS send_id, s.lead_id, s.subject, s.body, l.email, l.company
    FROM email_sends s
    JOIN leads l ON l.id = s.lead_id
    WHERE s.status = 'queued'
    ORDER BY s.id ASC
    LIMIT :lim
    """
    with eng.begin() as cx:
        rows = cx.execute(text(sql), {"lim": limit}).mappings().all()
        return [dict(r) for r in rows]

def _send_stub(to: str, subject: str, body: str) -> str:
    # replace later with real provider
    return f"mock-{int(datetime.utcnow().timestamp())}"

@task
def deliver(batch: List[Dict]) -> List[Dict]:
    out = []
    for r in batch:
        try:
            mid = _send_stub(r["email"], r["subject"], r["body"])
            out.append({"send_id": r["send_id"], "status": "sent", "mid": mid})
        except Exception:
            out.append({"send_id": r["send_id"], "status": "failed", "mid": None})
    return out

@task
def persist(results: List[Dict]) -> int:
    if not results:
        return 0
    with eng.begin() as cx:
        cx.execute(
            text("""
              UPDATE email_sends s
                 SET status = v.status,
                     provider_message_id = v.mid
              FROM (VALUES
                -- rows injected by SQLAlchemy param expansion
                -- (send_id, status, mid)
                -- we build it row-by-row to keep it simple
              ) AS v(send_id, status, mid)
              WHERE s.id = v.send_id
            """).bindparams(),
        )
    return 0  # we’ll use the simpler approach below (per-row) for reliability

@task
def persist_per_row(results: List[Dict]) -> int:
    cnt = 0
    with eng.begin() as cx:
        for r in results:
            cx.execute(
                text("UPDATE email_sends SET status=:st, provider_message_id=:mid WHERE id=:id"),
                {"st": r["status"], "mid": r["mid"], "id": r["send_id"]},
            )
            cnt += 1
    return cnt

@flow(name="send_queue")
def send_queue(batch_size: int = 50) -> int:
    logger = get_run_logger()
    queued = fetch_queued(batch_size)
    if not queued:
        logger.info("No queued emails.")
        return 0
    results = deliver(queued)
    updated = persist_per_row(results)
    logger.info(f"send_queue: updated {updated} rows")
    return updated

if __name__ == "__main__":
    send_queue(50)
