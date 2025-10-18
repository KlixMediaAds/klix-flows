from __future__ import annotations
from typing import List, Dict
from datetime import datetime
import os

from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text

DB = os.environ["DATABASE_URL"]
eng = create_engine(DB, pool_pre_ping=True, future=True)

DDL = """
CREATE TABLE IF NOT EXISTS email_sends (
  id bigserial PRIMARY KEY,
  lead_id bigint NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  subject text NOT NULL,
  body text NOT NULL,
  status text NOT NULL DEFAULT 'queued', -- queued|sent|failed
  provider_message_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_email_sends_lead_id ON email_sends(lead_id);
CREATE INDEX IF NOT EXISTS idx_email_sends_status  ON email_sends(status);

CREATE TABLE IF NOT EXISTS events (
  id bigserial PRIMARY KEY,
  type text NOT NULL,
  payload jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
"""

@task
def ensure_queue_tables() -> None:
    with eng.begin() as cx:
        for stmt in DDL.strip().split(";\n"):
            if stmt.strip():
                cx.execute(text(stmt))

@task
def validate_new_leads(limit: int = 50) -> List[Dict]:
    """
    Promote NEW/NULL leads to VALIDATED if email looks sane.
    Returns validated rows for personalization.
    """
    sql = """
    WITH cand AS (
      SELECT id, email, company
      FROM leads
      WHERE (status IS NULL OR status IN ('NEW','new',''))
        AND email ~ '^[^@]+@[^@]+\\.[^@]+$'
      ORDER BY discovered_at NULLS FIRST, id
      LIMIT :lim
    )
    UPDATE leads l
    SET status = 'VALIDATED'
    FROM cand
    WHERE l.id = cand.id
    RETURNING l.id, l.email, l.company
    """
    with eng.begin() as cx:
        rows = cx.execute(text(sql), {"lim": limit}).mappings().all()
        return [dict(r) for r in rows]

@task
def personalize(validated: List[Dict]) -> List[Dict]:
    """
    Build naive subject/body drafts.
    """
    out = []
    now = datetime.utcnow().strftime("%b %d")
    for r in validated:
        subj = f"{r['company']} <> quick idea"
        body = (
            f"Hi there,\n\n"
            f"I had a quick idea for {r['company']} based on what we do at Klix.\n"
            f"If you're open to it, I can share a 2-min Loom with specifics.\n\n"
            f"— Sent on {now}"
        )
        out.append({"lead_id": r["id"], "subject": subj, "body": body})
    return out

@task
def queue_email_sends(msgs: List[Dict]) -> int:
    if not msgs:
        return 0
    with eng.begin() as cx:
        cx.execute(
            text("""
              INSERT INTO email_sends (lead_id, subject, body, status)
              VALUES (:lead_id, :subject, :body, 'queued')
            """),
            msgs,
        )
    return len(msgs)

@task
def record_event(kind: str, payload: Dict) -> None:
    with eng.begin() as cx:
        cx.execute(
            text("INSERT INTO events (type, payload) VALUES (:t, CAST(:p AS jsonb))"),
            {"t": kind, "p": payload},
        )

@flow(name="email_queue")
def email_queue(limit: int = 50) -> Dict:
    logger = get_run_logger()
    ensure_queue_tables()
    validated = validate_new_leads(limit)
    drafts = personalize(validated)
    queued = queue_email_sends(drafts)
    stats = {"validated": len(validated), "queued": queued}
    logger.info(f"email_queue stats: {stats}")
    record_event("email_queue.finished", stats)
    return stats

if __name__ == "__main__":
    email_queue(50)
