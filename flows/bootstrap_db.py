# flows/bootstrap_db.py
from prefect import flow, task, get_run_logger
from sqlalchemy import text
from .db import engine  # uses your SQLAlchemy create_engine bound to DATABASE_URL

# Idempotent DDL (safe to run repeatedly). No UNIQUE on email to avoid dup errors.
SCHEMA_SQL = """
-- Ensure columns on leads
ALTER TABLE IF EXISTS leads
  ADD COLUMN IF NOT EXISTS status     text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

-- Helpful (non-unique) email index for lookups/dedup logic in code
CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);

-- Outbox/queue
CREATE TABLE IF NOT EXISTS email_sends (
  id                   bigserial PRIMARY KEY,
  lead_id              bigint REFERENCES leads(id) ON DELETE CASCADE,
  subject              text NOT NULL,
  body                 text NOT NULL,
  status               text NOT NULL DEFAULT 'queued',  -- queued|sent|failed
  provider_message_id  text,
  created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_email_sends_lead_id ON email_sends(lead_id);
CREATE INDEX IF NOT EXISTS idx_email_sends_status  ON email_sends(status);

-- Optional templates (not required by current flows)
CREATE TABLE IF NOT EXISTS email_templates (
  id          bigserial PRIMARY KEY,
  name        text UNIQUE NOT NULL,
  subject     text NOT NULL,
  body        text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Run/event log
CREATE TABLE IF NOT EXISTS events (
  id          bigserial PRIMARY KEY,
  type        text NOT NULL,
  payload     jsonb,
  created_at  timestamptz NOT NULL DEFAULT now()
);
"""

@task
def apply_schema() -> None:
    # Execute statements one by one under a single transaction
    stmts = [s.strip() for s in SCHEMA_SQL.strip().split(";\n") if s.strip()]
    with engine.begin() as conn:
        for s in stmts:
            conn.exec_driver_sql(s + ";")

@flow(name="bootstrap_db")
def bootstrap_db() -> str:
    logger = get_run_logger()
    logger.info("Applying idempotent schema migrations…")
    apply_schema()
    # quick visibility
    with engine.connect() as c:
        cnt = c.execute(text("SELECT count(*) FROM leads;")).scalar()
    logger.info(f"✅ Schema ensured. leads.count={cnt}")
    return "ok"

if __name__ == "__main__":
    bootstrap_db()
