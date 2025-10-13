# flows/bootstrap_db.py
from prefect import flow, task, get_run_logger
from sqlalchemy import text
from .db import get_engine  # IMPORTANT: use get_engine(), not a global engine

# ------------------------------ DEBUG SNAPSHOT (import-time) ------------------------------
# Proves which DB envs the container actually used; masks password in logs.
import os
import re
import logging
from urllib.parse import urlparse, parse_qs

_log = logging.getLogger(__name__)

def _mask_pw(url: str) -> str:
    # postgresql+psycopg://user:PASSWORD@host/db?...
    return re.sub(r'(postgresql\+psycopg://[^:]+:)[^@]+(@)', r'\1***\2', url or '')

def _log_db_snapshot():
    url = os.getenv("DATABASE_URL", "")
    user = os.getenv("PGUSER")
    host = os.getenv("PGHOST")
    db   = os.getenv("PGDATABASE")

    # Derive from URL if env vars not set
    try:
        if url and (not user or not host or not db):
            p = urlparse(url)
            if not user and p.username:
                user = p.username
            if not host and p.hostname:
                host = p.hostname
            if not db and p.path and len(p.path) > 1:
                db = p.path.lstrip("/")
        # Sanity checks
        if url:
            q = parse_qs(urlparse(url).query)
            if "channel_binding" in q:
                _log.warning("DATABASE_URL contains channel_binding; Neon may reject auth. Remove it.")
            if "sslmode" not in q or (q.get("sslmode", [""])[0] != "require"):
                _log.warning("DATABASE_URL missing 'sslmode=require'. Neon requires TLS; add '?sslmode=require'.")
        if host and "-pooler" not in host:
            _log.warning("Using non-pooler host '%s'. Prefer the Neon *-pooler host for stability.", host)
    except Exception as e:
        _log.warning("Could not parse DATABASE_URL for diagnostics: %s", e)

    _log.info(
        "DB env snapshot → host=%s user=%s db=%s url=%s",
        host, user, db, _mask_pw(url)
    )

_log_db_snapshot()
# --------------------------- END DEBUG SNAPSHOT (import-time) -----------------------------

# Idempotent DDL (safe to run repeatedly). No UNIQUE on email to avoid dup errors.
# Ensure 'leads' exists before creating tables that reference it.
SCHEMA_SQL = """
-- Base 'leads' table (minimal shape so FK references always succeed)
CREATE TABLE IF NOT EXISTS leads (
  id          bigserial PRIMARY KEY,
  email       text,
  status      text,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Ensure columns on leads if an older schema exists
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
    eng = get_engine()
    # Execute statements one-by-one to avoid multi-statement restrictions
    stmts = [s.strip() for s in SCHEMA_SQL.strip().split(";") if s.strip()]
    with eng.begin() as conn:
        for s in stmts:
            conn.exec_driver_sql(s + ";")

@flow(name="bootstrap_db")
def bootstrap_db() -> str:
    logger = get_run_logger()
    logger.info("Applying idempotent schema migrations…")
    apply_schema()

    # Make the post-migration probe resilient even if 'leads' were just created empty
    leads_count = None
    try:
        with get_engine().connect() as c:
            leads_count = c.execute(text("SELECT count(*) FROM leads;")).scalar()
    except Exception as e:
        logger.warning("Post-migration probe failed (non-fatal): %s", e)

    if leads_count is None:
        logger.info("✅ Schema ensured. leads.count=unknown (table present).")
    else:
        logger.info(f"✅ Schema ensured. leads.count={leads_count}")

    return "ok"

if __name__ == "__main__":
    bootstrap_db()
