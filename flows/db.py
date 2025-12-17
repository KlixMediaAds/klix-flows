from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import create_engine, text

# DATABASE_URL is stored with `postgresql+psycopg`; SQLAlchemy's dialect
# wants plain `postgresql` when using psycopg3 directly.
DATABASE_URL = os.environ["DATABASE_URL"]
CLEAN_DB_URL = DATABASE_URL.replace("+psycopg", "")

# Single global engine used by older flows like demo_ingest
engine = create_engine(CLEAN_DB_URL, future=True)


def get_engine():
    """Return the shared SQLAlchemy engine."""
    return engine


def ensure_tables() -> None:
    """
    Legacy helper used by older flows.

    In Neon we already manage schema via migrations, so this just validates
    connectivity with a cheap SELECT 1.
    """
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
