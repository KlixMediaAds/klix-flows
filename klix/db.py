"""
klix.db

Single source of truth for database connectivity.

Contracts this module MUST provide (used across the repo):
- engine (SQLAlchemy Engine)
- SessionLocal (sessionmaker)
- get_engine() helper (new/standardized; safe to depend on)
- get_session() context manager (optional convenience)

Notes:
- DATABASE_URL is expected to be provided via environment (e.g. /etc/klix/secret.env).
- We normalize common scheme/driver variants to reduce footguns.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def _normalize_database_url(raw: str) -> str:
    """
    Normalize DATABASE_URL variants to something SQLAlchemy can reliably use.

    We prefer psycopg2 for maximum compatibility with existing deps
    (requirements.txt includes psycopg2-binary).

    Normalizations:
    - postgres://  -> postgresql://
    - postgresql+psycopg:// -> postgresql+psycopg2://
    """
    url = (raw or "").strip()

    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]

    # If someone set psycopg3 dialect, normalize to psycopg2 dialect.
    if url.startswith("postgresql+psycopg://"):
        url = "postgresql+psycopg2://" + url[len("postgresql+psycopg://") :]

    return url


_RAW_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not _RAW_DATABASE_URL:
    # Keep this loud and explicit: many flows import klix.db at import-time.
    raise RuntimeError(
        "DATABASE_URL is not set in environment. "
        "Load /etc/klix/secret.env (or equivalent) before running flows."
    )

DATABASE_URL = _normalize_database_url(_RAW_DATABASE_URL)

# Global engine: used by flows and helper scripts.
engine: Engine = create_engine(DATABASE_URL, future=True)

# Global session factory: used by existing code (brain_gateway, email_spine_poller, etc.)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_engine() -> Engine:
    """Return the shared SQLAlchemy engine (stable import for new code)."""
    return engine


@contextmanager
def get_session() -> Iterator[Session]:
    """
    Context-managed DB session.

    Usage:
        from klix.db import get_session
        with get_session() as s:
            ...
    """
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
