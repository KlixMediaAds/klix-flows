# flows/db.py
from __future__ import annotations

import os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def _normalize_scheme(url: str) -> str:
    # Ensure SQLAlchemy uses psycopg v3
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql+psycopg://" + url.split("://", 1)[1]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url.split("://", 1)[1]
    return url

def _strip_param(url: str, key: str) -> str:
    p = urlparse(url)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k != key]
    return urlunparse(p._replace(query=urlencode(q)))

def _ensure_ssl_require(url: str) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    if q.get("sslmode") != "require":
        q["sslmode"] = "require"
        return urlunparse(p._replace(query=urlencode(q)))
    return url

def _build_url_from_env() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url:
        # Fallback to PG* envs if DATABASE_URL not provided
        user = os.getenv("PGUSER")
        pw   = os.getenv("PGPASSWORD")
        host = os.getenv("PGHOST")
        db   = os.getenv("PGDATABASE")
        if not all([user, pw, host, db]):
            raise RuntimeError(
                "DATABASE_URL not set and PG* envs incomplete "
                "(need PGUSER, PGPASSWORD, PGHOST, PGDATABASE)."
            )
        url = f"postgresql+psycopg://{user}:{pw}@{host}/{db}"

    url = _normalize_scheme(url)
    url = _strip_param(url, "channel_binding")  # Neon: do not send channel_binding
    url = _ensure_ssl_require(url)              # Neon: TLS required
    return url

_ENGINE: Engine | None = None

def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(_build_url_from_env(), pool_pre_ping=True)
    return _ENGINE
