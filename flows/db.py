from __future__ import annotations
import os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from sqlalchemy import create_engine

REQUIRED_QS = {"sslmode": "require", "channel_binding": "require"}

def _to_psycopg_v3(url: str) -> str:
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    # normalize scheme to psycopg v3
    if url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg://" + url.split("://", 1)[1]
    elif url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url.split("://", 1)[1]

    # ensure required query params
    p = urlparse(url)
    qs = dict(parse_qsl(p.query, keep_blank_values=True))
    changed = False
    for k, v in REQUIRED_QS.items():
        if qs.get(k) != v:
            qs[k] = v
            changed = True
    if changed:
        p = p._replace(query=urlencode(qs))
        url = urlunparse(p)
    return url

def get_engine():
    raw = os.getenv("DATABASE_URL", "")
    url = _to_psycopg_v3(raw)
    return create_engine(url, pool_pre_ping=True)
