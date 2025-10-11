import os
from sqlalchemy import create_engine, text

def get_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(url, pool_pre_ping=True)

def ensure_tables():
    eng = get_engine()
    with eng.begin() as cx:
        cx.execute(text("""
        CREATE TABLE IF NOT EXISTS leads (
            id BIGSERIAL PRIMARY KEY,
            company TEXT,
            email TEXT,
            website TEXT,
            source TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))
