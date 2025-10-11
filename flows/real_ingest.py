from prefect import flow, task
from sqlalchemy import text
from typing import List, Dict
from flows.db import get_engine, ensure_tables

@task
def insert_many(leads: List[Dict]):
    eng = get_engine()
    with eng.begin() as cx:
        cx.execute(
            text("INSERT INTO leads (company, email, website, source) VALUES (:company, :email, :website, :source)"),
            leads,
        )

@flow(name="real_ingest")
def real_ingest(leads: List[Dict]):
    ensure_tables()
    if not leads:
        return {"inserted": 0}
    insert_many.submit(leads)
    return {"inserted": len(leads)}

if __name__ == "__main__":
    real_ingest([
        {"company": "Blue Spa", "email": "info@bluespa.example", "website": "https://bluespa.example", "source": "json"},
        {"company": "Rose Florist", "email": "hello@roseflorist.example", "website": "https://roseflorist.example", "source": "json"},
    ])
