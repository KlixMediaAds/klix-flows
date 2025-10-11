from prefect import flow, task
from sqlalchemy import text
from flows.db import get_engine, ensure_tables

@task
def insert_one(lead: dict):
    eng = get_engine()
    with eng.begin() as cx:
        cx.execute(
            text("INSERT INTO leads (company, email, website, source) VALUES (:company, :email, :website, :source)"),
            lead,
        )

@flow(name="demo_ingest")
def demo_ingest(lead: dict):
    ensure_tables()
    insert_one.submit(lead)
    return {"inserted": 1}

if __name__ == "__main__":
    demo_ingest({"company": "TestCo", "email": "owner@testco.example", "website": "https://testco.example", "source": "demo"})
