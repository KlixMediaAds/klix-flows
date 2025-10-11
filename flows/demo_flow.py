from __future__ import annotations
import uuid
from prefect import flow, task
from sqlalchemy import select
from flows.db import SessionLocal
from flows.schema import Lead

@task
def insert_lead(email: str, name: str | None = None, company: str | None = None, source: str = "manual"):
    with SessionLocal() as s:
        lead = Lead(email=email, name=name, company=company, source=source)
        s.add(lead)
        s.commit()
        s.refresh(lead)
        print(f"✅ Inserted lead id={lead.id} email={lead.email}")
        return str(lead.id)

@task
def read_lead(lead_id: str):
    with SessionLocal() as s:
        row = s.execute(select(Lead).where(Lead.id == uuid.UUID(lead_id))).scalar_one()
        print(f"🔎 Read back lead: {row.email} (status={row.status})")

@flow
def demo_ingest(email: str = "test@example.com", name: str = "Test User", company: str = "Acme Co"):
    lead_id = insert_lead(email=email, name=name, company=company)
    read_lead(lead_id)

if __name__ == "__main__":
    demo_ingest()
