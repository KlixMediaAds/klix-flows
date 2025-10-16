from datetime import datetime, timezone
from typing import Dict, Any, List
import csv, os
from sqlalchemy.dialects.postgresql import insert as pg_insert
from flows.db import SessionLocal
from flows.schema import leads

# EDIT THIS IMPORT if your function/file is named differently:
try:
    from klix.lead_finder.lead_finder_us import run_lead_finder  # expects list[dict] or None
except Exception:
    run_lead_finder = None

def _load_from_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append({
                "email": row.get("email") or row.get("Email") or row.get("EMAIL"),
                "company": row.get("company") or row.get("Company"),
                "first_name": row.get("first_name") or row.get("FirstName") or row.get("First Name"),
                "last_name": row.get("last_name") or row.get("LastName") or row.get("Last Name"),
                "source": row.get("source") or "legacy_csv",
                "meta": {k:v for k,v in row.items() if k.lower() not in {"email","company","first_name","last_name","source"}},
            })
    return [x for x in out if x["email"]]

def find_leads(target: int = 100) -> int:
    rows: List[Dict[str, Any]] = []
    if callable(run_lead_finder):
        try:
            rows = run_lead_finder(limit=target) or []
        except TypeError:
            rows = run_lead_finder() or []
    if not rows:
        rows = _load_from_csv(os.path.join(os.path.dirname(__file__), "leads_new.csv"))[:target]

    now = datetime.now(timezone.utc)
    inserted = 0
    with SessionLocal() as s:
        for row in rows:
            stmt = (
                pg_insert(leads)
                .values(
                    email=row["email"],
                    company=row.get("company"),
                    first_name=row.get("first_name"),
                    last_name=row.get("last_name"),
                    source=row.get("source","unknown"),
                    discovered_at=now,
                    meta=row.get("meta", {}),
                )
                .on_conflict_do_nothing(index_elements=["email"])
            )
            res = s.execute(stmt)
            if res.rowcount:
                inserted += 1
        s.commit()
    return inserted
