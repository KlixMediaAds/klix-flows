from __future__ import annotations
import hashlib, json
from typing import Iterable, Dict, Any
from datetime import datetime, timezone
from sqlalchemy import text
from klix.db import engine

def _dk(email: str|None, company: str|None) -> str:
    email = (email or "").strip().lower()
    company = (company or "").strip()
    return hashlib.sha1(f"{email}\0{company}".encode("utf-8")).hexdigest()

def upsert_leads_into_neon(items: Iterable[Dict[str, Any]]) -> int:
    """
    Inserts/updates leads into Neon.
    - dedupe_key = sha1(email + "\\0" + company)
    - ON CONFLICT: update core fields; merge meta as jsonb.
    Returns: number of attempted rows (skips rows lacking email).
    """
    attempted = 0
    with engine.begin() as c:
        for it in items:
            email = (it.get("email") or "").strip().lower()
            if not email:
                continue
            attempted += 1
            company = (it.get("company") or "").strip()
            params = {
                "email": email,
                "company": company or None,
                "first_name": (it.get("first_name") or None),
                "last_name": (it.get("last_name") or None),
                "source": (it.get("source") or None),
                "status": (it.get("status") or "NEW"),
                "meta_json": json.dumps(it.get("meta") or {}),
                "discovered_at": (it.get("discovered_at") or datetime.now(timezone.utc)),
                "dk": _dk(email, company),
            }
            c.execute(text("""
                INSERT INTO public.leads
                    (email, company, first_name, last_name, source, status, meta, discovered_at, dedupe_key)
                VALUES
                    (:email, :company, :first_name, :last_name, :source, :status, CAST(:meta_json AS jsonb), :discovered_at, :dk)
                ON CONFLICT (dedupe_key) DO UPDATE
                SET company   = EXCLUDED.company,
                    first_name= COALESCE(EXCLUDED.first_name, public.leads.first_name),
                    last_name = COALESCE(EXCLUDED.last_name,  public.leads.last_name),
                    source    = COALESCE(EXCLUDED.source,     public.leads.source),
                    status    = COALESCE(EXCLUDED.status,     public.leads.status),
                    meta      = COALESCE(public.leads.meta, '{}'::jsonb) || COALESCE(EXCLUDED.meta, '{}'::jsonb)
            """), params)
    return attempted
