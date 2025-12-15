# flows/real_ingest.py
from __future__ import annotations

from typing import List, Dict, Any, Tuple
from datetime import datetime
import json
import re
import os

from prefect import flow, task, get_run_logger
from sqlalchemy import MetaData, Table, select, insert, update
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from flows.db import get_engine, ensure_tables  # keep for compatibility

EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Verification gating (matches your email_builder gate semantics)
ALLOW_RISKY = os.getenv("EMAIL_BUILDER_ALLOW_RISKY", "false").lower() == "true"
VALID_STATUSES = {"valid"}
RISKY_STATUSES = {"risky"}


def _engine():
    return get_engine()


def _tbl(name: str) -> Table:
    md = MetaData()
    return Table(name, md, autoload_with=_engine())


# ---------- tasks ----------

@task
def load_raw_leads(leads: List[Dict[str, Any]]) -> List[int]:
    """
    Insert incoming leads as status='new'. Returns inserted lead IDs.
    NOTE: This flow does NOT verify emails. Verification is done by email_verifier_flow.
    """
    logger = get_run_logger()
    t = _tbl("leads")
    ids: List[int] = []
    with Session(_engine()) as s:
        for ld in leads:
            row = {
                "company": ld.get("company"),
                "email": ld.get("email"),
                "website": ld.get("website"),
                "source": ld.get("source", "json"),
                "status": "new",
                "created_at": datetime.utcnow(),
            }
            rid = s.execute(insert(t).values(**row).returning(t.c.id)).scalar()
            ids.append(int(rid))
        s.commit()
    logger.info(f"Loaded {len(ids)} leads as status='new'")
    return ids


@task
def validate_leads(lead_ids: List[int]) -> Tuple[List[int], List[int]]:
    """
    Regex email validation. Marks validated / dnc. Returns (good_ids, bad_ids).
    This is NOT deliverability verification; email_verifier_flow is the real gate.
    """
    logger = get_run_logger()
    t = _tbl("leads")
    good, bad = [], []
    with Session(_engine()) as s:
        rows = s.execute(select(t.c.id, t.c.email).where(t.c.id.in_(lead_ids))).all()
        for lid, email in rows:
            ok = bool(EMAIL_RX.match((email or "").strip()))
            s.execute(
                update(t)
                .where(t.c.id == lid)
                .values(status="validated" if ok else "dnc")
            )
            (good if ok else bad).append(lid)
        s.commit()
    logger.info(f"Validated {len(good)} ok, {len(bad)} dnc")
    return good, bad


@task
def personalize_leads(valid_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Minimal templating (legacy). Returns list of dicts with lead_id, subject, body.
    """
    logger = get_run_logger()
    t = _tbl("leads")
    drafts: List[Dict[str, Any]] = []
    with Session(_engine()) as s:
        rows = s.execute(
            select(t.c.id, t.c.company, t.c.website).where(t.c.id.in_(valid_ids))
        ).all()
        for lid, company, website in rows:
            subj = f"{company}: quick idea to boost inquiries"
            body = (
                f"Hi {company},\n\n"
                f"We build short-form ad creatives tailored to {company}.\n"
                f"Thoughts on testing 2–3 concepts against your audience?\n\n"
                f"Site noted: {website or 'n/a'}\n"
                f"— Klix Media"
            )
            drafts.append({"lead_id": int(lid), "subject": subj, "body": body})
    logger.info(f"Drafted {len(drafts)} messages")
    return drafts


@task
def queue_email_sends(drafts: List[Dict[str, Any]]) -> int:
    """
    Legacy queue -> now HARD-GATED by leads.email_verification_status.

    HARD GUARANTEE (no bypass):
      - We NEVER enqueue unless leads.email_verification_status is:
          - 'valid' OR
          - 'risky' (only if ALLOW_RISKY=true)
      - We set email_sends.to_email from leads.email
      - We upsert on (lead_id, send_type) without overwriting sent/failed rows
    """
    logger = get_run_logger()
    sends = _tbl("email_sends")
    leads = _tbl("leads")

    if not drafts:
        logger.info("queue_email_sends: no drafts provided")
        return 0

    now = datetime.utcnow()
    queued_or_refreshed = 0
    blocked_invalid = 0
    blocked_risky = 0
    blocked_unknown = 0
    blocked_missing_email = 0

    with Session(_engine()) as s:
        for d in drafts:
            lead_id = int(d["lead_id"])

            row = s.execute(
                select(
                    leads.c.id,
                    leads.c.email,
                    leads.c.email_verification_status,
                    leads.c.company,
                    leads.c.website,
                ).where(leads.c.id == lead_id)
            ).first()

            if not row:
                continue

            _lid, to_email, vstatus, company, website = row
            to_email = (to_email or "").strip()
            vstatus = (vstatus or "").strip().lower()

            if not to_email:
                blocked_missing_email += 1
                continue

            # Verification gate
            if vstatus in VALID_STATUSES:
                pass
            elif vstatus in RISKY_STATUSES:
                if ALLOW_RISKY:
                    pass
                else:
                    blocked_risky += 1
                    continue
            elif vstatus == "invalid":
                blocked_invalid += 1
                continue
            else:
                blocked_unknown += 1
                continue

            subj = (d.get("subject") or "").strip() or f"{company or 'Quick question'}"
            body = (d.get("body") or "").strip() or (
                f"Hi there,\n\nQuick question about {company or 'your site'}.\n\n— Klix Media"
            )

            stmt = pg_insert(sends).values(
                lead_id=lead_id,
                send_type="cold",
                prompt_angle_id=os.getenv("COLD_DEFAULT_PROMPT_ANGLE_ID","site-copy-hook").strip(),
                status="queued",
                subject=subj,
                body=body,
                to_email=to_email,
                created_at=now,
                updated_at=now,
            )

            # On conflict: only refresh if existing row is queued/held.
            stmt = stmt.on_conflict_do_update(
                index_elements=[sends.c.lead_id, sends.c.send_type],
                set_={
                    "status": "queued",
                    "subject": stmt.excluded.subject,
                    "body": stmt.excluded.body,
                    "to_email": stmt.excluded.to_email,
                    "updated_at": stmt.excluded.updated_at,
                },
                where=sends.c.status.in_(["queued", "held"]),
            )

            res = s.execute(stmt)
            # rowcount is 1 for insert or update; 0 if conflict where-clause prevented update
            if getattr(res, "rowcount", 0) == 1:
                queued_or_refreshed += 1

        s.commit()

    logger.info(
        "queue_email_sends: queued_or_refreshed=%s blocked_invalid=%s blocked_risky=%s blocked_unknown=%s blocked_missing_email=%s allow_risky=%s",
        queued_or_refreshed,
        blocked_invalid,
        blocked_risky,
        blocked_unknown,
        blocked_missing_email,
        ALLOW_RISKY,
    )
    return queued_or_refreshed


@task
def record_flow_event(name: str, payload: Dict[str, Any]) -> None:
    """
    Best-effort event row; logs if the table is missing or any error occurs.
    """
    logger = get_run_logger()
    try:
        events = _tbl("events")
        with Session(_engine()) as s:
            s.execute(
                insert(events).values(
                    type=name,
                    payload=json.dumps(payload),
                    created_at=datetime.utcnow(),
                )
            )
            s.commit()
        logger.info(f"Recorded event '{name}'")
    except Exception as e:
        logger.warning(f"Event record skipped ({e}); payload={payload}")


# ---------- flow ----------

@flow(name="real_ingest")
def real_ingest(leads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    End-to-end ingest pipeline:
      load_raw_leads -> validate -> personalize -> queue -> record event

    NOTE: This flow does NOT perform deliverability verification.
    email_verifier_flow must run to populate leads.email_verification_status.
    """
    try:
        ensure_tables()
    except Exception:
        pass

    logger = get_run_logger()
    n = len(leads or [])
    if not n:
        logger.info("No leads provided.")
        return {"received": 0, "inserted": 0, "validated_ok": 0, "validated_dnc": 0, "queued": 0}

    logger.info(f"Received {n} leads")
    inserted_ids = load_raw_leads(leads)
    good_ids, bad_ids = validate_leads(inserted_ids)
    drafts = personalize_leads(good_ids)
    queued = queue_email_sends(drafts)

    summary = {
        "received": n,
        "inserted": len(inserted_ids),
        "validated_ok": len(good_ids),
        "validated_dnc": len(bad_ids),
        "queued": queued,
    }
    record_flow_event("real_ingest", summary)
    logger.info(f"Summary: {summary}")
    return summary


if __name__ == "__main__":
    sample = [
        {"company": "Blue Spa", "email": "info@bluespa.example", "website": "https://bluespa.example", "source": "json"},
        {"company": "Rose Florist", "email": "hello@roseflorist.example", "website": "https://roseflorist.example", "source": "json"},
    ]
    real_ingest(sample)
