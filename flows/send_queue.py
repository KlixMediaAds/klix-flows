from __future__ import annotations
import time, random
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Dict

from prefect import flow, task, get_run_logger
from prefect.tasks import NO_CACHE
from sqlalchemy import text

from flows.db import get_engine  # single source of engine

# ---------- pacing / window helpers ----------
def _parse_hours(span: str) -> tuple[dtime, dtime]:
    a, b = span.split("-")
    h1, m1 = map(int, a.split(":"))
    h2, m2 = map(int, b.split(":"))
    return dtime(h1, m1), dtime(h2, m2)

def _in_window(now: datetime, start: dtime, end: dtime) -> bool:
    t = now.time()
    return (start <= t <= end) if start <= end else (t >= start or t <= end)

# ---------- tasks (NO DB OBJECTS AS PARAMS) ----------
@task(name="fetch_queued", cache_policy=NO_CACHE)
def fetch_queued(batch_size: int, friendlies_first: bool) -> List[Dict]:
    """
    Pull up to N queued emails with join to leads.
    """
    order_clause = "s.id ASC"
    if friendlies_first:
        # very simple 'friendly' boost example; tweak as needed
        order_clause = ("(CASE WHEN LOWER(l.company) LIKE '%spa%' "
                        "OR LOWER(l.company) LIKE '%salon%' THEN 0 ELSE 1 END), s.id ASC")

    sql = f"""
    SELECT s.id AS send_id, s.lead_id, s.subject, s.body,
           l.email, l.company
    FROM email_sends s
    JOIN leads l ON l.id = s.lead_id
    WHERE s.status = 'queued'
    ORDER BY {order_clause}
    LIMIT :n
    """
    eng = get_engine()
    with eng.begin() as cx:
        rows = cx.execute(text(sql), {"n": batch_size}).mappings().all()
    return [dict(r) for r in rows]

@task(name="deliver", cache_policy=NO_CACHE)
def deliver(batch: List[Dict], dry_run: bool) -> List[Dict]:
    """
    Send (or simulate) one-by-one; return status records for persistence.
    Replace the stub with your real provider when ready.
    """
    results: List[Dict] = []
    for item in batch:
        if dry_run:
            msg_id = f"dry-{int(time.time())}"
            status = "sent"     # simulated success
        else:
            # TODO: call real provider here
            msg_id = f"mock-{int(time.time())}"
            status = "sent"
        results.append({"send_id": item["send_id"], "status": status, "message_id": msg_id})
    return results

@task(name="persist_results", cache_policy=NO_CACHE)
def persist_results(results: List[Dict]) -> int:
    eng = get_engine()
    with eng.begin() as cx:
        for r in results:
            cx.execute(
                text("""
                  UPDATE email_sends
                  SET status = :status, provider_message_id = :msg
                  WHERE id = :id
                """),
                {"status": r["status"], "msg": r["message_id"], "id": r["send_id"]},
            )
    return len(results)

# ---------- flow ----------
@flow(name="send_queue")
def send_queue(
    batch_size: int = 25,
    target_per_hour: int = 40,
    jitter_extra_s: int = 20,
    timezone: str = "America/Toronto",
    business_hours: str = "09:00-17:00",
    friendlies_first: bool = True,
    dry_run: bool = False,
    allow_weekend: bool = False,
) -> int:
    """
    Drain queued emails with pacing + business-hours gating.
    """
    log = get_run_logger()
    tz = ZoneInfo(timezone)
    start_t, end_t = _parse_hours(business_hours)

    now = datetime.now(tz)
    if not allow_weekend and now.weekday() >= 5:
        log.info("Weekend; skipping run.")
        return 0
    if not _in_window(now, start_t, end_t):
        log.info("Outside business hours; skipping run.")
        return 0

    batch = fetch_queued(batch_size, friendlies_first)
    if not batch:
        log.info("No queued emails found.")
        return 0

    # Pace sends: one-at-a-time to respect target_per_hour + jitter
    spacing = max(1, int(3600 / max(1, target_per_hour)))
    sent_total = 0
    for row in batch:
        results = deliver([row], dry_run)
        if not dry_run:
            sent_total += persist_results(results)
        else:
            log.info(f"DRY-RUN would mark send_id={row['send_id']} as 'sent'")
        sleep_s = spacing + random.randint(0, max(0, jitter_extra_s))
        log.info(f"Pacing sleep {sleep_s}s")
        time.sleep(sleep_s)

    if dry_run:
        log.info("Dry-run mode; no DB updates persisted.")
        return 0
    log.info(f"Updated {sent_total} email_sends rows.")
    return sent_total
