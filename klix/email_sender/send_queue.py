# flows/send_queue.py
from __future__ import annotations

import os, time, re
from datetime import datetime, time as dtime
from sqlalchemy import create_engine, text
from prefect import flow, get_run_logger

# your existing sender
from .main import send_email

ENG = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)
DAILY_CAP = int(os.getenv("SEND_DAILY_CAP", "50"))

def _within_window() -> bool:
    # server in UTC; e.g. 13:00-21:00 for 9–5 Toronto
    win = os.getenv("SEND_WINDOW", "13:00-21:00")
    s, e = [tuple(map(int, x.split(":"))) for x in win.split("-")]
    now = datetime.now().time()
    return dtime(*s) <= now <= dtime(*e)

def _clean_email(addr: str) -> str:
    if not addr:
        return addr
    a = addr.strip()
    # strip mailto: and any query string like ?subject=...
    if a.lower().startswith("mailto:"):
        a = a[7:]
    a = a.split("?", 1)[0]
    return a.strip().lower()

def _send_exact(to_email: str, subject: str, body: str) -> str:
    """
    Call your send_email while strongly preferring 'raw' rendering if available.
    Returns provider message id (or 'unknown-id').
    """
    try:
        # if your sender supports this, it will bypass any templating
        return send_email(to_email, subject, body, force_raw=True) or "unknown-id"
    except TypeError:
        try:
            return send_email(to_email, subject, body, render_mode="raw") or "unknown-id"
        except TypeError:
            return send_email(to_email, subject, body) or "unknown-id"

@flow(name="send-queue")
def send_queue(batch_size: int = 25, allow_weekend: bool = False, **kwargs):
    """
    Simple queue sender:
      - pulls next queued rows
      - sends using exact DB subject/body
      - updates status to sent/failed
    Accepts **kwargs so older deployments that pass extra params won't crash.
    Set dry_run via params or SEND_LIVE env.
    """
    logger = get_run_logger()

    # live iff SEND_LIVE=1 AND not dry_run param
    live_env = (os.getenv("SEND_LIVE", "0") == "1")
    live = live_env and not bool(kwargs.get("dry_run", False))

    if not allow_weekend and datetime.utcnow().weekday() >= 5:
        logger.info("Outside weekday window; exiting.")
        return 0
    if not _within_window():
        logger.info("Outside time window; exiting.")
        return 0

    sent_today = 0
    with ENG.begin() as cx:
        rows = cx.execute(text("""
            select s.id as send_id, s.lead_id, s.subject, s.body, l.email
              from email_sends s
              join leads l on l.id = s.lead_id
             where s.status='queued'
             order by s.id asc
             limit :lim
        """), {"lim": batch_size}).mappings().all()

    for r in rows:
        if sent_today >= DAILY_CAP:
            break

        to_raw = r["email"]
        to = _clean_email(to_raw)
        subj = r["subject"] or ""
        body = r["body"] or ""

        # Safe preview (no f-strings with backslashes)
        preview = (body or "")[:160].replace("\n", " ")
        logger.info(
            "MAIL_PREVIEW id=%s to=%s subj=%r body=%r",
            r["send_id"], to, (subj or "")[:72], preview
        )

        try:
            mid = "dry-run"
            if live:
                mid = _send_exact(to, subj, body)
            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='sent', provider_message_id=:mid, sent_at=now()
                     where id=:id
                """), {"mid": mid, "id": r["send_id"]})
            sent_today += 1
            time.sleep(1.2)  # light throttle
            logger.info("Sent #%s to %s", r["send_id"], to)
        except Exception as e:
            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='failed', error=:err
                     where id=:id
                """), {"err": str(e)[:300], "id": r["send_id"]})
            logger.error("FAILED #%s to %s -> %s", r["send_id"], to, e)

    return sent_today
