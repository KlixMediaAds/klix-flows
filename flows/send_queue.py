# flows/send_queue.py
import os
import time
import math
import random
import smtplib
import logging
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import create_engine, text
from prefect import get_run_logger

# ---------- CONFIG & GLOBALS -------------------------------------------------

# DB (pool_pre_ping avoids stale connections on short runs)
ENG = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)

# SMTP config (env-driven)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = (
    os.getenv("SMTP_USER")
    or os.getenv("SMTP_USERNAME")
    or os.getenv("SMTP_USER_ALEX")
    or os.getenv("SMTP_USER_MAIN")
)
SMTP_PASS = (
    os.getenv("SMTP_PASS")
    or os.getenv("SMTP_APP")
    or os.getenv("GMAIL_APP_PASSWORD_ALEX")
    or os.getenv("SMTP_PASSWORD")
)
FROM_ADDR = os.getenv("SMTP_FROM") or SMTP_USER

# Cadence & warm-up
SEND_WINDOW = os.getenv("SEND_WINDOW", "13:00-21:00")  # "HH:MM-HH:MM"
SEND_WINDOW_TZ = os.getenv("SEND_WINDOW_TZ", "local").lower()  # "local" or "utc"
SLOT_SECONDS = int(os.getenv("SEND_SLOT_SECONDS", "300"))  # align with cron (*/5 -> 300)
SEND_JITTER_MAX = int(os.getenv("SEND_JITTER_MAX", "0"))  # extra sleep at start (sec)
SEND_DAILY_CAP_DEFAULT = int(os.getenv("SEND_DAILY_CAP", "50"))  # guardrail if no warm-up
WARMUP_WEEKLY = os.getenv("WARMUP_WEEKLY", "")  # e.g. "10-20,20-40,40-80,80-150"
WARMUP_START = os.getenv("WARMUP_START", "")   # e.g. "2025-10-27" (UTC date)

# Live / dry-run toggle (can be overridden by Prefect param dry_run)
LIVE_DEFAULT = os.getenv("SEND_LIVE", "1") == "1"

# Exactly 1 email per run (blueprint)
BATCH_HARD_LIMIT = 1

# ---------- HELPERS ----------------------------------------------------------

def _now_local_or_utc():
    return dt.datetime.utcnow() if SEND_WINDOW_TZ == "utc" else dt.datetime.now()

def _parse_window(window: str):
    """
    "13:00-21:00" -> (start_dt_today, end_dt_today) in the chosen time base
    (UTC if SEND_WINDOW_TZ=utc else local time).
    """
    today = _now_local_or_utc().date()
    start_s, end_s = window.split("-")
    h1, m1 = map(int, start_s.split(":"))
    h2, m2 = map(int, end_s.split(":"))
    return (
        dt.datetime(today.year, today.month, today.day, h1, m1),
        dt.datetime(today.year, today.month, today.day, h2, m2),
    )

def _seconds_left_in_window(window: str) -> int:
    now = _now_local_or_utc()
    start_dt, end_dt = _parse_window(window)
    if now >= end_dt:
        return 0
    if now <= start_dt:
        return int((end_dt - start_dt).total_seconds())
    return int((end_dt - now).total_seconds())

def _within_window() -> bool:
    now = _now_local_or_utc().time()
    start_dt, end_dt = _parse_window(SEND_WINDOW)
    return start_dt.time() <= now <= end_dt.time()

def _smtp():
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    s.ehlo()
    try:
        s.starttls(); s.ehlo()
    except smtplib.SMTPException:
        pass
    if SMTP_USER and SMTP_PASS:
        s.login(SMTP_USER, (SMTP_PASS or "").replace(" ", ""))
    return s

def _send_raw(to_email: str, subject: str, body_text: str, body_html: str | None = None):
    msg = MIMEMultipart("alternative") if body_html else MIMEText(body_text or "", "plain", "utf-8")
    if body_html:
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = FROM_ADDR or SMTP_USER
    with _smtp() as s:
        s.sendmail(msg["From"], [to_email], msg.as_string())

def _today_sent_count() -> int:
    with ENG.begin() as c:
        return c.execute(text(
            "select count(*) from email_sends where status='sent' and sent_at::date=current_date"
        )).scalar_one()

def _warmup_cap(default_cap: int) -> int:
    """
    WARMUP_WEEKLY like "10-20,20-40,40-80,80-150"
    WARMUP_START like "YYYY-MM-DD" (UTC date).
    Returns today's target cap inside the band's [lo, hi], stable per day.
    """
    if not WARMUP_WEEKLY or not WARMUP_START:
        return default_cap
    try:
        start = dt.datetime.strptime(WARMUP_START, "%Y-%m-%d").date()
        today = dt.datetime.utcnow().date()
        week = max(0, (today - start).days // 7)  # 0-based index
        bands = []
        for part in WARMUP_WEEKLY.split(","):
            lo, hi = part.split("-")
            bands.append((int(lo), int(hi)))
        if not bands:
            return default_cap
        if week >= len(bands):
            # after plan ends, use last band's high as steady state
            return bands[-1][1]
        lo, hi = bands[week]
        seed = int(today.strftime("%Y%m%d"))
        random.seed(seed)
        return random.randint(lo, hi)
    except Exception:
        return default_cap

# ---------- FLOW ENTRYPOINT --------------------------------------------------

def send_queue(batch_size: int = 1, allow_weekend: bool = True, **kwargs):
    """
    Sends exactly one email per run (hard limit), with:
      - JITTER at start (SEND_JITTER_MAX seconds)
      - DAILY CAP derived from warm-up (WARMUP_WEEKLY/WARMUP_START) or SEND_DAILY_CAP
      - PROBABILISTIC SPACING to spread sends across remaining slots in the window
    """
    logger = get_run_logger()
    log = logging.getLogger(__name__)

    # Honor Prefect's dry_run param if provided
    live = LIVE_DEFAULT
    try:
        if 'dry_run' in kwargs:
            live = not bool(kwargs.get('dry_run'))
    except Exception:
        pass

    # Weekend guard (if desired)
    if not allow_weekend and dt.datetime.utcnow().weekday() >= 5:
        logger.info("Outside weekday window; exiting.")
        return 0

    # Time window guard
    if not _within_window():
        logger.info("Outside time window (%s, tz=%s); exiting.", SEND_WINDOW, SEND_WINDOW_TZ)
        return 0

    # Jitter to break patterned start times
    if SEND_JITTER_MAX > 0:
        d = random.randint(0, SEND_JITTER_MAX)
        logger.info("JITTER: sleeping %ss before evaluate/send", d)
        time.sleep(d)

    # Compute today's cap (warm-up → cap; else fallback to SEND_DAILY_CAP_DEFAULT)
    cap_today = _warmup_cap(SEND_DAILY_CAP_DEFAULT)
    sent_today = _today_sent_count()
    if sent_today >= cap_today:
        logger.info("DAILY-CAP: sent_today=%s >= cap_today=%s; skipping.", sent_today, cap_today)
        return 0

    # Probabilistic spacing across remaining slots (assumes cron every SLOT_SECONDS)
    sec_left = _seconds_left_in_window(SEND_WINDOW)
    slots_left = max(1, math.ceil(sec_left / max(1, SLOT_SECONDS)))
    remaining_allowed = max(0, cap_today - sent_today)
    p = min(1.0, remaining_allowed / slots_left)
    if p < 1.0:
        r = random.random()
        if r > p:
            logger.info(
                "SPACING: skipping this slot (p=%.2f, r=%.2f, remaining=%s, slots_left=%s)",
                p, r, remaining_allowed, slots_left
            )
            return 0

    # Hard limit to 1 email/run regardless of param (blueprint)
    effective_batch = max(1, min(int(batch_size or 1), BATCH_HARD_LIMIT))

    # Fetch one queued row deterministically (oldest first)
    with ENG.begin() as cx:
        rows = cx.execute(text("""
            select s.id as send_id, s.lead_id, s.subject, s.body, l.email
              from email_sends s
              join leads l on l.id = s.lead_id
             where s.status='queued'
             order by s.id asc
             limit :lim
        """), {"lim": effective_batch}).mappings().all()

    if not rows:
        logger.info("No queued emails found.")
        return 0

    sent_count = 0

    for r in rows:
        if sent_today + sent_count >= cap_today:
            logger.info("DAILY-CAP mid-run: reached cap %s.", cap_today)
            break

        subj = (r["subject"] or "").strip()
        body = (r["body"] or "")
        to   = (r["email"] or "").strip()

        # Preview log (safe)
        logger.info(
            "MAIL_PREVIEW id=%s to=%s subj=%r body=%r",
            r["send_id"], to, (subj or "")[:72], (body or "")[:160].replace("\n", " ")
        )

        try:
            if live:
                _send_raw(to, subj, body)
                provider_id = "smtp"
            else:
                provider_id = "dry-run"

            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='sent', provider_message_id=:mid, sent_at=now()
                     where id=:id
                """), {"mid": provider_id, "id": r["send_id"]})

            sent_count += 1
            # small think-time; jitter already handled at start
            time.sleep(0.8)
            logger.info("Sent #%s to %s", r["send_id"], to)

        except Exception as e:
            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='failed', error=:err
                     where id=:id
                """), {"err": str(e)[:300], "id": r["send_id"]})
            logger.error("FAILED #%s to %s -> %s", r["send_id"], to, e)

    return sent_count
