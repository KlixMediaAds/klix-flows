# flows/send_queue.py
import os
import re
import time
import math
import random
import smtplib
import logging
import datetime as dt
import json
from typing import Optional, List, Dict, Any
from urllib.parse import quote as urlquote

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import psycopg2
from sqlalchemy import create_engine, text
from prefect import get_run_logger

# Optional Discord helper (PYTHONPATH must include /opt/klix/klix-flows)
try:
    from libs.discord import post_discord
except Exception:
    def post_discord(_msg: str):
        # no-op if helper not available; avoid crashing the flow
        pass

# ---------- CONFIG & GLOBALS -------------------------------------------------

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
SEND_WINDOW = os.getenv("SEND_WINDOW", "13:00-21:00")     # "HH:MM-HH:MM"
SEND_WINDOW_TZ = os.getenv("SEND_WINDOW_TZ", "local").lower()  # "local" or "utc"
SLOT_SECONDS = int(os.getenv("SEND_SLOT_SECONDS", "300")) # align with cron (*/5 -> 300)
SEND_JITTER_MAX = int(os.getenv("SEND_JITTER_MAX", "0"))  # extra sleep at start (sec)
SEND_DAILY_CAP_DEFAULT = int(os.getenv("SEND_DAILY_CAP", "50"))
WARMUP_WEEKLY = os.getenv("WARMUP_WEEKLY", "")            # e.g. "10-20,20-40,40-80,80-150"
WARMUP_START = os.getenv("WARMUP_START", "")              # e.g. "2025-10-27" (UTC date)

# Live / dry-run toggle (can be overridden by Prefect param)
LIVE_DEFAULT = os.getenv("SEND_LIVE", "1") == "1"

# Optional first-party tracking (FastAPI endpoints you’ll add in svc/webhooks.py)
TRACKING_BASE = os.getenv("TRACKING_BASE_URL", "").rstrip("/")  # e.g. https://your.server

# Exactly 1 email per run (blueprint)
BATCH_HARD_LIMIT = 1

# ---------- INTERNAL DB NORMALIZATION (for psycopg2 use) ---------------------

def _normalize_db(url: str) -> str:
    if not url:
        return url
    if url.startswith("postgresql+psycopg://"):
        url = "postgresql://" + url.split("postgresql+psycopg://", 1)[1]
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

# ---------- TIME / WINDOW HELPERS -------------------------------------------

def _now_local_or_utc():
    return dt.datetime.utcnow() if SEND_WINDOW_TZ == "utc" else dt.datetime.now()

def _parse_window(window: str):
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

# ---------- SMTP -------------------------------------------------------------

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

def _send_raw(to_email: str, subject: str, body_text: str, body_html: Optional[str] = None):
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")
    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = FROM_ADDR or SMTP_USER
    with _smtp() as s:
        s.sendmail(msg["From"], [to_email], msg.as_string())

# ---------- DB HELPERS -------------------------------------------------------

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
        week = max(0, (today - start).days // 7)
        bands = []
        for part in WARMUP_WEEKLY.split(","):
            lo, hi = part.split("-")
            bands.append((int(lo), int(hi)))
        if not bands:
            return default_cap
        if week >= len(bands):
            return bands[-1][1]
        lo, hi = bands[week]
        seed = int(today.strftime("%Y%m%d"))
        random.seed(seed)
        return random.randint(lo, hi)
    except Exception:
        return default_cap

def _ensure_from_domain_column():
    try:
        with ENG.begin() as c:
            c.execute(text("ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS from_domain text"))
    except Exception:
        pass

def _sender_domain() -> str:
    sender = (FROM_ADDR or SMTP_USER or "").strip()
    if "@" in sender:
        return sender.split("@")[-1].lower()
    return "unknown"

def _domain_send_ok(domain: str) -> bool:
    """Check email_health.daily_cap by sender domain vs today's sent count for that domain."""
    db = _normalize_db(os.environ.get("DATABASE_URL", ""))
    today = dt.datetime.utcnow().date()
    with psycopg2.connect(db) as c, c.cursor() as cur:
        cur.execute("SELECT COALESCE(daily_cap,40) FROM email_health WHERE domain=%s", (domain,))
        row = cur.fetchone()
        cap = row[0] if row else 40
        cur.execute("""
            SELECT COUNT(*) FROM email_sends
            WHERE COALESCE(from_domain,'') = %s
              AND status='sent'
              AND sent_at::date=%s
        """, (domain, today))
        sent_today = cur.fetchone()[0]
        return sent_today < cap

def _log_provider_event(send_id: Optional[int], event_type: str, rcpt: Optional[str], sender_domain: str, payload: Optional[dict] = None):
    payload = payload or {}
    with ENG.begin() as cx:
        cx.execute(text("""
            INSERT INTO provider_events(email_send_id, provider, event_type, email, domain, payload)
            VALUES (:sid, :prov, :etype, :email, :domain, :payload)
        """), {
            "sid": send_id,
            "prov": "smtp",
            "etype": event_type,
            "email": rcpt,
            "domain": sender_domain,
            "payload": payload
        })

# ---------- (Optional) First-party tracking helpers --------------------------

URL_RE = re.compile(r'(https?://[^\s<>"\'\)]+)')

def _wrap_click(url: str, send_id: int) -> str:
    if not TRACKING_BASE:
        return url
    return f"{TRACKING_BASE}/t/c?eid={send_id}&url={urlquote(url, safe='')}"

def _append_pixel(html: str, send_id: int) -> str:
    if not TRACKING_BASE:
        return html
    pixel = f'<img src="{TRACKING_BASE}/t/pixel?eid={send_id}" width="1" height="1" style="display:none" />'
    return (html or "").rstrip() + pixel

def _make_bodies_for_send(send_id: int, body: str) -> tuple[str, Optional[str]]:
    """
    Returns (body_text, body_html). If HTML is not provided, we generate a simple HTML mirror.
    Also: wrap first URL for click tracking (if TRACKING_BASE set) and append pixel to HTML.
    """
    body_text = body or ""
    first_url = None
    m = URL_RE.search(body_text)
    if m:
        first_url = m.group(1)

    body_html = None
    if "<html" in body.lower() or "</p>" in body.lower() or "</br>" in body.lower():
        body_html = body  # already HTML-ish
    else:
        # basic auto-HTML
        safe = (body_text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;"))
        safe = safe.replace("\n", "<br>")
        body_html = f"<html><body>{safe}</body></html>"

    # wrap first URL in both text/html if tracking is enabled
    if TRACKING_BASE and first_url:
        wrapped = _wrap_click(first_url, send_id)
        body_text = body_text.replace(first_url, wrapped, 1)
        body_html = body_html.replace(first_url, wrapped, 1)

    # append pixel to HTML if tracking enabled
    if TRACKING_BASE:
        body_html = _append_pixel(body_html, send_id)

    return body_text, body_html

# ---------- FLOW ENTRYPOINT --------------------------------------------------

def send_queue(batch_size: int = 1, allow_weekend: bool = True, **kwargs):
    """
    Sends up to one email per run (hard limit), with:
      - JITTER at start
      - DAILY CAP via warm-up (WARMUP_WEEKLY/WARMUP_START) or SEND_DAILY_CAP
      - PER-DOMAIN CAP via email_health (domain caps)
      - PROBABILISTIC SPACING across remaining slots
      - Delivered telemetry into provider_events
      - Discord alerts on cap hits (if configured)
    """
    logger = get_run_logger()
    log = logging.getLogger(__name__)

    _ensure_from_domain_column()
    sender_domain = _sender_domain()

    # Respect Prefect dry_run param if provided
    live = LIVE_DEFAULT
    try:
        if 'dry_run' in kwargs:
            live = not bool(kwargs.get('dry_run'))
    except Exception:
        pass

    # Weekend guard
    if not allow_weekend and dt.datetime.utcnow().weekday() >= 5:
        logger.info("Outside weekday window; exiting.")
        return 0

    # Time window guard
    if not _within_window():
        logger.info("Outside time window (%s, tz=%s); exiting.", SEND_WINDOW, SEND_WINDOW_TZ)
        return 0

    # Domain cap guard
    try:
        if not _domain_send_ok(sender_domain):
            msg = f"⛔ Domain cap hit for {sender_domain}. Skipping this run."
            logger.info("DOMAIN-CAP: %s is at cap; skipping this slot.", sender_domain)
            try: post_discord(msg)
            except Exception: pass
            return 0
    except Exception as e:
        logger.warning("DOMAIN-CAP check failed (%s); proceeding with global cap only.", e)

    # Jitter
    if SEND_JITTER_MAX > 0:
        d = random.randint(0, SEND_JITTER_MAX)
        logger.info("JITTER: sleeping %ss before evaluate/send", d)
        time.sleep(d)

    # Global daily cap
    cap_today = _warmup_cap(SEND_DAILY_CAP_DEFAULT)
    sent_today_global = _today_sent_count()
    if sent_today_global >= cap_today:
        msg = f"⛔ Global cap reached: sent_today={sent_today_global} cap={cap_today}. Skipping."
        logger.info("DAILY-CAP (global): sent_today=%s >= cap_today=%s; skipping.",
                    sent_today_global, cap_today)
        try: post_discord(msg)
        except Exception: pass
        return 0

    # Probabilistic spacing across remaining slots
    sec_left = _seconds_left_in_window(SEND_WINDOW)
    slots_left = max(1, math.ceil(sec_left / max(1, SLOT_SECONDS)))
    remaining_allowed = max(0, cap_today - sent_today_global)
    p = min(1.0, remaining_allowed / slots_left)
    if p < 1.0:
        r = random.random()
        if r > p:
            logger.info(
                "SPACING: skipping this slot (p=%.2f, r=%.2f, remaining=%s, slots_left=%s)",
                p, r, remaining_allowed, slots_left
            )
            return 0

    # Hard limit to 1 email/run
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
        # Re-check domain cap mid-run
        try:
            if not _domain_send_ok(sender_domain):
                logger.info("DOMAIN-CAP mid-run: %s capped; aborting remaining sends.", sender_domain)
                try: post_discord(f"⛔ Domain cap hit mid-run for {sender_domain}. Aborting.")
                except Exception: pass
                break
        except Exception as e:
            logger.warning("DOMAIN-CAP mid-run check failed (%s); continuing.", e)

        # Re-check global cap mid-run
        if sent_today_global + sent_count >= cap_today:
            logger.info("DAILY-CAP mid-run: reached global cap %s.", cap_today)
            try: post_discord(f"⛔ Global cap hit mid-run (cap={cap_today}). Aborting.")
            except Exception: pass
            break

        send_id = int(r["send_id"])
        subj = (r["subject"] or "").strip()
        body = (r["body"] or "")
        to   = (r["email"] or "").strip()

        # Build text/html bodies (and add tracking if configured)
        body_text, body_html = _make_bodies_for_send(send_id, body)

        logger.info(
            "MAIL_PREVIEW id=%s to=%s subj=%r body=%r",
            send_id, to, (subj or "")[:72], (body or "")[:160].replace("\n", " ")
        )

        try:
            if live:
                _send_raw(to, subj, body_text, body_html)
                provider_id = "smtp"
            else:
                provider_id = "dry-run"

            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='sent',
                           provider_message_id=:mid,
                           sent_at=now(),
                           from_domain=:fd
                     where id=:id
                """), {"mid": provider_id, "fd": sender_domain, "id": send_id})

            # provider_events: delivered
            try:
                _log_provider_event(send_id, "delivered", to, sender_domain, {"via": "smtp"})
            except Exception:
                pass

            sent_count += 1
            time.sleep(0.8)
            logger.info("Sent #%s to %s", send_id, to)

        except Exception as e:
            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='failed', error=:err, from_domain=:fd
                     where id=:id
                """), {"err": str(e)[:300], "fd": sender_domain, "id": send_id})
            logger.error("FAILED #%s to %s -> %s", send_id, to, e)
            try: post_discord(f"❌ Send failed for id={send_id} to={to}: {e}")
            except Exception: pass

    return sent_count
