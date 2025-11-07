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

def _send_raw(
    to_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    from_email: Optional[str] = None,
):
    """
    Low-level SMTP send.

    from_email overrides the header + envelope sender. If not provided, we fall
    back to FROM_ADDR / SMTP_USER as before.
    """
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")

    sender = (from_email or FROM_ADDR or SMTP_USER or "").strip()

    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = sender

    with _smtp() as s:
        s.sendmail(sender, [to_email], msg.as_string())

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

# ---------- INBOX / WARMUP STATE HELPERS ------------------------------------


def _load_inbox_state() -> List[Dict[str, Any]]:
    """
    Load all active inboxes and their:
      - base_cap (per inbox or per domain, falling back to SEND_DAILY_CAP_DEFAULT)
      - cold_sent_today (from email_sends)
      - warmup_sent_today (from warmup_analytics->email_date_data)
      - cold_remaining = base_cap - cold_sent_today - warmup_sent_today
    """
    today = dt.datetime.utcnow().date()
    today_str = today.isoformat()

    with ENG.begin() as cx:
        inbox_rows = (
            cx.execute(
                text(
                    """
                    SELECT inbox_id, email_address, domain, provider, daily_cap, active
                    FROM inboxes
                    WHERE active = TRUE
                    """
                )
            )
            .mappings()
            .all()
        )

        # cold sends per inbox (only what *this* system has sent)
        cold_rows = (
            cx.execute(
                text(
                    """
                    SELECT inbox_id, COUNT(*) AS sent_today
                    FROM email_sends
                    WHERE status='sent'
                      AND sent_at::date = current_date
                    GROUP BY inbox_id
                    """
                )
            )
            .all()
        )
        cold_by_inbox = {row[0]: row[1] for row in cold_rows}

        # most recent warmup metrics per email
        warm_rows = (
            cx.execute(
                text(
                    """
                    SELECT DISTINCT ON (email) email, metric
                    FROM warmup_analytics
                    ORDER BY email, ts DESC
                    """
                )
            )
            .mappings()
            .all()
        )

        domain_caps_rows = (
            cx.execute(
                text(
                    """
                    SELECT domain, daily_cap
                    FROM email_health
                    """
                )
            )
            .all()
        )
        domain_caps = {row[0]: row[1] for row in domain_caps_rows}

    warm_by_email: Dict[str, int] = {}
    for row in warm_rows:
        email = row["email"]
        metric = row["metric"]
        sent_today = 0
        try:
            if isinstance(metric, str):
                m = json.loads(metric)
            else:
                m = metric or {}
            by_date = (
                m.get("email_date_data", {})
                .get(email, {})
            )
            day_data = by_date.get(today_str, {}) or {}
            sent_today = int(day_data.get("sent", 0))
        except Exception:
            sent_today = 0
        warm_by_email[email] = sent_today

    inboxes: List[Dict[str, Any]] = []
    for ir in inbox_rows:
        inbox_id = ir["inbox_id"]
        email_address = ir["email_address"]
        domain = (ir["domain"] or "").lower()
        provider = ir["provider"]
        active = ir["active"]

        domain_cap = domain_caps.get(domain)
        # Base cap: inbox.daily_cap → domain_cap → global default
        base_cap = ir["daily_cap"] or domain_cap or SEND_DAILY_CAP_DEFAULT

        cold_sent = int(cold_by_inbox.get(inbox_id, 0) or 0)
        warm_sent = int(warm_by_email.get(email_address, 0) or 0)
        cold_remaining = max(0, base_cap - cold_sent - warm_sent)

        inboxes.append(
            {
                "inbox_id": inbox_id,
                "email_address": email_address,
                "domain": domain,
                "provider": provider,
                "active": active,
                "base_cap": base_cap,
                "cold_sent_today": cold_sent,
                "warmup_sent_today": warm_sent,
                "cold_remaining": cold_remaining,
            }
        )

    return inboxes


def _choose_inbox_for_send(inboxes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Pick an inbox that still has cold capacity today.

    Strategy: among inboxes with cold_remaining > 0, pick the one with the
    smallest cold_sent_today (simple load-balancing).
    """
    candidates = [ib for ib in inboxes if ib.get("cold_remaining", 0) > 0 and ib.get("active")]
    if not candidates:
        return None
    return min(candidates, key=lambda ib: ib.get("cold_sent_today", 0))

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
      - GLOBAL CAP via warm-up ramp (WARMUP_WEEKLY/WARMUP_START) or SEND_DAILY_CAP
      - PER-DOMAIN / PER-INBOX CAPS via email_health + inboxes + warmup_analytics
      - PROBABILISTIC SPACING across remaining slots
      - Delivered telemetry into provider_events
      - Discord alerts on cap hits (if configured)
    """
    logger = get_run_logger()
    log = logging.getLogger(__name__)

    _ensure_from_domain_column()

    # Respect Prefect dry_run param if provided
    live = LIVE_DEFAULT
    try:
        if "dry_run" in kwargs:
            live = not bool(kwargs.get("dry_run"))
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

    # Load inbox + warmup state
    try:
        inboxes = _load_inbox_state()
    except Exception as e:
        logger.error("Failed to load inbox state: %s", e)
        return 0

    if not inboxes:
        logger.info("No active inboxes configured; skipping send_queue.")
        return 0

    # Global cap via warm-up ramp (interpreted as *total* volume: cold + warmup)
    cap_today = _warmup_cap(SEND_DAILY_CAP_DEFAULT)
    cold_sent_total = sum(ib["cold_sent_today"] for ib in inboxes)
    warmup_total = sum(ib["warmup_sent_today"] for ib in inboxes)
    total_load_today = cold_sent_total + warmup_total

    if total_load_today >= cap_today:
        msg = (
            f"⛔ Global cap reached: total_load_today={total_load_today} "
            f"(cold={cold_sent_total}, warmup={warmup_total}) cap={cap_today}. Skipping."
        )
        logger.info(msg)
        try:
            post_discord(msg)
        except Exception:
            pass
        return 0

    # Probabilistic spacing across remaining slots
    sec_left = _seconds_left_in_window(SEND_WINDOW)
    slots_left = max(1, math.ceil(sec_left / max(1, SLOT_SECONDS)))
    remaining_allowed = max(0, cap_today - total_load_today)
    p = min(1.0, remaining_allowed / slots_left)
    if p < 1.0:
        r = random.random()
        if r > p:
            logger.info(
                "SPACING: skipping this slot (p=%.2f, r=%.2f, remaining=%s, slots_left=%s)",
                p,
                r,
                remaining_allowed,
                slots_left,
            )
            return 0

    # Hard limit to 1 email/run
    effective_batch = max(1, min(int(batch_size or 1), BATCH_HARD_LIMIT))

    # Fetch queued rows (oldest first)
    with ENG.begin() as cx:
        rows = (
            cx.execute(
                text(
                    """
                    SELECT s.id AS send_id, s.lead_id, s.subject, s.body, l.email
                    FROM email_sends s
                    JOIN leads l ON l.id = s.lead_id
                    WHERE s.status='queued'
                    ORDER BY s.id ASC
                    LIMIT :lim
                    """
                ),
                {"lim": effective_batch},
            )
            .mappings()
            .all()
        )

    if not rows:
        logger.info("No queued emails found.")
        return 0

    sent_count = 0

    for r in rows:
        # Global cap mid-run (total volume: cold + warmup)
        if total_load_today + sent_count >= cap_today:
            logger.info("DAILY-CAP mid-run: reached global cap %s.", cap_today)
            try:
                post_discord(f"⛔ Global cap hit mid-run (cap={cap_today}). Aborting.")
            except Exception:
                pass
            break

        inbox = _choose_inbox_for_send(inboxes)
        if inbox is None:
            logger.info("No inbox with remaining cold capacity; stopping sends.")
            try:
                post_discord(":no_entry: All inbox caps exhausted for today; stopping send_queue.")
            except Exception:
                pass
            break

        from_email = inbox["email_address"]
        sender_domain = inbox["domain"]

        if inbox["cold_remaining"] <= 0:
            logger.info("Inbox %s has no cold_remaining; skipping.", from_email)
            continue

        send_id = int(r["send_id"])
        subj = (r["subject"] or "").strip()
        body = (r["body"] or "")
        to = (r["email"] or "").strip()

        # Build text/html bodies (and add tracking if configured)
        body_text, body_html = _make_bodies_for_send(send_id, body)

        logger.info(
            "MAIL_PREVIEW id=%s to=%s via=%s subj=%r body=%r",
            send_id,
            to,
            from_email,
            (subj or "")[:72],
            (body or "")[:160].replace("\n", " "),
        )

        try:
            if live:
                _send_raw(to, subj, body_text, body_html, from_email=from_email)
                provider_id = "smtp"
            else:
                provider_id = "dry-run"

            with ENG.begin() as cx:
                cx.execute(
                    text(
                        """
                        UPDATE email_sends
                           SET status='sent',
                               provider_message_id=:mid,
                               sent_at=now(),
                               from_domain=:fd,
                               inbox_id=:iid
                         WHERE id=:id
                        """
                    ),
                    {
                        "mid": provider_id,
                        "fd": sender_domain,
                        "iid": inbox["inbox_id"],
                        "id": send_id,
                    },
                )

            # provider_events: delivered
            try:
                _log_provider_event(send_id, "delivered", to, sender_domain, {"via": "smtp"})
            except Exception:
                pass

            sent_count += 1
            total_load_today += 1
            inbox["cold_sent_today"] += 1
            inbox["cold_remaining"] = max(0, inbox["cold_remaining"] - 1)

            time.sleep(0.8)
            logger.info("Sent #%s to %s via %s", send_id, to, from_email)

        except Exception as e:
            with ENG.begin() as cx:
                cx.execute(
                    text(
                        """
                        UPDATE email_sends
                           SET status='failed',
                               error=:err,
                               from_domain=:fd,
                               inbox_id=:iid
                         WHERE id=:id
                        """
                    ),
                    {
                        "err": str(e)[:300],
                        "fd": sender_domain,
                        "iid": inbox["inbox_id"],
                        "id": send_id,
                    },
                )
            logger.error("FAILED #%s to %s via %s -> %s", send_id, to, from_email, e)
            try:
                post_discord(f"❌ Send failed for id={send_id} to={to} via {from_email}: {e}")
            except Exception:
                pass

    return sent_count
