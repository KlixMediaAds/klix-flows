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
from prefect import get_run_logger, flow

# ---------------------------------------------------------------------------
# Optional Discord helper
# ---------------------------------------------------------------------------
try:
    from libs.discord import post_discord
except Exception:  # pragma: no cover
    def post_discord(_msg: str) -> None:  # type: ignore
        # no-op if helper not available; avoid crashing the flow
        pass


# ---------------------------------------------------------------------------
# DB engine (used by legacy v1 send_queue & helper queries)
# ---------------------------------------------------------------------------
ENG = create_engine(os.environ["DATABASE_URL"].replace("+psycopg", ""), pool_pre_ping=True, future=True)


# ---------------------------------------------------------------------------
# SMTP config (env-driven, with per-inbox overrides)
# ---------------------------------------------------------------------------

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# Global fallback (used if we can't match a specific inbox)
SMTP_USER = (
    os.getenv("SMTP_USER")
    or os.getenv("SMTP_USERNAME")
    or os.getenv("SMTP_USER_MAIN")
)
SMTP_PASS = (
    os.getenv("SMTP_PASS")
    or os.getenv("SMTP_APP")
    or os.getenv("SMTP_PASSWORD")
)
FROM_ADDR = os.getenv("SMTP_FROM") or SMTP_USER

# Per-inbox overrides (optional but recommended)
SMTP_USER_JESS = os.getenv("SMTP_USER_JESS")
SMTP_PASS_JESS = os.getenv("SMTP_PASS_JESS") or SMTP_PASS
SMTP_FROM_JESS = os.getenv("SMTP_FROM_JESS") or SMTP_USER_JESS

SMTP_USER_ALEX = os.getenv("SMTP_USER_ALEX")
SMTP_PASS_ALEX = os.getenv("SMTP_PASS_ALEX") or SMTP_PASS
SMTP_FROM_ALEX = os.getenv("SMTP_FROM_ALEX") or SMTP_USER_ALEX

SMTP_USER_ERICA = os.getenv("SMTP_USER_ERICA")
SMTP_PASS_ERICA = os.getenv("SMTP_PASS_ERICA") or SMTP_PASS
SMTP_FROM_ERICA = os.getenv("SMTP_FROM_ERICA") or SMTP_USER_ERICA

SMTP_USER_TEAM = os.getenv("SMTP_USER_TEAM")
SMTP_PASS_TEAM = os.getenv("SMTP_PASS_TEAM") or SMTP_PASS
SMTP_FROM_TEAM = os.getenv("SMTP_FROM_TEAM") or SMTP_USER_TEAM


def _resolve_smtp_credentials(for_email: Optional[str]) -> tuple[str, int, Optional[str], Optional[str], str]:
    """
    Given the desired from_email (inbox address), choose SMTP user/pass and
    envelope sender.

    This lets each inbox use its *own* Gmail app password & login.
    Falls back to global SMTP_USER/SMTP_PASS/SMTP_FROM if no specific match.
    """
    addr = (for_email or "").strip().lower()

    def _norm(v: Optional[str]) -> Optional[str]:
        return v.strip().lower() if v else None

    # Jess
    if addr and (addr == _norm(SMTP_USER_JESS) or addr == _norm(SMTP_FROM_JESS)):
        user = SMTP_USER_JESS or SMTP_USER
        pwd = SMTP_PASS_JESS or SMTP_PASS
        sender = SMTP_FROM_JESS or addr or FROM_ADDR or user
    # Alex
    elif addr and (addr == _norm(SMTP_USER_ALEX) or addr == _norm(SMTP_FROM_ALEX)):
        user = SMTP_USER_ALEX or SMTP_USER
        pwd = SMTP_PASS_ALEX or SMTP_PASS
        sender = SMTP_FROM_ALEX or addr or FROM_ADDR or user
    # Erica
    elif addr and (addr == _norm(SMTP_USER_ERICA) or addr == _norm(SMTP_FROM_ERICA)):
        user = SMTP_USER_ERICA or SMTP_USER
        pwd = SMTP_PASS_ERICA or SMTP_PASS
        sender = SMTP_FROM_ERICA or addr or FROM_ADDR or user
    # Team
    elif addr and (addr == _norm(SMTP_USER_TEAM) or addr == _norm(SMTP_FROM_TEAM)):
        user = SMTP_USER_TEAM or SMTP_USER
        pwd = SMTP_PASS_TEAM or SMTP_PASS
        sender = SMTP_FROM_TEAM or addr or FROM_ADDR or user
    else:
        # Fallback: use globals
        user = SMTP_USER
        pwd = SMTP_PASS
        sender = (FROM_ADDR or user or addr or "no-reply@klixads.org")

    return SMTP_HOST, SMTP_PORT, user, pwd, sender


def _smtp(for_email: Optional[str] = None):
    """
    Open an SMTP connection using credentials appropriate for `for_email`.
    """
    host, port, user, pwd, _ = _resolve_smtp_credentials(for_email)

    s = smtplib.SMTP(host, port, timeout=30)
    s.ehlo()
    try:
        s.starttls()
        s.ehlo()
    except smtplib.SMTPException:
        # Some providers may not support STARTTLS; we just continue without it.
        pass
    if user and pwd:
        s.login(user, (pwd or "").replace(" ", ""))
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

    IMPORTANT:
    - We also route SMTP_USER/SMTP_PASS based on from_email so each inbox
      uses its own Gmail login (alex, erica, jess, team, etc.).
    """
    # Build MIME message
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")

    # Choose envelope + header sender based on from_email
    _, _, _, _, sender = _resolve_smtp_credentials(from_email)
    sender = sender.strip()

    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = sender

    with _smtp(sender) as s:
        s.sendmail(sender, [to_email], msg.as_string())


# ---------------------------------------------------------------------------
# Cadence / window config (used by v1 + v2)
# ---------------------------------------------------------------------------

SEND_WINDOW = os.getenv("SEND_WINDOW", "13:00-21:00")           # "HH:MM-HH:MM"
SEND_WINDOW_TZ = os.getenv("SEND_WINDOW_TZ", "local")           # "local", "utc", or IANA like "America/Toronto"
SLOT_SECONDS = int(os.getenv("SEND_SLOT_SECONDS", "300"))       # old v1 usage
SEND_JITTER_MAX = int(os.getenv("SEND_JITTER_MAX", "0"))        # extra sleep at start (sec)
SEND_DAILY_CAP_DEFAULT = int(os.getenv("SEND_DAILY_CAP", "50"))

# Live / dry-run toggle (can be overridden by Prefect param)
LIVE_DEFAULT = os.getenv("SEND_LIVE", "1") == "1"

# Optional first-party tracking (FastAPI endpoints later)
TRACKING_BASE = os.getenv("TRACKING_BASE_URL", "").rstrip("/")  # e.g. https://your.server

# Exactly 1 email per run (legacy v1)
BATCH_HARD_LIMIT = 1


# ---------------------------------------------------------------------------
# INTERNAL DB NORMALIZATION (for psycopg2 use) - used by v2
# ---------------------------------------------------------------------------

def _normalize_db(url: str) -> str:
    if not url:
        return url
    if url.startswith("postgresql+psycopg://"):
        url = "postgresql://" + url.split("postgresql+psycopg://", 1)[1]
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url


# ---------------------------------------------------------------------------
# TIME / WINDOW HELPERS (used by v1 + v2)
# ---------------------------------------------------------------------------

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _now_local_or_utc() -> dt.datetime:
    """Return *naive* datetime representing "now" in the configured window TZ.

    - SEND_WINDOW_TZ="utc"        → use UTC
    - SEND_WINDOW_TZ="local"/""   → use server local time
    - SEND_WINDOW_TZ=<IANA name>  → e.g. "America/Toronto" via zoneinfo, then
      stripped to naive so arithmetic stays consistent.
    """
    raw = SEND_WINDOW_TZ or "local"
    tz = raw.lower()

    # Pure UTC
    if tz == "utc":
        return dt.datetime.utcnow()

    # Server local time
    if tz == "local":
        return dt.datetime.now()

    # IANA timezone like "America/Toronto"
    if ZoneInfo is not None:
        try:
            aware = dt.datetime.now(ZoneInfo(raw))
            return aware.replace(tzinfo=None)
        except Exception:
            pass

    # Fallback: server local
    return dt.datetime.now()


def _parse_window(window: str) -> tuple[dt.datetime, dt.datetime]:
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


# ---------------------------------------------------------------------------
# Simple body builder used by v2 (tracking-ready but minimal)
# ---------------------------------------------------------------------------

def _make_bodies_for_send(send_id: int, body: str) -> tuple[str, Optional[str]]:
    """
    Return (text_body, html_body) for a given email_sends row.

    For now, we:
      - Treat `body` as plain text.
      - Optionally wrap it in a very simple HTML template.
      - Leave room for future tracking / unsubscribe tokens if TRACKING_BASE is set.
    """
    text_body = body or ""

    if TRACKING_BASE:
        # Example tracking pixel (optional)
        pixel_url = f"{TRACKING_BASE}/t/open?sid={send_id}"
        html_body = f"""
        <html>
          <body>
            <pre style="font-family: inherit; white-space: pre-wrap;">{text_body}</pre>
            <img src="{pixel_url}" width="1" height="1" style="display:none;" />
          </body>
        </html>
        """
    else:
        html_body = f"""
        <html>
          <body>
            <pre style="font-family: inherit; white-space: pre-wrap;">{text_body}</pre>
          </body>
        </html>
        """

    return text_body, html_body


# ---------------------------------------------------------------------------
# Provider-event logging stub (v2 wraps in try/except)
# ---------------------------------------------------------------------------

def _log_provider_event(
    send_id: Optional[int],
    event_type: str,
    to_email: Optional[str],
    domain: Optional[str],
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Minimal stub for logging provider events.

    v2 already wraps this in try/except, so it's safe if we keep it best-effort.
    If you later create an email_provider_events table, you can write into it here.
    """
    logger = logging.getLogger("send_queue.provider")
    logger.info(
        "provider_event send_id=%s type=%s to=%s domain=%s payload=%s",
        send_id,
        event_type,
        to_email,
        domain,
        json.dumps(payload or {}),
    )


# ---------------------------------------------------------------------------
# Legacy v1 send_queue (kept minimal so old scripts don't break)
# ---------------------------------------------------------------------------

def _pick_one_queued() -> Optional[Dict[str, Any]]:
    """
    Very simple picker: grab a single queued email_sends row.

    v2 does more advanced inbox-aware picking; this is only for old flows that
    might still call send_queue_flow directly.
    """
    with ENG.begin() as c:
        row = c.execute(
            text(
                """
                SELECT id, to_email, subject, body
                FROM email_sends
                WHERE status = 'queued'
                ORDER BY id ASC
                LIMIT 1
                """
            )
        ).mappings().first()
    return dict(row) if row else None


def _mark_sent(send_id: int, domain: str, provider_id: str = "smtp") -> None:
    with ENG.begin() as c:
        c.execute(
            text(
                """
                UPDATE email_sends
                   SET status = 'sent',
                       provider_message_id = :pid,
                       sent_at = NOW(),
                       updated_at = NOW(),
                       from_domain = :dom
                 WHERE id = :sid
                """
            ),
            {"sid": send_id, "pid": provider_id, "dom": domain},
        )


def _mark_failed(send_id: int, domain: str, error: str) -> None:
    with ENG.begin() as c:
        c.execute(
            text(
                """
                UPDATE email_sends
                   SET status = 'failed',
                       error = :err,
                       updated_at = NOW(),
                       from_domain = :dom
                 WHERE id = :sid
                """
            ),
            {"sid": send_id, "err": error[:300], "dom": domain},
        )


def _sender_domain_from_addr() -> str:
    sender = (FROM_ADDR or SMTP_USER or "").strip()
    if "@" in sender:
        return sender.split("@")[-1].lower()
    return "unknown"


@flow(name="send-queue-v1")
def send_queue_flow(dry_run: Optional[bool] = None) -> int:
    """
    Legacy single-inbox send loop.

    You are now using send_queue_v2 as the real engine, but keeping this around
    avoids breaking any old scripts that still import send_queue_flow.
    """
    logger = get_run_logger()

    # Weekend / window checks reused from v2
    try:
        if not _within_window():
            logger.info(
                "send_queue_v1: outside window (%s, tz=%s); exiting.",
                SEND_WINDOW,
                SEND_WINDOW_TZ,
            )
            return 0
    except Exception as e:
        logger.error("send_queue_v1: invalid SEND_WINDOW %r (%s); exiting.", SEND_WINDOW, e)
        return 0

    live = LIVE_DEFAULT
    if dry_run is not None:
        live = not bool(dry_run)

    job = _pick_one_queued()
    if not job:
        logger.info("send_queue_v1: nothing queued.")
        return 0

    send_id = int(job["id"])
    to_email = (job.get("to_email") or "").strip()
    subject = (job.get("subject") or "").strip()
    body = job.get("body") or ""

    if not to_email:
        logger.error("send_queue_v1: job %s missing to_email; marking failed.", send_id)
        _mark_failed(send_id, _sender_domain_from_addr(), "missing recipient email")
        return 0

    body_text, body_html = _make_bodies_for_send(send_id, body)

    try:
        if live:
            _send_raw(
                to_email=to_email,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                from_email=FROM_ADDR,
            )
            provider_id = "smtp"
        else:
            provider_id = "dry-run"

        _mark_sent(send_id, _sender_domain_from_addr(), provider_id)
        _log_provider_event(
            send_id,
            "delivered",
            to_email,
            _sender_domain_from_addr(),
            {"via": "smtp", "live": live, "engine": "send_queue_v1"},
        )
        logger.info("send_queue_v1: sent id=%s to=%s", send_id, to_email)
        return 1

    except Exception as e:
        err_s = str(e)
        logger.error("send_queue_v1: FAILED id=%s to=%s -> %s", send_id, to_email, err_s)
        _mark_failed(send_id, _sender_domain_from_addr(), err_s)
        try:
            post_discord(f"❌ send_queue_v1 failed for id={send_id} to={to_email}: {err_s}")
        except Exception:
            pass
        return 0
