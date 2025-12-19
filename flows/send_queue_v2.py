import os
import random
import time
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import flow, get_run_logger
import re

# ---------------------------------------------------------------------------
# Reuse helpers + config from your existing v1 module
# ---------------------------------------------------------------------------

try:
    # Preferred when running as a package: python -m flows.send_queue_v2
    from .send_queue import (
        _normalize_db,
        _make_bodies_for_send,
        _log_provider_event,
        _send_raw,
        FROM_ADDR,
        SEND_WINDOW,
        SEND_WINDOW_TZ,
        LIVE_DEFAULT,
        _within_window,
    )
except ImportError:  # fallback for legacy CLI usage
    from send_queue import (  # type: ignore
        _normalize_db,
        _make_bodies_for_send,
        _log_provider_event,
        _send_raw,
        FROM_ADDR,
        SEND_WINDOW,
        SEND_WINDOW_TZ,
        LIVE_DEFAULT,
        _within_window,
    )

# Centralized Discord alert helper (severity + context routing)
from flows.utils.discord_alerts import send_discord_alert  # type: ignore

# Provider abstraction (Era 1.8+)
from klix.providers import get_provider


# Hard safety: never send legacy placeholder / fallback content
LEGACY_GARBAGE_PATTERNS = [
    r"\b-\s*Josh\b",
    r"Open to a quick note\?",
    r"I was reviewing\b",
    r"Quick question about your website",
]

def _is_legacy_garbage(subject: str, body: str) -> bool:
    text = (subject or "") + "\n" + (body or "")
    for pat in LEGACY_GARBAGE_PATTERNS:
        if re.search(pat, text, flags=re.IGNORECASE):
            return True
    return False


# Hard safety: never send to known test / placeholder recipients
SUPPRESSED_RECIPIENT_PATTERNS = [
    r"\\bYOUREMAIL\\b",
    r"\\bexample\\.com\\b",
    r"\\byou\\+test\\b",
    r"\\bfriend-test-\\d+@",
    r"\\bbouncetest-\\d+@",
    r"\\bdefinitely-not-a-real-domain\\b",
    r"@klixads\\.org\\b",
    r"@klixmedia\\.org\\b",
    r"@klixmedia\\.ca\\b",
]


def _is_suppressed_recipient(email: str) -> bool:
    """
    Hard safety gate: block known placeholder/test recipients.

    Uses:
      - SUPPRESSED_RECIPIENT_PATTERNS (regex patterns in this module)
      - optional env: KLIX_SUPPRESSED_RECIPIENTS="a@b.com,c@d.com"
    """
    if not email:
        return False

    e = email.strip().lower()

    # Env allowlist-style suppression (explicit addresses)
    raw = os.getenv("KLIX_SUPPRESSED_RECIPIENTS", "")
    if raw and raw.strip():
        suppressed = {x.strip().lower() for x in raw.split(",") if x.strip()}
        if e in suppressed:
            return True

    # Pattern suppression (placeholders / internal test domains)
    for pat in SUPPRESSED_RECIPIENT_PATTERNS:
        if re.search(pat, e, flags=re.IGNORECASE):
            return True

    return False


# ---------------------------------------------------------------------------
# Env knobs for v2
# ---------------------------------------------------------------------------

DB_DSN = _normalize_db(os.environ.get("DATABASE_URL", ""))

INBOX_SLOT_MIN_SECONDS = int(os.getenv("INBOX_SLOT_MIN_SECONDS", "600"))
INBOX_SLOT_MAX_SECONDS = int(os.getenv("INBOX_SLOT_MAX_SECONDS", "1800"))
SEND_JITTER_MAX = int(os.getenv("SEND_JITTER_MAX", "60"))
BATCH_HARD_LIMIT = int(os.getenv("BATCH_HARD_LIMIT", "20"))

# Safety: avoid infinite looping when the queue is "eligible but unsendable"
# (e.g., domain cooldown blocks, or all inboxes at cap). This is per-run.
MAX_PASSES_PER_RUN = int(os.getenv("KLIX_SENDQ_MAX_PASSES_PER_RUN", "25"))

# Optional: allow >1 send per inbox per run (still respects inbox cap / 24h governor)
MAX_PER_INBOX_PER_RUN = int(os.getenv("KLIX_SENDQ_MAX_PER_INBOX_PER_RUN", "0") or 0)  # 0 => unlimited

# In-memory per-domain throttle (short-term jitter)
DOMAIN_MIN_GAP_SECONDS = int(os.getenv("DOMAIN_MIN_GAP_SECONDS", "120"))
_domain_last: Dict[str, dt.datetime] = {}

# Test override: allow us to bypass SMTP/providers while still running the full pipeline
TEST_MODE = os.getenv("KLIX_TEST_MODE", "false").lower() in ("1", "true", "yes", "on")

# Test override for window (used separately)
KLIX_IGNORE_WINDOW = os.getenv("KLIX_IGNORE_WINDOW", "false").lower() in ("1", "true", "yes", "on")

# NEW: explicit cooldown bypass (cleaner than INBOX_SLOT_MIN/MAX=0)
KLIX_IGNORE_COOLDOWN = os.getenv("KLIX_IGNORE_COOLDOWN", "false").lower() in ("1", "true", "yes", "on")

# Governor caps
MAX_COLD_PER_DAY = int(os.getenv("KLIX_MAX_COLD_PER_DAY", "0") or 0)
MAX_COLD_PER_INBOX_PER_DAY = int(os.getenv("KLIX_MAX_COLD_PER_INBOX_PER_DAY", "0") or 0)
ERROR_RATE_STOP_THRESHOLD = float(os.getenv("KLIX_ERROR_RATE_STOP_THRESHOLD", "0") or 0.0)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _db_conn(cursor_factory=None):
    """
    Basic connection helper. If cursor_factory is provided, it will be used
    for .cursor(); otherwise default cursor is used.
    """
    if not DB_DSN:
        raise RuntimeError("DATABASE_URL is not set")
    if cursor_factory is None:
        return psycopg2.connect(DB_DSN)
    return psycopg2.connect(DB_DSN, cursor_factory=cursor_factory)


def _load_active_inboxes_with_stats() -> List[Dict[str, Any]]:
    """
    Uses sql/get_inbox_stats_today.sql to load:
      inbox_id, email_address, domain, daily_cap, active,
      sent_today, last_sent_at,
      provider_type, provider_config
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(base_dir, "sql", "get_inbox_stats_today.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()

    with _db_conn(cursor_factory=RealDictCursor) as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    for r in rows:
        r.setdefault("sent_today", 0)
        # last_sent_at will be a datetime with tzinfo from Postgres (TIMESTAMPTZ) or None
    return rows


def _claim_one_for_inbox(inbox_id) -> Optional[Dict[str, Any]]:
    """
    Atomically claim a single queued email_sends row using
    FOR UPDATE SKIP LOCKED and set status='sending' + inbox_id.
    Returns the full email_sends row as a dict, or None if none available.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(base_dir, "sql", "claim_one_send_for_inbox.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()

    with _db_conn(cursor_factory=RealDictCursor) as conn, conn.cursor() as cur:
        cur.execute(query, {"inbox_id": inbox_id})
        row = cur.fetchone()
        conn.commit()

    if not row:
        return None
    return dict(row)


def _requeue_job(send_id: int) -> None:
    """
    If a job was claimed (status='sending') but we decide not to send it,
    move it back to 'queued' so it can be picked up later by another run.
    """
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE email_sends
               SET status = 'queued',
                   inbox_id = NULL,
                   updated_at = NOW()
             WHERE id = %s
               AND status = 'sending'
            """,
            (send_id,),
        )
        conn.commit()


def _finalize_ok(send_id: int, inbox: Dict[str, Any], to_email: str, provider_id: str, live: bool) -> None:
    domain = inbox.get("domain") or ""
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE email_sends
               SET status = 'sent',
                   provider_message_id = %s,
                   sent_at = NOW(),
                   updated_at = NOW(),
                   from_domain = %s
             WHERE id = %s
            """,
            (provider_id, domain, send_id),
        )
        conn.commit()

    try:
        _log_provider_event(
            send_id,
            "delivered",
            to_email,
            domain,
            {"via": "smtp", "live": live, "engine": "send_queue_v2"},
        )
    except Exception:
        pass


def _finalize_fail(send_id: int, inbox: Dict[str, Any], to_email: str, err: str, live: bool) -> None:
    domain = inbox.get("domain") or ""
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE email_sends
               SET status = 'failed',
                   error = %s,
                   updated_at = NOW(),
                   from_domain = %s
             WHERE id = %s
            """,
            (err[:300], domain, send_id),
        )
        conn.commit()

    try:
        _log_provider_event(
            send_id,
            "failed",
            to_email,
            domain,
            {"via": "smtp", "live": live, "engine": "send_queue_v2", "error": err},
        )
    except Exception:
        pass


def _touch_inbox_after_send(inbox_id, now: Optional[dt.datetime] = None) -> None:
    now = now or dt.datetime.now(dt.timezone.utc)
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE inboxes
               SET last_sent_at = %s,
                   last_used_at = %s
             WHERE inbox_id = %s
            """,
            (now, now, inbox_id),
        )
        conn.commit()


def _get_lead_email(lead_id: Optional[int]) -> Optional[str]:
    """
    Fallback to fetch the lead's email by id.

    NOTE: We do not apply verification logic here; the main flow checks
    email_verification_status explicitly per job and fails invalid leads
    before we reach this point.
    """
    if not lead_id:
        return None
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT email FROM leads WHERE id = %s", (lead_id,))
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Recipient normalization helper
# ---------------------------------------------------------------------------


def _normalize_recipient(raw: Optional[str]) -> str:
    """
    Normalize a recipient email address coming from email_sends / leads.
    Handles things like:
      - "mailto:user@example.com?subject=Hey"
      - "user@example.com?subject=Hey&body=..."
    """
    addr = (raw or "").strip()
    if not addr:
        return ""
    if addr.lower().startswith("mailto:"):
        addr = addr[7:]
    addr = addr.split("?", 1)[0]
    return addr


def _is_denylisted_bounce(email: str) -> bool:
    if not email:
        return False
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM email_denylist
             WHERE lower(email) = lower(%s)
               AND reason = 'bounce'
             LIMIT 1
            """,
            (email,),
        )
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Governor stats helpers (24h cold sends + error rate)
# ---------------------------------------------------------------------------


def _load_recent_cold_stats(logger) -> Tuple[int, Dict[str, int], int, int, float]:
    """
    Returns:
        total_cold_sent_24h (int)
        per_inbox_cold_sent_24h (dict: inbox_id (str) -> count)
        success_count_24h (int)
        error_count_24h (int)
        failure_rate (float 0..1)
    """
    total_cold = 0
    per_inbox: Dict[str, int] = {}
    success_count = 0
    error_count = 0
    failure_rate = 0.0

    try:
        with _db_conn(cursor_factory=RealDictCursor) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT inbox_id, COUNT(*) AS cnt
                  FROM email_sends
                 WHERE send_type = 'cold'
                   AND status = 'sent'
                   AND sent_at >= NOW() - INTERVAL '24 hours'
                 GROUP BY inbox_id
                """
            )
            rows = cur.fetchall()
            for r in rows:
                inbox_id = r["inbox_id"]
                cnt = int(r["cnt"])
                if inbox_id is not None:
                    key = str(inbox_id)
                    per_inbox[key] = cnt
                    total_cold += cnt

            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN event_type = 'send_success' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN event_type = 'send_error' THEN 1 ELSE 0 END)   AS error_count
                  FROM send_events
                 WHERE created_at >= NOW() - INTERVAL '24 hours'
                """
            )
            row = cur.fetchone() or {}
            success_count = int(row.get("success_count") or 0)
            error_count = int(row.get("error_count") or 0)
            denom = success_count + error_count
            if denom > 0:
                failure_rate = error_count / denom

    except Exception as e:
        logger.error(f"send_queue_v2: failed to load recent cold stats: {e}")
        raise

    return total_cold, per_inbox, success_count, error_count, failure_rate


# ---------------------------------------------------------------------------
# Domain cooldown + event log helpers
# ---------------------------------------------------------------------------


def log_send_event(inbox_id, email_id, event_type: str, message: str) -> None:
    logger = get_run_logger()
    try:
        with _db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO send_events (inbox_id, email_id, event_type, message)
                VALUES (%s, %s, %s, %s)
                """,
                (inbox_id, email_id, event_type, message),
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"log_send_event failed: {e}")


def check_and_reserve_domain_capacity(domain: str, logger, now: Optional[dt.datetime] = None) -> bool:
    if not domain:
        logger.warning("check_and_reserve_domain_capacity called with empty domain; allowing send.")
        return True

    now = now or dt.datetime.now(dt.timezone.utc)

    with _db_conn(cursor_factory=RealDictCursor) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain, daily_cap, sent_today, last_sent_at
              FROM domains
             WHERE domain = %s
             FOR UPDATE
            """,
            (domain,),
        )
        row = cur.fetchone()

        if row is None:
            cur.execute(
                """
                INSERT INTO domains (domain)
                VALUES (%s)
                ON CONFLICT (domain) DO NOTHING
                RETURNING domain, daily_cap, sent_today, last_sent_at
                """,
                (domain,),
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    SELECT domain, daily_cap, sent_today, last_sent_at
                      FROM domains
                     WHERE domain = %s
                    """,
                    (domain,),
                )
                row = cur.fetchone()

        if row["last_sent_at"] is not None and row["last_sent_at"].date() != now.date():
            cur.execute(
                """
                UPDATE domains
                   SET sent_today = 0
                 WHERE domain = %s
                """,
                (domain,),
            )
            row["sent_today"] = 0

        daily_cap = row["daily_cap"]
        sent_today = row["sent_today"]

        if daily_cap is not None and sent_today is not None and sent_today >= daily_cap:
            logger.info("Domain %s at cap (%s/%s); skipping.", domain, sent_today, daily_cap)
            return False

        cur.execute(
            """
            UPDATE domains
               SET sent_today = sent_today + 1,
                   last_sent_at = %s
             WHERE domain = %s
            """,
            (now, domain),
        )
        conn.commit()
        return True


# ---------------------------------------------------------------------------
# Cooldown helpers
# ---------------------------------------------------------------------------


def _inbox_cooldown_ok(inbox: Dict[str, Any], logger=None, force_send: bool = False) -> bool:
    """
    Enforces a random cooldown between INBOX_SLOT_MIN_SECONDS and
    INBOX_SLOT_MAX_SECONDS since the last sent_at for this inbox.

    Handles both tz-aware and naive datetimes from Postgres.
    """

    # force_send bypasses inbox cooldown (used for controlled debugging / backfills)
    if force_send:
        if logger:
            logger.info("send_queue_v2: force_send=True; bypassing inbox cooldown.")
        return True

    if KLIX_IGNORE_COOLDOWN:
        return True

    last = inbox.get("last_sent_at")
    if not last:
        return True

    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    else:
        last = last.astimezone(dt.timezone.utc)

    now = dt.datetime.now(dt.timezone.utc)
    gap = random.randint(INBOX_SLOT_MIN_SECONDS, INBOX_SLOT_MAX_SECONDS)
    diff = (now - last).total_seconds()
    ok = diff > gap

    if (not ok) and logger:
        logger.debug(
            "send_queue_v2: cooldown block inbox=%s last_sent_at=%s diff=%.1fs need>%ss",
            inbox.get("email_address"),
            last.isoformat(),
            diff,
            gap,
        )
    return ok


def _domain_cooldown_ok(domain: str) -> bool:
    if not domain:
        return True
    last = _domain_last.get(domain)
    if not last:
        return True
    now = dt.datetime.utcnow()
    return (now - last).total_seconds() > DOMAIN_MIN_GAP_SECONDS


def _touch_domain_gate(domain: str) -> None:
    if not domain:
        return
    _domain_last[domain] = dt.datetime.utcnow()


def _sleep_jitter() -> None:
    if SEND_JITTER_MAX <= 0:
        return
    delay = random.uniform(0, SEND_JITTER_MAX)
    time.sleep(delay)


# ---------------------------------------------------------------------------
# Provider wrapper with KLIX_TEST_MODE enforcement
# ---------------------------------------------------------------------------


def send_email_safely(
    inbox: Dict[str, Any],
    send_id: int,
    to_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str],
) -> str:
    logger = get_run_logger()
    provider_type = (inbox.get("provider_type") or "smtp_app_pw").strip().lower()
    provider = get_provider(provider_type)

    if TEST_MODE:
        logger.info(
            "[TEST_MODE] Skipping provider send for id=%s to=%s via %s (provider_type=%s)",
            send_id,
            to_email,
            inbox.get("email_address"),
            provider_type,
        )
        log_send_event(
            inbox.get("inbox_id"),
            send_id,
            "test_send",
            f"KLIX_TEST_MODE_skip_provider (provider_type={provider_type})",
        )
        return "test-mode"

    try:
        result = provider.send(
            inbox=inbox,
            to_email=to_email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            headers=None,
            live=True,
            send_id=send_id,
        )
    except Exception as e:
        logger.error("send_queue_v2: provider %s crashed for send_id=%s -> %s", provider_type, send_id, e)
        raise

    if not result.ok:
        err = result.error or f"provider {result.provider_name} reported failure"
        logger.error("send_queue_v2: provider %s reported failure for send_id=%s -> %s", provider_type, send_id, err)
        raise RuntimeError(err)

    return result.provider_message_id or provider_type


# ---------------------------------------------------------------------------
# Core send loop
# ---------------------------------------------------------------------------


@flow(name="send-queue-v2")
def send_queue_v2_flow(
    batch_size: int = BATCH_HARD_LIMIT,
    allow_weekend: bool = True,
    dry_run: Optional[bool] = None,
    force_send: bool = False,
) -> int:
    logger = get_run_logger()

    if TEST_MODE:
        logger.info("send_queue_v2: KLIX_TEST_MODE=true; provider sends will be skipped, pipeline will run.")

    if KLIX_IGNORE_COOLDOWN:
        logger.info("send_queue_v2: KLIX_IGNORE_COOLDOWN=true; skipping inbox cooldown enforcement.")

    # Weekend guard
    if not allow_weekend and dt.datetime.utcnow().weekday() >= 5:
        logger.info("send_queue_v2: weekend and allow_weekend=False; exiting.")
        return 0

    # Time window guard (reuse v1 logic) with override
    try:
        if KLIX_IGNORE_WINDOW:
            logger.info(
                "send_queue_v2: KLIX_IGNORE_WINDOW=true; skipping SEND_WINDOW enforcement (%s, tz=%s).",
                SEND_WINDOW,
                SEND_WINDOW_TZ,
            )
        else:
            if (not force_send) and not _within_window():
                logger.info("send_queue_v2: outside window (%s, tz=%s); exiting.", SEND_WINDOW, SEND_WINDOW_TZ)
                return 0
    except Exception as e:
        logger.error("send_queue_v2: invalid SEND_WINDOW %r (%s); exiting.", SEND_WINDOW, e)
        return 0

    # Live / dry-run
    live = LIVE_DEFAULT
    if dry_run is not None:
        live = not bool(dry_run)

    if live and not FROM_ADDR:
        msg = "send_queue_v2: FROM_ADDR / SMTP_USER not configured; cannot send live emails."
        logger.error(msg)
        try:
            send_discord_alert(
                title="SEND ENGINE HALT — Missing FROM_ADDR",
                body=msg,
                severity="critical",
                context={"flow": "send_queue_v2", "env": os.getenv("KLIX_ALERT_ENV_TAG", "prod")},
            )
        except Exception:
            pass
        return 0

    # Governor: 24h stats (cold sends + error rate)
    if MAX_COLD_PER_DAY > 0 or MAX_COLD_PER_INBOX_PER_DAY > 0 or ERROR_RATE_STOP_THRESHOLD > 0:
        try:
            total_cold_24h, per_inbox_cold_24h, success_24h, error_24h, failure_rate = _load_recent_cold_stats(logger)
        except Exception:
            msg = "send_queue_v2: failed to load recent cold stats; governor stopping run."
            logger.error(msg)
            try:
                send_discord_alert(
                    title="SEND ENGINE HALT — Governor Stats Failed",
                    body=msg,
                    severity="critical",
                    context={"flow": "send_queue_v2", "env": os.getenv("KLIX_ALERT_ENV_TAG", "prod")},
                )
            except Exception:
                pass
            return 0

        logger.info(
            "send_queue_v2: governor stats — cold_24h=%s, success_24h=%s, error_24h=%s, failure_rate=%.3f",
            total_cold_24h,
            success_24h,
            error_24h,
            failure_rate,
        )

        if ERROR_RATE_STOP_THRESHOLD > 0 and (success_24h + error_24h) > 0:
            if failure_rate >= ERROR_RATE_STOP_THRESHOLD:
                msg = f"send_queue_v2: failure_rate={failure_rate:.3f} >= {ERROR_RATE_STOP_THRESHOLD:.3f}; halting run."
                logger.error(msg)
                log_send_event(None, None, "governor_error_rate_exceeded", msg)
                try:
                    send_discord_alert(
                        title="SEND ENGINE HALT — Error Rate Governor",
                        body=msg,
                        severity="critical",
                        context={
                            "flow": "send_queue_v2",
                            "env": os.getenv("KLIX_ALERT_ENV_TAG", "prod"),
                            "failure_rate": failure_rate,
                            "success_24h": success_24h,
                            "error_24h": error_24h,
                        },
                    )
                except Exception:
                    pass
                return 0

        if MAX_COLD_PER_DAY > 0 and total_cold_24h >= MAX_COLD_PER_DAY:
            msg = f"send_queue_v2: global cold cap reached — {total_cold_24h}/{MAX_COLD_PER_DAY} in last 24h; halting run."
            logger.info(msg)
            log_send_event(None, None, "governor_cap_reached_global", msg)
            try:
                send_discord_alert(
                    title="SEND GOVERNOR — Global Cold Cap Reached",
                    body=msg,
                    severity="warning",
                    context={"flow": "send_queue_v2", "env": os.getenv("KLIX_ALERT_ENV_TAG", "prod")},
                )
            except Exception:
                pass
            return 0
    else:
        total_cold_24h = 0
        per_inbox_cold_24h = {}

    cold_sent_this_run = 0

    try:
        inboxes = _load_active_inboxes_with_stats()
    except Exception as e:
        msg = f"send_queue_v2: failed to load inbox stats: {e}"
        logger.error(msg)
        try:
            send_discord_alert(
                title="SEND ENGINE ERROR — Inbox Stats Load Failed",
                body=msg,
                severity="critical",
                context={"flow": "send_queue_v2", "env": os.getenv("KLIX_ALERT_ENV_TAG", "prod")},
            )
        except Exception:
            pass
        return 0

    if not inboxes:
        logger.info("send_queue_v2: no active inboxes; exiting.")
        return 0

    # Skip counters (so "sent=0" is explainable)
    skip_inactive = 0
    skip_inbox_cap = 0
    skip_inbox_cooldown = 0
    skip_domain_cooldown = 0
    skip_no_job = 0

    # New counters for round-robin behavior
    passes = 0
    progress_cycles = 0

    sent = 0
    _sleep_jitter()

    # Track per-inbox sends within this run (independent of daily caps)
    per_inbox_sent_this_run: Dict[str, int] = {}

    # -----------------------------------------------------------------------
    # ROUND-ROBIN LOOP
    # We keep doing passes over inboxes until:
    #  - sent >= batch_size, OR
    #  - a full pass produces zero sends (no progress), OR
    #  - MAX_PASSES_PER_RUN is hit (safety).
    # -----------------------------------------------------------------------
    while sent < batch_size and passes < MAX_PASSES_PER_RUN:
        passes += 1
        random.shuffle(inboxes)

        sent_this_pass = 0

        for inbox in inboxes:
            if sent >= batch_size:
                break

            if not inbox.get("active"):
                skip_inactive += 1
                continue

            inbox_id = inbox.get("inbox_id")
            if inbox_id is None:
                skip_inactive += 1
                continue
            inbox_id_key = str(inbox_id)

            # Optional per-inbox-per-run cap
            if MAX_PER_INBOX_PER_RUN > 0:
                already = int(per_inbox_sent_this_run.get(inbox_id_key, 0))
                if already >= MAX_PER_INBOX_PER_RUN:
                    skip_inbox_cap += 1
                    continue

            daily_cap = int(inbox.get("daily_cap") or 0)
            cold_24h_for_inbox = int(per_inbox_cold_24h.get(inbox_id_key, 0))

            if daily_cap <= 0:
                skip_inbox_cap += 1
                continue

            effective_inbox_cap = daily_cap
            if MAX_COLD_PER_INBOX_PER_DAY > 0:
                effective_inbox_cap = min(effective_inbox_cap, MAX_COLD_PER_INBOX_PER_DAY)

            if cold_24h_for_inbox >= effective_inbox_cap:
                skip_inbox_cap += 1
                continue

            if not _inbox_cooldown_ok(inbox, logger=logger, force_send=force_send):
                skip_inbox_cooldown += 1
                continue

            domain = (inbox.get("domain") or "").lower()
            if not _domain_cooldown_ok(domain):
                skip_domain_cooldown += 1
                continue

            job = _claim_one_for_inbox(inbox_id)
            if not job:
                skip_no_job += 1
                continue

            send_id = int(job["id"])
            subject = (job.get("subject") or "").strip()
            body = job.get("body") or ""

            raw_to_email = job.get("to_email")
            to_email = _normalize_recipient(raw_to_email)

            # SAFETY: refuse test/placeholder recipients
            if _is_suppressed_recipient(to_email):
                _mark_failed(conn, send_id, 'blocked: suppressed_recipient: internal_address')
                logger.warning("send_queue_v2: blocked suppressed_recipient send_id=%s to=%s", send_id, to_email)
                continue

            # SAFETY: refuse legacy fallback/placeholder emails
            if _is_legacy_garbage(subject, body):
                _mark_failed(conn, send_id, 'blocked: legacy hardcoded Josh fallback template (send_queue guard)')
                continue

            # HARD GATE: denylisted bounce recipients
            if _is_denylisted_bounce(to_email):
                err = "blocked: recipient denylisted (bounce)"
                _finalize_fail(send_id, inbox, to_email, err, live=False)
                try:
                    log_send_event(inbox_id, send_id, "send_error", err)
                except Exception:
                    pass
                continue

            send_type = (job.get("send_type") or "").strip().lower()
            prompt_angle_id = job.get("prompt_angle_id")

            # HARD GATE: require prompt profile + template provenance
            prompt_profile_id = job.get("prompt_profile_id")
            template_id = job.get("template_id")
            if not (prompt_profile_id and str(prompt_profile_id).strip()):
                err = "blocked: missing prompt_profile_id"
                _finalize_fail(send_id, inbox, to_email, err, live=False)
                try:
                    log_send_event(inbox_id, send_id, "send_error", err)
                except Exception:
                    pass
                continue

            if not (template_id and str(template_id).strip()):
                err = "blocked: missing template_id"
                _finalize_fail(send_id, inbox, to_email, err, live=False)
                try:
                    log_send_event(inbox_id, send_id, "send_error", err)
                except Exception:
                    pass
                continue
            lead_id = job.get("lead_id")

            # Hard guard: never send to invalid leads
            if lead_id:
                try:
                    with _db_conn() as conn, conn.cursor() as cur:
                        cur.execute("SELECT email_verification_status FROM leads WHERE id = %s", (lead_id,))
                        row = cur.fetchone()
                    status = row[0] if row else None
                except Exception as e:
                    status = "lookup_error"
                    logger.error(
                        "send_queue_v2: failed to read email_verification_status for lead_id=%s on job %s: %s",
                        lead_id,
                        send_id,
                        e,
                    )

                if status == "invalid":
                    err = "lead email marked invalid by verifier"
                    _finalize_fail(send_id, inbox, "", err, live=False)
                    log_send_event(inbox_id, send_id, "send_error", "lead email marked invalid; auto-failed job")
                    continue
                elif status == "lookup_error":
                    err = "could not read email_verification_status; safest to fail job"
                    _finalize_fail(send_id, inbox, "", err, live=False)
                    log_send_event(
                        inbox_id, send_id, "send_error", "failed to read email_verification_status; auto-failed job"
                    )
                    continue

            # Prompt spine guardrail for cold emails
            if send_type == "cold":
                if not prompt_angle_id:
                    err = "cold email missing prompt spine (prompt_angle_id)"
                    _finalize_fail(send_id, inbox, "", err, live=False)
                    log_send_event(inbox_id, send_id, "send_error", "missing_prompt_spine for cold email; auto-failed job")
                    continue

                if MAX_COLD_PER_DAY > 0:
                    if total_cold_24h + cold_sent_this_run >= MAX_COLD_PER_DAY:
                        _requeue_job(send_id)
                        passes = MAX_PASSES_PER_RUN
                        break

            # HARD RULE: send_queue_v2 must NEVER infer recipient from leads.
            # If email_sends.to_email is missing/invalid, fail the row loudly.
            if not to_email or "@" not in to_email:
                err = "missing or invalid recipient email"
                _finalize_fail(send_id, inbox, "", err, live=False)
                log_send_event(inbox_id, send_id, "send_error", "missing or invalid recipient email; auto-failed job")
                continue

            # Domain DB capacity reservation
            if not check_and_reserve_domain_capacity(domain, logger):
                _requeue_job(send_id)
                log_send_event(inbox_id, send_id, "send_skipped_domain_cap", f"domain {domain} at cap; requeued job")
                continue

            body_text, body_html = _make_bodies_for_send(send_id, body)

            if send_type == "cold" and not (body_text or body_html):
                err = "cold email missing body content after _make_bodies_for_send"
                _finalize_fail(send_id, inbox, to_email, err, live=False)
                log_send_event(inbox_id, send_id, "send_error", "missing body content for cold email; auto-failed job")
                continue

            logger.info(
                "send_queue_v2: MAIL_PREVIEW id=%s to=%s via=%s subj=%r",
                send_id,
                to_email,
                inbox.get("email_address"),
                (subject or "")[:72],
            )

            try:
                if live:
                    provider_id = send_email_safely(
                        inbox=inbox,
                        send_id=send_id,
                        to_email=to_email,
                        subject=subject,
                        body_text=body_text,
                        body_html=body_html,
                    )
                else:
                    provider_id = "dry-run"

                _finalize_ok(send_id, inbox, to_email, provider_id, live)
                if live:
                    _touch_inbox_after_send(inbox_id)
                _touch_domain_gate(domain)

                sent += 1
                sent_this_pass += 1
                inbox["last_sent_at"] = dt.datetime.now(dt.timezone.utc)

                per_inbox_sent_this_run[inbox_id_key] = int(per_inbox_sent_this_run.get(inbox_id_key, 0)) + 1

                if send_type == "cold":
                    cold_sent_this_run += 1
                    per_inbox_cold_24h[inbox_id_key] = cold_24h_for_inbox + 1

                log_send_event(
                    inbox_id,
                    send_id,
                    "send_success",
                    f"sent to {to_email} via {inbox.get('email_address')} (provider={provider_id})",
                )

            except Exception as e:
                err_s = str(e)
                _finalize_fail(send_id, inbox, to_email, err_s, live)
                log_send_event(
                    inbox_id,
                    send_id,
                    "send_error",
                    f"error sending to {to_email} via {inbox.get('email_address')}: {err_s}",
                )
                try:
                    send_discord_alert(
                        title="SEND ENGINE ERROR — Per-Email Failure",
                        body=f"send_queue_v2 failed for id={send_id} to {to_email} via {inbox.get('email_address')}: {err_s}",
                        severity="error",
                        context={
                            "flow": "send_queue_v2",
                            "env": os.getenv("KLIX_ALERT_ENV_TAG", "prod"),
                            "send_id": send_id,
                            "inbox_id": str(inbox_id),
                            "domain": domain,
                        },
                    )
                except Exception:
                    pass

        if sent_this_pass <= 0:
            break

        progress_cycles += 1

    logger.info(
        "send_queue_v2: summary sent=%s (batch_size=%s) passes=%s progress_cycles=%s skips: inactive=%s cap=%s cooldown=%s domain_cooldown=%s no_job=%s",
        sent,
        batch_size,
        passes,
        progress_cycles,
        skip_inactive,
        skip_inbox_cap,
        skip_inbox_cooldown,
        skip_domain_cooldown,
        skip_no_job,
    )
    return sent


if __name__ == "__main__":
    # Allow: python -m flows.send_queue_v2
    try:
        sent = send_queue_v2_flow()
        print(f"CLI send_queue_v2 done, sent={sent}")
    except NameError:
        # Fallback in case the flow name changes in future refactors
        from flows.send_queue_v2 import send_queue_v2_flow as _f

        sent = _f()
        print(f"CLI send_queue_v2 done, sent={sent}")
