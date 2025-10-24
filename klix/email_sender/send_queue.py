# flows/send_queue.py
from __future__ import annotations

import os, time, re, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
from datetime import datetime, time as dtime
from sqlalchemy import create_engine, text
from prefect import flow, get_run_logger

ENG = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)

# SMTP config (env-driven)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER") or os.getenv("SMTP_USERNAME") \
            or os.getenv("SMTP_USER_ALEX") or os.getenv("SMTP_USER_MAIN")
SMTP_PASS = os.getenv("SMTP_PASS") or os.getenv("SMTP_APP") \
            or os.getenv("GMAIL_APP_PASSWORD_ALEX") or os.getenv("SMTP_PASSWORD")
FROM_ADDR = os.getenv("SMTP_FROM") or SMTP_USER or "noreply@klix.local"

DAILY_CAP = int(os.getenv("SEND_DAILY_CAP", "50"))

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _within_window() -> bool:
    # default wide-open if not set; you can tighten in env
    win = os.getenv("SEND_WINDOW", "00:00-23:59")
    s, e = [tuple(map(int, x.split(":"))) for x in win.split("-")]
    now = datetime.now().time()
    return dtime(*s) <= now <= dtime(*e)

def _clean_email(addr: str | None) -> str:
    a = (addr or "").strip()
    if not a:
        return ""
    if a.lower().startswith("mailto:"):
        a = a[7:]
    a = a.split("?", 1)[0]
    return a.strip().lower()

def _smtp():
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    s.ehlo()
    try:
        s.starttls()
        s.ehlo()
    except smtplib.SMTPException:
        # some servers might already be TLS; ignore
        pass
    if (SMTP_USER or "") and (SMTP_PASS or ""):
        s.login(str(SMTP_USER).strip(), str(SMTP_PASS).strip())
    return s

def _send_raw(to_email: str, subject: str, body_text: str, body_html: str | None = None) -> str:
    # Always work with strings; no .strip() on None
    subj = (subject or "").strip()
    from_addr = (FROM_ADDR or "").strip() or "noreply@klix.local"
    to_addr = (to_email or "").strip()

    if not EMAIL_RE.match(to_addr):
        raise ValueError(f"Invalid recipient address: {to_addr!r}")

    # Text/HTML MIME
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html or "", "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")

    msg["Subject"] = Header(subj, "utf-8")
    # If you want a friendly display name, set SMTP_FROM_NAME env
    from_name = (os.getenv("SMTP_FROM_NAME") or "").strip()
    msg["From"] = formataddr((from_name, from_addr)) if from_name else from_addr
    msg["To"] = to_addr

    with _smtp() as s:
        s.sendmail(from_addr, [to_addr], msg.as_string())

    # If you wire a real provider later, return their message-id here
    return "smtp-ok"

@flow(name="send-queue")
def send_queue(batch_size: int = 25, allow_weekend: bool = True, **kwargs) -> int:
    """
    Pull 'queued' rows from email_sends, send via SMTP, and mark sent/failed.
    Live only if SEND_LIVE=1 AND not dry_run param.
    """
    logger = get_run_logger()

    live_env = (os.getenv("SEND_LIVE", "0") == "1")
    dry_run = bool(kwargs.get("dry_run", False))
    live = (live_env and not dry_run)

    # Weekend / window guards
    if not allow_weekend and datetime.utcnow().weekday() >= 5:
        logger.info("Outside weekday window; exiting.")
        return 0
    if not _within_window():
        logger.info("Outside time window; exiting.")
        return 0

    # Pull rows
    with ENG.begin() as cx:
        rows = cx.execute(text("""
            select s.id as send_id, s.lead_id, s.subject, s.body, s.body_html, l.email
              from email_sends s
              join leads l on l.id = s.lead_id
             where s.status = 'queued'
             order by s.id asc
             limit :lim
        """), {"lim": batch_size}).mappings().all()

    if not rows:
        logger.info("No queued rows.")
        return 0

    sent_count = 0

    for r in rows:
        if sent_count >= DAILY_CAP:
            break

        to_raw  = _clean_email(r.get("email"))
        subj    = (r.get("subject") or "")
        body    = (r.get("body") or "")
        body_ht = r.get("body_html")  # may be None

        preview = (body or body_ht or "")[:160].replace("\n", " ")
        logger.info("MAIL_PREVIEW id=%s to=%s subj=%r body=%r",
                    r["send_id"], to_raw, (subj or "")[:72], preview)

        try:
            if not live:
                mid = "dry-run"
            else:
                mid = _send_raw(to_raw, subj, body, body_ht)

            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='sent',
                           provider_message_id=:mid,
                           sent_at=now()
                     where id=:id
                """), {"mid": mid, "id": r["send_id"]})

            sent_count += 1
            time.sleep(1.0)  # small throttle
            logger.info("Sent #%s to %s", r["send_id"], to_raw)

        except Exception as e:
            err = (str(e) or "")[:300]
            with ENG.begin() as cx:
                cx.execute(text("""
                    update email_sends
                       set status='failed',
                           error=:err
                     where id=:id
                """), {"err": err, "id": r["send_id"]})
            logger.error("FAILED #%s to %s -> %s", r["send_id"], to_raw, err)

    return sent_count
