from datetime import datetime, timezone
import os, time, random, ssl, smtplib
from email.message import EmailMessage

from sqlalchemy import select, update, text
from sqlalchemy.sql import func
from prefect import flow, get_run_logger

from flows.db import SessionLocal
from flows.schema import leads, email_sends

def _fetch_batch(s, limit:int):
    q = (
        select(
            email_sends.c.id,
            email_sends.c.lead_id,
            leads.c.email,
            email_sends.c.send_type,
            email_sends.c.subject,
            email_sends.c.body,
        )
        .select_from(email_sends.join(leads, email_sends.c.lead_id == leads.c.id))
        .where(email_sends.c.status == "queued")
        .order_by(email_sends.c.id.asc())
        .limit(limit)
    )
    return s.execute(q).fetchall()

def _sent_today(s) -> int:
    # Count rows marked sent today (UTC)
    r = s.execute(text("select count(*) from email_sends where sent_at::date = CURRENT_DATE")).scalar()
    return int(r or 0)

def _smtp_send(to_addr: str, subject: str, body: str) -> str:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pw   = os.getenv("SMTP_PASS")
    from_email = os.getenv("FROM_EMAIL", user or "")
    from_name  = os.getenv("FROM_NAME", "")

    if not host or not from_email:
        raise RuntimeError("SMTP_HOST / FROM_EMAIL not set")

    msg = EmailMessage()
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"] = to_addr
    msg["Subject"] = subject
    # basic unsubscribe header; customize later
    msg["List-Unsubscribe"] = f"<mailto:{from_email}?subject=unsubscribe>"
    msg.set_content(body)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=30) as s:
        try:
            s.starttls(context=ctx)
        except smtplib.SMTPNotSupportedError:
            pass
        if user:
            s.login(user, pw)
        # returns dict of failures; empty dict == success
        failures = s.send_message(msg)
    if failures:
        raise RuntimeError(f"SMTP send failures: {failures}")
    # We don't get a provider message-id via SMTP; return a synthetic one
    return f"smtp-{int(time.time()*1000)}"

@flow(name="send_queue")
def send_queue(batch_size: int = 25, allow_weekend: bool = False) -> int:
    log = get_run_logger()
    live = os.getenv("SEND_LIVE", "0") == "1"
    daily_cap = int(os.getenv("SEND_DAILY_CAP", "500"))

    # simple weekend guard (UTC)
    if not allow_weekend and datetime.now(timezone.utc).weekday() >= 5:
        log.info("Weekend window closed; skipping.")
        return 0

    sent = 0
    with SessionLocal() as s:
        # cap check (only matters for live)
        if live:
            already = _sent_today(s)
            if already >= daily_cap:
                log.info(f"Daily cap reached ({already}/{daily_cap}); skipping.")
                return 0

        batch = _fetch_batch(s, batch_size)
        if not batch:
            log.info("No queued emails.")
            return 0

        for row in batch:
            es_id, lead_id, email, send_type, subject, body = row

            # jitter to avoid bursts
            time.sleep(random.uniform(1.0, 3.5))

            if not live:
                log.info(f"DRY RUN — would send to {email} (email_sends.id={es_id})")
                continue

            # enforce cap mid-batch
            if _sent_today(s) >= daily_cap:
                log.info("Daily cap reached mid-batch; stopping early.")
                break

            try:
                provider_message_id = _smtp_send(email, subject, body)
                s.execute(
                    update(email_sends)
                    .where(email_sends.c.id == es_id)
                    .values(status="sent", sent_at=func.now(), provider_message_id=provider_message_id)
                )
                sent += 1
                s.commit()
            except Exception as e:
                s.execute(
                    update(email_sends)
                    .where(email_sends.c.id == es_id)
                    .values(status="failed", error=str(e))
                )
                s.commit()
                log.error(f"Failed to send to {email}: {e}")

    log.info(f"Processed batch (live={live}); sent={sent}")
    return sent
