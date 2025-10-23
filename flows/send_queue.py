import os, time, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, time as dtime
from sqlalchemy import create_engine, text
from prefect import get_run_logger

ENG = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER") or os.getenv("SMTP_USERNAME") or os.getenv("SMTP_USER_ALEX") or os.getenv("SMTP_USER_MAIN")
SMTP_PASS = os.getenv("SMTP_PASS") or os.getenv("SMTP_APP") or os.getenv("GMAIL_APP_PASSWORD_ALEX") or os.getenv("SMTP_PASSWORD")
FROM_ADDR = os.getenv("SMTP_FROM") or SMTP_USER

DAILY_CAP = int(os.getenv("SEND_DAILY_CAP","50"))
LIVE = os.getenv("SEND_LIVE","1") == "1"
try:
    if 'dry_run' in kwargs:
        LIVE = not bool(kwargs.get('dry_run'))
except Exception:
    pass   # default to live since you’re clearly sending

def within_window():
    win = os.getenv("SEND_WINDOW","00:00-23:59")
    s,e = [tuple(map(int,x.split(":"))) for x in win.split("-")]
    now = datetime.now().time()
    return dtime(*s) <= now <= dtime(*e)

def _smtp():
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    s.ehlo()
    try:
        s.starttls(); s.ehlo()
    except smtplib.SMTPException:
        pass
    if SMTP_USER and SMTP_PASS:
        s.login(SMTP_USER, SMTP_PASS.replace(" ",""))
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

def send_queue(batch_size=25, allow_weekend=True):
    logger = get_run_logger()
    if not allow_weekend and datetime.utcnow().weekday()>=5:
        logger.info("Outside weekday window; exiting.")
        return 0
    if not within_window():
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
        subj = r["subject"] or ""
        san = (r.get('body') or '')[:160].replace('\n',' ')
        logger = get_run_logger()
        logger.info('MAIL_PREVIEW id=%s to=%s subj=%r body=%r', r['send_id'], r['email'], (subj or '')[:72], san)
        body = r["body"] or ""
        to   = r["email"]

        san = (body or '')[:160]
        san = san.replace('\n',' ')
        try:
            if LIVE:
                _send_raw(to, subj, body)
                mid = "smtp"
            else:
                mid = "dry-run"
            with ENG.begin() as cx:
                cx.execute(text("""
                  update email_sends
                     set status='sent', provider_message_id=:mid, sent_at=now()
                   where id=:id
                """), {"mid": mid, "id": r["send_id"]})
            sent_today += 1
            time.sleep(1.2)
            logger.info(f"Sent #{r['send_id']} to {to}")
        except Exception as e:
            with ENG.begin() as cx:
                cx.execute(text("""
                  update email_sends
                     set status='failed', error=:err
                   where id=:id
                """), {"err": str(e)[:300], "id": r["send_id"]})
            logger.error(f"FAILED #{r['send_id']} to {to} → {e}")

    return sent_today
