import os, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS") or os.getenv("SMTP_APP")  # support either
FROM_ADDR = os.getenv("SMTP_FROM", SMTP_USER or "no-reply@klixads.org")

def _smtp():
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    s.ehlo()
    try:
        s.starttls()
        s.ehlo()
    except smtplib.SMTPException:
        pass
    if SMTP_USER and SMTP_PASS:
        s.login(SMTP_USER, SMTP_PASS)
    return s

def send_email(to_email: str, subject: str, body_text: str, body_html: str | None = None) -> str | None:
    """
    Send exactly the subject/body provided (no templates).
    Returns a provider message id if available (None for raw SMTP).
    """
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")

    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = FROM_ADDR

    with _smtp() as s:
        s.sendmail(FROM_ADDR, [to_email], msg.as_string())
    # raw SMTP doesn't give us a message id; Prefect caller handles None
    return None
