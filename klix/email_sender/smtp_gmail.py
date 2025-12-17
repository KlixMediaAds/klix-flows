import os
import smtplib
from email.message import EmailMessage
from email.utils import make_msgid

# Basic SMTP config from env
HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
PORT = int(os.getenv("SMTP_PORT", "587"))
USER = os.environ.get("SMTP_USER")
PASS = os.environ.get("SMTP_PASS")

# From address:
# 1) EMAIL_FROM (if set)
# 2) SMTP_FROM (from your secret.env)
# 3) fallback to USER
FROM = (
    os.environ.get("EMAIL_FROM")
    or os.environ.get("SMTP_FROM")
    or USER
)

if not HOST or not PORT or not USER or not PASS or not FROM:
    missing = []
    if not HOST: missing.append("SMTP_HOST")
    if not PORT: missing.append("SMTP_PORT")
    if not USER: missing.append("SMTP_USER")
    if not PASS: missing.append("SMTP_PASS")
    if not FROM: missing.append("EMAIL_FROM/SMTP_FROM/SMTP_USER")
    raise RuntimeError(f"SMTP config missing: {', '.join(missing)}")

def send(to, subject, body):
    """
    Send a single email via Gmail SMTP using env config.

    Env vars used:
      - SMTP_HOST
      - SMTP_PORT
      - SMTP_USER
      - SMTP_PASS
      - EMAIL_FROM (optional)
      - SMTP_FROM  (optional)
    """
    msg = EmailMessage()
    msg["From"] = FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg["Message-ID"] = make_msgid(domain=USER.split("@")[-1])
    msg.set_content(body)

    with smtplib.SMTP(HOST, PORT, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(USER, PASS)
        s.send_message(msg)

    return msg["Message-ID"]
