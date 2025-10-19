import os, smtplib
from email.message import EmailMessage
from email.utils import make_msgid

HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
PORT = int(os.getenv("SMTP_PORT", "587"))
USER = os.environ["SMTP_USER"]
PASS = os.environ["SMTP_PASS"]
FROM = os.environ["EMAIL_FROM"]

def send(to, subject, body):
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
