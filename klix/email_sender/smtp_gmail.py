import os, smtplib, ssl, uuid
from email.message import EmailMessage

def _gen_message_id(from_addr: str) -> str:
    domain = (from_addr or "klix.local").split("@")[-1]
    return f"<klix-{uuid.uuid4()}@{domain}>"

def send(to: str, subject: str, body: str) -> str:
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]          # full Gmail/Workspace address
    pwd  = os.environ["SMTP_PASS"]          # Gmail App Password (NOT your login)
    from_addr = os.getenv("EMAIL_FROM", user)
    reply_to  = os.getenv("REPLY_TO", "")

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to
    msg["Subject"] = subject
    msg["Message-ID"] = _gen_message_id(from_addr)
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=30) as s:
        s.ehlo()
        s.starttls(context=ctx)
        s.ehlo()
        s.login(user, pwd)
        s.send_message(msg)

    return msg["Message-ID"]
