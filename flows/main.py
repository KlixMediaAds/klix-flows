from __future__ import annotations
import os, smtplib, html as _html
from email.message import EmailMessage
from email.utils import make_msgid, formatdate

def _to_html(md: str) -> str:
    if not md: 
        return ""
    safe = _html.escape(md).replace("\r\n","\n").replace("\r","\n")
    parts = []
    for chunk in safe.split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.append("<p>" + chunk.replace("\n", "<br>") + "</p>")
    return "".join(parts) or ("<p>" + safe + "</p>")

def send_email(to_addr: str, subject: str, body_text: str, body_html: str | None = None) -> str:
    """
    Send exactly the provided body (no legacy template / signature).
    """
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME") or os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD") or os.getenv("SMTP_APP")

    from_addr  = os.getenv("SMTP_FROM", username or "alex@klixads.org")
    from_name  = os.getenv("SMTP_FROM_NAME", "Klix")
    from_header = f"{from_name} <{from_addr}>" if ("<" not in from_addr and from_name) else from_addr

    msg = EmailMessage()
    msg["From"] = from_header
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=os.getenv("SMTP_MSGID_DOMAIN","klixads.org"))

    bt = body_text or ""
    bh = body_html if body_html is not None else _to_html(bt)

    msg.set_content(bt)           # text/plain
    if bh:
        msg.add_alternative(bh, subtype="html")

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.ehlo()
        try:
            s.starttls()
        except smtplib.SMTPException:
            pass
        if username and password:
            s.login(username, password)
        s.send_message(msg)

    return msg["Message-ID"].strip("<>")
