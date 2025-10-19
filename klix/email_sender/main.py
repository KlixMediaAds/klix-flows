import os
from .smtp_gmail import send as smtp_send
try:
    from .postmark import send as pm_send
except Exception:
    pm_send = None

def send_email(to, subject, body):
    prov = os.getenv("EMAIL_PROVIDER", "smtp").lower()
    if prov in ("smtp", "gmail"):
        return smtp_send(to, subject, body)
    if prov == "postmark" and pm_send:
        return pm_send(to, subject, body)
    raise RuntimeError(f"Unknown or unavailable provider: {prov}")
