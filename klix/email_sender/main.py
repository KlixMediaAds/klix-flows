import os

def send_email(to, subject, body):
    prov = os.getenv("EMAIL_PROVIDER", "smtp").lower()
    if prov == "smtp":
        from .smtp_gmail import send as smtp_send
        return smtp_send(to, subject, body)
    elif prov == "postmark":
        from .postmark import send as pm_send
        return pm_send(to, subject, body)
    else:
        raise RuntimeError(f"Unknown provider: {prov}")
