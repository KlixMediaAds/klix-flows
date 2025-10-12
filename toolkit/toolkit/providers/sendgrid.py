# toolkit/providers/sendgrid.py
from __future__ import annotations
import os, time
import requests

class SendError(Exception): ...

def send_email_sendgrid(to: str, subject: str, body: str) -> str:
    api_key = os.getenv("SENDGRID_API_KEY")
    from_email = os.getenv("FROM_EMAIL")
    from_name = os.getenv("FROM_NAME", "")
    if not (api_key and from_email):
        raise SendError("SendGrid env not set")

    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": from_email, **({"name": from_name} if from_name else {})},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # simple retry/backoff
    delay = 1.0
    for attempt in range(4):
        r = requests.post("https://api.sendgrid.com/v3/mail/send", json=payload, headers=headers, timeout=15)
        if 200 <= r.status_code < 300:
            # SendGrid doesn’t return an ID; use response headers as our message id
            return r.headers.get("X-Message-Id") or f"sendgrid-{int(time.time())}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay); delay *= 2
            continue
        raise SendError(f"HTTP {r.status_code}: {r.text[:200]}")
    raise SendError("SendGrid: exhausted retries")
