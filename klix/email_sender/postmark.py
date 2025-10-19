import os, requests
PM_URL = "https://api.postmarkapp.com/email"
TOKEN  = os.environ["POSTMARK_TOKEN"]
FROM   = os.environ["EMAIL_FROM"]

def send(to, subject, body):
    r = requests.post(
        PM_URL,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Postmark-Server-Token": TOKEN,
        },
        json={"From": FROM, "To": to, "Subject": subject, "TextBody": body},
        timeout=20,
    )
    r.raise_for_status()
    return r.json().get("MessageID","")
