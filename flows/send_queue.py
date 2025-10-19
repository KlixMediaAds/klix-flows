from __future__ import annotations
import os, time, random
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine, text

# --- Engine accessor: prefer flows.db.get_engine if present, else fall back ---
try:
    from flows.db import get_engine as _get_engine  # optional, if you have it
    def get_engine():
        return _get_engine()
except Exception:
    def get_engine():
        url = os.environ["DATABASE_URL"]
        return create_engine(url)

DEFAULT_TARGET_PER_HOUR = 40
DEFAULT_JITTER_EXTRA_S  = 20
DEFAULT_TZ              = "America/Toronto"
DEFAULT_BUSINESS_HOURS  = "09:00-17:00"
DEFAULT_BATCH_SIZE      = 25
DEFAULT_FRIENDLIES_FIRST= True
DEFAULT_DRY_RUN         = False

def _parse_hours(span: str):
    a,b = span.split("-"); h1,m1 = map(int,a.split(":")); h2,m2 = map(int,b.split(":"))
    return dtime(h1,m1), dtime(h2,m2)

def _in_window(now: datetime, start: dtime, end: dtime) -> bool:
    t = now.time()
    return (t>=start and t<=end) if start<=end else (t>=start or t<=end)

@task
def fetch_queued(e, n, friendlies_first):
    order = "case when send_type='friendly' then 0 else 1 end, id" if friendlies_first else "id"
    sql = f"""select id, lead_id, send_type, subject,
                     coalesce(body_html,'') body_html,
                     coalesce(body_text,'') body_text
                from email_sends
               where status='queued'
            order by {order}
               limit :n"""
    return list(e.execute(text(sql), {"n": n}).mappings())

@task
def get_lead_email(e, lead_id: int) -> str|None:
    return e.execute(text("select email from leads where id=:id"), {"id": lead_id}).scalar()

@task
def mark_sent(e, send_id: int, provider_message_id: str):
    e.execute(text("""update email_sends
                         set status='sent', provider_message_id=:pmid, sent_at=now()
                       where id=:id"""), {"pmid": provider_message_id, "id": send_id})

@task
def mark_failed(e, send_id: int, reason: str):
    e.execute(text("""update email_sends
                         set status='failed', failure_reason=:r
                       where id=:id"""), {"r": reason[:500], "id": send_id})

def _send_via_postmark(api_token: str, frm: str, to: str, subject: str, html: str, text: str) -> str:
    import requests
    payload = {"From": frm, "To": to, "Subject": subject, "MessageStream": "outbound"}
    if html: payload["HtmlBody"] = html
    if text: payload["TextBody"] = text
    r = requests.post("https://api.postmarkapp.com/email",
                      json=payload,
                      headers={"X-Postmark-Server-Token": api_token, "Accept": "application/json"})
    r.raise_for_status()
    return r.json().get("MessageID","")

@task
def dispatch_one(row, lead_email: str, from_email: str, pm_api: str, dry_run: bool):
    if dry_run:
        return True, "dry-run"
    try:
        pmid = _send_via_postmark(pm_api, from_email, lead_email, row["subject"], row["body_html"], row["body_text"])
        return True, pmid
    except Exception as e:
        return False, str(e)

@flow(name="send_queue")  # valid name (no slash)
def send_queue(batch_size: int = DEFAULT_BATCH_SIZE,
               target_per_hour: int = DEFAULT_TARGET_PER_HOUR,
               jitter_extra_s: int = DEFAULT_JITTER_EXTRA_S,
               timezone: str = DEFAULT_TZ,
               business_hours: str = DEFAULT_BUSINESS_HOURS,
               friendlies_first: bool = DEFAULT_FRIENDLIES_FIRST,
               dry_run: bool = DEFAULT_DRY_RUN):
    """
    Drain up to batch_size queued emails with pacing, jitter, friendlies-first, and business-hours gating.
    """
    log = get_run_logger()
    tz = ZoneInfo(timezone)
    start_t, end_t = _parse_hours(business_hours)
    now = datetime.now(tz)
    if not _in_window(now, start_t, end_t) and not dry_run:
        log.info(f"Outside business hours {business_hours} {timezone}; exiting.")
        return 0

    from_email = os.environ.get("SENDER_FROM", "Klix <hello@klixads.org>")
    pm_api = os.environ.get("POSTMARK_API_TOKEN")
    if not pm_api and not dry_run:
        raise RuntimeError("POSTMARK_API_TOKEN not set")

    base_gap = max(1.0, 3600.0 / max(1, target_per_hour))
    e = get_engine().begin()
    rows = fetch_queued(e, batch_size, friendlies_first)
    if not rows:
        log.info("No queued emails.")
        return 0

    sent = 0
    for i, row in enumerate(rows, 1):
        to = get_lead_email(e, row["lead_id"])
        if not to:
            mark_failed(e, row["id"], "lead email missing")
            continue

        ok, info = dispatch_one(row, to, from_email, pm_api, dry_run)
        if ok:
            mark_sent(e, row["id"], info); sent += 1
        else:
            mark_failed(e, row["id"], info)

        if not dry_run and i < len(rows):
            time.sleep(base_gap + random.uniform(0, jitter_extra_s))

    log.info(f"Done. Sent {sent}/{len(rows)}.")
    return sent

if __name__ == "__main__":
    send_queue()
