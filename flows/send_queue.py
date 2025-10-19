from __future__ import annotations
import os, time, random
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from prefect import flow, get_run_logger
from sqlalchemy import create_engine, text

# ---- DB engine (no external imports) ----
def get_engine():
    url = os.environ["DATABASE_URL"]
    return create_engine(url, pool_pre_ping=True)

# ---- Tunables ----
DEFAULT_TARGET_PER_HOUR   = 40
DEFAULT_JITTER_EXTRA_S    = 20
DEFAULT_TZ                = "America/Toronto"
DEFAULT_BUSINESS_HOURS    = "09:00-17:00"
DEFAULT_BATCH_SIZE        = 25
DEFAULT_FRIENDLIES_FIRST  = True
DEFAULT_DRY_RUN           = False

def _parse_hours(span: str):
    a,b = span.split("-"); h1,m1 = map(int,a.split(":")); h2,m2 = map(int,b.split(":"))
    return dtime(h1,m1), dtime(h2,m2)

def _in_window(now: datetime, start: dtime, end: dtime) -> bool:
    t = now.time()
    return (t>=start and t<=end) if start<=end else (t>=start or t<=end)

def _table_columns(conn, table: str) -> set[str]:
    rows = conn.execute(text("""
        select column_name
          from information_schema.columns
         where table_name = :t
           and table_schema = current_schema()
    """), {"t": table}).all()
    return {r[0] for r in rows}

def _fetch_queued(conn, n: int, friendlies_first: bool):
    cols = _table_columns(conn, "email_sends")
    has_html = "body_html" in cols
    has_text = "body_text" in cols

    # Project columns that exist; synthesize blanks for missing ones so downstream code is simple.
    select_html = "body_html" if has_html else "'' as body_html"
    select_text = "body_text" if has_text else "'' as body_text"
    order = "case when send_type='friendly' then 0 else 1 end, id" if friendlies_first else "id"

    sql = f"""
        select id, lead_id, send_type, subject,
               {select_html},
               {select_text}
          from email_sends
         where status='queued'
      order by {order}
         limit :n
    """
    rows = list(conn.execute(text(sql), {"n": n}).mappings())
    return rows

def _get_lead(conn, lead_id: int) -> dict | None:
    r = conn.execute(text("""
        select id, email, company, website, first_name, last_name
          from leads
         where id=:id
    """), {"id": lead_id}).mappings().first()
    return dict(r) if r else None

def _mark_sent(conn, send_id: int, provider_message_id: str):
    conn.execute(text("""
        update email_sends
           set status='sent',
               provider_message_id=:pmid,
               sent_at=now()
         where id=:id
    """), {"pmid": provider_message_id, "id": send_id})

def _mark_failed(conn, send_id: int, reason: str):
    conn.execute(text("""
        update email_sends
           set status='failed',
               failure_reason=:r
         where id=:id
    """), {"r": (reason or "")[:500], "id": send_id})

def _send_via_postmark(api_token: str, frm: str, to: str, subject: str, html: str | None, text_body: str | None) -> str:
    import requests
    payload = {"From": frm, "To": to, "Subject": subject, "MessageStream": "outbound"}
    if html:      payload["HtmlBody"] = html
    if text_body: payload["TextBody"] = text_body
    if "HtmlBody" not in payload and "TextBody" not in payload:
        payload["TextBody"] = "(missing body)"
    r = requests.post(
        "https://api.postmarkapp.com/email",
        json=payload,
        headers={"X-Postmark-Server-Token": api_token, "Accept": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("MessageID", "")

def _fallback_build(lead: dict, subject: str) -> tuple[str, str | None, str]:
    # Very lightweight, human-ish default if DB lacks bodies.
    name = (lead.get("first_name") or lead.get("company") or "there")
    site = (lead.get("website") or "").strip()
    subj = subject or f"{lead.get('company') or 'Your site'}: quick idea to boost inquiries"
    text = (
        f"Hey {name},\n\n"
        f"I took a quick look at {site or 'your site'} and have a small test worth trying. "
        f"If it flops, I’ll jot what I learned; if it works, you’ll see more inquiries.\n\n"
        f"– Josh @ Klix"
    )
    return subj, None, text  # (subject, html, text)

@flow(name="send_queue")
def send_queue(batch_size: int = DEFAULT_BATCH_SIZE,
               target_per_hour: int = DEFAULT_TARGET_PER_HOUR,
               jitter_extra_s: int = DEFAULT_JITTER_EXTRA_S,
               timezone: str = DEFAULT_TZ,
               business_hours: str = DEFAULT_BUSINESS_HOURS,
               friendlies_first: bool = DEFAULT_FRIENDLIES_FIRST,
               dry_run: bool = DEFAULT_DRY_RUN):
    """
    Drain up to batch_size queued emails with pacing, jitter, friendlies-first, and business-hours gating.
    Schema-aware: works whether or not email_sends has body_html/body_text columns.
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
    sent = 0

    eng = get_engine()
    with eng.begin() as conn:
        rows = _fetch_queued(conn, batch_size, friendlies_first)
        if not rows:
            log.info("No queued emails.")
            return 0

        for i, row in enumerate(rows, 1):
            lead = _get_lead(conn, row["lead_id"]) or {}
            to_email = lead.get("email")
            if not to_email:
                _mark_failed(conn, row["id"], "lead email missing")
                log.warning(f"#{row['id']} lead {row['lead_id']} has no email; marked failed.")
                continue

            subj = (row.get("subject") or "").strip()
            html = (row.get("body_html") or "").strip() or None
            text_body = (row.get("body_text") or "").strip() or None

            # If both bodies are empty, build a fallback on the fly
            if not html and not text_body:
                subj, html, text_body = _fallback_build(lead, subj)

            if dry_run:
                log.info(f"[DRY-RUN] Would send #{row['id']} to {to_email} subj={subj!r}")
                # Do NOT mark as sent/failed in dry-run.
            else:
                try:
                    pmid = _send_via_postmark(pm_api, from_email, to_email, subj, html, text_body)
                    _mark_sent(conn, row["id"], pmid)
                    sent += 1
                    log.info(f"Sent #{row['id']} to {to_email} pmid={pmid}")
                except Exception as e:
                    _mark_failed(conn, row["id"], str(e))
                    log.error(f"Failed #{row['id']} to {to_email}: {e}")

            if not dry_run and i < len(rows):
                time.sleep(base_gap + random.uniform(0, jitter_extra_s))

    log.info(f"Done. Sent {sent}/{len(rows)}.")
    return sent

if __name__ == "__main__":
    send_queue()
