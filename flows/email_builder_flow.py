from __future__ import annotations
import os
from typing import Optional, Tuple
from prefect import flow, get_run_logger
from sqlalchemy import create_engine, text

def get_engine():
    url = os.environ["DATABASE_URL"]
    return create_engine(url, pool_pre_ping=True)

def _table_columns(conn, table: str) -> set[str]:
    rows = conn.execute(text("""
        select column_name
          from information_schema.columns
         where table_name = :t
           and table_schema = current_schema()
    """), {"t": table}).all()
    return {r[0] for r in rows}

def _leads_to_build(conn, limit: int):
    # Leads with an email and no existing email_sends row (queued or sent)
    return list(conn.execute(text("""
        select l.id, l.email, l.company, l.website, l.first_name, l.last_name
          from leads l
         where l.email is not null
           and not exists (
                 select 1 from email_sends s
                  where s.lead_id = l.id
           )
      order by l.id asc
         limit :n
    """), {"n": limit}).mappings())

def _insert_send(conn, cols: set[str], lead: dict,
                 subject: str, html: Optional[str], text_body: Optional[str],
                 send_type: str):
    # Build an INSERT that only references columns that exist
    fields = ["lead_id", "send_type", "status", "subject"]
    values = { "lead_id": lead["id"], "send_type": send_type, "status": "queued", "subject": subject }

    if "body_html" in cols:
        fields.append("body_html"); values["body_html"] = html or ""
    if "body_text" in cols:
        fields.append("body_text"); values["body_text"] = text_body or ""

    cols_sql = ", ".join(fields)
    params_sql = ", ".join([f":{k}" for k in fields])
    conn.execute(text(f"insert into email_sends ({cols_sql}) values ({params_sql})"), values)

def _fallback_copy(lead: dict) -> Tuple[str, Optional[str], str, str]:
    # subject, html, text, send_type
    subj = f"{(lead.get('company') or 'Your site')}: quick idea to boost inquiries"
    name = lead.get("first_name") or (lead.get("company") or "there")
    site = (lead.get("website") or "").strip() or "your site"
    text = (
        f"Hey {name},\n\n"
        f"I took a quick look at {site} and have a small test worth trying. "
        f"If it flops, I’ll jot what I learned; if it works, you’ll see more inquiries.\n\n"
        f"– Josh @ Klix"
    )
    return subj, None, text, "cold"

@flow(name="email_builder")
def email_builder(limit: int = 25, friendlies_domains: list[str] | None = None):
    """
    Build and queue up to `limit` emails.
    - Uses klix.email_builder.main.build_email_for_lead(lead) if available,
      falling back to a simple human-ish template.
    - Writes only the columns that exist in email_sends (schema-aware).
    - Marks new rows as status='queued'.
    """
    log = get_run_logger()
    friendlies_domains = [d.lower() for d in (friendlies_domains or [])]

    # Try to import the user's real builder
    builder_fn = None
    try:
        from klix.email_builder.main import build_email_for_lead  # type: ignore
        builder_fn = build_email_for_lead
        log.info("Using klix.email_builder.main.build_email_for_lead")
    except Exception as e:
        log.warning(f"Real builder not found; using fallback. ({type(e).__name__}: {e})")

    eng = get_engine()
    built = 0
    with eng.begin() as conn:
        cols = _table_columns(conn, "email_sends")
        leads = _leads_to_build(conn, limit)
        if not leads:
            log.info("No leads to build.")
            return 0

        for lead in leads:
            try:
                if builder_fn:
                    subject, html, text_body, send_type = builder_fn(lead)  # expected signature
                else:
                    subject, html, text_body, send_type = _fallback_copy(lead)

                # Friendlies-first override by domain match
                dom = (lead["email"].split("@", 1)[-1] or "").lower()
                if dom in friendlies_domains:
                    send_type = "friendly"

                _insert_send(conn, cols, lead, subject, html, text_body, send_type)
                built += 1
            except Exception as e:
                log.error(f"Failed to build for lead {lead['id']}: {e}")

    log.info(f"Queued {built} emails.")
    return built

if __name__ == "__main__":
    email_builder()
