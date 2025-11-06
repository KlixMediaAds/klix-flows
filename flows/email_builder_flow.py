from __future__ import annotations
from typing import List, Dict, Any
import os

from prefect import flow, get_run_logger
from sqlalchemy import create_engine, text

from klix.email_builder.main import build_email_for_lead


def _engine():
    url = os.environ["DATABASE_URL"]
    return create_engine(url, pool_pre_ping=True)


def _existing_columns(conn, table: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            select column_name
              from information_schema.columns
             where table_name = :t
               and table_schema = current_schema()
            """
        ),
        {"t": table},
    ).all()
    return {r[0] for r in rows}


def _fetch_candidates(limit: int) -> list[dict]:
    e = _engine()
    with e.begin() as c:
        rows = list(
            c.execute(
                text(
                    """
                    select id, email, company, website, first_name, last_name, status, discovered_at
                      from leads l
                     where coalesce(email,'') <> ''
                       and not exists (select 1 from email_sends s where s.lead_id = l.id)
                     order by discovered_at desc nulls last, id desc
                     limit :lim
                    """
                ),
                {"lim": max(5 * limit, 50)},
            ).mappings()
        )
    return [dict(r) for r in rows]


@flow(name="email_builder")
def email_builder(limit: int = 60, friendlies_domains: List[str] | None = None):
    """
    Queue ALL eligible leads. Send type:
      - 'friendly' if domain in friendlies_domains, else 'cold'
    Inserts status='queued' and writes into body_text/body_html if present; falls back to body.
    Also stores prompt_profile_id when available.
    """
    logger = get_run_logger()
    friendlies_domains = friendlies_domains or ["klixads.org", "gmail.com"]
    friendly_set = {d.lower() for d in friendlies_domains}

    cands = _fetch_candidates(limit)
    logger.info(f"Fetched {len(cands)} candidate leads")

    queued = 0
    eng = _engine()
    with eng.begin() as conn:
        cols = _existing_columns(conn, "email_sends")
        use_text = "body_text" in cols
        use_html = "body_html" in cols
        use_body = "body" in cols and not (use_text or use_html)
        has_prompt_profile_id = "prompt_profile_id" in cols

        for lead in cands:
            email = (lead.get("email") or "").strip()
            dom = email.split("@", 1)[1].lower() if "@" in email else ""
            try:
                subj, body_text, body_html, st_guess, prompt_profile_id = build_email_for_lead(lead)
            except Exception as ex:
                logger.warning(f"Builder failed for lead {lead.get('id')}: {ex}")
                continue

            send_type = st_guess or ("friendly" if dom in friendly_set else "cold")
            fields = ["lead_id", "send_type", "status", "subject"]
            params: Dict[str, Any] = {
                "lead_id": lead["id"],
                "send_type": send_type,
                "status": "queued",
                "subject": subj or "",
            }

            # Put bodies in the right columns
            if use_text:
                fields.append("body_text")
                params["body_text"] = body_text or ""
            elif use_body:
                fields.append("body")
                params["body"] = body_text or ""

            if use_html and (body_html or ""):
                fields.append("body_html")
                params["body_html"] = body_html or ""

            # NEW: store prompt_profile_id when provided
            if has_prompt_profile_id and prompt_profile_id:
                fields.append("prompt_profile_id")
                params["prompt_profile_id"] = prompt_profile_id

            placeholders = ",".join(f":{k}" for k in fields)
            sql = f"""
              insert into email_sends ({','.join(fields)}) values ({placeholders})
              on conflict (lead_id, send_type) do nothing
            """
            conn.execute(text(sql), params)

            # confirm the row exists (idempotent)
            ok = conn.execute(
                text(
                    """
                    select 1 from email_sends
                     where lead_id = :lid and send_type = :stype
                    """
                ),
                {"lid": lead["id"], "stype": send_type},
            ).fetchone()
            if ok:
                queued += 1
                if queued >= limit:
                    break

    logger.info(f"Queued {queued} emails.")
    return queued
