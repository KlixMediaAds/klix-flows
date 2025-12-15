from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from sqlalchemy import text
from prefect import flow, get_run_logger

from klix.db import get_engine

# Extract “wasn't delivered to X@Y.com …”
BOUNCE_TO_RE = re.compile(r"(?:delivered to|wasn[’']t delivered to)\s+([^\s]+@[^\s]+)", re.I)

def _extract_bounced_to(snippet: str) -> str | None:
    if not snippet:
        return None
    m = BOUNCE_TO_RE.search(snippet)
    if not m:
        return None
    return m.group(1).strip().lower()

@flow(name="bounce-sweeper")
def bounce_sweeper_flow(limit: int = 500) -> int:
    """
    Reads Gmail DSN/bounce messages from email_events and writes:
      - email_denylist (reason=bounce)
      - leads.email_verification_status='invalid' (source=bounce-gmail)
      - fails queued email_sends to denylisted recipients (safety)

    Cursor is stored in bounce_sweeper_state.last_email_event_id.
    """
    logger = get_run_logger()
    eng = get_engine()

    with eng.begin() as c:
        last_id = c.execute(text("select last_email_event_id from bounce_sweeper_state where id=1")).scalar() or 0

    # Pull new DSN-ish emails after cursor
    q = """
    select id, created_at, account, from_address, subject, snippet
    from email_events
    where id > :last_id
      and (
        lower(coalesce(from_address,'')) like '%mailer-daemon%'
        or lower(coalesce(from_address,'')) like '%postmaster%'
        or lower(coalesce(subject,'')) like '%delivery status notification%'
        or lower(coalesce(subject,'')) like '%undeliver%'
        or lower(coalesce(subject,'')) like '%delivery failure%'
        or lower(coalesce(subject,'')) like '%returned mail%'
      )
    order by id asc
    limit :limit;
    """

    with eng.begin() as c:
        rows = c.execute(text(q), {"last_id": last_id, "limit": limit}).mappings().all()

    if not rows:
        logger.info("bounce_sweeper: no new DSN events after cursor=%s", last_id)
        return 0

    bounced: List[Tuple[int, str]] = []
    max_seen = last_id

    for r in rows:
        max_seen = max(max_seen, int(r["id"]))
        bounced_to = _extract_bounced_to(r.get("snippet") or "")
        if bounced_to:
            bounced.append((int(r["id"]), bounced_to))

    logger.info("bounce_sweeper: scanned=%s, extracted_bounces=%s", len(rows), len(bounced))

    if bounced:
        # Upsert denylist
        upsert = """
        insert into email_denylist (email, reason, source, last_seen_at)
        values (:email, 'bounce', 'email_events', now())
        on conflict (email) do update
          set last_seen_at = excluded.last_seen_at;
        """
        # Mark leads invalid if match
        mark_leads = """
        update leads
        set
          email_verification_status = 'invalid',
          email_verification_source = 'bounce-gmail',
          email_verification_score = 0,
          email_verification_checked_at = now()
        where lower(email) = :email
          and coalesce(email_verification_status,'') <> 'invalid';
        """
        # Fail queued sends to that email (defensive)
        fail_queued = """
        update email_sends
        set
          status = 'failed',
          error = coalesce(nullif(error,''),'') || ' | auto-failed: bounced recipient (gmail dsn)'
        where status = 'queued'
          and lower(to_email) = :email;
        """

        with eng.begin() as c:
            for _, em in bounced:
                c.execute(text(upsert), {"email": em})
                c.execute(text(mark_leads), {"email": em})
                c.execute(text(fail_queued), {"email": em})

    # Advance cursor even if we couldn’t parse all snippets (so we don’t loop forever)
    with eng.begin() as c:
        c.execute(
            text("update bounce_sweeper_state set last_email_event_id=:v, updated_at=now() where id=1"),
            {"v": max_seen},
        )

    logger.info("bounce_sweeper: advanced cursor -> %s", max_seen)
    return len(bounced)

if __name__ == "__main__":
    bounce_sweeper_flow()
