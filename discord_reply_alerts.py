#!/usr/bin/env python
"""
Discord reply/attention alerts based on live email_events.

Logic:
- Ensure a tracking table email_event_alerts exists.
- Select unalerted, attention-worthy events from email_events.
- Post each as a Discord message.
- Record alerted events in email_event_alerts to avoid duplicates.
"""

import os
import sys
import textwrap
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests


def env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"ERROR: environment variable {name} is not set.", file=sys.stderr)
        sys.exit(1)
    return value


def format_discord_message(row: dict) -> str:
    """
    Build a compact but high-signal Discord message for a new email event.
    """
    eid = row["id"]
    account = row["account"]
    received_at = row["received_at"] or row["created_at"]
    if isinstance(received_at, datetime):
        received_str = received_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        received_str = str(received_at)

    from_addr = row["from_address"] or "(unknown sender)"
    to_addr = row["to_address"] or "(unknown recipient)"
    subject = (row["subject"] or "(no subject)").strip()
    if len(subject) > 180:
        subject = subject[:177] + "..."

    signal_type = row["signal_type"] or "-"
    importance = row["importance_score"]
    requires_host = row["requires_host_attention"]
    is_unread = row["is_unread_at_poll"]

    snippet = (row["snippet"] or "").strip().replace("\r", " ")
    snippet = " ".join(snippet.split())  # collapse whitespace
    if not snippet:
        snippet = "(empty snippet)"
    if len(snippet) > 400:
        snippet = snippet[:397] + "..."

    lines = [
        f"📬 **New Email Event (id {eid})**",
        f"Time: {received_str}",
        f"Account: `{account}`",
        f"From: `{from_addr}`",
        f"To: `{to_addr}`",
        f"Subject: {subject}",
        f"Signal: `{signal_type}` | Importance: `{importance}`",
        f"Host Attention: `{requires_host}` | Unread at poll: `{is_unread}`",
        "",
        "Body preview:",
        "```",
        snippet,
        "```",
    ]

    return "\n".join(lines)


def ensure_alerts_table(conn) -> None:
    """
    Create the tracking table if it does not exist.

    We keep this separate so we don't have to modify existing schemas.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS email_event_alerts (
        email_event_id   BIGINT PRIMARY KEY
                         REFERENCES email_events(id) ON DELETE CASCADE,
        alerted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def fetch_new_events(conn, limit: int = 25):
    """
    Fetch unalerted, attention-worthy events from email_events.

    Strategy:
    - Left join against email_event_alerts.
    - Only select rows where we have not yet written an alert record.
    - Filter to events that actually require host attention, or are high-importance.
    """
    query = """
    SELECT
        e.id,
        e.created_at,
        e.received_at,
        e.account,
        e.from_address,
        e.to_address,
        e.subject,
        e.snippet,
        e.signal_type,
        e.importance_score,
        e.requires_host_attention,
        e.is_unread_at_poll
    FROM email_events e
    LEFT JOIN email_event_alerts a
        ON a.email_event_id = e.id
    WHERE a.email_event_id IS NULL
      AND (
            e.requires_host_attention IS TRUE
         OR (e.requires_host_attention IS NULL AND e.importance_score IS NOT NULL AND e.importance_score >= 3)
      )
    ORDER BY e.received_at NULLS FIRST, e.created_at ASC
    LIMIT %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, (limit,))
        rows = cur.fetchall()
    return rows


def send_discord_message(webhook_url: str, content: str) -> bool:
    """
    Post a message to Discord.

    - Treat HTTP 200/204 as success.
    - If we hit rate limiting (429), log it and skip that event for now.
    """
    payload = {"content": content}
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
    except Exception as exc:
        print(f"[ERROR] Discord request failed: {exc}", file=sys.stderr)
        return False

    if resp.status_code in (200, 204):
        return True

    if resp.status_code == 429:
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        print(f"[ERROR] Discord webhook returned 429 (rate limited): {data}", file=sys.stderr)
        return False

    try:
        body = resp.json()
    except Exception:
        body = {"raw_text": resp.text}

    print(f"[ERROR] Discord webhook returned {resp.status_code}: {body}", file=sys.stderr)
    return False


def mark_alerted(conn, alerted_ids) -> None:
    """
    Write alert records for successfully-posted events.

    Using a separate step so we don't mark events that failed to post.
    """
    if not alerted_ids:
        print("No events were successfully alerted; database not updated.")
        return

    insert_sql = """
    INSERT INTO email_event_alerts (email_event_id, alerted_at)
    VALUES (%s, NOW())
    ON CONFLICT (email_event_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.executemany(insert_sql, [(eid,) for eid in alerted_ids])
    conn.commit()
    print(f"Marked {len(alerted_ids)} events as alerted in email_event_alerts.")


def main() -> int:
    db_url = env_required("DATABASE_URL")
    webhook_url = env_required("DISCORD_REPLY_WEBHOOK_URL")

    conn = psycopg2.connect(db_url)

    try:
        ensure_alerts_table(conn)

        rows = fetch_new_events(conn, limit=25)
        if not rows:
            print("No new attention-worthy email events to alert.")
            return 0

        print(f"Found {len(rows)} new attention-worthy email events to alert.")
        alerted_ids = []

        for row in rows:
            msg = format_discord_message(row)
            eid = row["id"]
            print(f"Sending Discord alert for email_event id={eid} … ", end="", flush=True)
            ok = send_discord_message(webhook_url, msg)
            if ok:
                print("OK")
                alerted_ids.append(eid)
                time.sleep(0.5)
            else:
                print("FAILED")

        mark_alerted(conn, alerted_ids)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
