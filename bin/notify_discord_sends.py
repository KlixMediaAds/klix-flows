#!/usr/bin/env python
import os
import textwrap
from datetime import datetime, timezone

import psycopg2
import requests


def get_db_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set in environment")
    return psycopg2.connect(db_url)


def fetch_recent_sends(conn, window_minutes: int = 5):
    """
    Return emails sent in the last `window_minutes` minutes.
    Uses from_domain + to_email (matches your schema hint).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              id,
              subject,
              body,
              from_domain,
              to_email,
              sent_at
            FROM email_sends
            WHERE sent_at >= now() - interval %s
              AND status = 'sent'
            ORDER BY sent_at DESC
            LIMIT 10;
            """,
            (f"{window_minutes} minutes",),
        )
        rows = cur.fetchall()

    results = []
    for id_, subject, body, from_domain, to_email, sent_at in rows:
        domain = from_domain or "(unknown)"
        results.append(
            {
                "id": id_,
                "subject": subject or "(no subject)",
                "body": body or "",
                "from_domain": domain,
                "to_email": to_email or "",
                "sent_at": sent_at,
            }
        )
    return results


def fetch_domain_counts_today(conn):
    """
    Per-domain send counts for today, based on from_domain.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COALESCE(from_domain, '(unknown)') AS domain,
              count(*) AS sent_today
            FROM email_sends
            WHERE status = 'sent'
              AND sent_at::date = current_date
            GROUP BY domain
            ORDER BY sent_today DESC;
            """
        )
        rows = cur.fetchall()

    return [{"domain": r[0], "sent_today": r[1]} for r in rows]


def build_discord_message(recent_sends, domain_counts, window_minutes: int = 5):
    if not recent_sends:
        return None

    now = datetime.now(timezone.utc).astimezone()
    header = f"🚀 KlixOS cold email sends (last {window_minutes} min)\n`{now.isoformat(timespec='seconds')}`"

    if domain_counts:
        domain_lines = [
            f"- **{d['domain']}** → {d['sent_today']} sent today"
            for d in domain_counts
        ]
        domains_block = "**Domains today:**\n" + "\n".join(domain_lines)
    else:
        domains_block = "_No domain stats yet today._"

    email_lines = []
    for e in recent_sends:
        body_snippet = (e["body"] or "").strip()
        if len(body_snippet) > 300:
            body_snippet = body_snippet[:297] + "..."

        email_lines.append(
            textwrap.dedent(
                f"""
                • **ID:** `{e['id']}`
                  **From domain:** `{e['from_domain']}`
                  **To:** `{e['to_email']}`
                  **Sent at:** `{e['sent_at']}`
                  **Subject:** {e['subject']}

                  ```text
                  {body_snippet}
                  ```
                """
            ).strip()
        )

    emails_block = "**Recent emails:**\n" + "\n\n".join(email_lines)

    content = f"{header}\n\n{domains_block}\n\n{emails_block}"
    if len(content) > 1900:
        content = content[:1890] + "\n\n...(truncated)..."
    return content


def post_to_discord(webhook_url: str, content: str):
    resp = requests.post(webhook_url, json={"content": content}, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"Discord webhook error: {resp.status_code} {resp.text}")


def main():
    webhook_url = os.environ.get("DISCORD_SEND_WEBHOOK_URL")
    if not webhook_url:
        print("DISCORD_SEND_WEBHOOK_URL not set; skipping Discord notify.")
        return

    conn = get_db_conn()
    try:
        recent = fetch_recent_sends(conn, window_minutes=5)
        if not recent:
            print("No recent sent emails; nothing to notify.")
            return

        domain_counts = fetch_domain_counts_today(conn)
        content = build_discord_message(recent, domain_counts, window_minutes=5)
        if not content:
            print("Nothing to send to Discord (content empty).")
            return

        post_to_discord(webhook_url, content)
        print(f"Posted {len(recent)} recent sends to Discord.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
