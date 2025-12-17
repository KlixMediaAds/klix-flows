#!/usr/bin/env python
import os
import sys
import datetime as dt
import json

from sqlalchemy import text

# --- Ensure we can import the klix package when run as a script ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # /opt/klix/klix-flows
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from klix.brain_gateway import db_session

try:
    import requests
except ImportError:
    print("requests not installed. Install with: pip install requests")
    sys.exit(1)


def build_health_report(as_of_date: dt.date | None = None) -> str:
    """
    Build a plain-text health summary for all active inboxes.
    """
    if as_of_date is None:
        as_of_date = dt.date.today()

    header = f"**Klix Inbox Health — {as_of_date.isoformat()}**"
    lines = [header, ""]

    with db_session() as session:
        rows = session.execute(
            text(
                """
                SELECT
                    i.email_address,
                    i.daily_cap,
                    i.paused,
                    h.sent_7d,
                    h.bounces_7d,
                    h.replies_7d,
                    h.hard_bounce_rate_7d,
                    h.status
                FROM inboxes i
                LEFT JOIN inbox_health h
                  ON h.inbox_id = i.inbox_id
                 AND h.as_of_date = :as_of_date
                WHERE i.active = true
                ORDER BY i.email_address
                """
            ),
            {"as_of_date": as_of_date},
        ).fetchall()

        if not rows:
            lines.append("_No active inboxes found._")
            return "\n".join(lines)

        for row in rows:
            email = row.email_address
            daily_cap = row.daily_cap
            paused = row.paused

            sent_7d = row.sent_7d or 0
            bounces_7d = row.bounces_7d or 0
            replies_7d = row.replies_7d or 0
            bounce_rate = float(row.hard_bounce_rate_7d or 0.0)
            status = row.status or "yellow"

            if status == "green":
                emoji = "🟢"
            elif status == "red":
                emoji = "🔴"
            else:
                emoji = "🟡"

            paused_str = " (paused)" if paused else ""
            cap_str = f"cap={daily_cap}" if daily_cap is not None else "cap=none"
            bounce_pct = f"{bounce_rate * 100:.1f}%"

            line = (
                f"{emoji} `{email}`{paused_str} — {cap_str}, "
                f"sent_7d={sent_7d}, bounces_7d={bounces_7d} ({bounce_pct}), "
                f"replies_7d={replies_7d}"
            )
            lines.append(line)

    return "\n".join(lines)


def send_discord_report(message: str) -> None:
    webhook_url = os.environ.get("DISCORD_HEALTH_WEBHOOK_URL")
    if not webhook_url:
        print("DISCORD_HEALTH_WEBHOOK_URL not set; printing message instead:\n")
        print(message)
        return

    payload = {
        "content": message
    }

    resp = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    if resp.status_code >= 400:
        print(f"❌ Failed to send Discord health report: {resp.status_code} {resp.text}")
    else:
        print("✅ Discord health report sent.")


if __name__ == "__main__":
    date_str = os.environ.get("KLIX_HEALTH_REPORT_DATE")
    if date_str:
        as_of_date = dt.date.fromisoformat(date_str)
    else:
        as_of_date = None

    msg = build_health_report(as_of_date=as_of_date)
    send_discord_report(msg)
