#!/usr/bin/env python
import os
import sys
import datetime as dt

from sqlalchemy import text

# --- Ensure we can import the klix package when run as a script ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # /opt/klix/klix-flows
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from klix.brain_gateway import db_session


def compute_and_upsert_health(as_of_date: dt.date | None = None) -> None:
    """
    For each active inbox, compute 7d health metrics and upsert into inbox_health.

    v1.1 (2025-12-08):
    - sent_7d, bounces_7d still come from email_sends (authoritative send log)
    - replies_7d now comes from email_events (Unified Email Spine), counting
      lead replies where email_events.signal_type = 'lead' and
      email_events.to_address contains the inbox email.
    """
    if as_of_date is None:
        as_of_date = dt.date.today()

    print(f"📊 Computing inbox_health for as_of_date = {as_of_date.isoformat()}")

    seven_days_ago = as_of_date - dt.timedelta(days=7)

    with db_session() as session:
        # 1) Fetch all active inboxes
        inbox_rows = session.execute(
            text(
                """
                SELECT inbox_id, email_address, domain, daily_cap, active, paused
                FROM inboxes
                WHERE active = true
                """
            )
        ).fetchall()

        if not inbox_rows:
            print("No active inboxes found.")
            return

        print(f"Found {len(inbox_rows)} active inbox(es).")

        for inbox in inbox_rows:
            inbox_id = inbox.inbox_id
            email_address = inbox.email_address

            print(f"\n🔍 Inbox {email_address} ({inbox_id})")

            # 2) Compute sent_7d and bounces_7d from email_sends
            sent_row = session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE status IN ('sent', 'bounced')
                        ) AS sent_7d,
                        COUNT(*) FILTER (
                            WHERE status = 'bounced'
                        ) AS bounces_7d
                    FROM email_sends
                    WHERE inbox_id = :inbox_id
                      AND sent_at >= :since
                    """
                ),
                {"inbox_id": inbox_id, "since": seven_days_ago},
            ).one()

            sent_7d = sent_row.sent_7d or 0
            bounces_7d = sent_row.bounces_7d or 0

            # 3) Compute replies_7d from email_events (Unified Email Spine)
            #
            # We treat "lead" signals as real human replies that matter for
            # inbox health and domain reputation. Matching is done by checking
            # that to_address contains the raw inbox email, to handle both:
            #   - "jess@klixads.ca"
            #   - "Jess <jess@klixads.ca>"
            replies_row = session.execute(
                text(
                    """
                    SELECT COUNT(*) AS replies_7d
                    FROM email_events
                    WHERE received_at >= :since
                      AND signal_type = 'lead'
                      AND to_address ILIKE '%' || :email_address || '%'
                    """
                ),
                {"since": seven_days_ago, "email_address": email_address},
            ).one()

            replies_7d = replies_row.replies_7d or 0

            # 4) Compute hard_bounce_rate_7d
            if sent_7d > 0:
                hard_bounce_rate_7d = bounces_7d / float(sent_7d)
            else:
                hard_bounce_rate_7d = 0.0

            # 5) Determine status color
            #    green = sent_7d >= 20 and bounce_rate < 3%
            #    red   = sent_7d >= 20 and bounce_rate > 7%
            #    else  = yellow
            if sent_7d >= 20 and hard_bounce_rate_7d < 0.03:
                status = "green"
            elif sent_7d >= 20 and hard_bounce_rate_7d > 0.07:
                status = "red"
            else:
                status = "yellow"

            print(
                f"  sent_7d={sent_7d}, bounces_7d={bounces_7d}, "
                f"replies_7d={replies_7d}, hard_bounce_rate_7d={hard_bounce_rate_7d:.3f}, status={status}"
            )

            # 6) Upsert into inbox_health
            session.execute(
                text(
                    """
                    INSERT INTO inbox_health (
                        inbox_id,
                        as_of_date,
                        sent_7d,
                        bounces_7d,
                        replies_7d,
                        hard_bounce_rate_7d,
                        status,
                        last_updated_at
                    )
                    VALUES (
                        :inbox_id,
                        :as_of_date,
                        :sent_7d,
                        :bounces_7d,
                        :replies_7d,
                        :hard_bounce_rate_7d,
                        :status,
                        now()
                    )
                    ON CONFLICT (inbox_id, as_of_date)
                    DO UPDATE SET
                        sent_7d = EXCLUDED.sent_7d,
                        bounces_7d = EXCLUDED.bounces_7d,
                        replies_7d = EXCLUDED.replies_7d,
                        hard_bounce_rate_7d = EXCLUDED.hard_bounce_rate_7d,
                        status = EXCLUDED.status,
                        last_updated_at = now();
                    """
                ),
                {
                    "inbox_id": inbox_id,
                    "as_of_date": as_of_date,
                    "sent_7d": sent_7d,
                    "bounces_7d": bounces_7d,
                    "replies_7d": replies_7d,
                    "hard_bounce_rate_7d": hard_bounce_rate_7d,
                    "status": status,
                },
            )

        print("\n✅ inbox_health rollup complete.")


if __name__ == "__main__":
    date_str = os.environ.get("KLIX_ROLLUP_DATE")
    if date_str:
        as_of_date = dt.date.fromisoformat(date_str)
    else:
        as_of_date = None

    compute_and_upsert_health(as_of_date=as_of_date)
