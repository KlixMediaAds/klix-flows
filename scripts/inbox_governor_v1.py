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


def apply_governor(as_of_date: dt.date | None = None) -> None:
    """
    Governor v1: safe-down only.

    Reads inbox_health for as_of_date and adjusts inbox caps:

    Rules (MVP):
      - If sent_7d < 20: do nothing (not enough data).
      - If hard_bounce_rate_7d > 0.10 and sent_7d >= 30:
          -> pause inbox (paused = true)
          -> lower daily_cap to min_cap (or 2 if null).
      - Else if hard_bounce_rate_7d > 0.05:
          -> reduce daily_cap by 50%, but not below min_cap (or 2).
      - Else:
          -> leave caps unchanged (no auto-scale up).
    """
    if as_of_date is None:
        as_of_date = dt.date.today()

    print(f"🧠 Inbox Governor v1 for as_of_date = {as_of_date.isoformat()}")

    with db_session() as session:
        # Join inboxes with inbox_health for today
        rows = session.execute(
            text(
                """
                SELECT
                    i.inbox_id,
                    i.email_address,
                    i.daily_cap,
                    i.min_cap,
                    i.max_cap,
                    i.active,
                    i.paused,
                    h.sent_7d,
                    h.bounces_7d,
                    h.hard_bounce_rate_7d,
                    h.status
                FROM inboxes i
                LEFT JOIN inbox_health h
                  ON h.inbox_id = i.inbox_id
                 AND h.as_of_date = :as_of_date
                WHERE i.active = true
                """
            ),
            {"as_of_date": as_of_date},
        ).fetchall()

        if not rows:
            print("No active inboxes found for governor.")
            return

        print(f"Found {len(rows)} active inbox(es) to evaluate.")

        for row in rows:
            inbox_id = row.inbox_id
            email_address = row.email_address
            daily_cap = row.daily_cap
            min_cap = row.min_cap
            max_cap = row.max_cap
            paused = row.paused

            sent_7d = row.sent_7d
            bounces_7d = row.bounces_7d
            bounce_rate = row.hard_bounce_rate_7d

            print(f"\n📬 Inbox {email_address} ({inbox_id})")

            if sent_7d is None:
                print("  → No inbox_health row for today; skipping.")
                continue

            sent_7d = sent_7d or 0
            bounces_7d = bounces_7d or 0
            bounce_rate = float(bounce_rate or 0.0)

            print(
                f"  sent_7d={sent_7d}, bounces_7d={bounces_7d}, "
                f"bounce_rate={bounce_rate:.3f}, daily_cap={daily_cap}, "
                f"min_cap={min_cap}, max_cap={max_cap}, paused={paused}"
            )

            # Safety: skip if not enough volume
            if sent_7d < 20:
                print("  → sent_7d < 20, not enough data; no changes.")
                continue

            # Resolve min_cap / max_cap / daily_cap defaults
            resolved_min_cap = min_cap if min_cap is not None else 2
            resolved_max_cap = max_cap if max_cap is not None else daily_cap or resolved_min_cap
            resolved_daily_cap = daily_cap if daily_cap is not None else resolved_min_cap

            new_paused = paused
            new_daily_cap = resolved_daily_cap
            action = "none"

            # Rule 1: severe bounce rate -> pause inbox
            if bounce_rate > 0.10 and sent_7d >= 30:
                new_paused = True
                new_daily_cap = resolved_min_cap
                action = "pause"
            # Rule 2: elevated bounce rate -> reduce cap by 50%
            elif bounce_rate > 0.05:
                reduced_cap = max(int(resolved_daily_cap * 0.5), resolved_min_cap)
                # Clamp to max_cap
                new_daily_cap = min(reduced_cap, resolved_max_cap)
                action = "reduce_cap"

            if action == "none":
                print("  → Bounce rate acceptable; no changes.")
                continue

            print(
                f"  → Action: {action}. "
                f"daily_cap: {resolved_daily_cap} → {new_daily_cap}, "
                f"paused: {paused} → {new_paused}"
            )

            # Apply updates
            session.execute(
                text(
                    """
                    UPDATE inboxes
                    SET daily_cap = :new_daily_cap,
                        paused = :new_paused
                    WHERE inbox_id = :inbox_id
                    """
                ),
                {
                    "new_daily_cap": new_daily_cap,
                    "new_paused": new_paused,
                    "inbox_id": inbox_id,
                },
            )

        print("\n✅ Inbox Governor v1 run complete.")


if __name__ == "__main__":
    date_str = os.environ.get("KLIX_GOVERNOR_DATE")
    if date_str:
        as_of_date = dt.date.fromisoformat(date_str)
    else:
        as_of_date = None

    apply_governor(as_of_date=as_of_date)
