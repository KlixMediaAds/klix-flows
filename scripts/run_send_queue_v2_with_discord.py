"""
Discord-instrumented runner for send_queue_v2.

This wraps the existing send_queue_v2_flow and sends:
- a start embed
- a completion embed
- an error embed on crash

Usage (example):
    source .venv/bin/activate
    set -o allexport; source /etc/klix/secret.env; set +o allexport
    python scripts/run_send_queue_v2_with_discord.py
"""

import sys
import socket
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flows.utils.discord_client import send_embed, send_error
from flows.send_queue_v2 import send_queue_v2_flow


def main(
    batch_size: int = 20,
    allow_weekend: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Run send_queue_v2 with Discord instrumentation.

    NOTE:
    - For safety, KLIX_TEST_MODE in /etc/klix/secret.env is the primary
      "kill switch". If KLIX_TEST_MODE=true, the flow exits before touching
      the queue, regardless of this dry_run flag.
    - Set dry_run=True only for special tests where you explicitly want
      the flow to behave as a dry run.
    """
    host = socket.gethostname()
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Start embed
    send_embed(
        title="🚀 send_queue_v2 started",
        description="Batch tick started.",
        fields={
            "host": host,
            "time_utc": now,
            "batch_size": str(batch_size),
            "allow_weekend": str(allow_weekend),
            "dry_run": str(dry_run),
        },
    )

    try:
        # Run the real flow
        result = send_queue_v2_flow(
            batch_size=batch_size,
            allow_weekend=allow_weekend,
            dry_run=dry_run,
        )

        fields = {
            "host": host,
            "time_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "batch_size": str(batch_size),
            "allow_weekend": str(allow_weekend),
            "dry_run": str(dry_run),
        }

        # If the flow returns a dict of stats, include key fields
        if isinstance(result, dict):
            for key in ("attempted", "sent", "skipped_caps", "skipped_window"):
                if key in result:
                    fields[key] = str(result[key])

        send_embed(
            title="✅ send_queue_v2 completed",
            description="Batch processed successfully.",
            color=0x57F287,
            fields=fields,
        )

    except Exception as e:
        # Error embed
        send_error("send_queue_v2 runner top-level crash", e)
        raise


if __name__ == "__main__":
    # Default to live sending; rely on KLIX_TEST_MODE for global test mode.
    main(
        batch_size=20,
        allow_weekend=False,
        dry_run=False,
    )
