"""
Simple heartbeat script for Klix OS.

Usage:
    source .venv/bin/activate
    set -o allexport; source /etc/klix/secret.env; set +o allexport
    python scripts/heartbeat_ping.py
"""

import socket
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root (the repo folder) is on sys.path so `flows` can be imported.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flows.utils.discord_client import send_message


def main() -> None:
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    host = socket.gethostname()

    msg = (
        "💓 Klix OS heartbeat\n"
        f"- host: `{host}`\n"
        f"- time (UTC): `{now}`\n"
        "\n"
        "If you stop seeing this daily, something is wrong with the server or scheduler."
    )

    send_message(msg)


if __name__ == "__main__":
    main()
