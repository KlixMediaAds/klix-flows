import os
from typing import Optional

from flows.utils.discord_client import send_message

# Backwards-compatible wrapper around the new centralized Discord client.
# Existing code imports: from libs.discord import post_discord

# Old env vars (still present in /etc/klix/secret.env)
_DEFAULT_LEGACY = os.getenv("DISCORD_WEBHOOK_URL")
_ALERTS_LEGACY = os.getenv("DISCORD_ALERTS_URL")

# New standardized env vars
_DEFAULT_MAIN = os.getenv("DISCORD_WEBHOOK_MAIN")
_ERRORS_MAIN = os.getenv("DISCORD_WEBHOOK_ERRORS")

DEFAULT_WEBHOOK = _DEFAULT_LEGACY or _DEFAULT_MAIN
ERROR_WEBHOOK = _ALERTS_LEGACY or _ERRORS_MAIN or DEFAULT_WEBHOOK


def post_discord(msg: str, *, use_error_channel: bool = False, webhook: Optional[str] = None) -> None:
    """
    Backwards-compatible function used across flows.

    - If webhook is provided, use that.
    - Else if use_error_channel=True, send to error/alerts webhook.
    - Else send to default/main webhook.
    """
    target = webhook
    if target is None:
        target = ERROR_WEBHOOK if use_error_channel else DEFAULT_WEBHOOK

    # Fall back to DEFAULT_WEBHOOK if everything is missing
    if target is None:
        # Last resort: still try to send to whatever DEFAULT_WEBHOOK is,
        # but also print so failures are visible in logs.
        print("[libs.discord] ⚠️ No webhook URL configured; message was:", msg)
        return

    send_message(msg, webhook=target)
