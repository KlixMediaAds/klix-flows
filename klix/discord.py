import os
import json
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Base / fallback webhook:
# Prefer a dedicated alerts URL, else fall back to the health webhook.
_BASE_DEFAULT = os.getenv("DISCORD_ALERTS_URL") or os.getenv("DISCORD_HEALTH_WEBHOOK_URL")

# Per-channel overrides (extend as needed)
_CHANNEL_WEBHOOKS = {
    # Unified Email Spine alerts (lead replies, security, failed payments)
    "email_spine": os.getenv("DISCORD_EMAIL_SPINE_WEBHOOK_URL") or _BASE_DEFAULT,
    # Daily stats / health, if you ever want to route here
    "daily_stats": os.getenv("DISCORD_DAILY_STATS_WEBHOOK_URL") or _BASE_DEFAULT,
}


def _get_webhook_for_channel(channel: str) -> Optional[str]:
    """
    Resolve the Discord webhook URL for a logical channel key.
    """
    url = _CHANNEL_WEBHOOKS.get(channel) or _BASE_DEFAULT
    return url


def send_discord_message(
    channel: str,
    content: str,
    username: Optional[str] = None,
) -> None:
    """
    Lightweight helper used by flows (e.g., email_spine_poller) to send Discord alerts.

    - channel: logical name (e.g. 'email_spine', 'daily_stats')
    - content: plain-text or markdown content
    - username: optional override for the bot display name
    """
    webhook_url = _get_webhook_for_channel(channel)

    if not webhook_url:
        # Fallback: log + print so we never blow up a flow if env is missing.
        msg = f"[discord] No webhook configured for channel={channel!r}. Content:\\n{content}"
        logger.warning(msg)
        print(msg)
        return

    payload = {
        "content": content,
    }
    if username:
        payload["username"] = username

    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code >= 400:
            logger.error(
                "[discord] Failed to send message to channel=%r: %s %s",
                channel,
                resp.status_code,
                resp.text[:200],
            )
            print(
                f"[discord] Error sending to {channel!r}: "
                f"{resp.status_code} {resp.text[:200]!r}"
            )
        else:
            logger.info("[discord] Sent message to channel=%r", channel)
    except Exception as e:  # pragma: no cover
        logger.exception("[discord] Exception while sending Discord message to %r", channel)
        print(f"[discord] Exception while sending message to {channel!r}: {e!r}")


__all__ = ["send_discord_message"]
