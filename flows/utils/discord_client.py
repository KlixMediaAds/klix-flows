import os
import time
import traceback
from typing import Optional, Dict, Any

import requests

# Main + error webhooks (already set in /etc/klix/secret.env)
DEFAULT_WEBHOOK = os.getenv("DISCORD_WEBHOOK_MAIN")
ERROR_WEBHOOK = os.getenv("DISCORD_WEBHOOK_ERRORS") or DEFAULT_WEBHOOK


def _post(url: str, payload: Dict[str, Any]) -> None:
    """Robust sender with retries, rate-limit handling, and console visibility."""
    if not url:
        print("[discord_client] ❌ No webhook URL provided.")
        return

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=5)

            if resp.status_code in (200, 204):
                return

            if resp.status_code == 429:
                retry = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"[discord_client] ⏳ Rate limited, retrying in {retry}s")
                time.sleep(retry)
                continue

            # Other 4xx → do not retry endlessly
            if 400 <= resp.status_code < 500:
                print(f"[discord_client] ❌ Client error: {resp.status_code} {resp.text[:200]}")
                return

            print(f"[discord_client] ⚠️ Server error: {resp.status_code} {resp.text[:200]}")

        except Exception as e:
            print(f"[discord_client] Exception: {e}")
            time.sleep(1 + attempt)


def send_message(content: str, *, webhook: Optional[str] = None, username: str = "Klix OS") -> None:
    _post(webhook or DEFAULT_WEBHOOK, {"content": content, "username": username})


def send_embed(
    title: str,
    description: str,
    *,
    fields: Dict[str, str] | None = None,
    color: int = 0x5865F2,
    webhook: Optional[str] = None,
    username: str = "Klix OS",
) -> None:
    embed: Dict[str, Any] = {"title": title, "description": description, "color": color}
    if fields:
        embed["fields"] = [{"name": k, "value": v, "inline": False} for k, v in fields.items()]

    _post(webhook or DEFAULT_WEBHOOK, {"username": username, "embeds": [embed]})


def send_error(context: str, exc: Exception, *, webhook: Optional[str] = None) -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    payload: Dict[str, Any] = {
        "username": "Klix OS – Errors",
        "embeds": [
            {
                "title": f"❌ Error: {context}",
                "description": f"```{str(exc)[:1800]}```",
                "color": 0xFF0000,
                "fields": [
                    {
                        "name": "Traceback",
                        "value": f"```{tb[:1800]}```",
                    }
                ],
            }
        ],
    }
    _post(webhook or ERROR_WEBHOOK, payload)
