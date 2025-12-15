import os
from typing import Optional, Dict, Any

from . import discord_client

"""
Centralized Discord alert helper for KlixOS.

All new alert-style notifications should go through send_discord_alert().
This module is responsible for:
- Severity-based routing (critical/error/info)
- Choosing the appropriate webhook URL
- Adding lightweight context to messages
- Never crashing callers (best-effort only)
"""

# Base webhooks from existing config
_DEFAULT_MAIN = discord_client.DEFAULT_WEBHOOK
_ERROR_MAIN = discord_client.ERROR_WEBHOOK

# Optional specialized env vars (if present)
_ALERTS_LEGACY = os.getenv("DISCORD_ALERTS_URL")
_SEND_WEBHOOK = os.getenv("DISCORD_SEND_WEBHOOK_URL")
_HEALTH_WEBHOOK = os.getenv("DISCORD_HEALTH_WEBHOOK_URL")
_REPLIES_WEBHOOK = os.getenv("DISCORD_REPLIES_URL")

# Effective routing targets
ALERT_WEBHOOK = _ALERTS_LEGACY or _ERROR_MAIN or _DEFAULT_MAIN
INFO_WEBHOOK = _SEND_WEBHOOK or _HEALTH_WEBHOOK or _DEFAULT_MAIN
DEFAULT_WEBHOOK = _DEFAULT_MAIN or ALERT_WEBHOOK or INFO_WEBHOOK


def _build_content_prefix(severity: str) -> str:
    s = severity.lower()
    if s == "critical":
        return "🚨 [CRITICAL]"
    if s == "error":
        return "❌ [ERROR]"
    if s == "info":
        return "ℹ️ [INFO]"
    return f"[{severity.upper()}]"


def _choose_webhook(severity: str) -> Optional[str]:
    s = severity.lower()
    if s in ("critical", "error"):
        return ALERT_WEBHOOK or DEFAULT_WEBHOOK
    if s == "info":
        return INFO_WEBHOOK or DEFAULT_WEBHOOK
    return DEFAULT_WEBHOOK or ALERT_WEBHOOK or INFO_WEBHOOK


def _format_context(context: Optional[Dict[str, Any]]) -> str:
    if not context:
        return ""
    parts = []
    for k, v in context.items():
        parts.append(f"- **{k}**: `{v}`")
    return "\n\n**Context:**\n" + "\n".join(parts)


def send_discord_alert(
    title: str,
    body: str,
    *,
    severity: str = "error",
    context: Optional[Dict[str, Any]] = None,
    webhook: Optional[str] = None,
) -> None:
    """
    Send a structured alert to Discord.

    Parameters
    ----------
    title:
        Short, high-signal title (e.g. "SECRETS ALERT — OpenAI").
    body:
        Human-readable description of what happened and what to check.
    severity:
        "critical", "error", or "info" (used for routing + prefix).
    context:
        Optional dict of extra key/value pairs (env, host, flow_name, etc.).
    webhook:
        Optional override for the webhook URL (e.g. forcing a specific channel).
    """
    target = webhook or _choose_webhook(severity)

    if not target:
        print(
            "[discord_alerts] ⚠️ No webhook URL resolved; "
            f"severity={severity}, title={title!r}, body={body!r}"
        )
        return

    prefix = _build_content_prefix(severity)
    context_block = _format_context(context)

    full_description = body
    if context_block:
        full_description += "\n\n" + context_block

    embed = {
        "title": f"{prefix} {title}",
        "description": full_description,
        "color": 0xFF0000 if severity.lower() in ("critical", "error") else 0x5865F2,
    }

    payload = {
        "username": "Klix OS – Alerts",
        "embeds": [embed],
    }

    try:
        # Reuse the robust sender (retries, rate-limits, logging)
        discord_client._post(target, payload)  # type: ignore[attr-defined]
    except Exception as e:
        # Never crash the caller; log and move on.
        print(
            "[discord_alerts] Exception while sending alert:",
            repr(e),
            "title=",
            title,
            "severity=",
            severity,
        )
