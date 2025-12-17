"""
flows.automated_runbook — Daily Runbook v4.2 (Automated Orchestrator)

This Prefect flow:

1. Calls klix.runbook_checks.run_all_checks() to get a structured snapshot.
2. Logs the snapshot summary to the Prefect logger.
3. Sends a Discord alert via flows.utils.discord_alerts.send_discord_alert,
   with a compact Markdown summary of key check statuses.

Intended usage:
- As a scheduled Prefect deployment (e.g. every weekday morning).
- Or ad-hoc via `python -m flows.automated_runbook`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from prefect import flow, get_run_logger

from klix.runbook_checks import run_all_checks
from flows.utils.discord_alerts import send_discord_alert


def _status_emoji(status: str) -> str:
    s = status.lower()
    if s == "critical":
        return "🔴"
    if s == "warn":
        return "🟡"
    return "🟢"


def _overall_to_severity(overall_status: str) -> str:
    """
    Map runbook overall_status -> Discord severity.

    - critical → "critical"
    - warn     → "error"
    - ok       → "info"
    """
    s = overall_status.lower()
    if s == "critical":
        return "critical"
    if s == "warn":
        return "error"
    return "info"


_CHECK_LABELS: Dict[str, str] = {
    "env_sanity": "Env sanity / kill-switches",
    "python_env": "Python + DB env",
    "send_window": "Send window logic",
    "send_queue_deployment": "SendQueue deployment",
    "supervisor_deployment": "Cold supervisor deployment",
    "worker": "Worker (systemd) health",
    "inbox_health": "Inbox health (7d)",
    "angle_performance": "Angle performance (7d)",
    "recent_performance": "Recent email performance",
    "fuel_gauge": "Fuel gauge (eligible/queued)",
    "queue_states": "Queue states (held/queued)",
    "daily_sends": "Daily cold sends",
    "end_of_day_caps": "End-of-day caps",
}


def _build_markdown_summary(snapshot: Dict[str, Any]) -> str:
    """
    Render a compact Markdown summary for Discord based on the snapshot.
    """
    overall_status = snapshot.get("overall_status", "ok")
    timestamp = snapshot.get("timestamp", "")
    checks: Dict[str, Dict[str, Any]] = snapshot.get("checks", {})

    overall_emoji = _status_emoji(overall_status)
    lines: List[str] = []

    # Header
    lines.append(f"{overall_emoji} **KlixOS Daily Runbook v4.2**")
    lines.append(f"- Overall status: **{overall_status.upper()}**")
    if timestamp:
        lines.append(f"- Timestamp (UTC): `{timestamp}`")
    lines.append("")

    # Per-check lines (one-liners)
    for key, result in checks.items():
        status = result.get("status", "ok")
        details = result.get("details", {}) or {}
        issues = details.get("issues")

        # Normalize issues to a short text
        issue_text = ""
        if isinstance(issues, list) and issues:
            # Take first 1–2 issues max for brevity
            trimmed = issues[:2]
            issue_text = " — " + " | ".join(trimmed)
        elif isinstance(issues, str) and issues:
            issue_text = " — " + issues

        label = _CHECK_LABELS.get(key, key)
        emoji = _status_emoji(status)
        lines.append(f"{emoji} **{label}**: `{status}`{issue_text}")

    return "\n".join(lines)


@flow(name="automated-runbook-v4-2")
def automated_runbook_flow() -> Dict[str, Any]:
    """
    Prefect flow wrapper for the v4.2 runbook checks.

    Returns
    -------
    snapshot : dict
        {
          "timestamp": ...,
          "overall_status": "ok" | "warn" | "critical",
          "checks": {
              ...
          }
        }
    """
    logger = get_run_logger()
    snapshot = run_all_checks()

    overall_status = snapshot.get("overall_status", "ok")
    logger.info("Runbook snapshot overall_status=%s", overall_status)
    logger.info("Raw snapshot:\n%s", json.dumps(snapshot, indent=2, default=str))

    # Build Discord-friendly summary
    md_summary = _build_markdown_summary(snapshot)
    severity = _overall_to_severity(overall_status)

    # Send to Discord via centralized alerts helper
    try:
        send_discord_alert(
            title="Daily Runbook v4.2 Snapshot",
            body=md_summary,
            severity=severity,
            context={
                "overall_status": overall_status,
                "flow_name": "automated-runbook-v4-2",
            },
        )
        logger.info("Sent Discord runbook summary with severity=%s", severity)
    except Exception as e:
        logger.error("Failed to send Discord runbook summary: %r", e)

    return snapshot


if __name__ == "__main__":
    # Ad-hoc CLI entrypoint:
    #   PYTHONPATH=. python -m flows.automated_runbook
    snap = automated_runbook_flow()
    print(json.dumps(snap, indent=2, default=str))
