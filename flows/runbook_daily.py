"""
flows.runbook_daily — Prefect wrapper for klix.runbook_checks (Daily Runbook v4.2)

This flow runs the full runbook check suite and logs a structured snapshot.
If overall_status is "critical", the flow fails so Prefect can alert.

Intended usage:
  - Ad-hoc:  python -m flows.runbook_daily
  - Prefect deployment: schedule 1–2x per day via work pool 'klix-managed'.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from prefect import flow, get_run_logger

from klix.runbook_checks import run_all_checks


@flow(name="runbook-daily")
def runbook_daily_flow() -> Tuple[Dict[str, Any], str]:
    """
    Run all health checks from klix.runbook_checks and log the result.

    Returns:
        (snapshot, overall_status)
    """
    logger = get_run_logger()

    snapshot, overall_status = run_all_checks()

    # Compact headline
    logger.info(f"[RUNBOOK] overall_status={overall_status}")

    # Full JSON snapshot (stringified so it’s easy to copy from Prefect UI)
    try:
        pretty = json.dumps(snapshot, indent=2, default=str)
    except TypeError:
        # Fallback: best-effort string cast if something isn’t JSON serializable
        pretty = json.dumps(
            {
                "timestamp": snapshot.get("timestamp"),
                "overall_status": snapshot.get("overall_status"),
                "checks_keys": list(snapshot.get("checks", {}).keys()),
            },
            indent=2,
            default=str,
        )

    logger.info("[RUNBOOK] snapshot:\n" + pretty)

    # Fail the flow on CRITICAL so Prefect shows it as red
    if overall_status == "critical":
        raise RuntimeError("Runbook overall_status=critical")

    return snapshot, overall_status


if __name__ == "__main__":
    # Local/manual execution
    runbook_daily_flow()
