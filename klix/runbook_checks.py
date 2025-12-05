"""
klix.runbook_checks — Daily Runbook v4.2 (Function-Based, Automation-Ready)

This module converts the manual Daily Runbook v4.1 into a set of
callable, machine-readable health checks that can be orchestrated
by Prefect or simple CLI scripts.

Each check returns a dict of the form:
    {
        "status": "ok" | "warn" | "critical",
        "details": {...}
    }

The top-level helper `run_all_checks()` executes all checks and
returns a structured snapshot, including an `overall_status` field.

Era 1.9.6 addition:
- Warmup / noise hygiene via `check_warmup_ratio_7d`, powered by:
    - email_replies.is_warmup / noise_type
    - v_reply_noise_stats_7d
"""

from __future__ import annotations

import datetime as dt
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from klix.brain_gateway import db_session
from flows.lead_fuel_gauge import get_fuel_metrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

StatusDict = Dict[str, Any]


def _make_result(status: str, details: Dict[str, Any]) -> StatusDict:
    """Keep consistent shape for all checks."""
    return {
        "status": status,
        "details": details,
    }


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    """
    Runs a subprocess and returns rc, stdout, stderr.

    Special handling for Prefect CLI:
    - If the command starts with ["prefect", ...], rewrite it to
      [sys.executable, "-m", "prefect", ...] so it works inside workers
      where the 'prefect' binary may not be on PATH but the module is
      importable.
    """
    import sys

    full_env = os.environ.copy()

    # Normalize Prefect CLI calls to `python -m prefect ...`
    if cmd and cmd[0] == "prefect":
        cmd = [sys.executable, "-m", "prefect", *cmd[1:]]

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=full_env,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()

    """Runs a subprocess and returns rc, stdout, stderr."""
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


# ---------------------------------------------------------------------------
# 0. Env sanity / kill-switches
# ---------------------------------------------------------------------------

def check_env_sanity() -> StatusDict:
    keys = [
        "KLIX_TEST_MODE",
        "SEND_LIVE",
        "SEND_WINDOW",
        "SEND_WINDOW_TZ",
        "KLIX_IGNORE_WINDOW",
        "KLIX_ERROR_RATE_STOP_THRESHOLD",
    ]
    values: Dict[str, Optional[str]] = {k: os.getenv(k) for k in keys}

    issues: List[str] = []

    test_mode = values.get("KLIX_TEST_MODE", "")
    send_live = values.get("SEND_LIVE", "")
    send_window = values.get("SEND_WINDOW", "")
    send_window_tz = values.get("SEND_WINDOW_TZ", "")
    ignore_window = values.get("KLIX_IGNORE_WINDOW", "")
    error_stop = values.get("KLIX_ERROR_RATE_STOP_THRESHOLD", "")

    if test_mode and test_mode.lower() in ("1", "true", "yes", "on"):
        issues.append("KLIX_TEST_MODE is true → no live sends.")

    if send_live != "1":
        issues.append(f"SEND_LIVE={send_live!r}, expected '1'.")

    # Allow the exact "09:00-17:00" as standard, plus a single-space variant
    if send_window not in ("09:00-17:00", "09:00-17:00 "):
        if send_window:
            issues.append(f"Unexpected SEND_WINDOW={send_window!r}")

    if send_window_tz != "America/Toronto":
        issues.append(f"Unexpected SEND_WINDOW_TZ={send_window_tz!r}")

    if ignore_window and ignore_window.lower() in ("1", "true", "yes", "on"):
        issues.append("KLIX_IGNORE_WINDOW is true → bypassing business-hour window.")

    try:
        if error_stop:
            float(error_stop)
    except ValueError:
        issues.append(f"KLIX_ERROR_RATE_STOP_THRESHOLD not float: {error_stop!r}")

    status = "ok"
    if any("bypassing" in i.lower() for i in issues):
        status = "critical"
    elif issues:
        status = "warn"

    return _make_result(status, {"env": values, "issues": issues})


# ---------------------------------------------------------------------------
# 1. Python / DB environment sanity
# ---------------------------------------------------------------------------

def check_python_environment() -> StatusDict:
    details: Dict[str, Any] = {}
    issues: List[str] = []

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        issues.append("DATABASE_URL is not set.")
    details["DATABASE_URL_present"] = bool(database_url)

    openai_key = os.getenv("OPENAI_API_KEY")
    details["OPENAI_API_KEY_present"] = bool(openai_key)

    try:
        with db_session() as session:
            session.execute(text("SELECT 1"))
        details["db_ok"] = True
    except Exception as e:
        details["db_ok"] = False
        details["db_error"] = repr(e)
        issues.append("DB connectivity check failed (SELECT 1).")

    status = "ok" if not issues else "warn"
    if not database_url or not details.get("db_ok"):
        status = "critical"

    details["issues"] = issues
    return _make_result(status, details)


# ---------------------------------------------------------------------------
# 2. Send window logic
# ---------------------------------------------------------------------------

def check_send_window_logic() -> StatusDict:
    try:
        from flows.send_queue_v2 import _within_window, SEND_WINDOW, SEND_WINDOW_TZ
    except Exception as e:
        return _make_result("critical", {"error": f"import failure: {e!r}"})

    now = dt.datetime.now()
    in_window = False
    err: Optional[str] = None

    try:
        in_window = bool(_within_window())
    except Exception as e:
        err = repr(e)

    details = {
        "now": now.isoformat(),
        "send_window": SEND_WINDOW,
        "send_window_tz": SEND_WINDOW_TZ,
        "within_window": in_window,
    }
    if err:
        details["error"] = err

    status = "ok"
    if err:
        status = "critical"
    elif not in_window:
        status = "warn"

    return _make_result(status, details)


# ---------------------------------------------------------------------------
# 3. Prefect deployment health
# ---------------------------------------------------------------------------

def _check_prefect_deployment(flow_name: str, deployment: str) -> StatusDict:
    """
    Shared helper for checking Prefect deployments.
    """
    rc, out, err = _run_cmd(["prefect", "deployment", "inspect", deployment])

    details: Dict[str, Any] = {
        "deployment": deployment,
        "inspect_stdout": out,
        "inspect_stderr": err,
        "inspect_rc": rc,
    }
    issues: List[str] = []

    if rc != 0:
        issues.append("prefect deployment inspect failed.")
        return _make_result("critical", {**details, "issues": issues})

    # Handle various output formats from different Prefect versions:
    active = (
        "Active: True" in out
        or "'active': True" in out
        or '"active": true' in out
        or "is schedule active: True" in out
    )
    details["active"] = active
    if not active:
        issues.append("Deployment schedule appears inactive.")

    status = "ok" if active else "warn"

    rc2, runs_out, runs_err = _run_cmd(
        ["prefect", "flow-run", "ls", "--flow-name", flow_name, "--limit", "5"]
    )
    details.update(
        {
            "runs_stdout": runs_out,
            "runs_stderr": runs_err,
            "runs_rc": rc2,
        }
    )

    if rc2 != 0:
        issues.append("prefect flow-run ls failed.")
        if status == "ok":
            status = "warn"

    details["issues"] = issues
    return _make_result(status, details)


def check_send_queue_deployment() -> StatusDict:
    return _check_prefect_deployment(
        flow_name="send-queue-v2",
        deployment="send-queue-v2/send-queue-v2",
    )


def check_supervisor_deployment() -> StatusDict:
    return _check_prefect_deployment(
        flow_name="cold-pipeline-supervisor",
        deployment="cold-pipeline-supervisor/cold-pipeline-supervisor",
    )


# ---------------------------------------------------------------------------
# 4. Worker health
# ---------------------------------------------------------------------------

def check_worker_health() -> StatusDict:
    unit = "prefect-worker-klix-managed.service"
    rc, out, err = _run_cmd(["systemctl", "status", unit, "--no-pager", "-l"])

    details = {
        "unit": unit,
        "status_stdout": out,
        "status_stderr": err,
        "status_rc": rc,
    }

    if rc != 0:
        return _make_result("critical", {**details, "issues": ["systemctl status failed"]})

    active = "Active: active (running)" in out
    issues: List[str] = []
    if not active:
        issues.append("Worker not active (running).")

    return _make_result(
        "ok" if active else "critical",
        {**details, "active_running": active, "issues": issues},
    )


# ---------------------------------------------------------------------------
# 5. Telemetry views
# ---------------------------------------------------------------------------

def check_inbox_health_7d() -> StatusDict:
    rows: List[Dict[str, Any]] = []
    issues: List[str] = []

    try:
        with db_session() as session:
            q = session.execute(
                text(
                    """
                    SELECT
                        email_address,
                        sends_7d,
                        replies_7d,
                        reply_rate_pct_7d,
                        bounces_7d,
                        complaints_7d,
                        last_sent_at_7d
                    FROM v_inbox_reply_health_7d
                    WHERE inbox_id IS NOT NULL
                    ORDER BY sends_7d DESC
                    """
                )
            )
            for r in q.mappings():
                rows.append(dict(r))
    except Exception as e:
        return _make_result("critical", {"error": repr(e)})

    if any((r.get("complaints_7d") or 0) > 0 for r in rows):
        issues.append("Complaints detected.")
        status = "critical"
    elif any((r.get("bounces_7d") or 0) > 5 for r in rows):
        issues.append("Bounce rate elevated.")
        status = "warn"
    else:
        status = "ok"

    return _make_result(status, {"rows": rows, "issues": issues})


def check_angle_performance_7d() -> StatusDict:
    rows: List[Dict[str, Any]] = []
    weak: List[Dict[str, Any]] = []

    try:
        with db_session() as session:
            q = session.execute(
                text(
                    """
                    SELECT
                        prompt_profile_id,
                        prompt_angle_id,
                        sends_7d,
                        replies_7d,
                        reply_rate_pct_7d,
                        avg_interest_score_7d,
                        positive_replies_7d
                    FROM v_profile_angle_stats_7d
                    ORDER BY replies_7d DESC, sends_7d DESC
                    """
                )
            )
            for r in q.mappings():
                d = dict(r)
                rows.append(d)
                if (d.get("sends_7d") or 0) >= 10 and (d.get("replies_7d") or 0) == 0:
                    weak.append(d)
    except Exception as e:
        return _make_result("critical", {"error": repr(e)})

    status = "ok" if not weak else "warn"
    issues: List[str] = []
    if weak:
        issues.append(f"{len(weak)} angle(s) have sends>=10 and replies_7d=0")

    return _make_result(status, {"rows": rows, "weak_angles": weak, "issues": issues})


def check_recent_email_performance(limit: int = 20) -> StatusDict:
    rows: List[Dict[str, Any]] = []
    issues: List[str] = []

    try:
        with db_session() as session:
            q = session.execute(
                text(
                    """
                    SELECT
                        email_send_id,
                        from_email,
                        to_email,
                        subject,
                        has_reply,
                        last_reply_category,
                        last_reply_interest_score,
                        effective_sent_at
                    FROM v_email_performance_basic
                    ORDER BY effective_sent_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            )
            for r in q.mappings():
                rows.append(dict(r))
    except Exception as e:
        return _make_result("critical", {"error": repr(e)})

    missing = [
        r for r in rows
        if r.get("has_reply") and not r.get("last_reply_category")
    ]
    if missing:
        issues.append(
            f"{len(missing)} recent replies lack categories (possible classifier regression)."
        )

    status = "ok" if not missing else "warn"

    return _make_result(
        status,
        {"rows": rows, "missing_categories": missing, "issues": issues},
    )


# ---------------------------------------------------------------------------
# 5b. Warmup / noise hygiene (Era 1.9.6)
# ---------------------------------------------------------------------------

def check_warmup_ratio_7d() -> StatusDict:
    """
    Governance view of warmup vs real replies.

    Signals:
      - CRITICAL:
          warmups_7d > 0 AND real_replies_7d == 0
      - WARN:
          warmups_7d / real_replies_7d >= 2.0 (and real_replies_7d > 0)
      - OK:
          otherwise.

    Also surfaces a per-inbox slice from v_reply_noise_stats_7d when available.
    """
    try:
        with db_session() as session:
            # Global warmup vs real replies
            global_row = (
                session.execute(
                    text(
                        """
                        SELECT
                          COUNT(*) FILTER (WHERE is_warmup = TRUE) AS warmups_7d,
                          COUNT(*) FILTER (WHERE is_warmup = FALSE OR is_warmup IS NULL) AS real_replies_7d
                        FROM email_replies
                        WHERE received_at >= NOW() - INTERVAL '7 days'
                        """
                    )
                )
                .mappings()
                .one()
            )
            warmups_7d = int(global_row.get("warmups_7d") or 0)
            real_replies_7d = int(global_row.get("real_replies_7d") or 0)

            # Per-inbox breakdown (best-effort; treat failures as non-critical)
            by_inbox_rows: List[Dict[str, Any]] = []
            by_inbox_error: Optional[str] = None
            try:
                q2 = session.execute(
                    text(
                        """
                        SELECT
                          inbox_email,
                          warmups_7d,
                          real_replies_7d,
                          total_replies_7d
                        FROM v_reply_noise_stats_7d
                        ORDER BY total_replies_7d DESC NULLS LAST
                        LIMIT 50
                        """
                    )
                )
                for r in q2.mappings():
                    by_inbox_rows.append(dict(r))
            except Exception as e2:
                by_inbox_error = repr(e2)

    except Exception as e:
        return _make_result("critical", {"error": repr(e)})

    issues: List[str] = []
    ratio: Optional[float] = None
    status = "ok"

    if warmups_7d > 0 and real_replies_7d == 0:
        status = "critical"
        issues.append(
            "Warmups detected in last 7d but zero real replies (possible over-warmup or dead funnel)."
        )
    elif warmups_7d > 0 and real_replies_7d > 0:
        ratio = warmups_7d / max(real_replies_7d, 1)
        if ratio >= 2.0:
            status = "warn"
            issues.append(
                f"High warmup ratio in last 7d: warmups_7d={warmups_7d}, real_replies_7d={real_replies_7d}."
            )

    details: Dict[str, Any] = {
        "warmups_7d": warmups_7d,
        "real_replies_7d": real_replies_7d,
        "warmup_to_real_ratio": ratio,
        "by_inbox": by_inbox_rows,
        "issues": issues,
    }
    if by_inbox_error:
        details["by_inbox_error"] = by_inbox_error

    return _make_result(status, details)


# ---------------------------------------------------------------------------
# 6. Lead tank / fuel gauge
# ---------------------------------------------------------------------------

def check_fuel_gauge() -> StatusDict:
    try:
        metrics = get_fuel_metrics()
        return _make_result("ok", metrics)
    except Exception as e:
        return _make_result("critical", {"error": repr(e)})


def check_queue_states() -> StatusDict:
    rows: List[Dict[str, Any]] = []
    try:
        with db_session() as session:
            q = session.execute(
                text(
                    """
                    SELECT status, COUNT(*) AS ct
                    FROM email_sends
                    WHERE sent_at::date = CURRENT_DATE
                    GROUP BY status
                    """
                )
            )
            for r in q.mappings():
                rows.append(dict(r))
    except Exception as e:
        return _make_result("critical", {"error": repr(e)})

    return _make_result("ok", {"rows": rows})


def check_daily_sends() -> StatusDict:
    today = dt.date.today().isoformat()
    rows: List[Dict[str, Any]] = []

    try:
        with db_session() as session:
            q = session.execute(
                text(
                    """
                    SELECT
                        id,
                        send_type,
                        status,
                        sent_at,
                        from_domain,
                        to_email
                    FROM email_sends
                    WHERE sent_at::date = CURRENT_DATE
                    ORDER BY sent_at DESC
                    """
                )
            )
            for r in q.mappings():
                rows.append(dict(r))
    except Exception as e:
        return _make_result("critical", {"error": repr(e)})

    return _make_result("ok", {"date": today, "count": len(rows), "rows": rows})


def check_end_of_day_caps() -> StatusDict:
    rows: List[Dict[str, Any]] = []

    try:
        with db_session() as session:
            q = session.execute(
                text(
                    """
                    SELECT
                        email_address,
                        sent_today,
                        daily_cap
                    FROM v_inbox_daily_caps
                    ORDER BY email_address
                    """
                )
            )
            for r in q.mappings():
                rows.append(dict(r))
    except Exception as e:
        # Treat missing view / query failure as non-blocking warning
        return _make_result(
            "warn",
            {
                "error": repr(e),
                "rows": [],
                "over_saturated": [],
                "under_utilized": [],
                "issues": [
                    "end_of_day_caps view not available or query failed; treating as non-blocking."
                ],
            },
        )

    under = [
        r
        for r in rows
        if (r.get("sent_today") or 0) < (0.25 * (r.get("daily_cap") or 1))
    ]
    over = [
        r
        for r in rows
        if (r.get("sent_today") or 0) > (r.get("daily_cap") or 0)
    ]

    issues: List[str] = []
    if under:
        issues.append(f"{len(under)} inbox(es) sent <25% of daily cap.")
    if over:
        issues.append(f"{len(over)} inbox(es) exceeded daily cap!")

    status = "ok"
    if over:
        status = "critical"
    elif under:
        status = "warn"

    return _make_result(
        status,
        {
            "rows": rows,
            "over_saturated": over,
            "under_utilized": under,
            "issues": issues,
        },
    )


# ---------------------------------------------------------------------------
# 7. Top-level aggregation
# ---------------------------------------------------------------------------


def run_all_checks() -> tuple[dict[str, object], str]:
    """
    Run all registered checks and compute an overall_status.

    Design:
    - If ANY check is 'critical' => overall_status='critical',
      EXCEPT for a small set of *non-plumbing* checks
      ('env_sanity', 'send_window') which may be critical
      for business semantics but should not bring down
      the runbook itself.
    - If no criticals but at least one 'warn' => 'warn'.
    - Otherwise 'ok'.
    """
    snapshot_checks: dict[str, StatusDict] = {}

    for name, fn in CHECKS:
        snapshot_checks[name] = fn()

    overall_status = "ok"

    for name, result in snapshot_checks.items():
        status = result.get("status", "ok")

        if status == "critical":
            # These are important, but not "plumbing is broken".
            if name in {"env_sanity", "send_window"}:
                if overall_status == "ok":
                    overall_status = "warn"
                continue

            # Any other critical → hard fail.
            overall_status = "critical"
            break

        elif status == "warn" and overall_status == "ok":
            overall_status = "warn"

    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "checks": snapshot_checks,
    }
    return snapshot, overall_status


if __name__ == "__main__":
    import json

    snapshot, status = run_all_checks()
    print(json.dumps(snapshot, indent=2, default=str))


# --- patched v4.2.1: robust send_window check ---
def check_send_window() -> StatusDict:
    """
    Inspect send_queue_v2's SEND_WINDOW / SEND_WINDOW_TZ / SEND_LIVE if available.

    If the flows.send_queue_v2 module is missing, treat this as a WARN,
    not CRITICAL, so the runbook can still complete while signalling
    the instrumentation gap.
    """
    issues: List[str] = []
    details: Dict[str, Any] = {}

    try:
        from flows.send_queue_v2 import (
            SEND_WINDOW,
            SEND_WINDOW_TZ,
            SEND_LIVE,
        )  # type: ignore
    except ModuleNotFoundError as exc:
        details["error"] = f"import failure: {exc!r}"
        issues.append(
            "flows.send_queue_v2 not importable; cannot introspect send window."
        )
        return _make_result("warn", {"issues": issues, **details})
    except Exception as exc:
        details["error"] = f"unexpected import failure: {exc!r}"
        issues.append("Unexpected error importing send_queue_v2.")
        return _make_result("critical", {"issues": issues, **details})

    details["SEND_WINDOW"] = SEND_WINDOW
    details["SEND_WINDOW_TZ"] = SEND_WINDOW_TZ
    details["SEND_LIVE"] = SEND_LIVE

    # Mirror env expectations:
    if SEND_LIVE != "1":
        issues.append(
            f"SEND_LIVE from send_queue_v2 is {SEND_LIVE!r}, expected '1'."
        )
    if SEND_WINDOW not in ("09:00-17:00", "09:00-17:00 "):
        issues.append(
            f"Unexpected SEND_WINDOW from send_queue_v2: {SEND_WINDOW!r}"
        )
    if SEND_WINDOW_TZ != "America/Toronto":
        issues.append(
            f"Unexpected SEND_WINDOW_TZ from send_queue_v2: {SEND_WINDOW_TZ!r}"
        )

    status = "ok" if not issues else "warn"
    return _make_result(status, {**details, "issues": issues})
