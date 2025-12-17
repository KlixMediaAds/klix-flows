import os
import socket
import sys
import time
from pathlib import Path
from typing import Tuple, Dict, Any

import psycopg2
from prefect import flow, get_run_logger

try:
    from openai import OpenAI
except ImportError:
    # If OpenAI isn't installed for some reason, we'll treat that as a failure in the check.
    OpenAI = None  # type: ignore

# Ensure repo root is on sys.path so `klix` package is importable
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Import the centralized Discord alert helper (works both as package and script)
try:
    from .utils.discord_alerts import send_discord_alert  # type: ignore
except ImportError:
    from utils.discord_alerts import send_discord_alert  # type: ignore

# SMTP integration provider
try:
    from klix.integrations.email_provider import SMTPEmailProvider
except ImportError:
    SMTPEmailProvider = None  # type: ignore


# ---------------------------------------------------------------------------
# Helpers / Config
# ---------------------------------------------------------------------------

ENV_TAG = os.getenv("KLIX_ALERT_ENV_TAG", "prod")
HOSTNAME = socket.gethostname()

DATABASE_URL = os.environ.get("DATABASE_URL", "")
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()

# SMTP health is optional and OFF by default.
# Turn it on only if you're actually using SMTP as a transport.
SMTP_HEALTH_ENABLED = (os.getenv("SMTP_HEALTH_ENABLED", "false").lower() == "true")

SMTP_HOST = (os.getenv("SMTP_HOST") or "").strip()
SMTP_PORT_RAW = os.getenv("SMTP_PORT", "587")
try:
    SMTP_PORT = int(SMTP_PORT_RAW)
except ValueError:
    SMTP_PORT = 587

SMTP_USER = (os.getenv("SMTP_USER") or "").strip()
SMTP_PASS = os.getenv("SMTP_PASS") or ""
SMTP_TEST_INBOX = (os.getenv("SMTP_TEST_INBOX") or "").strip()


def check_openai_key() -> Tuple[bool, str]:
    """
    Basic OpenAI health check.

    - Verifies that OPENAI_API_KEY is present.
    - If openai SDK is available, performs a tiny test call.
    """
    if not OPENAI_API_KEY:
        return False, "OPENAI_API_KEY is missing or blank in environment."

    if OpenAI is None:
        return False, "openai SDK is not installed or import failed."

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Very small test call; adjust model if needed.
    # Intentionally simple and low-cost.
    test_prompt = "Return the word 'ok'."

    try:
        start = time.perf_counter()
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Health check for KlixOS."},
                {"role": "user", "content": test_prompt},
            ],
            max_tokens=5,
        )
        latency_ms = int((time.perf_counter() - start) * 1000)

        content = (resp.choices[0].message.content or "").lower()
        if "ok" not in content:
            return False, f"OpenAI responded, but content unexpected: {content!r} (latency={latency_ms}ms)"

        return True, f"OpenAI health OK (latency={latency_ms}ms)."

    except Exception as e:
        return False, f"OpenAI health check failed: {type(e).__name__}: {e}"


def check_db_connectivity() -> Tuple[bool, str]:
    """
    Simple DB connectivity check using psycopg2.

    - Ensures DATABASE_URL is set.
    - Attempts a connect + SELECT 1; fails fast on errors.
    """
    if not DATABASE_URL:
        return False, "DATABASE_URL is not set in environment."

    try:
        start = time.perf_counter()
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        finally:
            conn.close()
        latency_ms = int((time.perf_counter() - start) * 1000)
        return True, f"DB connectivity OK (latency={latency_ms}ms)."
    except Exception as e:
        return False, f"DB connectivity check failed: {type(e).__name__}: {e}"


def check_smtp_health() -> Tuple[bool, str]:
    """
    SMTP health check via SMTPEmailProvider.

    - Only runs if SMTP_HEALTH_ENABLED=true.
    - Otherwise, it returns OK with a 'skipped' message.

    This avoids false-critical alerts in deployments that use Gmail OAuth
    or other non-SMTP transports.
    """
    if not SMTP_HEALTH_ENABLED:
        return True, "SMTP health check disabled (SMTP_HEALTH_ENABLED=false)."

    if SMTPEmailProvider is None:
        return False, "SMTPEmailProvider import failed (klix.integrations.email_provider not importable)."

    missing = []
    if not SMTP_HOST:
        missing.append("SMTP_HOST")
    if not SMTP_USER:
        missing.append("SMTP_USER")
    if not SMTP_TEST_INBOX:
        missing.append("SMTP_TEST_INBOX")

    if missing:
        return False, f"Missing SMTP config env vars: {', '.join(missing)}."

    try:
        provider = SMTPEmailProvider(
            host=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USER,
            password=SMTP_PASS,
            use_tls=True,
            timeout=10.0,
        )

        start = time.perf_counter()
        provider.send_email(
            from_addr=SMTP_USER,
            to_addr=SMTP_TEST_INBOX,
            subject="klix infra healthcheck",
            html_body="<p>ok</p>",
            text_body="ok",
            metadata={"headers": {"X-Klix-Healthcheck": "smtp"}},
        )
        latency_ms = int((time.perf_counter() - start) * 1000)
        return True, f"SMTP health OK (latency={latency_ms}ms)."
    except Exception as e:
        return False, f"SMTP health check failed: {type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Prefect flow
# ---------------------------------------------------------------------------


@flow(name="secrets_watchdog")
def secrets_watchdog_flow() -> None:
    """
    Periodic watchdog for API secrets & core infra.

    Current checks:
      - OpenAI API key presence + tiny health call.
      - DATABASE_URL presence + basic connectivity.
      - Optional SMTP health (gated by SMTP_HEALTH_ENABLED).

    On any failure, sends a single CRITICAL Discord alert with aggregated details.
    """
    logger = get_run_logger()
    context_base: Dict[str, Any] = {
        "env": ENV_TAG,
        "host": HOSTNAME,
        "flow": "secrets_watchdog",
    }

    # Run checks
    results: Dict[str, Tuple[bool, str]] = {
        "openai": check_openai_key(),
        "database": check_db_connectivity(),
        "smtp": check_smtp_health(),
    }

    # Log per-check results
    for name, (ok, message) in results.items():
        prefix = name.upper()
        if ok:
            logger.info(f"[secrets_watchdog] {prefix}: {message}")
        else:
            logger.error(f"[secrets_watchdog] {prefix} FAILURE: {message}")

    # Aggregate failures
    failures = {name: msg for name, (ok, msg) in results.items() if not ok}

    if failures:
        # Build a compact, readable body for Discord
        lines = [f"Environment: {ENV_TAG}", f"Host: {HOSTNAME}", "", "Failed checks:"]
        for name, msg in failures.items():
            lines.append(f"- {name}: {msg}")
        body = "\n".join(lines)

        send_discord_alert(
            title="INFRA HEALTH ALERT — One or more checks failed",
            body=body,
            severity="critical",
            context={**context_base, "failed_checks": list(failures.keys())},
        )

    # If everything is OK (including SMTP being disabled), no alert is sent; logs are enough.


if __name__ == "__main__":
    secrets_watchdog_flow()
