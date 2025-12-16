"""
SMTP App Password provider (smtp_app_pw).

This provider implements the current Gmail app-password behavior, but wrapped
behind the BaseEmailProvider interface.
"""

from __future__ import annotations

import os
import smtplib
from typing import Any, Dict, Mapping, Optional

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .base import BaseEmailProvider, SendResult


# ---------------------------------------------------------------------------
# SMTP env config (mirrors flows/send_queue.py)
# ---------------------------------------------------------------------------

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# Global fallback (used if we can't match a specific inbox)
SMTP_USER = (
    os.getenv("SMTP_USER")
    or os.getenv("SMTP_USERNAME")
    or os.getenv("SMTP_USER_MAIN")
)
SMTP_PASS = (
    os.getenv("SMTP_PASS")
    or os.getenv("SMTP_APP")
    or os.getenv("SMTP_PASSWORD")
)
FROM_ADDR = os.getenv("SMTP_FROM") or SMTP_USER

# Per-inbox overrides (same pattern as in flows/send_queue.py)
SMTP_USER_JESS = os.getenv("SMTP_USER_JESS")
SMTP_PASS_JESS = os.getenv("SMTP_PASS_JESS") or SMTP_PASS
SMTP_FROM_JESS = os.getenv("SMTP_FROM_JESS") or SMTP_USER_JESS

SMTP_USER_ALEX = os.getenv("SMTP_USER_ALEX")
SMTP_PASS_ALEX = os.getenv("SMTP_PASS_ALEX") or SMTP_PASS
SMTP_FROM_ALEX = os.getenv("SMTP_FROM_ALEX") or SMTP_USER_ALEX

SMTP_USER_ERICA = os.getenv("SMTP_USER_ERICA")
SMTP_PASS_ERICA = os.getenv("SMTP_PASS_ERICA") or SMTP_PASS
SMTP_FROM_ERICA = os.getenv("SMTP_FROM_ERICA") or SMTP_USER_ERICA

SMTP_USER_TEAM = os.getenv("SMTP_USER_TEAM")
SMTP_PASS_TEAM = os.getenv("SMTP_PASS_TEAM") or SMTP_PASS
SMTP_FROM_TEAM = os.getenv("SMTP_FROM_TEAM") or SMTP_USER_TEAM


def _norm(v: Optional[str]) -> Optional[str]:
    return v.strip().lower() if v else None


def _resolve_smtp_credentials(for_email: Optional[str]) -> tuple[str, int, Optional[str], Optional[str], str]:
    """
    Given the desired from_email (inbox address), choose SMTP user/pass and
    envelope sender.

    Mirrors flows/send_queue._resolve_smtp_credentials so we preserve existing
    per-inbox app-password routing.
    """
    addr = (for_email or "").strip().lower()

    # Jess
    if addr and (addr == _norm(SMTP_USER_JESS) or addr == _norm(SMTP_FROM_JESS)):
        user = SMTP_USER_JESS or SMTP_USER
        pwd = SMTP_PASS_JESS or SMTP_PASS
        sender = SMTP_FROM_JESS or addr or FROM_ADDR or user
    # Alex
    elif addr and (addr == _norm(SMTP_USER_ALEX) or addr == _norm(SMTP_FROM_ALEX)):
        user = SMTP_USER_ALEX or SMTP_USER
        pwd = SMTP_PASS_ALEX or SMTP_PASS
        sender = SMTP_FROM_ALEX or addr or FROM_ADDR or user
    # Erica
    elif addr and (addr == _norm(SMTP_USER_ERICA) or addr == _norm(SMTP_FROM_ERICA)):
        user = SMTP_USER_ERICA or SMTP_USER
        pwd = SMTP_PASS_ERICA or SMTP_PASS
        sender = SMTP_FROM_ERICA or addr or FROM_ADDR or user
    # Team
    elif addr and (addr == _norm(SMTP_USER_TEAM) or addr == _norm(SMTP_FROM_TEAM)):
        user = SMTP_USER_TEAM or SMTP_USER
        pwd = SMTP_PASS_TEAM or SMTP_PASS
        sender = SMTP_FROM_TEAM or addr or FROM_ADDR or user
    else:
        # Fallback: use globals
        user = SMTP_USER
        pwd = SMTP_PASS
        sender = (FROM_ADDR or user or addr or "no-reply@klixads.org")

    return SMTP_HOST, SMTP_PORT, user, pwd, sender


def _open_smtp(for_email: Optional[str] = None) -> smtplib.SMTP:
    """
    Open an SMTP connection using credentials appropriate for `for_email`.
    """
    host, port, user, pwd, _ = _resolve_smtp_credentials(for_email)

    s = smtplib.SMTP(host, port, timeout=30)
    s.ehlo()
    try:
        s.starttls()
        s.ehlo()
    except smtplib.SMTPException:
        # Some providers may not support STARTTLS; continue without it.
        pass
    if user and pwd:
        s.login(user, (pwd or "").replace(" ", ""))
    return s


def _smtp_send_raw(
    *,
    to_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str],
    from_email: Optional[str],
) -> str:
    """
    Low-level SMTP send.

    Returns a provider_message_id-like token ("smtp") for now, so callers can
    store something in provider_message_id if desired.
    """
    # Build MIME message
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")

    # Choose envelope + header sender based on from_email
    _, _, _, _, sender = _resolve_smtp_credentials(from_email)
    sender = sender.strip()

    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = sender

    with _open_smtp(sender) as s:
        s.sendmail(sender, [to_email], msg.as_string())

    # For now, Gmail SMTP doesn't give us a message ID easily; we just mark "smtp".
    return "smtp"


class SmtpAppPasswordProvider(BaseEmailProvider):
    """
    Concrete provider that sends via Gmail (or compatible) SMTP using
    app passwords, exactly like the existing v1/v2 logic.

    Provider type key in DB: "smtp_app_pw".
    """

    name = "smtp_app_pw"

    def send(
        self,
        *,
        inbox: Mapping[str, Any],
        to_email: str,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,  # currently unused but reserved
        live: bool = True,
        send_id: Optional[int] = None,
    ) -> SendResult:
        """
        Perform the actual SMTP send.

        If live=False, we *do not* talk to SMTP at all; we simply return a
        successful SendResult with provider_message_id="dry-run".
        """
        from_addr = (inbox.get("email_address") or "").strip().lower()

        if not live:
            # Simulation only; no network calls.
            return SendResult(
                ok=True,
                provider_name=self.name,
                provider_message_id="dry-run",
                extra={"live": False, "reason": "dry-run"},
            )

        try:
            provider_message_id = _smtp_send_raw(
                to_email=to_email,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                from_email=from_addr,
            )
            return SendResult(
                ok=True,
                provider_name=self.name,
                provider_message_id=provider_message_id,
                extra={"live": True},
            )
        except Exception as e:
            return SendResult(
                ok=False,
                provider_name=self.name,
                error=str(e),
                extra={"live": True},
            )
