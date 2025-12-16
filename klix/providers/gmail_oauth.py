"""
Gmail OAuth provider (gmail_oauth).

v1 design:
- Uses OAuth2 refresh_token per inbox (stored in inbox["provider_config"]["oauth"]["refresh_token"])
- Exchanges refresh_token -> access_token on each send via Google's token endpoint
- Uses SMTP + XOAUTH2 to send, reusing your existing MIME-style flow

This keeps Era 1.9 focused on "change auth layer, not full sending stack".
"""

from __future__ import annotations

import base64
import json
import os
import smtplib
import urllib.parse
import urllib.request
from typing import Any, Dict, Mapping, Optional

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from .base import BaseEmailProvider, SendResult


# ---------------------------------------------------------------------------
# OAuth + SMTP config
# ---------------------------------------------------------------------------

# Google OAuth2 token endpoint
GMAIL_OAUTH_TOKEN_URL = os.getenv("GMAIL_OAUTH_TOKEN_URL", "https://oauth2.googleapis.com/token")

# OAuth client credentials (v1: global)
GMAIL_OAUTH_CLIENT_ID = os.getenv("GMAIL_OAUTH_CLIENT_ID") or ""
GMAIL_OAUTH_CLIENT_SECRET = os.getenv("GMAIL_OAUTH_CLIENT_SECRET") or ""

# SMTP host/port (still Gmail for v1)
GMAIL_SMTP_HOST = os.getenv("GMAIL_SMTP_HOST", os.getenv("SMTP_HOST", "smtp.gmail.com"))
GMAIL_SMTP_PORT = int(os.getenv("GMAIL_SMTP_PORT", os.getenv("SMTP_PORT", "587")))


def _build_mime_message(
    *,
    from_email: str,
    to_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str],
) -> str:
    """
    Build a MIME message string for sending via SMTP.
    """
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEText(body_text or "", "plain", "utf-8")

    msg["Subject"] = subject or ""
    msg["To"] = to_email
    msg["From"] = from_email

    return msg.as_string()


def _smtp_send_xoauth2(
    *,
    from_email: str,
    to_email: str,
    mime_message: str,
    access_token: str,
) -> str:
    """
    Send a prepared MIME message over SMTP using XOAUTH2.

    Returns a simple provider_message_id token for storage/logging.
    """
    # Construct XOAUTH2 auth string per Gmail's spec
    auth_str = f"user={from_email}\x01auth=Bearer {access_token}\x01\x01".encode("utf-8")
    auth_b64 = base64.b64encode(auth_str).decode("ascii")

    s = smtplib.SMTP(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT, timeout=30)
    try:
        s.ehlo()
        try:
            s.starttls()
            s.ehlo()
        except smtplib.SMTPException:
            # If STARTTLS fails (unlikely with Gmail), continue without it.
            pass

        code, resp = s.docmd("AUTH", "XOAUTH2 " + auth_b64)
        if code != 235:
            raise smtplib.SMTPAuthenticationError(code, resp)

        s.sendmail(from_email, [to_email], mime_message)
    finally:
        try:
            s.quit()
        except Exception:
            pass

    # Gmail SMTP doesn't echo a message-id easily; we return a stable marker.
    return "gmail-oauth-smtp"


def _http_post_form(url: str, data: Dict[str, str]) -> Dict[str, Any]:
    """
    Minimal HTTP POST helper using stdlib only (no external deps).

    Sends application/x-www-form-urlencoded and parses JSON response.
    """
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"raw": body}


class GmailOAuthProvider(BaseEmailProvider):
    """
    Concrete provider using Gmail OAuth (SMTP + XOAUTH2).

    Provider type key in DB: "gmail_oauth".

    Requirements per inbox:
      inbox["provider_config"] is a dict-like JSON with:
        {
          "oauth": {
            "refresh_token": "...."
          },
          "gmail": {
            "email": "jess@klixads.ca"  # optional, falls back to inbox.email_address
          }
        }
    """

    name = "gmail_oauth"

    def _get_from_email(self, inbox: Mapping[str, Any]) -> str:
        # Prefer explicit gmail.email from provider_config, fall back to email_address
        provider_config = inbox.get("provider_config") or {}
        if isinstance(provider_config, str):
            try:
                provider_config = json.loads(provider_config)
            except Exception:
                provider_config = {}

        gmail_block = provider_config.get("gmail") or {}
        from_email = (
            gmail_block.get("email")
            or inbox.get("email_address")
            or ""
        )
        from_email = (str(from_email) or "").strip()
        if not from_email:
            raise ValueError(
                "GmailOAuthProvider: from_email is empty; check inbox.email_address/provider_config.gmail.email"
            )
        return from_email

    def _get_refresh_token(self, inbox: Mapping[str, Any]) -> str:
        provider_config = inbox.get("provider_config") or {}
        if isinstance(provider_config, str):
            try:
                provider_config = json.loads(provider_config)
            except Exception:
                provider_config = {}

        oauth_block = provider_config.get("oauth") or {}
        refresh_token = (oauth_block.get("refresh_token") or "").strip()
        if not refresh_token:
            raise ValueError("GmailOAuthProvider: missing oauth.refresh_token in provider_config")
        return refresh_token

    def _exchange_refresh_token(self, refresh_token: str) -> str:
        """
        Exchange a refresh_token for an access_token using Google's token endpoint.

        For v1, we do not persist the new access_token/expiry back to DB; we simply
        use the fresh token for this send. This keeps the provider fully stateless
        and avoids schema assumptions, at the cost of one HTTP call per send.
        """
        if not GMAIL_OAUTH_CLIENT_ID or not GMAIL_OAUTH_CLIENT_SECRET:
            raise RuntimeError("GmailOAuthProvider: GMAIL_OAUTH_CLIENT_ID/SECRET not set in environment")

        payload = {
            "client_id": GMAIL_OAUTH_CLIENT_ID,
            "client_secret": GMAIL_OAUTH_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        resp = _http_post_form(GMAIL_OAUTH_TOKEN_URL, payload)

        # Google success shape includes "access_token" and "token_type"
        access_token = resp.get("access_token")
        if not access_token:
            # Try to surface error details
            error_desc = resp.get("error_description") or resp.get("error") or "unknown_error"
            raise RuntimeError(f"GmailOAuthProvider: failed to refresh access token: {error_desc}")

        return str(access_token)

    def send(
        self,
        *,
        inbox: Mapping[str, Any],
        to_email: str,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,  # reserved for future use
        live: bool = True,
        send_id: Optional[int] = None,
    ) -> SendResult:
        """
        Perform an OAuth-backed SMTP send.

        If live=False, we do NOT talk to Google or SMTP; we simply return a
        successful SendResult with provider_message_id="dry-run".
        """
        # Dry run: no external calls
        if not live:
            return SendResult(
                ok=True,
                provider_name=self.name,
                provider_message_id="dry-run",
                extra={"live": False, "reason": "dry-run"},
            )

        # Step 0: resolve config from inbox row
        try:
            from_email = self._get_from_email(inbox)
            refresh_token = self._get_refresh_token(inbox)
        except Exception as e:
            return SendResult(
                ok=False,
                provider_name=self.name,
                error=str(e),
                extra={"stage": "config"},
            )

        # Step 1: get access_token from refresh_token
        try:
            access_token = self._exchange_refresh_token(refresh_token)
        except Exception as e:
            return SendResult(
                ok=False,
                provider_name=self.name,
                error=str(e),
                extra={"stage": "token_refresh"},
            )

        # Step 2: build MIME message
        try:
            mime_message = _build_mime_message(
                from_email=from_email,
                to_email=to_email,
                subject=subject,
                body_text=body_text,
                body_html=body_html,
            )
        except Exception as e:
            return SendResult(
                ok=False,
                provider_name=self.name,
                error=f"failed to build MIME message: {e}",
                extra={"stage": "mime_build"},
            )

        # Step 3: send via SMTP + XOAUTH2
        try:
            provider_message_id = _smtp_send_xoauth2(
                from_email=from_email,
                to_email=to_email,
                mime_message=mime_message,
                access_token=access_token,
            )
            return SendResult(
                ok=True,
                provider_name=self.name,
                provider_message_id=provider_message_id,
                extra={"live": True, "auth": "oauth2_xoauth2"},
            )
        except Exception as e:
            return SendResult(
                ok=False,
                provider_name=self.name,
                error=str(e),
                extra={"stage": "smtp_send", "live": True},
            )
