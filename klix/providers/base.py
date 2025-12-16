"""
Provider abstraction layer — base interfaces.

This module defines:
- SendResult: normalized result of a provider send attempt.
- BaseEmailProvider: interface all providers must implement.

Era 1.8 scope:
- Only one concrete provider exists: SmtpAppPasswordProvider (smtp_app_pw).
- send_queue_v2 will call providers via a helper in klix.providers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional


@dataclass
class SendResult:
    """
    Normalized result from a provider send() call.

    Fields:
      ok:                  True if the provider considers the send successful.
      provider_name:       Short identifier for the provider (e.g. "smtp_app_pw").
      provider_message_id: Optional provider-level message ID (if available).
      error:               Human-readable error string on failure (optional).
      extra:               Any additional provider-specific metadata you may want
                           to log or inspect later.
    """

    ok: bool
    provider_name: str
    provider_message_id: Optional[str] = None
    error: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


class BaseEmailProvider:
    """
    Base interface for email providers.

    All concrete providers must implement .send() with this signature and
    return a SendResult object. Providers should not mutate the inbox dict.
    """

    name: str = "base"

    def send(
        self,
        *,
        inbox: Mapping[str, Any],
        to_email: str,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        live: bool = True,
        send_id: Optional[int] = None,
    ) -> SendResult:
        """
        Send an email using the given inbox + payload.

        Arguments:
          inbox:      Mapping containing at least email_address + domain. May
                      also include provider_type / provider_config.
          to_email:   Recipient address.
          subject:    Subject line.
          body_text:  Plain-text body.
          body_html:  Optional HTML body.
          headers:    Optional extra headers to add (Reply-To, Message-ID, etc.).
          live:       If False, *must not* actually contact the provider, but
                      should still return SendResult(ok=True, ...).
          send_id:    Optional email_sends.id for logging / correlation.

        Returns:
          SendResult describing success/failure.
        """
        raise NotImplementedError("BaseEmailProvider.send() must be implemented by subclasses")
