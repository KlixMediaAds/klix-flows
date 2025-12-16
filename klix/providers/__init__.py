"""
Provider abstraction entrypoint.

Era 1.8+:
- smtp_app_pw (Gmail app passwords) is the legacy/default provider.
- gmail_oauth (Gmail via OAuth2 XOAUTH2) is the new Era 1.9 provider.
- Inbox rows use provider_type + provider_config to choose behavior.
"""

from __future__ import annotations

from typing import Dict, Optional

from .base import BaseEmailProvider, SendResult  # re-export
from .smtp_app_pw import SmtpAppPasswordProvider
from .gmail_oauth import GmailOAuthProvider


# Single shared instance per provider type (providers are stateless, so safe to reuse)
_smtp_app_pw = SmtpAppPasswordProvider()
_gmail_oauth = GmailOAuthProvider()

_PROVIDER_REGISTRY: Dict[str, BaseEmailProvider] = {
    # Legacy app-password SMTP (Era 1.8)
    "smtp_app_pw": _smtp_app_pw,
    # Alias for clarity if you ever used this name in DB
    "gmail_app_pw": _smtp_app_pw,

    # New Gmail OAuth provider (Era 1.9)
    "gmail_oauth": _gmail_oauth,
}

# Default provider type if inbox.provider_type is missing/unknown
DEFAULT_PROVIDER_TYPE = "smtp_app_pw"


def get_provider(provider_type: Optional[str]) -> BaseEmailProvider:
    """
    Resolve a provider instance from a provider_type string.

    Unknown provider types fall back to DEFAULT_PROVIDER_TYPE (smtp_app_pw).
    """
    key = (provider_type or DEFAULT_PROVIDER_TYPE).strip().lower()
    return _PROVIDER_REGISTRY.get(key, _smtp_app_pw)
