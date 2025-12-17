from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from email.utils import parseaddr
from typing import Optional, Tuple

from .email_spine_gmail import RawEmail

logger = logging.getLogger(__name__)


# --- Data structures ---------------------------------------------------------


@dataclass
class FinancialEvent:
    provider: str                   # e.g. 'vultr', 'neon', 'google_workspace'
    event_type: str                 # 'charge' | 'failed_payment' | 'refund' | 'price_change'
    amount: Optional[float] = None
    currency: str = "USD"
    billing_period: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class ClassifiedEmail:
    signal_type: str                # e.g. 'finance_billing', 'finance_failed_payment', 'ops_incident', 'lead'
    importance_score: int
    requires_host_attention: bool
    financial_event: Optional[FinancialEvent] = None


# --- Helpers -----------------------------------------------------------------


def _normalize(text: str) -> str:
    return (text or "").strip().lower()


def _extract_domain(address: str) -> str:
    """
    Extract bare domain from an email address.
    """
    _name, addr = parseaddr(address or "")
    if "@" not in addr:
        return ""
    return addr.split("@", 1)[1].lower()


def _is_newsletter_like(subject: str, snippet: str) -> bool:
    s = _normalize(subject)
    n = _normalize(snippet)
    if "unsubscribe" in n or "manage your preferences" in n:
        return True
    newsletter_keywords = [
        "newsletter",
        "roundup",
        "digest",
        "weekly update",
        "product updates",
        "what's new",
    ]
    return any(k in s for k in newsletter_keywords)


def _is_security_alert(subject: str, snippet: str, from_domain: str) -> bool:
    s = _normalize(subject)
    n = _normalize(snippet)

    security_keywords = [
        "security alert",
        "suspicious login",
        "new sign-in",
        "new login",
        "password reset",
        "2-step verification",
        "two-factor authentication",
        "verification code",
        "unusual activity",
    ]
    if any(k in s for k in security_keywords):
        return True

    if any(k in n for k in security_keywords):
        return True

    # common security-heavy domains
    if from_domain in {"google.com", "accounts.google.com", "github.com"} and (
        "security" in s or "security" in n
    ):
        return True

    return False


_AMOUNT_REGEX = re.compile(
    r"(?P<currency>\$|usd|cad|eur|gbp)?\s*(?P<amount>\d+(?:\.\d{1,2})?)",
    re.IGNORECASE,
)


def _extract_amount_and_currency(text: str) -> Tuple[Optional[float], str]:
    """
    Very light regex-based extraction of an amount + currency from text.
    Defaults to USD if currency not detectable.
    """
    match = _AMOUNT_REGEX.search(text or "")
    if not match:
        return None, "USD"

    amount_str = match.group("amount")
    currency_token = (match.group("currency") or "").upper()

    try:
        amount = float(amount_str)
    except ValueError:
        return None, "USD"

    currency_map = {
        "$": "USD",
        "USD": "USD",
        "CAD": "CAD",
        "EUR": "EUR",
        "GBP": "GBP",
    }
    currency = currency_map.get(currency_token, "USD")
    return amount, currency


def _guess_billing_period(subject: str, snippet: str) -> Optional[str]:
    s = _normalize(subject + " " + snippet)
    # very rough: infer month tokens like "December 2025", "Dec 2025"
    month_regex = re.compile(
        r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{4}",
        re.IGNORECASE,
    )
    m = month_regex.search(s)
    if m:
        return m.group(0)
    return None


def _detect_financial_event(
    subject: str,
    snippet: str,
    from_domain: str,
) -> Optional[FinancialEvent]:
    """
    Detect whether this email is a financial event, and if so, classify it.
    """
    s = _normalize(subject)
    n = _normalize(snippet)
    text = f"{subject} {snippet}"

    # Rough provider detection by domain
    provider_by_domain = {
        "vultr.com": "vultr",
        "neon.tech": "neon",
        "stripe.com": "stripe",
        "paypal.com": "paypal",
        "billing.google.com": "google_billing",
        "google.com": "google_workspace",
        "notion.so": "notion",
        "linear.app": "linear",
    }
    provider = provider_by_domain.get(from_domain, from_domain or "unknown")

    # Determine event_type
    if any(k in s for k in ["payment failed", "failed payment", "card declined", "declined"]):
        event_type = "failed_payment"
    elif any(k in s for k in ["receipt", "invoice", "payment received", "payment succeeded"]):
        event_type = "charge"
    elif "refund" in s:
        event_type = "refund"
    elif "price change" in s or "pricing update" in s:
        event_type = "price_change"
    else:
        # If nothing obviously financial, bail
        financial_keywords = ["invoice", "receipt", "payment", "charged", "billing", "subscription"]
        if not any(k in s for k in financial_keywords):
            return None
        event_type = "charge"

    amount, currency = _extract_amount_and_currency(text)
    billing_period = _guess_billing_period(subject, snippet)

    notes = None
    if amount is None:
        notes = "financial email detected, amount not parsed"

    return FinancialEvent(
        provider=provider,
        event_type=event_type,
        amount=amount,
        currency=currency,
        billing_period=billing_period,
        notes=notes,
    )


def _is_school_admin(subject: str, from_domain: str) -> bool:
    s = _normalize(subject)
    if "humber" in from_domain or "humber.ca" in from_domain:
        return True
    keywords = ["field experience", "course", "assignment", "registration", "tuition"]
    return any(k in s for k in keywords)


def _is_personal_admin(subject: str, from_domain: str) -> bool:
    s = _normalize(subject)
    personal_keywords = ["appointment", "booking", "confirmation", "receipt"]
    if any(k in s for k in personal_keywords):
        return True
    if from_domain.endswith(("gmail.com", "outlook.com", "hotmail.com")):
        if "re:" in s or "fw:" in s:
            return True
    return False


def _is_lead_reply(subject: str, snippet: str, to_address: str) -> bool:
    """
    Detect replies / interest signals, with a strong bias toward cold inbox traffic.
    """
    s = _normalize(subject)
    n = _normalize(snippet)
    to_addr = (to_address or "").lower()

    cold_inboxes = {
        "jess@klixads.ca",
        "erica@klixmedia.ca",
        "alex@klixads.org",
        "team@klixmedia.org",
    }

    # 0) If it's a cold inbox target, default to treating as lead unless clearly a newsletter
    if to_addr in cold_inboxes:
        if "unsubscribe" in n or "manage your preferences" in n:
            return False
        return True

    # Simple heuristic: replies to our outbound emails
    if s.startswith("re:") or s.startswith("fw:") or s.startswith("fwd:"):
        # Avoid obvious newsletters even if they reply-style
        if "unsubscribe" in n:
            return False
        return True

    # Catch "interested", "curious", etc.
    reply_keywords = ["interested", "let's chat", "call", "meeting", "proposal"]
    if any(k in n for k in reply_keywords):
        return True

    return False


# --- Main classification logic ----------------------------------------------


def classify_email(raw: RawEmail) -> ClassifiedEmail:
    """
    Rule-based classifier for RawEmail → signal_type, importance, attention, financial_event.

    This is deterministic, side-effect free, and safe to evolve over time.
    """
    subject = raw.subject or ""
    snippet = raw.snippet or ""
    from_domain = _extract_domain(raw.from_address)
    to_addr = raw.to_address or ""

    s_norm = _normalize(subject)
    n_norm = _normalize(snippet)

    # 1) Security / account alerts
    if _is_security_alert(subject, snippet, from_domain):
        return ClassifiedEmail(
            signal_type="security",
            importance_score=90,
            requires_host_attention=True,
            financial_event=None,
        )

    # 2) Financial events
    financial_event = _detect_financial_event(subject, snippet, from_domain)
    if financial_event:
        if financial_event.event_type == "failed_payment":
            return ClassifiedEmail(
                signal_type="finance_failed_payment",
                importance_score=95,
                requires_host_attention=True,
                financial_event=financial_event,
            )
        else:
            # Normal infra billing / charges
            return ClassifiedEmail(
                signal_type="finance_billing",
                importance_score=75,
                requires_host_attention=False,
                financial_event=financial_event,
            )

    # 3) Lead replies / sales signals
    if _is_lead_reply(subject, snippet, to_addr):
        return ClassifiedEmail(
            signal_type="lead",
            importance_score=85,
            requires_host_attention=True,
            financial_event=None,
        )

    # 4) School admin / program emails
    if _is_school_admin(subject, from_domain):
        return ClassifiedEmail(
            signal_type="school_admin",
            importance_score=70,
            requires_host_attention=True,
            financial_event=None,
        )

    # 5) Personal admin / life logistics
    if _is_personal_admin(subject, from_domain):
        return ClassifiedEmail(
            signal_type="personal_admin",
            importance_score=55,
            requires_host_attention=False,
            financial_event=None,
        )

    # 6) Obvious newsletters / low-priority promos
    if _is_newsletter_like(subject, snippet):
        return ClassifiedEmail(
            signal_type="newsletter",
            importance_score=10,
            requires_host_attention=False,
            financial_event=None,
        )

    # 7) Noise / low-signal messages (ads, promos)
    promo_keywords = ["sale", "discount", "offer", "promo", "promotion"]
    if any(k in s_norm for k in promo_keywords):
        return ClassifiedEmail(
            signal_type="noise",
            importance_score=5,
            requires_host_attention=False,
            financial_event=None,
        )

    # 8) Default → unknown
    return ClassifiedEmail(
        signal_type="unknown",
        importance_score=0,
        requires_host_attention=False,
        financial_event=None,
    )


__all__ = ["FinancialEvent", "ClassifiedEmail", "classify_email"]
