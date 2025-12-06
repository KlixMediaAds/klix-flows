from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import logging
import re

from .email_spine_gmail import RawEmail

logger = logging.getLogger(__name__)


@dataclass
class ClassifiedEmail:
    signal_type: str
    importance_score: int
    requires_host_attention: bool
    metadata: Dict[str, Any]
    financial_event: Optional[Dict[str, Any]] = None  # for financial_email_events rows


# --- Vendor + pattern config (v1, simple + explicit) --- #

FINANCE_VENDORS = {
    "vultr.com": "vultr",
    "neon.tech": "neon",
    "stripe.com": "stripe",
    "paypal.com": "paypal",
    "goguardian.com": "goguardian",
    "google.com": "google",
    "billing.google.com": "google",
    "namecheap.com": "namecheap",
}

SCHOOL_DOMAINS = {
    "humber.ca",
    "humbermail.ca",
}

SECURITY_KEYWORDS = [
    "security alert",
    "suspicious",
    "unusual sign-in",
    "unusual login",
    "new sign-in",
    "new login",
    "was blocked",
    "password change",
    "verify it’s you",
]

FAILED_PAYMENT_KEYWORDS = [
    "payment failed",
    "payment declined",
    "card declined",
    "unable to process your payment",
    "action required on your payment",
    "couldn't charge your card",
    "cannot charge your card",
]

BILLING_KEYWORDS = [
    "invoice",
    "receipt",
    "payment received",
    "thanks for your payment",
    "has been charged",
    "has been billed",
    "your order",
    "subscription renewal",
    "renewal notice",
]

NEWSLETTER_HINTS = [
    "unsubscribe",
    "manage preferences",
    "view this email in your browser",
]

PROMO_HINTS = [
    "sale",
    "discount",
    "offer",
    "limited time",
    "coupon",
]


CURRENCY_PATTERN = re.compile(
    r"(?P<currency>\$|usd|cad|eur)?\s*(?P<amount>\d{1,6}(?:[.,]\d{2})?)",
    re.IGNORECASE,
)

BILLING_PERIOD_HINTS = {
    "per month": "monthly",
    "monthly": "monthly",
    "every month": "monthly",
    "per year": "yearly",
    "yearly": "yearly",
    "annual": "yearly",
    "one-time": "one_off",
    "one time": "one_off",
}


def _lower(s: str) -> str:
    return (s or "").lower()


def _extract_domain(addr: str) -> str:
    addr = addr or ""
    if "<" in addr and ">" in addr:
        # "Name <email@domain.com>"
        inside = addr.split("<", 1)[1].split(">", 1)[0]
        addr = inside.strip()

    parts = addr.split("@")
    if len(parts) != 2:
        return ""
    return parts[1].lower()


def _detect_finance_vendor(from_addr: str) -> Optional[str]:
    domain = _extract_domain(from_addr)
    for needle, provider in FINANCE_VENDORS.items():
        if needle in domain:
            return provider
    return None


def _detect_school(from_addr: str) -> bool:
    domain = _extract_domain(from_addr)
    return any(d in domain for d in SCHOOL_DOMAINS)


def _contains_any(text: str, needles: list[str]) -> bool:
    text_l = _lower(text)
    return any(n in text_l for n in needles)


def _guess_billing_period(text: str) -> Optional[str]:
    text_l = _lower(text)
    for phrase, period in BILLING_PERIOD_HINTS.items():
        if phrase in text_l:
            return period
    return None


def _extract_amount_and_currency(text: str) -> Tuple[Optional[float], Optional[str]]:
    text_l = _lower(text)
    match = CURRENCY_PATTERN.search(text_l)
    if not match:
        return None, None

    raw_amount = match.group("amount")
    raw_currency = match.group("currency") or "usd"

    try:
        amount = float(raw_amount.replace(",", "."))
    except ValueError:
        amount = None

    currency = raw_currency.strip().upper()
    if currency == "$":
        currency = "USD"

    return amount, currency


def classify_email(raw: RawEmail) -> ClassifiedEmail:
    """
    Pure rule-based classification for v1.

    Returns:
        ClassifiedEmail with:
            - signal_type
            - importance_score (0–100)
            - requires_host_attention
            - metadata (for email_events.metadata)
            - financial_event (for financial_email_events) or None
    """
    subject_l = _lower(raw.subject)
    snippet_l = _lower(raw.snippet)
    from_addr = raw.from_address or ""
    meta: Dict[str, Any] = {}

    signal_type = "unknown"
    importance = 0
    requires_host_attention = False
    financial_event: Optional[Dict[str, Any]] = None

    # --- 1. Finance detection (billing / failed payments) --- #
    finance_vendor = _detect_finance_vendor(from_addr)
    is_failed_payment = _contains_any(subject_l + " " + snippet_l, FAILED_PAYMENT_KEYWORDS)
    is_billing = _contains_any(subject_l + " " + snippet_l, BILLING_KEYWORDS)

    if finance_vendor and is_failed_payment:
        signal_type = "finance_failed_payment"
        importance = 95
        requires_host_attention = True

        amount, currency = _extract_amount_and_currency(raw.subject + " " + raw.snippet)
        billing_period = _guess_billing_period(raw.subject + " " + raw.snippet)

        financial_event = {
            "provider": finance_vendor,
            "event_type": "failed_payment",
            "amount": amount,
            "currency": currency or "USD",
            "billing_period": billing_period,
            "raw_payload": raw.payload,
        }
        meta.update(
            {
                "provider": finance_vendor,
                "event_type": "failed_payment",
                "amount": amount,
                "currency": currency,
                "billing_period": billing_period,
            }
        )
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
            financial_event=financial_event,
        )

    if finance_vendor and is_billing:
        signal_type = "finance_billing"
        importance = 75
        requires_host_attention = False  # not always critical, but high signal

        amount, currency = _extract_amount_and_currency(raw.subject + " " + raw.snippet)
        billing_period = _guess_billing_period(raw.subject + " " + raw.snippet)

        financial_event = {
            "provider": finance_vendor,
            "event_type": "charge",
            "amount": amount,
            "currency": currency or "USD",
            "billing_period": billing_period,
            "raw_payload": raw.payload,
        }
        meta.update(
            {
                "provider": finance_vendor,
                "event_type": "charge",
                "amount": amount,
                "currency": currency,
                "billing_period": billing_period,
            }
        )
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
            financial_event=financial_event,
        )

    # --- 2. Security --- #
    if _contains_any(subject_l + " " + snippet_l, SECURITY_KEYWORDS) or "security alert" in subject_l:
        signal_type = "security"
        importance = 92
        requires_host_attention = True
        meta["reason"] = "security_keywords"
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
        )

    # --- 3. School admin --- #
    if _detect_school(from_addr):
        signal_type = "school_admin"
        importance = 70
        requires_host_attention = True
        meta["reason"] = "school_domain"
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
        )

    # --- 4. Lead / replies to outreach --- #
    # Simple v1 heuristic: Re: or Fwd: plus not from your own addresses/domains.
    is_reply = subject_l.startswith("re:") or subject_l.startswith("fwd:")
    from_domain = _extract_domain(from_addr)
    is_own_domain = any(
        needle in from_domain
        for needle in ["gmail.com", "klixmedia", "klix", "kolasajosh", "favourdesirous"]
    )

    if is_reply and not is_own_domain:
        signal_type = "lead"
        importance = 85
        requires_host_attention = True
        meta["reason"] = "reply_from_external"
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
        )

    # --- 5. Newsletter / promotion / noise --- #
    if _contains_any(subject_l + " " + snippet_l, NEWSLETTER_HINTS):
        signal_type = "newsletter"
        importance = 10
        requires_host_attention = False
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
        )

    if _contains_any(subject_l + " " + snippet_l, PROMO_HINTS):
        signal_type = "promotion"
        importance = 5
        requires_host_attention = False
        return ClassifiedEmail(
            signal_type=signal_type,
            importance_score=importance,
            requires_host_attention=requires_host_attention,
            metadata=meta,
        )

    # --- Default: unknown / noise --- #
    # Future: could distinguish 'personal_admin' vs 'noise' with more rules.
    signal_type = "unknown"
    importance = 0
    requires_host_attention = False

    return ClassifiedEmail(
        signal_type=signal_type,
        importance_score=importance,
        requires_host_attention=requires_host_attention,
        metadata=meta,
    )
