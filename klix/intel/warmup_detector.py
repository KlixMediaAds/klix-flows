"""
Warmup / noise detector for email replies.

This module provides deterministic rules to classify replies as:
- warmup deliverability tests
- auto-replies / out-of-office
- bounces / delivery failures
- generic noise

It is intentionally:
- side-effect free
- pure Python
- reusable from ingest flows, classifiers, and governance checks
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class WarmupDetectionInput:
    """Minimal fields needed for warmup / noise detection."""

    subject: Optional[str] = None
    body: Optional[str] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    provider_message_id: Optional[str] = None


@dataclass
class WarmupDetectionResult:
    """
    Result of warmup / noise detection.

    is_warmup:
        True  -> definitely a warmup deliverability email.
        False -> not a warmup (but may still be other noise).

    noise_type:
        'warmup' | 'auto_reply' | 'bounce' | 'other_noise' | None
    """

    is_warmup: bool
    noise_type: Optional[str]
    reason: str


# ---------------------------------------------------------------------
#  Internal helpers (pattern detectors)
# ---------------------------------------------------------------------


def _normalize(text: Optional[str]) -> str:
    return (text or "").strip()


def _is_instantly_style_warmup(subject: str, body: str) -> bool:
    """
    Detect Instantly-style warmup replies by pattern, **without** hardcoding
    specific IDs.

    Mirrors the logic currently used in scripts/classify_replies_once.py:

      Examples:
        "Jess - still hiring? | DTSHV8V 7QMG3D1"
        "New customers? | YD3WJ4P 7QMG3D1"

      General rule:
        - After a " | " in the subject, we see two or more code-like tokens:
          [A-Z0-9]{5,10} [A-Z0-9]{5,10} ...
    """
    subject = subject or ""
    body = body or ""

    # Look at the part after " | " in the subject.
    if "|" in subject:
        tail = subject.split("|", 1)[1].strip()
        tokens = tail.split()
        code_tokens = [t for t in tokens if re.fullmatch(r"[A-Z0-9]{5,10}", t)]
        if len(code_tokens) >= 2:
            return True

    # Extra safety: if both subject and body are full of short all-caps codes,
    # treat as warmup too. This keeps behaviour consistent with existing logic.
    combined = f"{subject} {body}"
    codes = re.findall(r"\\b[A-Z0-9]{5,10}\\b", combined)
    if len(codes) >= 4 and "|" in subject:
        return True

    return False


def _is_auto_reply(text_all: str) -> bool:
    lower = text_all.lower()
    ooo_phrases = [
        "out of office",
        "out-of-office",
        "automatic reply",
        "auto reply",
        "autoreply",
        "i am currently away from the office",
        "i am away from the office",
    ]
    return any(p in lower for p in ooo_phrases)


def _is_bounce(text_all: str, from_address: Optional[str]) -> bool:
    lower = text_all.lower()
    bounce_phrases = [
        "delivery has failed",
        "delivery failed",
        "your message could not be delivered",
        "your message couldn't be delivered",
        "mail delivery subsystem",
        "undeliverable",
        "returned to sender",
        "permanent error",
        "permanent failure",
    ]
    if any(p in lower for p in bounce_phrases):
        return True

    # Some providers use specific from addresses for bounces
    if from_address:
        from_lower = from_address.lower()
        if any(k in from_lower for k in ["mailer-daemon", "postmaster", "no-reply@google.com"]):
            return True

    return False


def _is_generic_warmup_text(text_all: str) -> bool:
    """
    Extra safety net for explicit warmup explanations used by some tools.
    """
    lower = text_all.lower()
    phrases = [
        "this is a warmup email",
        "warmup email",
        "warming up your inbox",
        "making sure your emails land in the inbox",
        "deliverability warmup",
        "email warmup service",
    ]
    return any(p in lower for p in phrases)


# ---------------------------------------------------------------------
#  Public API
# ---------------------------------------------------------------------


def detect_warmup(
    subject: Optional[str] = None,
    body: Optional[str] = None,
    from_address: Optional[str] = None,
    to_address: Optional[str] = None,
    provider_message_id: Optional[str] = None,
) -> WarmupDetectionResult:
    """
    Main entrypoint.

    Returns:
        WarmupDetectionResult(
            is_warmup: bool,
            noise_type: Optional[str],
            reason: str
        )

    Contract (v1.0):
        - is_warmup == True  => noise_type is "warmup"
        - is_warmup == False => noise_type may still be:
              "auto_reply", "bounce", "other_noise", or None
    """
    subj = _normalize(subject)
    body_norm = _normalize(body)
    text_all = f"{subj}\\n{body_norm}".strip()

    # 1) Strong warmup signatures (Instantly-style subject codes or explicit text)
    if _is_instantly_style_warmup(subj, body_norm) or _is_generic_warmup_text(text_all):
        return WarmupDetectionResult(
            is_warmup=True,
            noise_type="warmup",
            reason="warmup_detector: instantly-style codes or explicit warmup phrasing",
        )

    # 2) Auto-reply / OOO
    if _is_auto_reply(text_all):
        return WarmupDetectionResult(
            is_warmup=False,
            noise_type="auto_reply",
            reason="warmup_detector: out-of-office / automatic reply pattern",
        )

    # 3) Bounce / delivery failure
    if _is_bounce(text_all, from_address):
        return WarmupDetectionResult(
            is_warmup=False,
            noise_type="bounce",
            reason="warmup_detector: delivery failure / bounce pattern",
        )

    # 4) No clear warmup / auto-reply / bounce pattern detected
    return WarmupDetectionResult(
        is_warmup=False,
        noise_type=None,
        reason="warmup_detector: no warmup/noise pattern detected",
    )


def detect_warmup_from_row(row: WarmupDetectionInput) -> WarmupDetectionResult:
    """
    Convenience wrapper for places that pass a structured object / row.
    """
    return detect_warmup(
        subject=row.subject,
        body=row.body,
        from_address=row.from_address,
        to_address=row.to_address,
        provider_message_id=row.provider_message_id,
    )
