from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# Very simple RFC-ish email regex for mock verification.
# This is intentionally conservative and only used for tag-only Phase 1.
_EMAIL_REGEX = re.compile(
    r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
    re.IGNORECASE,
)


@dataclass
class EmailVerificationResult:
    """
    Normalized result for a single email verification.

    status:
        "valid"   - looks syntactically valid
        "invalid" - clearly not an email
        "risky"   - verifier could not be confident (errors, etc.)

    score:
        Optional numeric confidence score. For the mock verifier we use:
        - 1.0 for "valid"
        - 0.0 for "invalid"
        - None for "risky" / unknown
    """
    status: str               # "valid" | "invalid" | "risky" | "catch_all" (future)
    score: Optional[float]
    source: str               # e.g. "mock-syntax", "neverbounce", etc.


class EmailVerifier:
    """
    Small abstraction over email verification.

    Phase 1 uses a mock syntax-only verifier so you can ship the
    Email Hygiene & Verification Spine without committing to a provider.
    Later, you can swap the internals here to call a real API.
    """

    def __init__(self, api_key: Optional[str] = None, source: str = "mock-syntax") -> None:
        # api_key is accepted for future provider integration but unused in mock mode.
        self.api_key = api_key
        self.source = source

    def verify(self, email: str) -> EmailVerificationResult:
        """
        Verify an email address.

        This method MUST NOT raise. On any unexpected error it will return
        a 'risky' status so callers can decide how to treat it.

        For now:
        - Trim whitespace
        - Reject empty strings
        - Run a basic regex check
        """
        try:
            if email is None:
                return EmailVerificationResult(status="invalid", score=0.0, source=self.source)

            normalized = email.strip()
            if not normalized:
                return EmailVerificationResult(status="invalid", score=0.0, source=self.source)

            # Basic syntax check.
            if not _EMAIL_REGEX.match(normalized):
                return EmailVerificationResult(status="invalid", score=0.0, source=self.source)

            # If we got here, it looks okay syntactically.
            # In a real provider-backed version, you would:
            # - call the API
            # - map provider codes to our status/score
            return EmailVerificationResult(status="valid", score=1.0, source=self.source)

        except Exception:
            # Never propagate errors to the caller. Treat as "risky".
            return EmailVerificationResult(status="risky", score=None, source=self.source)
