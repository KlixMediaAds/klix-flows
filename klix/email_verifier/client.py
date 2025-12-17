from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

import dns.resolver

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_BAD_DOMAINS = {
    "domain.com",
    "example.com",
    "example.org",
    "example.net",
    "test.com",
    "email.com",
    "invalid",
    "localhost",
    "local",
}

_BAD_LOCALPARTS = {
    "user",
    "test",
    "example",
    "asdf",
    "qwerty",
    "null",
    "none",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
}

_BAD_TLDS = {"example", "invalid", "localhost", "local", "test"}


def _split_email(email: str) -> Tuple[str, str]:
    local, domain = email.split("@", 1)
    return local.strip().lower(), domain.strip().lower().strip(".")


def _looks_placeholder(email: str) -> bool:
    if "@" not in email:
        return True

    local, domain = _split_email(email)

    if not local or not domain:
        return True

    if local in _BAD_LOCALPARTS:
        return True

    if domain in _BAD_DOMAINS:
        return True

    if "." not in domain:
        return True

    tld = domain.rsplit(".", 1)[-1]
    if tld in _BAD_TLDS:
        return True

    return False


@dataclass
class EmailVerificationResult:
    status: str
    score: Optional[float]
    source: str


class EmailVerifier:
    def __init__(
        self,
        api_key: Optional[str] = None,
        source: str = "dns-mx",
        timeout_seconds: float = 3.0,
        lifetime_seconds: float = 5.0,
    ) -> None:
        self.api_key = api_key
        self.source = source or "dns-mx"
        self.resolver = dns.resolver.Resolver()
        self.resolver.timeout = timeout_seconds
        self.resolver.lifetime = lifetime_seconds

    def verify(self, email: str) -> EmailVerificationResult:
        email = (email or "").strip()

        if not email or len(email) > 254:
            return EmailVerificationResult("invalid", 0.0, self.source)

        if _looks_placeholder(email):
            return EmailVerificationResult("invalid", 0.0, self.source)

        if not _EMAIL_RE.match(email):
            return EmailVerificationResult("invalid", 0.0, self.source)

        domain = email.split("@", 1)[1].lower().strip(".")

        try:
            answers = self.resolver.resolve(domain, "MX")
            if not list(answers):
                return EmailVerificationResult("invalid", 0.0, self.source)
            return EmailVerificationResult("valid", 1.0, self.source)

        except dns.resolver.NXDOMAIN:
            return EmailVerificationResult("invalid", 0.0, self.source)
        except dns.resolver.NoAnswer:
            return EmailVerificationResult("invalid", 0.0, self.source)
        except dns.exception.Timeout:
            return EmailVerificationResult("risky", None, self.source)
        except Exception:
            return EmailVerificationResult("risky", None, self.source)
