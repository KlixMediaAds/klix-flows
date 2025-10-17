# lib/bounce.py
# Classify SMTP/DSN bounce text into 'hard' or 'soft' (conservative default: soft)
# - 'hard' => permanent address/domain problems (user unknown, no such domain, bad mailbox)
# - 'soft' => temporary/policy/quota/network issues (rate limits, mailbox full, SPF/DKIM/DMARC/policy)
#
# Exports:
#   classify(message: str) -> 'hard' | 'soft'
#   classify_bounce(message: str) -> alias of classify

from __future__ import annotations
import re
from typing import Optional

# DSN status codes like "5.1.1", "4.2.2"
DSN_RX = re.compile(r"\b([245])\.(\d)\.(\d)\b")
# SMTP numeric codes like "550", "451"
SMTP_NUM_RX = re.compile(r"\b([245])\d{2}\b")

# Heuristic keyword groups (all matched on lowercased text)
HARD_KEYWORDS = [
    # recipient / address problems
    "user unknown", "unknown user", "no such user",
    "recipient address rejected", "bad destination mailbox address",
    "invalid recipient", "mailbox unavailable", "mailbox does not exist",
    "unrouteable address", "address does not exist",
    # domain / routing problems (usually permanent)
    "domain does not exist", "host or domain name not found", "nxdomain",
    "unresolvable destination domain", "no mx record",
]

SOFT_KEYWORDS = [
    # temporary / resource issues
    "mailbox full", "over quota", "quota exceeded", "resources temporarily unavailable",
    "temporary failure", "try again later", "server busy", "rate limit", "rate limited",
    "too many connections", "connection timed out",
    # greylists & deferals
    "greylist", "graylist", "temporar",  # covers "temporary"
]

POLICY_SOFT = [
    # treat policy/auth/content blocks as soft (fixable): do NOT DNC the address
    "spam", "blocked", "policy", "blacklist", "listed",
    "spf", "dkim", "dmarc", "authentication required",
    "relay access denied", "message rejected for policy reasons",
]

# Specific DSN subcodes to steer decisions
# 4.x.x => soft by definition
# 5.7.x => policy/auth failures (soft for our purposes)
SOFT_DSN_PATTERNS = [
    re.compile(r"\b5\.2\.(0|2|3)\b"),   # mailbox full / size / storage exceeded
    re.compile(r"\b5\.7\.\d+\b"),       # policy/auth failures (SPF/DKIM/DMARC/etc.)
]

HARD_DSN_PATTERNS = [
    re.compile(r"\b5\.1\.(0|1|10|3)\b"),  # bad destination address / user unknown
    re.compile(r"\b5\.4\.\d+\b"),         # routing/domain issues (no route / bad domain)
]

def _dsn_hardness(msg: str) -> Optional[str]:
    """
    Return 'hard' or 'soft' if a decisive DSN appears; else None.
    - 4.x.x => soft
    - explicit patterns override within 5.x.x
    """
    # Quick overall match to detect any DSN presence
    any_dsn = DSN_RX.search(msg)
    if not any_dsn:
        return None

    # If any 4.x.x appears, treat as soft
    for m in DSN_RX.finditer(msg):
        if m.group(1) == "4":
            return "soft"

    # Specific 5.x.x subcodes
    for rx in HARD_DSN_PATTERNS:
        if rx.search(msg):
            return "hard"
    for rx in SOFT_DSN_PATTERNS:
        if rx.search(msg):
            return "soft"

    # Fallback when 5.x.x present but not decisive: leave undecided
    return None

def _smtp_numeric_hardness(msg: str) -> Optional[str]:
    """
    Return 'hard' or 'soft' based on simple SMTP numeric codes when present.
    - 4xx => soft
    - 5xx => undecided (needs keyword disambiguation)
    """
    m = SMTP_NUM_RX.search(msg)
    if not m:
        return None
    lead = m.group(1)
    if lead == "4":
        return "soft"
    # 5xx exists — let keywords decide; return None here
    return None

def classify(message: str) -> str:
    """
    Classify bounce text into 'hard' or 'soft'.
    Conservative default: 'soft' (so we don't DNC valid addresses on policy blocks).
    """
    if not message:
        return "soft"
    msg = str(message).strip().lower()

    # 1) Decide by DSN if decisive
    by_dsn = _dsn_hardness(msg)
    if by_dsn:
        return by_dsn

    # 2) 4xx => soft; 5xx remains undecided here
    by_num = _smtp_numeric_hardness(msg)
    if by_num:
        return by_num

    # 3) Keyword heuristics
    if any(k in msg for k in HARD_KEYWORDS):
        return "hard"
    if any(k in msg for k in SOFT_KEYWORDS) or any(k in msg for k in POLICY_SOFT):
        return "soft"

    # 4) Final fallback: soft (retryable / fixable unknown)
    return "soft"

def classify_bounce(message: str) -> str:
    """Alias kept for compatibility with existing imports."""
    return classify(message)
