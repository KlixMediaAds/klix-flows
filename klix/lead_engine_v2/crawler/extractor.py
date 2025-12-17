from __future__ import annotations

import html
import re
import urllib.parse
from typing import List, Set

_EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b")

_OBFUSCATED = [
    (re.compile(r"\s*\[\s*at\s*\]\s*", re.I), "@"),
    (re.compile(r"\s*\(\s*at\s*\)\s*", re.I), "@"),
    (re.compile(r"\s+at\s+", re.I), "@"),
    (re.compile(r"\s*\[\s*dot\s*\]\s*", re.I), "."),
    (re.compile(r"\s*\(\s*dot\s*\)\s*", re.I), "."),
    (re.compile(r"\s+dot\s+", re.I), "."),
]

# Strong vendor/junk domains we never want to treat as a lead email
_BLOCKLIST_EXACT = {
    "robot.zapier.com",
    "sentry.wixpress.com",
}

_BLOCKLIST_DOMAIN_SUBSTR = (
    "wixpress.com",
    "sentry.io",
    "sentry-next.",
    "example.com",

    # common infra/registrar/placeholder domains that show up in templates
    "godaddy.com",
    "domain.com",
)

_BAD_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js")


def _deobfuscate(text: str) -> str:
    out = text
    for rx, repl in _OBFUSCATED:
        out = rx.sub(repl, out)
    return out


def _email_domain(e: str) -> str:
    e = (e or "").strip().lower()
    if "@" not in e:
        return ""
    return e.split("@", 1)[1].strip().lower()


def _is_junk_email(e: str) -> bool:
    low = (e or "").strip().lower()
    if not low or "@" not in low:
        return True

    # toss obvious non-emails
    if any(low.endswith(suf) for suf in _BAD_SUFFIXES):
        return True

    dom = _email_domain(low)
    if not dom:
        return True

    if dom in _BLOCKLIST_EXACT:
        return True

    for bad in _BLOCKLIST_DOMAIN_SUBSTR:
        if bad in dom:
            return True

    return False


def _clean_candidate(raw: str) -> str:
    s = (raw or "").strip()

    # decode URL-encoded junk like %20info@...
    try:
        s = urllib.parse.unquote(s)
    except Exception:
        pass

    s = s.strip()

    # strip obvious wrapping punctuation
    s = s.strip(" \t\r\n\"'<>[](){}.,;:")

    return s.lower().strip()


def extract_emails_from_html(raw_html: str) -> List[str]:
    """
    Extract a de-duplicated list of emails from HTML/text.
    - Unescapes HTML entities
    - Handles simple obfuscations
    - Filters known vendor/junk domains
    - Removes common URL-encoded/whitespace artifacts (e.g. %20info@... -> info@...)
    """
    if not raw_html:
        return []

    text = html.unescape(raw_html)
    text = _deobfuscate(text)

    found: Set[str] = set()

    # Direct regex hits
    for m in _EMAIL_RE.findall(text):
        cand = _clean_candidate(m)
        if cand and not _is_junk_email(cand):
            found.add(cand)

    # mailto: links sometimes contain extra params
    for m in re.findall(r"mailto:([^\"\'\s>]+)", text, flags=re.I):
        cand = m.split("?")[0]
        cand = _clean_candidate(cand)
        if _EMAIL_RE.fullmatch(cand) and not _is_junk_email(cand):
            found.add(cand)

    # Remove "artifact-prefixed" variants when the clean version exists
    # Example: "20info@domain.com" if "info@domain.com" also exists.
    to_drop: Set[str] = set()
    for e in list(found):
        for n in (1, 2, 3):
            if len(e) > n and e[:n].isdigit():
                stripped = e[n:]
                if stripped in found:
                    to_drop.add(e)

    for e in to_drop:
        found.discard(e)

    return sorted(found)
