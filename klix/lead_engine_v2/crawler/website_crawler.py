from __future__ import annotations

import os
import ssl
import time
import urllib.parse
from urllib import request as urllib_request
import urllib.robotparser
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

from urllib.error import HTTPError, URLError


@dataclass
class PageSnapshot:
    url: str
    status: int
    content_type: str
    body: str


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


_DEBUG = _env_bool("LEAD_ENGINE_V2_DEBUG", False)

# If true, disables TLS cert verification (useful on servers missing CA bundle).
# Keep OFF by default.
_ALLOW_INSECURE_SSL = _env_bool("LEAD_ENGINE_V2_ALLOW_INSECURE_SSL", False)

_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

_IGNORE_ROBOTS = _env_bool("LEAD_ENGINE_V2_IGNORE_ROBOTS", False)
_DOMAIN_BUDGET_S = _env_int("LEAD_ENGINE_V2_CRAWL_DOMAIN_BUDGET_S", 18)
_FAIL_FAST_LIMIT = _env_int("LEAD_ENGINE_V2_CRAWL_FAIL_FAST_LIMIT", 6)

_SEED_PATHS = (
    "/",
    "/contact",
    "/contact/",
    "/contact-us",
    "/contact-us/",
    "/about",
    "/about/",
    "/team",
    "/team/",
    "/locations",
    "/locations/",
    "/location",
    "/location/",
    "/book",
    "/book/",
    "/book-now",
    "/book-now/",
    "/booking",
    "/booking/",
    "/appointments",
    "/appointments/",
)

KEYWORD_PRIORITY = (
    "contact",
    "about",
    "team",
    "staff",
    "clinic",
    "locations",
    "location",
    "book",
    "booking",
    "appointment",
)

_BLOCK_NETLOCS_SUBSTR = (
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "youtube.com",
    "goo.gl",
    "bit.ly",
)


def normalize_root_url(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None

    if "://" not in s:
        s = "https://" + s

    try:
        u = urllib.parse.urlparse(s)
    except Exception:
        return None

    if u.scheme not in ("http", "https"):
        return None
    if not u.netloc:
        return None

    netloc = u.netloc.lower()
    for bad in _BLOCK_NETLOCS_SUBSTR:
        if bad in netloc:
            return None

    return urllib.parse.urlunparse((u.scheme, u.netloc, "/", "", "", ""))


def _same_domain(a: str, b: str) -> bool:
    try:
        ua = urllib.parse.urlparse(a)
        ub = urllib.parse.urlparse(b)
        return ua.netloc.lower() == ub.netloc.lower()
    except Exception:
        return False


def _strip_fragment(url: str) -> str:
    try:
        u = urllib.parse.urlparse(url)
        return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, u.query, ""))
    except Exception:
        return url


def _ssl_context():
    if not _ALLOW_INSECURE_SSL:
        return None
    try:
        return ssl._create_unverified_context()
    except Exception:
        return None


def _fetch_once(url: str, timeout_s: int = 15, _redirects: int = 0) -> Tuple[int, str, str]:
    # NOTE: urllib can treat 308 as an HTTPError and not follow it automatically.
    # We manually follow redirects (with a hard cap) while preserving our timeout + SSL behavior.

    if _redirects > 6:
        raise URLError(f"too_many_redirects: {url}")

    req = urllib_request.Request(url, headers=_DEFAULT_HEADERS)
    ctx = _ssl_context()

    try:
        with urllib_request.urlopen(req, timeout=timeout_s, context=ctx) as resp:
            status = getattr(resp, "status", 200) or 200
            ctype = (resp.headers.get("Content-Type") or "").lower()
            raw = resp.read()

    except HTTPError as e:
        status = int(getattr(e, "code", 0) or 0)
        headers = getattr(e, "headers", None)
        ctype = ((headers or {}).get("Content-Type") or "").lower()

        # --- Redirect handling (critical) ---
        if status in (301, 302, 303, 307, 308):
            try:
                loc = None
                if headers is not None:
                    loc = headers.get("Location") or headers.get("location")
                if loc:
                    nxt = urllib.parse.urljoin(url, loc.strip())
                    nxt = _strip_fragment(nxt)
                    if _DEBUG:
                        print("DEBUG redirect", status, url, "->", nxt)
                    return _fetch_once(nxt, timeout_s=timeout_s, _redirects=_redirects + 1)
            except Exception:
                pass

        try:
            raw = e.read() or b""
        except Exception:
            raw = b""

        if _DEBUG:
            print("DEBUG fetch HTTPError", status, url, "ctype:", ctype[:60], "len:", len(raw))

    except URLError as e:
        if _DEBUG:
            print("DEBUG fetch URLError", url, type(e).__name__, str(e)[:200])
        raise

    except Exception as e:
        if _DEBUG:
            print("DEBUG fetch EXC", url, type(e).__name__, str(e)[:200])
        raise

    try:
        body = raw.decode("utf-8", errors="replace")
    except Exception:
        body = raw.decode(errors="replace")

    return status, ctype, body


def _fetch(url: str, timeout_s: int = 15) -> Tuple[int, str, str]:
    try:
        return _fetch_once(url, timeout_s=timeout_s)
    except Exception:
        try:
            u = urllib.parse.urlparse(url)
            if u.scheme == "https":
                alt = urllib.parse.urlunparse(("http", u.netloc, u.path or "/", "", "", ""))
                return _fetch_once(alt, timeout_s=timeout_s)
        except Exception:
            pass
        raise


def _extract_links(base_url: str, html: str, max_links: int = 80) -> List[str]:
    out: List[str] = []
    if not html:
        return out

    import re
    for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.I):
        href = m.group(1).strip()
        if not href or href.startswith("#"):
            continue
        if href.startswith("mailto:") or href.startswith("tel:"):
            continue

        abs_url = urllib.parse.urljoin(base_url, href)
        abs_url = _strip_fragment(abs_url)
        out.append(abs_url)
        if len(out) >= max_links:
            break

    return out


def _score_url(u: str) -> int:
    low = u.lower()
    score = 0
    for i, kw in enumerate(KEYWORD_PRIORITY):
        if kw in low:
            score += (len(KEYWORD_PRIORITY) - i) * 10
    score -= min(len(low), 200) // 10
    return score


def crawl_domain(
    website_url: str,
    max_pages: int = 8,
    max_depth: int = 2,
    timeout_s: int = 15,
    delay_s: float = 0.2,
) -> List[PageSnapshot]:
    root = normalize_root_url(website_url)
    if not root:
        return []

    start_t = time.time()
    failures = 0

    parsed = urllib.parse.urlparse(root)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = None
    if not _IGNORE_ROBOTS:
        try:
            # RobotFileParser.read() can hang (urlopen without our timeout).
            # Fetch robots.txt ourselves with timeout_s and feed to rp.parse().
            from urllib import request as urllib_request
            rp = urllib.robotparser.RobotFileParser()
            req = urllib_request.Request(robots_url, headers=_DEFAULT_HEADERS)
            with urllib_request.urlopen(req, timeout=timeout_s) as resp:
                data = resp.read().decode('utf-8', errors='ignore')
            rp.parse(data.splitlines())
        except Exception:
            rp = None

    def allowed(url: str) -> bool:
        if _IGNORE_ROBOTS:
            return True
        if rp is None:
            return True
        try:
            return rp.can_fetch(_DEFAULT_HEADERS["User-Agent"], url)
        except Exception:
            return True

    def budget_ok() -> bool:
        return (time.time() - start_t) <= float(_DOMAIN_BUDGET_S)

    seen: Set[str] = set()
    queue: List[Tuple[str, int]] = []
    out: List[PageSnapshot] = []

    for p in _SEED_PATHS:
        queue.append((urllib.parse.urljoin(root, p), 0))

    while queue and len(out) < max_pages:
        if not budget_ok():
            if _DEBUG:
                print("DEBUG budget stop", root, "out:", len(out), "failures:", failures, "budget_s:", _DOMAIN_BUDGET_S)
            break
        if failures >= _FAIL_FAST_LIMIT:
            if _DEBUG:
                print("DEBUG fail-fast stop", root, "out:", len(out), "failures:", failures)
            break

        url, depth = queue.pop(0)
        url = _strip_fragment(url)

        if url in seen:
            continue
        seen.add(url)

        if depth > max_depth:
            continue
        if not _same_domain(root, url):
            continue
        if not allowed(url):
            continue

        try:
            status, ctype, body = _fetch(url, timeout_s=timeout_s)
        except Exception:
            failures += 1
            continue

        if status in (401, 403, 429):
            failures += 1
            if _DEBUG:
                print("DEBUG blocked status", status, "at", url)
            continue

        if "text/html" not in ctype and "application/xhtml" not in ctype:
            continue

        out.append(PageSnapshot(url=url, status=status, content_type=ctype, body=body))

        links = sorted(_extract_links(url, body), key=_score_url, reverse=True)
        for nxt in links:
            if nxt not in seen:
                queue.append((nxt, depth + 1))

        if delay_s:
            time.sleep(delay_s)

    return out
