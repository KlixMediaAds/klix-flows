from __future__ import annotations
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urljoin
import re, requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (compatible; KlixLeadEnrich/1.0)"}
HTTP_TIMEOUT = 10

VIBE_KEYWORDS = {
    "luxury": ["luxury","bespoke","premium","couture","fine","artisan","signature"],
    "minimalist": ["minimal","clean","simple","sleek","calm","neutral","monochrome"],
    "playful": ["fun","playful","whimsical","quirky","bold","bright","vibrant"],
    "spiritual": ["spiritual","chakra","reiki","aura","holistic","mindful","zen"],
    "natural": ["organic","natural","botanical","plant-based","eco","sustainable"],
    "clinical": ["medical","clinical","dermatology","laser","rx","physician"],
}

SERVICE_HINTS = {
    "facial","botox","laser","peel","injectable","filler","massage","wax",
    "brows","lips","hair","skincare","treatment","package","membership",
    "shop","products","acne","anti-aging","medspa","consultation"
}

PROD_HINTS = {
    "serum","cleanser","toner","moisturizer","oil","mask","bundle","kit","set",
    "refill","starter","trial","mini"
}

def _domain(url: str) -> str:
    try:
        p = urlparse(url)
        host = p.hostname or ""
        return host.lower()
    except Exception:
        return ""

def _fetch(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
        if r.ok and r.text:
            return r.text
    except Exception:
        pass
    return None

def _text(soup: BeautifulSoup, selectors: List[str], max_chars: int = 1200) -> str:
    out = []
    for sel in selectors:
        for node in soup.select(sel):
            t = node.get_text(" ", strip=True)
            if t:
                out.append(t)
        if out:
            break
    joined = " ".join(out).strip()
    if len(joined) > max_chars:
        joined = joined[:max_chars]
    return joined

def _tone_words(blurb: str) -> List[str]:
    blurb_l = blurb.lower()
    found = []
    for tone, words in VIBE_KEYWORDS.items():
        for w in words:
            if w in blurb_l:
                found.append(tone)
                break
    # unique while preserving order
    seen = set(); uniq = []
    for x in found:
        if x not in seen:
            seen.add(x); uniq.append(x)
    return uniq

def _services(blurb: str) -> List[str]:
    bl = blurb.lower()
    hits = sorted({w for w in SERVICE_HINTS if w in bl})
    return hits[:10]

def _hero_product(soup: BeautifulSoup) -> Optional[str]:
    # Try to find a likely product name via common link/card selectors
    for sel in ["a[href*='product']", "a.product-card__title", ".product-title", "a[href*='/shop/']"]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt and len(txt) < 80:
                return txt
    return None

def _about_names(soup: BeautifulSoup) -> List[str]:
    # scan common 'about' pages links inline on home
    about_link = soup.find("a", string=re.compile(r"\babout\b", re.I))
    if about_link and about_link.get("href"):
        href = about_link["href"]
        # best effort: follow absolute or relative
        return []
    # fallback: pull proper names from body text (lightweight; not perfect)
    body_txt = _text(soup, ["main", "body"], max_chars=2000)
    names = []
    for m in re.finditer(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b", body_txt):
        cand = m.group(1)
        if len(cand.split()) in (2,3):  # First Last or First M Last
            names.append(cand)
    # de-dupe and trim
    seen = set(); uniq = []
    for n in names:
        if n not in seen:
            seen.add(n); uniq.append(n)
    return uniq[:5]

def enrich_meta(website_url: str, html: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    dom = _domain(website_url)
    if dom:
        out["website_domain"] = dom

    if html is None:
        html = _fetch(website_url)
    if not html:
        return out

    soup = BeautifulSoup(html, "html.parser")

    # Build a blurb from title/description/hero text
    title = (soup.title.get_text(" ", strip=True) if soup.title else "") or ""
    og_desc = soup.find("meta", attrs={"property":"og:description"})
    m_desc = soup.find("meta", attrs={"name":"description"})
    desc = (og_desc.get("content") if og_desc and og_desc.get("content") else "") or (m_desc.get("content") if m_desc and m_desc.get("content") else "")
    hero = _text(soup, ["h1", ".hero", ".headline", ".banner", ".section-hero", ".section .heading"], max_chars=400)
    blurb = " ".join([title, desc, hero]).strip()

    if blurb:
        tw = _tone_words(blurb)
        if tw: out["tone_words"] = tw
        sv = _services(blurb)
        if sv: out["services"] = sv

    hp = _hero_product(soup)
    if hp:
        out["hero_product"] = hp

    names = _about_names(soup)
    if names:
        out["about_names"] = names

    return out
