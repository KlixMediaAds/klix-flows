# lib/enrich.py
import re, requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent":"Mozilla/5.0"}

def _get(url, timeout=8):
    if not url:
        return None
    if not url.startswith("http"):
        url = "https://" + url
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS)
        r.raise_for_status()
        return r
    except Exception:
        return None

def fetch_site_title_tagline(url: str, timeout: int = 8):
    r = _get(url, timeout)
    if not r: return "", ""
    soup = BeautifulSoup(r.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    meta_desc = soup.find("meta", attrs={"name":"description"})
    desc = (meta_desc.get("content","").strip() if meta_desc else "")
    if not desc:
        h1 = soup.find("h1")
        desc = (h1.get_text(strip=True) if h1 else "")
    return title, desc

def enrich_snippets(url: str):
    """
    Best-effort extra enrichment:
    - about line (first short paragraph)
    - 2-3 product names/scents if visible in nav/cards
    - founder/person if in schema.org or about page
    """
    about, products, founder = "", [], ""
    r = _get(url)
    if not r: return {"AboutLine":about, "Products":products, "Founder":founder}

    soup = BeautifulSoup(r.text, "html.parser")

    # About-ish: first <p> that is 40-180 chars and not boilerplate
    paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    for p in paras:
        if 40 <= len(p) <= 180 and not re.search(r"cookies|privacy|terms|shipping|cart", p, re.I):
            about = p
            break

    # Product names from nav/links/cards
    candidates = set()
    for a in soup.find_all("a"):
        txt = (a.get_text(" ", strip=True) or "")
        if 3 <= len(txt) <= 40 and re.search(r"candle|collection|shop|scent|tin|soy|wax|gift", txt, re.I):
            candidates.add(txt)
    # Also capture card titles
    for h in soup.find_all(["h2","h3","span","div"]):
        txt = (h.get_text(" ", strip=True) or "")
        if 3 <= len(txt) <= 36 and re.search(r"candle|scent|collection|gift|bundle", txt, re.I):
            candidates.add(txt)
    products = list(candidates)[:3]

    # Try schema.org Person/Organization for founder/brand
    founder = ""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = script.string
            if not data: continue
            if "Person" in data and "name" in data:
                m = re.search(r'"@type"\s*:\s*"Person".*?"name"\s*:\s*"([^"]+)"', data, re.S)
                if m: founder = m.group(1); break
            if "Organization" in data and "founder" in data:
                m = re.search(r'"founder"\s*:\s*{[^}]*"name"\s*:\s*"([^"]+)"', data, re.S)
                if m: founder = m.group(1); break
        except Exception:
            pass

    return {"AboutLine":about, "Products":products, "Founder":founder}
