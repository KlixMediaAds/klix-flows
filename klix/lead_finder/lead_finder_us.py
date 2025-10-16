# lead_finder_us.py  —  Klix Lead Finder (US) • v2.1 (Concurrent)
# ---------------------------------------------------------------
# v2.1 additions:
#  - Threaded enrichment (website fetch + parse + social/verify) using ThreadPoolExecutor
#  - Main thread is the only writer to SQLite (thread-safe pattern)
#  - Optional external config (config.yaml) for VIBE_KEYWORDS / UNIQUE_CUES / tunables
#  - Better debug logging around parsing failures

import os, csv, re, time, sqlite3, tldextract, argparse, sys, json, traceback, logging
from datetime import datetime, timezone, date
from urllib.parse import urlparse, urljoin, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ============== ENV / CONFIG =================
load_dotenv(override=True)
PLACES_KEY   = (os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()
SHEET_ID     = (os.getenv("SHEET_ID") or "").strip()
SHEETS_CRED  = (os.getenv("GOOGLE_SHEETS_CRED") or "").strip()
NEVERBOUNCE  = (os.getenv("NEVERBOUNCE_KEY") or "").strip()

HTTP_TIMEOUT     = 12
PAGE_TOKEN_WAIT  = 2.0
DB_PATH          = "leads.db"
CSV_NEW_PATH     = "leads_new.csv"
CSV_ALL_PATH     = "leads_all.csv"
CONFIG_YAML      = "config.yaml"  # optional

# -------- logging (simple, structured) --------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
logi = logging.info
logd = logging.debug
logw = logging.warning

def require_key():
    if not PLACES_KEY:
        sys.exit("ERROR: Set GOOGLE_PLACES_API_KEY in a .env file next to this script.")

def to_int(x, default=0):
    try:
        return int(float(x))
    except Exception:
        return default

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

# --------------- config loader ----------------
def load_external_config():
    """Optionally load keyword lists/tunables from config.yaml."""
    try:
        import yaml
        if os.path.exists(CONFIG_YAML):
            with open(CONFIG_YAML, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    except Exception as e:
        logw(f"[CONFIG] Couldn't load {CONFIG_YAML}: {e}")
    return {}

CFG_FILE = load_external_config()

DEFAULT_US_CITIES = CFG_FILE.get("default_us_cities") or [
    "New York, NY","Los Angeles, CA","Chicago, IL","Houston, TX","Phoenix, AZ",
    "Philadelphia, PA","San Antonio, TX","San Diego, CA","Dallas, TX","San Jose, CA",
    "Austin, TX","Jacksonville, FL","Fort Worth, TX","Columbus, OH","Charlotte, NC",
    "San Francisco, CA","Indianapolis, IN","Seattle, WA","Denver, CO","Washington, DC"
]

DEFAULT_NICHES = CFG_FILE.get("default_niches") or [
    ("skincare brand", "country"),
    ("jewelry brand", "country"),
    ("pet accessories boutique", "country"),
    ("gourmet coffee", "cities"),
    ("artisanal chocolate", "country"),
    ("stationery shop", "country"),
    ("home decor boutique", "cities"),
    ("candle shop", "cities"),
    ("saas for freelancers", "country"),
    ("notion template seller", "country"),
    ("business coach", "country"),
    ("app developer", "country"),
    ("realtor", "cities"),
    ("coffee shop", "cities"),
    ("bakery", "cities"),
    ("florist", "cities"),
]

UA = {"User-Agent": "Mozilla/5.0 (compatible; KlixLeadFinder/2.1)"}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# ------- Email extraction improvements -------
BLACKLIST_EMAIL_DOMAINS = set(CFG_FILE.get("blacklist_email_domains") or [
    "godaddy.com","example.com","yourdomain.com","domain.com",
    "wixpress.com","wix.com","weebly.com","squarespace.com",
    "shopify.com","myshopify.com","bigcommerce.com","mailinator.com",
    "noemail.com","noreply.com","donotreply.com",
])

ROLE_EMAIL_PRIORITY = CFG_FILE.get("role_email_priority") or [
    "info@", "hello@", "contact@", "support@", "sales@", "hi@", "team@", "admin@", "owner@", "founder@", "booking@", "orders@"
]

OBFUSCATION_PATTERNS = [
    (re.compile(r"\s*\[\s*at\s*\]\s*", re.I), "@"),
    (re.compile(r"\s*\(\s*at\s*\)\s*", re.I), "@"),
    (re.compile(r"\s+at\s+", re.I), "@"),
    (re.compile(r"\s*\[\s*dot\s*\]\s*", re.I), "."),
    (re.compile(r"\s*\(\s*dot\s*\)\s*", re.I), "."),
    (re.compile(r"\s+dot\s+", re.I), "."),
]

# --- Heuristics / extractors (externalizable) ---
VIBE_KEYWORDS = CFG_FILE.get("vibe_keywords") or {
    "luxury": ["luxury","bespoke","premium","couture","fine","artisan","signature"],
    "minimalist": ["minimal","clean","simple","sleek","calm","neutral","monochrome"],
    "playful": ["fun","playful","whimsical","quirky","bold","bright","vibrant"],
    "spiritual": ["spiritual","chakra","reiki","aura","holistic","mindful","zen"],
    "natural": ["organic","natural","botanical","plant-based","eco","sustainable"],
    "clinical": ["medical","clinical","dermatology","laser","rx","physician"],
}
UNIQUE_CUES = CFG_FILE.get("unique_cues") or [
    "vegan","custom","handmade","bespoke","organic","3d","medical-grade","laser",
    "microblading","bridal","event","wholesale","subscription","refill","local",
    "small batch","cold-pressed","artisan","signature","seasonal","limited",
]

# ============== ROBUST HTTP (retry/backoff) ==============
def http_get_json(url, params, cfg, *, kind, debug=False):
    retries = cfg["max_retries"]
    backoff = cfg["retry_backoff"]
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT, headers=UA)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            if attempt >= retries:
                logw(f"[WARN] {kind}: request failed after {retries} retries: {e}")
                return None, "HTTP_ERROR"
            sleep = backoff * (2 ** attempt)
            logw(f"[RETRY] {kind}: network error, retrying in {sleep:.1f}s...")
            time.sleep(sleep)
            continue

        status = data.get("status", "OK")
        if status == "OK":
            cfg["api_calls"] += 1
            return data, "OK"

        if status == "OVER_QUERY_LIMIT":
            cfg["over_limit_hits"] += 1
            cfg["api_calls"] += 1
            cooldown = min(60, backoff * (2 ** attempt))
            logw(f"[RATE] Google OVER_QUERY_LIMIT on {kind}. Cooling {cooldown:.1f}s...")
            time.sleep(cooldown)
            if attempt >= retries:
                return None, "OVER_QUERY_LIMIT"
            continue

        if status in ("INVALID_REQUEST", "ZERO_RESULTS", "NOT_FOUND"):
            cfg["api_calls"] += 1
            if debug: logd(f"[DEBUG] {kind} status={status}")
            return data, status

        cfg["api_calls"] += 1
        if attempt >= retries:
            logw(f"[WARN] {kind}: unexpected status={status} after retries")
            return data, status
        sleep = backoff * (2 ** attempt)
        logw(f"[RETRY] {kind}: status={status}, waiting {sleep:.1f}s...")
        time.sleep(sleep)
    return None, "UNKNOWN"

# ============== DB (schema + migration) ==============
REQUIRED_COLUMNS = [
    ("dedupe_key","TEXT PRIMARY KEY"),("name","TEXT"),("niche_search","TEXT"),("city","TEXT"),
    ("website","TEXT"),("email","TEXT"),("phone","TEXT"),("address","TEXT"),
    ("instagram","TEXT"),("tiktok","TEXT"),("site_title","TEXT"),("site_desc","TEXT"),
    ("icebreaker","TEXT"),("score","INTEGER"),("source","TEXT"),("first_seen","TEXT"),
    ("last_seen","TEXT"),("tagline","TEXT"),("unique_service","TEXT"),("vibe","TEXT"),
    ("owner_name","TEXT"),("owner_story","TEXT"),("public_quote","TEXT"),
    ("owner_interests","TEXT"),("personal_social","TEXT"),("compliment_candidate","TEXT"),
    ("reviews_snapshot","TEXT"),("email_status","TEXT"),("ig_fresh","INTEGER"),("tt_fresh","INTEGER"),
    ("last_post_date","TEXT"),("content_freq","TEXT"),("platform_primary","TEXT"),
    ("rating","REAL"),("ratings_total","INTEGER"),
    ("product_to_feature","TEXT"),("recent_activity","TEXT"),("differentiator","TEXT"),
    ("testimonial_quote","TEXT"),("pain_point","TEXT"),("tech_stack","TEXT"),
    ("mission_statement","TEXT"),("content_gap","TEXT"),("emails_all","TEXT"),
]

def ensure_db(debug=False):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS leads (dedupe_key TEXT PRIMARY KEY)")
    conn.commit()
    migrate_schema(conn)
    return conn

def migrate_schema(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(leads)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col, coltype in REQUIRED_COLUMNS:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE leads ADD COLUMN {col} {coltype}")
            logi(f"[MIGRATE] Added column: {col} {coltype}")
    conn.commit()

# ============== CSV / Sheet headers ==============
HEADER = [
    "name","niche_search","city","website","email","phone","address","instagram","tiktok",
    "site_title","site_desc","icebreaker","score","source","first_seen","last_seen",
    "tagline","unique_service","vibe","owner_name","owner_story","public_quote",
    "owner_interests","personal_social","compliment_candidate","reviews_snapshot",
    "email_status","ig_fresh","tt_fresh","last_post_date","content_freq","platform_primary",
    "rating","ratings_total","product_to_feature","recent_activity","differentiator",
    "testimonial_quote","pain_point","tech_stack","mission_statement","content_gap",
]

def export_all_to_csv(conn, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(HEADER)
        sel = ",".join(HEADER)
        for r in conn.execute(f"SELECT {sel} FROM leads ORDER BY score DESC, last_seen DESC"):
            w.writerow(r)

def export_new_to_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER); w.writeheader()
        for r in rows:
            rr = dict(r); rr["score"] = to_int(rr.get("score"), 0)
            w.writerow({k: rr.get(k,"") for k in HEADER})

# ============== Inputs (cities / niches) ==============
def load_cities():
    if os.path.exists("cities_us.txt"):
        with open("cities_us.txt","r",encoding="utf-8") as f:
            cities = [ln.strip() for ln in f if ln.strip()]
            if cities: return cities
    return DEFAULT_US_CITIES

def load_niches():
    if os.path.exists("niches.txt"):
        out = []
        with open("niches.txt","r",encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln: continue
                if "|" in ln:
                    q, mode = [p.strip() for p in ln.split("|", 1)]
                else:
                    q, mode = ln, "cities"
                mode = "country" if mode.lower().startswith("country") else "cities"
                out.append((q, mode))
        if out: return out
    return DEFAULT_NICHES

# ============== Utilities for site parsing ==============
def fetch_html(url):
    try:
        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
        ctype = r.headers.get("Content-Type","")
        if "text/html" in ctype or "application/xhtml+xml" in ctype:
            return r.text
        return ""
    except Exception as e:
        logd(f"[HTTP] fetch_html error for {url}: {e}")
        return ""

def same_host(url, href):
    try:
        bu = urlparse(url); hu = urlparse(urljoin(url, href))
        return bu.netloc and bu.netloc == hu.netloc
    except Exception:
        return False

def _normalize_email(raw: str) -> str:
    e = (raw or "").strip()
    if e.lower().startswith("mailto:"): e = e[7:]
    e = unquote(e).replace("&amp;","&").replace("&lt;","<").replace("&gt;",">").strip(" <>;,")
    for pat, repl in OBFUSCATION_PATTERNS: e = pat.sub(repl, e)
    return e.lower()

def _is_blacklisted(email: str) -> bool:
    if not email or "@" not in email: return True
    try: dom = email.split("@",1)[1].lower()
    except Exception: return True
    return any(dom == x or dom.endswith(f".{x}") for x in BLACKLIST_EMAIL_DOMAINS)

def _domain_of(url: str) -> str:
    try:
        ext = tldextract.extract(url or "")
        return ".".join(p for p in [ext.domain, ext.suffix] if p)
    except Exception:
        return ""

def _rank_email(email: str, website: str) -> int:
    score = 0
    if not email or "@" not in email: return -999
    local, dom = email.split("@", 1)
    local = local.lower(); dom = dom.lower()
    site_dom = _domain_of(website)
    if site_dom and (dom == site_dom or dom.endswith(f".{site_dom}")): score += 50
    for i, role in enumerate(ROLE_EMAIL_PRIORITY):
        if local.startswith(role[:-1]): score += (30 - i); break
    if any(x in dom for x in ("gmail.com","outlook.com","hotmail.com","yahoo.com","icloud.com","proton.me","protonmail.com")):
        score += 5
    if len(local) <= 2: score -= 5
    if _is_blacklisted(email): score -= 100
    return score

def _decode_cloudflare_email_protection(html: str):
    out = []
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        nodes = soup.select("[data-cfemail]")
        for n in nodes:
            hexstr = (n.get("data-cfemail") or "").strip()
            if not hexstr: continue
            r = int(hexstr[:2], 16)
            email = "".join([chr(int(hexstr[i:i+2], 16) ^ r) for i in range(2, len(hexstr), 2)])
            e = _normalize_email(email)
            if EMAIL_RE.fullmatch(e) and not _is_blacklisted(e): out.append(e)
    except Exception as e:
        logd(f"[CFEMAIL] decode fail: {e}")
    return out

def _collect_emails_from_jsonld(soup):
    out = []
    try:
        for tag in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(tag.string or "{}")
            except Exception:
                continue
            def add_email(v):
                if isinstance(v, str): out.append(_normalize_email(v))
            if isinstance(data, dict):
                if "email" in data: add_email(data["email"])
                cp = data.get("contactPoint")
                if isinstance(cp, dict) and "email" in cp: add_email(cp["email"])
                elif isinstance(cp, list):
                    for c in cp:
                        if isinstance(c, dict) and "email" in c: add_email(c["email"])
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "email" in item: add_email(item["email"])
    except Exception as e:
        logd(f"[JSON-LD] parse fail: {e}")
    return out

def fetch_contact_variants(base_url: str, max_pages: int = 2):
    pages = []
    base_html = fetch_html(base_url)
    if base_html: pages.append((base_url, base_html))
    try:
        soup = BeautifulSoup(base_html or "", "html.parser")
        cand_paths = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip(); low = href.lower()
            if same_host(base_url, href) and any(kw in low for kw in ("/contact","contact","contact-us","/about","about","/support","/help","help")):
                cand_paths.append(urljoin(base_url, href))
        uniq, seen = [], set()
        for u in cand_paths:
            if u not in seen:
                uniq.append(u); seen.add(u)
        for u in uniq[:max_pages]:
            h = fetch_html(u)
            if h: pages.append((u, h))
    except Exception as e:
        logd(f"[CONTACT] link parse fail for {base_url}: {e}")
    return pages

def extract_emails_strong(html: str, base_url: str):
    if not html: return []
    emails = []
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            emails.append(_normalize_email(href))
    emails.extend(_decode_cloudflare_email_protection(html))
    emails.extend(_collect_emails_from_jsonld(soup))
    raw_text = soup.get_text("\n", strip=True)
    txt = raw_text
    for pat, repl in OBFUSCATION_PATTERNS: txt = pat.sub(repl, txt)
    for m in EMAIL_RE.findall(txt): emails.append(_normalize_email(m))
    clean, seen = [], set()
    for e in emails:
        if not e or "@" not in e: continue
        if e.endswith((".png",".jpg",".jpeg",".gif",".svg",".webp")): continue
        if e not in seen:
            seen.add(e); clean.append(e)
    clean.sort(key=lambda e: _rank_email(e, base_url), reverse=True)
    return clean

# --- More extractors (unchanged core logic) ---
NAME_NEAR_TITLES = re.compile(r"(founder|owner|co[- ]founder|cofounder|about\s+the\s+founder)[:\s\-]*([A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2})", re.I)

def detect_tech_stack(html):
    h = html.lower(); tags = []
    if "cdn.shopify.com" in h or "myshopify" in h: tags.append("Shopify")
    if "wp-content" in h: tags.append("WordPress")
    if "woocommerce" in h: tags.append("WooCommerce")
    if "wixstatic" in h: tags.append("Wix")
    if "squarespace.com" in h: tags.append("Squarespace")
    if "bigcommerce" in h: tags.append("BigCommerce")
    if "weeblycloud" in h: tags.append("Weebly")
    if "klaviyo" in h: tags.append("Klaviyo")
    if "gtm.js" in h or "googletagmanager" in h: tags.append("GTM")
    return ", ".join(sorted(set(tags)))

def short_handle(*vals, limit=50):
    for v in vals:
        v = (v or "").strip()
        if v: return (v[:limit]).strip()
    return ""

def extract_headings(soup):
    return [el.get_text(" ", strip=True) for el in soup.find_all(["h1","h2","h3","h4"])]

def extract_testimonial(soup):
    try:
        cand = soup.find(lambda t: t and t.name in ("blockquote", "q"))
        if cand:
            txt = cand.get_text(" ", strip=True)
            if 10 <= len(txt) <= 200: return txt
    except Exception as e:
        logd(f"[TESTIMONIAL] blockquote fail: {e}")
    def has_reviewish(tag):
        if not tag or tag.name not in ("p","div"): return False
        classes = tag.get("class", []); 
        if isinstance(classes, str): classes = [classes]
        cls = " ".join(classes).lower()
        return any(k in cls for k in ("review","testimonial","quote"))
    try:
        rev = soup.find(has_reviewish)
        if rev:
            txt = rev.get_text(" ", strip=True)
            if 10 <= len(txt) <= 200: return txt
    except Exception as e:
        logd(f"[TESTIMONIAL] class fail: {e}")
    return ""

def extract_mission(text):
    m = re.search(r"(our\s+mission.*?$|we\s+believe.*?$|we\s+are\s+committed.*?$)", text, re.I | re.M)
    if m: return (m.group(0)[:200]).strip()
    return ""

def parse_site_info(html, base_url=""):
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")
    desc = ""
    md = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    if md and md.get("content"): desc = md["content"].strip()[:300]
    text = soup.get_text("\n", strip=True); lowtxt = text.lower()
    def clean(u): return (u or "").split("?")[0]
    links = [a.get("href","") for a in soup.find_all("a", href=True)]
    insta   = next((l for l in links if "instagram.com" in l), "")
    tiktok  = next((l for l in links if "tiktok.com"    in l), "")
    fb      = next((l for l in links if "facebook.com"  in l), "")
    li      = next((l for l in links if "linkedin.com"  in l), "")
    tagline = ""
    og_site = soup.find("meta", attrs={"property":"og:site_name"})
    if og_site and og_site.get("content"): tagline = og_site["content"].strip()
    if not tagline:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            h1txt = h1.get_text(strip=True)
            if 4 <= len(h1txt) <= 80: tagline = h1txt
    headings = " ".join([el.get_text(" ", strip=True).lower() for el in soup.find_all(["h2","h3","li","strong","em"])][:120])
    unique_service = ""
    for cue in UNIQUE_CUES:
        if cue in headings: unique_service = cue; break
    vibe = ""
    vibe_src = (title + " " + desc + " " + headings).lower()
    for label, kws in VIBE_KEYWORDS.items():
        if any(k in vibe_src for k in kws): vibe = label; break
    owner_name = ""
    person = soup.find(attrs={"itemtype": re.compile("schema.org/Person", re.I)})
    if person:
        nm = person.find(attrs={"itemprop":"name"})
        if nm and nm.get_text(strip=True): owner_name = nm.get_text(strip=True)
    if not owner_name:
        meta_author = soup.find("meta", attrs={"name":"author"})
        if meta_author and meta_author.get("content"):
            cand = meta_author["content"].strip()
            if 2 <= len(cand.split()) <= 3: owner_name = cand
    if not owner_name:
        m = NAME_NEAR_TITLES.search(text)
        if m: owner_name = m.group(2).strip()
    if owner_name and (len(owner_name) > 40 or any(w in owner_name.lower() for w in ("florals","about","story"))):
        owner_name = ""
    owner_story = ""
    public_quote = ""
    if "“" in text or '"' in text:
        q = re.search(r"[“\"](.{10,180})[”\"]", text)
        if q: public_quote = q.group(1).strip()
    personal_social = ""
    if li: personal_social = clean(li)
    elif insta and "instagram.com/" in insta and "/p/" not in insta: personal_social = clean(insta)
    platform_primary = "IG" if insta else ("TikTok" if tiktok else ("FB" if fb else ("LinkedIn" if li else "")))
    product_to_feature = ""
    for h in extract_headings(soup):
        if any(w in h.lower() for w in ("collection","candle","kit","set","class","service","facial","package","bundle","membership","course","workshop","custom")):
            if 3 <= len(h) <= 80:
                product_to_feature = h.strip(); break
    differentiator = ""
    for tag in ["handmade","women-owned","minority-owned","veteran-owned","sustainable","eco","small batch","family-owned","award-winning","local"]:
        if tag in lowtxt: differentiator = tag; break
    mission_statement = extract_mission(text)
    testimonial_quote = extract_testimonial(soup)
    tech_stack = detect_tech_stack(html)

    emails = []
    pages = fetch_contact_variants(base_url, max_pages=2)
    for _url, _html in pages:
        emails.extend(extract_emails_strong(_html, base_url))
    dedup, seen = [], set()
    for e in emails:
        if e not in seen and not _is_blacklisted(e):
            seen.add(e); dedup.append(e)
    emails = dedup[:10]

    return {
        "site_title": (title or "")[:150], "site_desc": desc,
        "emails": emails, "instagram": clean(insta) if insta else "",
        "tiktok": clean(tiktok) if tiktok else "", "facebook": clean(fb) if fb else "",
        "linkedin": clean(li) if li else "", "tagline": tagline[:120],
        "unique_service": unique_service, "vibe": vibe, "owner_name": owner_name,
        "owner_story": owner_story, "public_quote": public_quote, "owner_interests": "",
        "personal_social": personal_social, "compliment_candidate": short_handle(tagline, title, unique_service),
        "reviews_snapshot": "", "platform_primary": platform_primary,
        "product_to_feature": product_to_feature, "differentiator": differentiator,
        "mission_statement": mission_statement, "testimonial_quote": testimonial_quote,
        "tech_stack": tech_stack, "emails_all": ", ".join(emails) if emails else "",
    }

# --- Email verification (NeverBounce single-check) ---
def verify_email_neverbounce(email, timeout=8):
    key = NEVERBOUNCE
    if not key or not email: return ""
    try:
        url = "https://api.neverbounce.com/v4/single/check"
        params = {"key": key, "email": email, "address_info": "0", "credits_info": "0"}
        r = requests.get(url, params=params, timeout=timeout); r.raise_for_status()
        data = r.json(); result = (data.get("result") or "").lower()
        if result in ("valid","invalid","catchall","unknown","disposable"): return result
        return ""
    except Exception as e:
        logw(f"[NB] verify error for {email}: {e}")
        return ""

# --- Social freshness (best-effort) ---
DATE_RE = re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})')

def sniff_recent_date_from_html(html):
    m = DATE_RE.search(html or "")
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2005 <= y <= 2100:
            try: return date(y, clamp(mo,1,12), clamp(d,1,28)).isoformat()
            except Exception: pass
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        t = soup.find("time")
        if t and t.get("datetime"):
            dt = t["datetime"][:10]
            if re.match(r"\d{4}-\d{2}-\d{2}", dt):
                y = int(dt[:4])
                if 2005 <= y <= 2100: return dt
    except Exception: pass
    return ""

def social_freshness(url):
    if not url: return 0, ""
    try:
        html = fetch_html(url)
        if not html: return 0, ""
        last = sniff_recent_date_from_html(html)
        if not last: return 0, ""
        try:
            dt = datetime.fromisoformat(last)
            days = (datetime.now() - dt).days
            return (1 if days <= 30 else 0), last
        except Exception:
            return 0, last
    except Exception:
        return 0, ""

def content_freq_from_date(iso_str):
    if not iso_str: return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        days = (datetime.now() - dt).days
        if days <= 30: return "Active"
        if days <= 90: return "Dormant"
        return "Inactive"
    except Exception:
        return ""

def content_gap_calc(insta, tiktok, ig_fresh, tt_fresh):
    gaps = []
    if insta and not tiktok: gaps.append("IG only")
    if tiktok and not insta: gaps.append("TikTok only")
    if insta and tiktok:
        if ig_fresh and not tt_fresh: gaps.append("IG fresh; TikTok stale")
        if tt_fresh and not ig_fresh: gaps.append("TikTok fresh; IG stale")
    return ", ".join(gaps)

def score_row(row, niche_keywords):
    score = 0
    if row.get("website"):   score += 2
    if row.get("email"):     score += 3
    if row.get("instagram"): score += 2
    if row.get("tiktok"):    score += 3
    if row.get("phone"):     score += 1
    text = (row.get("site_title","") + " " + row.get("site_desc","")).lower()
    if any(k in text for k in niche_keywords): score += 1
    if row.get("email_status","") == "valid": score += 1
    if to_int(row.get("ig_fresh"),0) or to_int(row.get("tt_fresh"),0): score += 1
    return score

def dedupe_key(name, website, phone, address):
    domain = _domain_of(website)
    name_norm  = (name or "").strip().lower()
    phone_norm = re.sub(r"[^\d]+", "", (phone or ""))
    addr_norm  = (address or "").strip().lower()
    if domain:    return f"domain:{domain}"
    if phone_norm:return f"name_phone:{name_norm}|{phone_norm}"
    if addr_norm: return f"name_addr:{name_norm}|{addr_norm}"
    return f"name_only:{name_norm}"

# ============== Discovery / Enrich ==============
def places_text_search(query, pagetoken, cfg, debug=False):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"key": PLACES_KEY, "query": query}
    if pagetoken: params["pagetoken"] = pagetoken
    return http_get_json(url, params, cfg, kind="textsearch", debug=debug)

def place_details(place_id, cfg, debug=False):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,website,formatted_phone_number,formatted_address,rating,user_ratings_total"
    params = {"key": PLACES_KEY, "place_id": place_id, "fields": fields}
    data, status = http_get_json(url, params, cfg, kind="details", debug=debug)
    if not data: return {}
    return data.get("result", {})

# -------- per-result processing (THREAD) --------
def process_one(details, niche, city, sleep_sec):
    """Fetch site, parse, social freshness & NB verify. Returns (row_dict, niche_keywords) or None."""
    try:
        name    = (details.get("name") or "").strip()
        website = details.get("website","") or ""
        phone   = details.get("formatted_phone_number","") or ""
        address = details.get("formatted_address","") or ""
        rating  = details.get("rating", None)
        ratings_total = details.get("user_ratings_total", None)

        site_title = site_desc = email = instagram = tiktok = ""
        tagline = unique_service = vibe = owner_name = owner_story = ""
        public_quote = owner_interests = personal_social = compliment_candidate = ""
        platform_primary = ""
        product_to_feature = differentiator = mission_statement = testimonial_quote = ""
        tech_stack = ""; emails_all = ""

        if website:
            html = fetch_html(website); 
            if sleep_sec: time.sleep(sleep_sec)
            if html:
                info = parse_site_info(html, base_url=website)
                site_title = info["site_title"]; site_desc = info["site_desc"]
                instagram  = info["instagram"];  tiktok   = info["tiktok"]
                email_list = info["emails"];     email    = email_list[0] if email_list else ""
                tagline   = info["tagline"];     unique_service = info["unique_service"]
                vibe      = info["vibe"];        owner_name     = info["owner_name"]
                owner_story=info["owner_story"]; public_quote   = info["public_quote"]
                owner_interests = info.get("owner_interests",""); personal_social = info["personal_social"]
                compliment_candidate = info["compliment_candidate"]; platform_primary = info["platform_primary"]
                product_to_feature = info["product_to_feature"]; differentiator = info["differentiator"]
                mission_statement = info["mission_statement"]; testimonial_quote = info["testimonial_quote"]
                tech_stack = info["tech_stack"]; emails_all = info.get("emails_all","")

        # Social freshness + NeverBounce in-thread
        ig_fresh = tt_fresh = 0
        ig_last = tt_last = ""
        if instagram:
            ig_fresh, ig_last = social_freshness(instagram)
        if tiktok:
            tt_fresh, tt_last = social_freshness(tiktok)
        last_post_date = ig_last or tt_last
        content_freq = content_freq_from_date(last_post_date)
        content_gap = content_gap_calc(instagram, tiktok, ig_fresh, tt_fresh)
        email_status = verify_email_neverbounce(email)

        niche_keywords = [n.lower() for n in niche.split()]
        row = {
            "dedupe_key":  dedupe_key(name, website, phone, address),
            "name": name, "niche_search": niche, "city": city,
            "website": website, "email": email, "phone": phone, "address": address,
            "instagram": instagram, "tiktok": tiktok,
            "site_title": site_title, "site_desc": site_desc, "icebreaker": "",
            "source": "google_places",
            "tagline": tagline, "unique_service": unique_service, "vibe": vibe,
            "owner_name": owner_name, "owner_story": owner_story, "public_quote": public_quote,
            "owner_interests": owner_interests, "personal_social": personal_social,
            "compliment_candidate": compliment_candidate, "reviews_snapshot": "",
            "platform_primary": platform_primary, "rating": rating if rating is not None else None,
            "ratings_total": ratings_total if ratings_total is not None else None,
            "product_to_feature": product_to_feature, "recent_activity": "",
            "differentiator": differentiator, "testimonial_quote": testimonial_quote,
            "pain_point": "", "tech_stack": tech_stack, "mission_statement": mission_statement,
            "content_gap": content_gap, "emails_all": emails_all,
            "ig_fresh": ig_fresh, "tt_fresh": tt_fresh, "last_post_date": last_post_date,
            "content_freq": content_freq, "email_status": email_status,
        }
        # score pre-upsert
        row["score"] = score_row(row, niche_keywords)
        return row, niche_keywords
    except Exception as e:
        logd(f"[THREAD] process_one error: {e}\n{traceback.format_exc()}")
        return None

# ------------- stage into outputs (MAIN) -------------
def existing_val(existing_row, idx, new_val):
    try: ev = existing_row[idx]
    except Exception: ev = None
    if ev is None or ev == "": return new_val
    return ev

def upsert_row(conn, row, *, debug=False):
    cur = conn.cursor()
    cur.execute("SELECT * FROM leads WHERE dedupe_key=?", (row["dedupe_key"],))
    existing = cur.fetchone()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if existing:
        existing_score = to_int(existing[13], 0); new_score = to_int(row.get("score"), 0)
        merged = {
            "name":        existing[1] or row["name"],
            "niche_search":existing[2] or row["niche_search"],
            "city":        existing[3] or row["city"],
            "website":     existing[4] or row["website"],
            "email":       existing[5] or row["email"],
            "phone":       existing[6] or row["phone"],
            "address":     existing[7] or row["address"],
            "instagram":   existing[8] or row["instagram"],
            "tiktok":      existing[9] or row["tiktok"],
            "site_title":  existing[10] or row["site_title"],
            "site_desc":   existing[11] or row["site_desc"],
            "icebreaker":  existing[12] or row.get("icebreaker",""),
            "score":       max(existing_score, new_score),
            "source":      existing[14] or row["source"],
            "tagline": existing_val(existing, 18, row.get("tagline","")),
            "unique_service": existing_val(existing, 19, row.get("unique_service","")),
            "vibe": existing_val(existing, 20, row.get("vibe","")),
            "owner_name": existing_val(existing, 21, row.get("owner_name","")),
            "owner_story": existing_val(existing, 22, row.get("owner_story","")),
            "public_quote": existing_val(existing, 23, row.get("public_quote","")),
            "owner_interests": existing_val(existing, 24, row.get("owner_interests","")),
            "personal_social": existing_val(existing, 25, row.get("personal_social","")),
            "compliment_candidate": existing_val(existing, 26, row.get("compliment_candidate","")),
            "reviews_snapshot": existing_val(existing, 27, row.get("reviews_snapshot","")),
            "email_status": existing_val(existing, 28, row.get("email_status","")),
            "ig_fresh": existing_val(existing, 29, row.get("ig_fresh",0)),
            "tt_fresh": existing_val(existing, 30, row.get("tt_fresh",0)),
            "last_post_date": existing_val(existing, 31, row.get("last_post_date","")),
            "content_freq": existing_val(existing, 32, row.get("content_freq","")),
            "platform_primary": existing_val(existing, 33, row.get("platform_primary","")),
            "rating": existing_val(existing, 34, row.get("rating",None)),
            "ratings_total": existing_val(existing, 35, row.get("ratings_total",None)),
            "product_to_feature": existing_val(existing, 36, row.get("product_to_feature","")),
            "recent_activity": existing_val(existing, 37, row.get("recent_activity","")),
            "differentiator": existing_val(existing, 38, row.get("differentiator","")),
            "testimonial_quote": existing_val(existing, 39, row.get("testimonial_quote","")),
            "pain_point": existing_val(existing, 40, row.get("pain_point","")),
            "tech_stack": existing_val(existing, 41, row.get("tech_stack","")),
            "mission_statement": existing_val(existing, 42, row.get("mission_statement","")),
            "content_gap": existing_val(existing, 43, row.get("content_gap","")),
            "emails_all": existing_val(existing, 44, row.get("emails_all","")),
        }
        cur.execute("""
        UPDATE leads SET
            name=?, niche_search=?, city=?, website=?, email=?, phone=?, address=?,
            instagram=?, tiktok=?, site_title=?, site_desc=?, icebreaker=?, score=?, source=?,
            last_seen=?, tagline=?, unique_service=?, vibe=?, owner_name=?, owner_story=?,
            public_quote=?, owner_interests=?, personal_social=?, compliment_candidate=?,
            reviews_snapshot=?, email_status=?, ig_fresh=?, tt_fresh=?, last_post_date=?,
            content_freq=?, platform_primary=?, rating=?, ratings_total=?,
            product_to_feature=?, recent_activity=?, differentiator=?, testimonial_quote=?,
            pain_point=?, tech_stack=?, mission_statement=?, content_gap=?, emails_all=?
        WHERE dedupe_key=?""", (
            merged["name"], merged["niche_search"], merged["city"], merged["website"],
            merged["email"], merged["phone"], merged["address"], merged["instagram"],
            merged["tiktok"], merged["site_title"], merged["site_desc"], merged["icebreaker"],
            merged["score"], merged["source"], now, merged["tagline"], merged["unique_service"],
            merged["vibe"], merged["owner_name"], merged["owner_story"], merged["public_quote"],
            merged["owner_interests"], merged["personal_social"], merged["compliment_candidate"],
            merged["reviews_snapshot"], merged["email_status"], merged["ig_fresh"], merged["tt_fresh"],
            merged["last_post_date"], merged["content_freq"], merged["platform_primary"],
            merged["rating"], merged["ratings_total"], merged["product_to_feature"],
            merged["recent_activity"], merged["differentiator"], merged["testimonial_quote"],
            merged["pain_point"], merged["tech_stack"], merged["mission_statement"],
            merged["content_gap"], merged["emails_all"], row["dedupe_key"]
        ))
        conn.commit()
        return False
    else:
        row = dict(row); row["first_seen"] = now; row["last_seen"] = now; row["score"] = to_int(row.get("score"), 0)
        cur.execute("""
        INSERT INTO leads (
            dedupe_key, name, niche_search, city, website, email, phone, address,
            instagram, tiktok, site_title, site_desc, icebreaker, score, source, first_seen, last_seen,
            tagline, unique_service, vibe, owner_name, owner_story, public_quote, owner_interests,
            personal_social, compliment_candidate, reviews_snapshot, email_status, ig_fresh, tt_fresh,
            last_post_date, content_freq, platform_primary, rating, ratings_total,
            product_to_feature, recent_activity, differentiator, testimonial_quote, pain_point,
            tech_stack, mission_statement, content_gap, emails_all
        ) VALUES (
            :dedupe_key, :name, :niche_search, :city, :website, :email, :phone, :address,
            :instagram, :tiktok, :site_title, :site_desc, :icebreaker, :score, :source, :first_seen, :last_seen,
            :tagline, :unique_service, :vibe, :owner_name, :owner_story, :public_quote, :owner_interests,
            :personal_social, :compliment_candidate, :reviews_snapshot, :email_status, :ig_fresh, :tt_fresh,
            :last_post_date, :content_freq, :platform_primary, :rating, :ratings_total,
            :product_to_feature, :recent_activity, :differentiator, :testimonial_quote, :pain_point,
            :tech_stack, :mission_statement, :content_gap, :emails_all
        )""", row)
        conn.commit()
        return True

# ============== Tab naming + Sheets sync (unchanged) ==============
def sanitize_tab_name(name):
    bad = ':\\/?*[]'
    for ch in bad: name = name.replace(ch, '-')
    return name[:99].strip() or "Sheet"

def compute_tab_name(cfg, niche_for_per_niche=None):
    base = cfg["sheet_name"]; mode = cfg["sheet_mode"]
    if mode == "per_day": return sanitize_tab_name(f"{base} {date.today().isoformat()}")
    if mode == "per_niche" and niche_for_per_niche: return sanitize_tab_name(f"{base} - {niche_for_per_niche}")
    return sanitize_tab_name(base)

def append_rows_by_tab(tab_to_rows):
    if not SHEET_ID or not SHEETS_CRED:
        logi("[SHEETS] Skipping (SHEET_ID or GOOGLE_SHEETS_CRED not set)."); return
    if not tab_to_rows:
        logi("[SHEETS] No new rows to append."); return
    try:
        import gspread
        with open(SHEETS_CRED, "r", encoding="utf-8") as f:
            client_email = json.load(f).get("client_email", "<unknown>")
        logi(f"[SHEETS] Using service account: {client_email}")
        logi(f"[SHEETS] Target Sheet ID: {SHEET_ID}")
        gc = gspread.service_account(filename=SHEETS_CRED); sh = gc.open_by_key(SHEET_ID)
        for tab, rows in tab_to_rows.items():
            if not rows: continue
            try: ws = sh.worksheet(tab)
            except Exception: 
                ws = sh.add_worksheet(title=tab, rows="1000", cols="60"); ws.append_row(HEADER)
            try: first_cell = ws.acell("A1").value
            except Exception: first_cell = None
            if not first_cell: ws.append_row(HEADER)
            values = [[r.get(k,"") for k in HEADER] for r in rows]
            ws.append_rows(values, value_input_option="RAW")
            logi(f"[SHEETS] Appended {len(values)} rows to '{tab}'.")
    except Exception as e:
        logw(f"[SHEETS] Error: {e}"); logw(traceback.format_exc())

# ============== Search helpers ==============
def run_text_search(query, cfg, debug=False):
    max_per_query = cfg["max_per_query"]
    all_results, fetched, token = [], 0, None
    while True:
        data, status = places_text_search(query, token, cfg, debug=debug)
        if not data: break
        results = data.get("results", [])
        for r in results:
            all_results.append(r); fetched += 1
            if fetched >= max_per_query: return all_results
        token = data.get("next_page_token")
        if not token: break
        time.sleep(PAGE_TOKEN_WAIT)
    return all_results

# ============== City/niche processing with concurrency ==============
def process_batch_with_threads(details_list, niche, city, cfg, csv_buffer, sheet_bucket, counters):
    """Submit details to thread pool, enrich in parallel, upsert in main."""
    results = []
    with ThreadPoolExecutor(max_workers=cfg["max_workers"]) as ex:
        futures = [ex.submit(process_one, d, niche, city, cfg["sleep"]) for d in details_list]
        for fut in as_completed(futures):
            out = fut.result()
            if not out: continue
            row, niche_keywords = out
            # Email-first guard
            if not row["email"] or _is_blacklisted(row["email"]):
                counters["skipped_no_email"] += 1
                continue
            # upsert (MAIN THREAD ONLY)
            is_new = upsert_row(cfg["conn"], row)
            if is_new:
                cur = cfg["conn"].cursor()
                cur.execute("SELECT first_seen,last_seen FROM leads WHERE dedupe_key=?", (row["dedupe_key"],))
                ts = cur.fetchone()
                row["first_seen"] = ts[0] if ts else ""
                row["last_seen"]  = ts[1] if ts else ""
                csv_buffer.append(row)
                tab = compute_tab_name(cfg, row.get("niche_search"))
                sheet_bucket.setdefault(tab, []).append(row)
            results.append(row)
    return results

def collect_details_for_query(results, cfg):
    """Call place_details sequentially to respect rate limits; returns list of details dicts."""
    details_list = []
    for r in results:
        pid = r.get("place_id")
        if not pid: continue
        d = place_details(pid, cfg)
        time.sleep(cfg["sleep"])
        if d: details_list.append(d)
    return details_list

def process_city_batch(niche, cities, cfg, csv_buffer, sheet_bucket, *, counters):
    start_city_window = time.time()
    for city in cities:
        if time.time() - start_city_window > cfg["city_timeout"]:
            logw(f"[TIMEOUT] City batch exceeded {cfg['city_timeout']}s. Skipping remaining cities for '{niche}'.")
            return
        logi(f"Searching: {niche} in {city}")
        results = run_text_search(f"{niche} in {city}, United States", cfg)
        time.sleep(cfg["sleep"])
        details_list = collect_details_for_query(results, cfg)
        process_batch_with_threads(details_list, niche, city, cfg, csv_buffer, sheet_bucket, counters)

# ============== Main ==============
def main():
    parser = argparse.ArgumentParser(description="Klix Lead Finder (US)")
    parser.add_argument("--max", type=int, default=40)
    parser.add_argument("--sleep", type=float, default=0.9)
    parser.add_argument("--country_only", action="store_true")
    parser.add_argument("--top", type=int, default=0)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--max_retries", type=int, default=3)
    parser.add_argument("--retry_backoff", type=float, default=1.5)
    parser.add_argument("--city_timeout", type=int, default=600)
    parser.add_argument("--api_budget", type=int, default=1000)
    parser.add_argument("--warn_at", type=float, default=0.8)
    parser.add_argument("--sheet_name", type=str, default="Leads")
    parser.add_argument("--sheet_mode", type=str, default="per_day", choices=["fixed","per_day","per_niche"])
    parser.add_argument("--max_workers", type=int, default=int(CFG_FILE.get("max_workers", 10)))
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = {
        "max_per_query": max(1, args.max),
        "sleep": max(0.2, args.sleep),
        "max_retries": max(0, args.max_retries),
        "retry_backoff": max(0.5, args.retry_backoff),
        "city_timeout": max(60, args.city_timeout),
        "api_calls": 0,
        "over_limit_hits": 0,
        "api_budget": max(1, args.api_budget),
        "warn_at": min(0.99, max(0.1, args.warn_at)),
        "conn": None,
        "sheet_name": args.sheet_name,
        "sheet_mode": args.sheet_mode,
        "max_workers": max(1, args.max_workers),
    }

    if args.rebuild and os.path.exists(DB_PATH):
        os.remove(DB_PATH); logi("[REBUILD] Deleted existing leads.db")

    require_key()
    conn = ensure_db(debug=args.debug)
    cfg["conn"] = conn

    if args.debug:
        cur = conn.cursor(); cur.execute("PRAGMA table_info(leads)")
        cols = [r[1] for r in cur.fetchall()]
        logd(f"[DEBUG] Current DB columns: {json.dumps(cols)}")

    cities = load_cities(); niches = load_niches()
    logi(f"Loaded {len(cities)} cities, {len(niches)} niches")

    csv_buffer, sheet_bucket = [], {}
    counters = {"skipped_no_email": 0}

    try:
        for niche, mode in niches:
            mode_eff = "country" if args.country_only else mode
            logi(f"=== Niche: {niche} (mode: {mode_eff}) ===")

            used_frac = cfg["api_calls"] / cfg["api_budget"]
            if used_frac >= cfg["warn_at"]:
                pct = int(used_frac * 100)
                logw(f"[BUDGET] ~{pct}% of daily API budget this run ({cfg['api_calls']}/{cfg['api_budget']}).")

            if mode_eff == "country":
                logi(f"Searching US-wide: {niche}")
                results = run_text_search(f"{niche} United States", cfg)
                details_list = collect_details_for_query(results, cfg)
                process_batch_with_threads(details_list, niche, "", cfg, csv_buffer, sheet_bucket, counters)
            else:
                process_city_batch(niche, cities, cfg, csv_buffer, sheet_bucket, counters=counters)

            if cfg["over_limit_hits"] >= 3:
                logw("[RATE] Multiple OVER_QUERY_LIMIT events. Consider increasing --sleep or resuming later.")
    finally:
        csv_buffer.sort(key=lambda r: to_int(r.get("score"), 0), reverse=True)
        csv_write = csv_buffer if args.top in (0, None) else csv_buffer[:args.top]
        export_new_to_csv(csv_write, CSV_NEW_PATH)
        export_all_to_csv(conn, CSV_ALL_PATH)
        if args.top not in (0, None):
            allowed_set = {id(r) for r in csv_write}
            filtered = {tab:[r for r in rows if id(r) in allowed_set] for tab,rows in sheet_bucket.items()}
            sheet_bucket = filtered
        append_rows_by_tab(sheet_bucket)

        used_frac = cfg["api_calls"] / cfg["api_budget"]
        pct = min(100, int(used_frac * 100))
        logi(f"✅ Run complete: {len(csv_buffer)} new leads captured.")
        logi(f"    → New-only export: {CSV_NEW_PATH} ({len(csv_write)} rows)")
        logi(f"    → FULL export:     {CSV_ALL_PATH} (all rows in DB)")
        logi(f"[BUDGET] Approx API calls this run: {cfg['api_calls']} / {cfg['api_budget']} (~{pct}%).")
        if cfg["over_limit_hits"] > 0:
            logw(f"[RATE] OVER_QUERY_LIMIT events: {cfg['over_limit_hits']}.")
        logi(f"[FILTER] Skipped for missing/blacklisted email: {counters['skipped_no_email']}")
        logi("Tip: sort by 'score' desc to prioritize.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INTERRUPT] Stopped by user after summary. Exiting cleanly.")
