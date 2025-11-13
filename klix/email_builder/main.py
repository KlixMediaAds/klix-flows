from __future__ import annotations
from typing import Tuple, Optional, Dict, Any
import re, html, random
from urllib.parse import urlparse


# Return: (subject, body_text, body_html, send_type_guess, prompt_profile_id)
def build_email_for_lead(lead: Dict[str, Any]) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    """
    Lightweight, non-botty builder.

    - 2–4 short lines max
    - No links, no emojis, no bullets
    - Uses company/website and any meta hints if present
    - Adds style variation so not every email looks the same
    """
    company = (lead.get("company") or "").strip()
    website = (lead.get("website") or "").strip()
    first = (lead.get("first_name") or "").strip()
    meta = lead.get("meta") or {}  # optional JSON field if present

    host = _domain_from_url(website) or ""
    callname = first if first else "there"
    brand_for_subj = company or (host.split(".", 1)[0].title() if host else "your brand")

    # ---- observation phrase (more variety) ----------------------------------
    tagline = _safe(meta.get("tagline") or meta.get("Tagline"))
    category = _safe(meta.get("category") or meta.get("Category"))
    last_product = _safe(meta.get("last_product") or meta.get("LastProduct"))
    last_post = _safe(meta.get("last_post") or meta.get("LastPost"))
    city = _safe(meta.get("city") or meta.get("City"))

    obs_candidates = []

    if tagline:
        obs_candidates.append(f"your line \"{tagline}\"")
    if last_product:
        obs_candidates.append(f"how you present {last_product}")
    if last_post:
        obs_candidates.append("a recent post on your page")
    if category and company:
        obs_candidates.append(f"how {company} leans into {category.lower()}")
    if city and company:
        obs_candidates.append(f"how {company} fits the {city} crowd")
    if company:
        obs_candidates.append(f"the way {company}'s site is laid out")
        obs_candidates.append(f"the overall feel of {company}'s site")
    if host and not company:
        obs_candidates.append(f"how your site on {host} feels")
        obs_candidates.append("the way your homepage is set up")

    if not obs_candidates:
        obs_candidates.append("your brand")

    obs_phrase = random.choice(obs_candidates)

    # ---- what-I-do + CTA variants ------------------------------------------
    what_i_do_options = [
        "I make short, native-feel clips that look like they belong in the feed.",
        "I build 15–20s clips that feel more like regular posts than polished ads.",
        "I focus on tiny, scroll-friendly clips—close shots, simple pacing, no heavy production.",
        "I cut simple product moments that feel like someone filming on their phone.",
        "I keep things low-friction: one product, one angle, one clear feel.",
    ]
    what_i_do = random.choice(what_i_do_options)

    cta_pool = [
        "Open to a tiny 15–20s test?",
        "Okay if I mock up a 20s sample this week?",
        "Should I send over one rough 20s cut so you can see it in your feed context?",
        "Want me to try a single 20s clip and send it over?",
        "If you’re curious, I can send one small test clip and you can ignore it if it misses.",
    ]
    cta = random.choice(cta_pool)

    # ---- subject style variants --------------------------------------------
    subj_variants = []

    subj_variants.append(f"curious about a tiny test for {brand_for_subj}")
    subj_variants.append("tiny video thought")
    subj_variants.append(f"this might be too specific for {brand_for_subj}")
    subj_variants.append(f"a clip idea for {brand_for_subj}")
    subj_variants.append(f"oddly specific idea for {brand_for_subj}")

    subject = random.choice(subj_variants)[:80]

    # ---- body style variants -----------------------------------------------
    style = random.choice(["question_break", "edit_queue", "simple", "oddly_specific"])

    if style == "question_break":
        # short opener + linebreak
        body_lines = [
            f"Hi {callname}, random question:",
            "",
            f"have you ever tested a super simple 15–20s clip built around {obs_phrase}?",
            "",
            what_i_do,
            "",
            cta,
        ]
    elif style == "edit_queue":
        body_lines = [
            f"Hey {callname}, I was editing a short product clip and it reminded me of {obs_phrase}.",
            "",
            what_i_do,
            "",
            cta,
        ]
    elif style == "oddly_specific":
        body_lines = [
            f"Hi {callname}, this is oddly specific but I had a shot in mind built around {obs_phrase}.",
            "",
            "One tiny scene, one angle, subtle movement and text—more like someone filming on their phone than a polished ad.",
            "",
            cta,
        ]
    else:  # "simple"
        body_lines = [
            f"Hi {callname}, I got a quick 20s video idea from looking at {obs_phrase}.",
            "",
            what_i_do,
            "",
            cta,
        ]

    body_text = "\n".join(body_lines)

    # No HTML for first touch (plain reads cleaner, fewer spam signals)
    body_html: Optional[str] = None

    # ---- normalize punctuation/spaces but keep line breaks ------------------
    subject = _normalize(subject)
    body_text = _normalize(body_text, keep_newlines=True)

    # Let the flow decide cold vs friendly; no prompt profile for now
    send_type_guess: Optional[str] = None
    prompt_profile_id: Optional[str] = None
    return subject, body_text, body_html, send_type_guess, prompt_profile_id


# ------------------------- helpers -------------------------------------------

def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    u = urlparse(url if "://" in url else f"https://{url}")
    host = u.hostname or ""
    # strip common www
    return host.replace("www.", "") if host else ""


def _safe(v: Any) -> str:
    s = (str(v) if v is not None else "").strip()
    return s[:160] if s else ""


def _normalize(s: str, keep_newlines: bool = False) -> str:
    """Normalize punctuation and spacing. If keep_newlines=True, preserve line breaks."""
    if not s:
        return s
    s = html.unescape(s)
    s = s.replace("’", "'").replace("“", '"').replace("”", '"').replace("–", "-").replace("—", "-")

    if keep_newlines:
        lines = s.splitlines()
        cleaned = []
        for line in lines:
            line = re.sub(r"\s{2,}", " ", line)
            line = re.sub(r"\s*([,.:;!?])", r"\1", line)   # no space before punctuation
            line = re.sub(r"\s*-\s*", "-", line)           # keep our 15–20s tight
            cleaned.append(line.strip())
        s = "\n".join(cleaned)
        s = re.sub(r"\n{3,}", "\n\n", s)
        return s.strip()

    # single-line normalization
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"\s*([,.:;!?])", r"\1", s)
    s = re.sub(r"\s*-\s*", "-", s)
    return s.strip()
