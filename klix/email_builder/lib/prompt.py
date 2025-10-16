# lib/prompt.py — diversified cold-email generator (safe lexical variety + smooth scoring + externalizable config)
import os, json, random, re, time, hashlib
from typing import Tuple, Dict, List, Any
from datetime import datetime, timezone

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# ==============================================================================
# Config loading (optional external file for easy tweaking)
# Place a JSON file at: lib/prompt_config.json  (all keys optional).
# If missing, built-in defaults are used.
# ==============================================================================
_DEFAULT_CFG = {
    "buzzwords_and_jargon": [
        "synergy","leverage","cutting-edge","game-changer","disruptive","innovative",
        "paradigm shift","unlock your potential","value proposition","win-win","low-hanging fruit",
        "circle back","touch base","seamless integration","robust solution","10x your",
        "skyrocket your sales","boost your roi","supercharge your growth","guaranteed results",
        "hyper-personalized at scale","cold outreach","email sequence","cadence","performance iteration",
        "i specialize in helping","we handle concept → script","i came across your profile"
    ],
    "cliche_openers": [
        "hope you're having a great week.","hope this email finds you well.",
        "i hope you are well.","my name is","just checking in."
    ],
    "weak_or_aggressive_ctas": [
        "book a demo","hop on a quick call","schedule a 15-minute meeting",
        "grab a virtual coffee","let me know what you think",
        "looking forward to hearing from you","are you free to chat this week?"
    ],
    "confidence_killers": [
        "sorry to bother you,","i know your time is valuable, but","i know you're busy, but",
        "just wanted to","if you have a moment,"
    ],
    "no_real_people": ["real customer","real customers","real people","actor","actress","testimonial","filmed with","on-camera talent"],
    "ai_ugc_phrases": [
        "an AI-made, lo-fi clip", "a simple AI UGC-style moment",
        "a small AI clip with that organic feel", "an AI-shot, cozy product vignette",
        "a short AI piece that feels native", "a quiet, AI-crafted product scene"
    ],
    "cta_buckets": {
        "peek": [
            "Want a 20-sec peek?", "Open to a tiny preview?", "Worth a 15-sec look?",
            "Want me to send a rough cut?", "Open to a quick sample?"
        ],
        "permission": [
            "Mind if I send two mini concepts?", "Okay if I share a tiny test?",
            "Cool if I send a micro-example?", "Okay if I send a quick mock?"
        ],
        "micro_commit": [
            "Should I send two tiny options?", "Want me to sketch two routes?",
            "Want two small directions to compare?"
        ]
    },
    "softeners": [
        "Quick thought:", "Curious if this fits:",
        "Could be off here:", "If this misses, no worries —",
        "Random idea sparked by {signal}:", "Not sure if this is useful, but —"
    ],
    # Style profiles (paragraphs / rhythm)
    "style_profiles": [
        {"id":"p2_softener",   "paras":2, "softener":True,  "max_words":120, "linebreak_mid":False},
        {"id":"p1_midbreak",   "paras":1, "softener":False, "max_words":105, "linebreak_mid":True},
        {"id":"p2_direct",     "paras":2, "softener":False, "max_words":115, "linebreak_mid":False},
        {"id":"p1_micro",      "paras":1, "softener":False, "max_words":85,  "linebreak_mid":False},
        {"id":"p1_visual",     "paras":1, "softener":False, "max_words":95,  "linebreak_mid":False},
        {"id":"p2_visual_soft","paras":2, "softener":True,  "max_words":120, "linebreak_mid":False},
        {"id":"p1_invert",     "paras":1, "softener":False, "max_words":100, "linebreak_mid":False, "invert":True},
        {"id":"p2_clipped",    "paras":2, "softener":False, "max_words":110, "linebreak_mid":False, "clipped":True}
    ],
    # Scoring weights (tunable)
    "weights": {
        "w_contraction": 0.25,
        "w_question":    0.20,
        "w_personal":    0.15,   # multiplied by min(3, personalized_hits)
        # Smooth word count bonus: bonus = w_wc_max * exp(- (dist^2) / (2*sigma^2))
        "w_wc_max":      0.25,
        "wc_ideal":      90,
        "wc_sigma":      20
    },
    "system_text": (
        "You are a considerate human marketer writing short, specific cold emails for small businesses. "
        "Tone: warm, curious, modest. Use an 'I' voice with contractions. Keep it human and concrete.\n"
        "IMPORTANT: Ensure your entire output is only the JSON object specified at the end.\n"
        "Structure: Prefer 1–2 short paragraphs (may be a single tight paragraph). No emojis, no bullets, no links.\n"
        "Core Idea: Propose an AI video that looks like organic UGC (lo-fi, simple, cozy product moments), "
        "without implying real customers, actors, or testimonials.\n"
        "CRITICAL: First sentence references a specific signal from the business data. "
        "Avoid corporate jargon and cliches. Avoid hype. Keep body ≤ 120 words. Subject ≤ ~6–7 words.\n"
        "Do NOT add a signature or brand line.\n"
    )
}

def _load_config() -> Dict[str, Any]:
    # Look beside this file for prompt_config.json
    here = os.path.dirname(os.path.abspath(__file__))
    cfg_path = os.path.join(here, "prompt_config.json")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            # Merge shallowly: user values override defaults
            cfg = _DEFAULT_CFG.copy()
            for k, v in user_cfg.items():
                if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                    merged = cfg[k].copy()
                    merged.update(v)
                    cfg[k] = merged
                else:
                    cfg[k] = v
            return cfg
        except Exception:
            pass
    return _DEFAULT_CFG

CFG = _load_config()

# Shorthands from config
BUZZWORDS_AND_JARGON = set(CFG["buzzwords_and_jargon"])
CLICHE_OPENERS       = set(CFG["cliche_openers"])
WEAK_OR_AGGRESSIVE_CTAS = set(CFG["weak_or_aggressive_ctas"])
CONFIDENCE_KILLERS   = set(CFG["confidence_killers"])
NO_REAL_PEOPLE       = set(CFG["no_real_people"])
AI_UGC_PHRASES       = list(CFG["ai_ugc_phrases"])
CTA_BUCKETS          = dict(CFG["cta_buckets"])
SOFTENERS            = list(CFG["softeners"])
STYLE_PROFILES       = list(CFG["style_profiles"])
W                    = dict(CFG["weights"])
SYSTEM_TEXT          = CFG["system_text"]

# ==============================================================================
# Helpers
# ==============================================================================
def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def shorten(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[:n]

def pick_product(biz: Dict) -> str:
    prods = biz.get("Products") or []
    if isinstance(prods, str):
        prods = [p.strip() for p in prods.split(",") if p.strip()]
    return (prods[0] if prods else "product").strip()

def benefit_hook(biz: Dict) -> str:
    niche = (biz.get("Niche","") or "").lower()
    if "candle" in niche: return "calm"
    if "spa" in niche or "salon" in niche: return "relax"
    if "dental" in niche: return "gentle"
    if "flor" in niche: return "fresh"
    return "simple"

JUNK_PATTERNS = re.compile(
    r"(requires javascript|coming soon|under construction|lorem ipsum|^home$|^index$|^welcome$)", re.I
)

def observation_from(biz: Dict) -> str:
    candidates = [
        biz.get("AboutLine",""),
        biz.get("Tagline",""),
        biz.get("SiteTitle",""),
        (", ".join(biz.get("Products") or [])),
        biz.get("BusinessName",""),
    ]
    for c in candidates:
        c = (c or "").strip()
        if not c: continue
        if len(c) < 4: continue
        if JUNK_PATTERNS.search(c): continue
        return c
    return biz.get("BusinessName","your brand")

DASHES = "—–"
SIGNATURE_MARKERS = re.compile(r"^\s*[–—-]\s*klix\s*media\b.*$", re.I | re.M)

def normalize_text(s: str) -> str:
    if not s: return s
    s = SIGNATURE_MARKERS.sub("", s)
    for ch in DASHES:
        s = s.replace(ch, "-")
    s = re.sub(r"\s*-\s*", " - ", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s

def _has_contraction(text: str) -> bool:
    return bool(re.search(r"\b(I’m|I'm|don’t|doesn’t|can’t|won’t|we’re|we’ve|you’ll|you’re|that’s|it’s)\b", text, re.I))

def _contains_any(text: str, words: set) -> bool:
    t = (text or "").lower()
    return any(w in t for w in words)

def _starts_with_cliche(text: str) -> bool:
    t = (text or "").lower().strip()
    return any(t.startswith(c) for c in CLICHE_OPENERS)

def _ends_with_question(text: str) -> bool:
    return (text or "").strip().endswith("?")

def _personalized(text: str, biz: Dict) -> int:
    hits = 0
    for key in ("BusinessName","City","Niche","SiteTitle","Tagline","AboutLine"):
        v = (biz.get(key) or "").strip().lower()
        if v and v in (text or "").lower():
            hits += 1
    return hits

def _violation(text: str) -> bool:
    t = (text or "").lower()
    return (
        _contains_any(t, BUZZWORDS_AND_JARGON) or
        _contains_any(t, CONFIDENCE_KILLERS) or
        _contains_any(t, WEAK_OR_AGGRESSIVE_CTAS) or
        _contains_any(t, NO_REAL_PEOPLE) or
        _starts_with_cliche(t)
    )

def lexical_mutate(s: str) -> str:
    """Safe lexical variety: tiny synonym & rhythm shifts. (No sentence inversion.)"""
    swaps = {
        r"\bquick\b": "small",
        r"\bidea\b": "thought",
        r"\bcozy\b": "calm",
        r"\bclip\b": "piece",
        r"\bscene\b": "moment",
        r"\bvibe\b": "feel",
        r"\bsubtle\b": "soft",
        r"\bgentle\b": "quiet",
        r"\btext\b": "on-screen text",
        r"\bpreview\b": "peek",
        r"\bmock\b": "rough cut",
    }
    out = s
    if swaps:
        k = random.randint(2, min(5, len(swaps)))
        for pat, rep in random.sample(list(swaps.items()), k=k):
            out = re.sub(pat, rep, out, flags=re.I)
    return out

def ngram_fingerprint(s: str, n: int = 3) -> str:
    toks = re.findall(r"[a-z0-9']+", (s or "").lower())
    grams = [" ".join(toks[i:i+n]) for i in range(max(0, len(toks)-n+1))]
    return hashlib.sha1(("|".join(grams)).encode("utf-8")).hexdigest()

# Smooth Gaussian-like wordcount bonus
def _wc_bonus(words: int) -> float:
    ideal = float(W.get("wc_ideal", 90))
    sigma = float(W.get("wc_sigma", 20))
    wmax  = float(W.get("w_wc_max", 0.25))
    dist = words - ideal
    return wmax * pow(2.718281828, - (dist * dist) / (2.0 * sigma * sigma))

def score_humanness(candidate: Dict, biz: Dict) -> float:
    subj = candidate.get("subject",""); body = candidate.get("body_md","")
    if not subj or not body: return 0.0
    if _violation(subj) or _violation(body): return 0.1
    words = len(body.split())

    score = 1.0
    if _has_contraction(body): score += float(W.get("w_contraction", 0.25))
    if "?" in body:            score += float(W.get("w_question",    0.20))
    score += float(W.get("w_personal", 0.15)) * min(3, _personalized(body, biz))
    score += _wc_bonus(words)
    # small bonus for organic terms
    if "ugc" in body.lower() or "lo-fi" in body.lower() or "lofi" in body.lower():
        score += 0.05
    return score

# ==============================================================================
# Subject & style selection
# ==============================================================================
def choose_style() -> dict:
    # Weighted mildly toward two-paragraph formats
    weighted = []
    for prof in STYLE_PROFILES:
        w = 2 if prof["id"].startswith("p2") else 1
        weighted.extend([prof] * w)
    return random.choice(weighted)

def choose_subject(biz: Dict) -> str:
    patterns = [
        lambda b: f"Idea for {shorten(b.get('BusinessName','Your brand'), 12)}",
        lambda b: f"Simple {pick_product(b)} clip",
        lambda b: f"{b.get('City','')} vibe, tiny idea".strip().replace("  "," "),
        lambda b: f"Small concept for {shorten(b.get('SiteTitle', b.get('BusinessName','you')), 12)}",
        lambda b: f"{benefit_hook(b)} concept",
        lambda b: "Tiny video thought",
    ]
    subj = random.choice(patterns)(biz)
    # small casing diversity (never all-caps)
    if random.random() < 0.35:
        subj = subj.lower()
    elif random.random() < 0.2:
        subj = subj.capitalize()
    return subj[:80]

def choose_cta() -> str:
    bucket = random.choice(list(CTA_BUCKETS.keys()))
    return random.choice(CTA_BUCKETS[bucket])

def softener_line(biz: Dict) -> str:
    sig = biz.get("LastPostDate","a recent post")
    return random.choice(SOFTENERS).format(signal=sig)

# ==============================================================================
# Prompt build
# ==============================================================================
def build_user_block(biz: Dict, angle_id: str, style: dict) -> str:
    prod_list = ", ".join(biz.get("Products") or [])[:60]
    about = observation_from(biz)
    lines = [
        "Output format requirement: Ensure your entire output is only the JSON object specified below.",
        "",
        "Business:",
        f"- Name: {biz.get('BusinessName','')}",
        f"- Niche: {biz.get('Niche','')}",
        f"- City: {biz.get('City','')}",
        f"- Website: {biz.get('Website','')}",
        f"- SiteTitle: {biz.get('SiteTitle','')}",
        f"- Tagline: {biz.get('Tagline','')}",
        f"- AboutLine: {about}",
        f"- Products: {prod_list}",
        f"- Founder: {biz.get('Founder','')}",
        f"- Instagram: {biz.get('Instagram','')} (LastPost: {biz.get('LastPostDate','')})",
        f"- Notes: {biz.get('Notes','')}",
        "",
        f"Angle: {angle_id}",
        "",
        "Voice & Constraints:",
        "- Use a warm, modest 'I' voice. Begin with a concrete observation tied to the data above.",
        "- The idea is a tiny video that feels organic/natural on social (no actors/customers/testimonials).",
        "- Creative Spark: give one specific visual or small sensory moment.",
        f"- Structure: {style.get('paras',2)} paragraph(s). Keep body ≤ {style.get('max_words',120)} words; Subject ≤ ~6–7 words.",
        "- CTA: End with a low-friction question.",
        "- Avoid emojis, links, bullets, corporate jargon, hype, and cliche openers.",
        "- Do NOT add a signature or brand line.",
        "",
        "Output JSON precisely:",
        "{\"subject\":\"...\",\"body_md\":\"...\"}"
    ]
    return "\n".join(lines)

# ==============================================================================
# OpenAI call
# ==============================================================================
RESPONSES_ONLY_PREFIXES = ("gpt-4.1", "gpt-4.1-mini", "gpt-5", "gpt-5.", "o3", "o4", "o1")

def _normalize_model_name(model_name: str) -> str:
    if not model_name:
        return "gpt-4o-mini"
    m = model_name.lower().strip()
    if m in {"gpt-5","gpt5","gpt-5-thinking"}:
        return "gpt-4o-mini"
    return model_name

def _is_responses_model(name: str) -> bool:
    base = (name or "").split(":")[0].lower().strip()
    return any(base.startswith(p) for p in RESPONSES_ONLY_PREFIXES)

def _call_model(messages: List[Dict], model_name: str, n: int = 5):
    api_key = (os.getenv("OPENAI_API_KEY","") or "").strip()
    if not api_key or OpenAI is None:
        return None

    model_name = _normalize_model_name(model_name)
    client = OpenAI(api_key=api_key)

    class _Msg:
        def __init__(self, content):
            self.message = type("m", (), {"content": content})
    class _Shim:
        def __init__(self, texts):
            self.choices = [_Msg(t) for t in texts]

    backoff = 1.0
    for _ in range(5):
        try:
            if not _is_responses_model(model_name):
                return client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    n=n,
                    temperature=0.95,
                    top_p=0.96,
                    presence_penalty=0.35,
                    frequency_penalty=0.2,
                    max_tokens=340,
                )
            else:
                try:
                    r = client.responses.create(
                        model=model_name,
                        messages=messages,
                        n=n,
                        temperature=0.95,
                        top_p=0.96,
                        max_completion_tokens=340,
                        response_format={"type": "text"},
                    )
                except TypeError:
                    r = client.responses.create(
                        model=model_name,
                        messages=messages,
                        n=n,
                        temperature=0.95,
                        top_p=0.96,
                        max_output_tokens=340,
                        response_format={"type": "text"},
                    )
                texts = []
                if hasattr(r, "output") and r.output:
                    for item in r.output:
                        parts = getattr(item, "content", []) or []
                        text = "".join(getattr(p, "text", "") for p in parts)
                        if text:
                            texts.append(text.strip())
                if not texts and hasattr(r, "output_text") and r.output_text:
                    texts = [r.output_text.strip()]
                if not texts:
                    texts = [""] * n
                if len(texts) < n:
                    texts += [texts[0]] * (n - len(texts))
                else:
                    texts = texts[:n]
                return _Shim(texts)
        except Exception as e:
            msg = str(e)
            if any(x in msg for x in ("insufficient_quota", "RateLimit", "429")):
                time.sleep(backoff); backoff = min(backoff*2, 8.0); continue
            if "Unsupported parameter" in msg or "not compatible with v1/chat/completions" in msg:
                model_name = "gpt-4.1-mini"
                continue
            raise

def _extract_json(text: str) -> Dict:
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return {}

# ==============================================================================
# Drafting
# ==============================================================================
def _fallback_rule_based(biz: Dict, subj: str, style: dict) -> Dict:
    about = observation_from(biz)
    ai_ugc = random.choice(AI_UGC_PHRASES)
    cta = choose_cta()

    if style["paras"] == 1:
        body = (
            f"I noticed “{about}” and liked how clear it reads. "
            f"Small thought: {ai_ugc}—close-ups, quiet ambient SFX, on-screen text that matches your tone. "
            f"{cta}"
        )
    else:
        body = (
            f"I noticed “{about}” and liked how clear it reads.\n\n"
            f"Small thought: {ai_ugc}—close-ups, quiet ambient SFX, on-screen text that matches your tone.\n\n"
            f"{cta}"
        )
    body = lexical_mutate(normalize_text(body))
    return {"subject": subj, "body_md": body}

def draft_email(biz: Dict, angle_id: str, model_name: str = "gpt-4o-mini") -> Dict:
    style = choose_style()
    subject = choose_subject(biz)

    api_key = (os.getenv("OPENAI_API_KEY","") or "").strip()
    if not api_key or OpenAI is None:
        return _fallback_rule_based(biz, subject, style)

    messages = [{"role":"system","content":SYSTEM_TEXT}]
    # Two concise few-shots to anchor tone while allowing variety
    fewshots = [
        {
            "user": (
                "Output format requirement: Ensure your entire output is only the JSON object specified.\n\n"
                "Business:\n- Name: Central Park Candle Company\n- Niche: Candle shop\n"
                "- City: New York\n- SiteTitle: Central Park Candle Co.\n"
                "- Tagline: Soy candles inspired by NYC\n- Products: Fall Collection, Cedar Grove, Park Bench\n"
                "- Notes: fall scents launching\n\n"
                "Angle: product-hook\n\n"
                "Voice & Constraints:\n- Observation first.\n- ≤120 words; 1–2 paragraphs.\n"
                "- No links, no emojis, no bullets. End with a low-friction question.\n"
                "Output JSON: {\"subject\":\"...\",\"body_md\":\"...\"}"
            ),
            "assistant": json.dumps({
                "subject":"Idea for Cedar Grove",
                "body_md":(
                    "Your park-inspired theme reads instantly—“Cedar Grove” feels calm and grounded.\n\n"
                    "Idea: a small AI piece with that organic feel—soft focus on the jar, rain tracing the window, "
                    "quiet SFX, and on-screen text: “Your quiet moment is waiting.” No cast—just product and mood.\n\n"
                    "Open to a tiny preview?"
                )
            })
        },
        {
            "user": (
                "Output format requirement: Ensure your entire output is only the JSON object specified.\n\n"
                "Business:\n- Name: Glow Dental\n- Niche: Dentistry\n- City: Toronto\n"
                "- Tagline: Gentle care, modern tech\n- Products: Invisalign, Whitening Kit\n"
                "- Notes: community posts\n\nAngle: benefits-hook\n\n"
                "Voice & Constraints:\n- Keep it human; ≤120 words; 1–2 paragraphs. No hype.\n"
                "Output JSON: {\"subject\":\"...\",\"body_md\":\"...\"}"
            ),
            "assistant": json.dumps({
                "subject":"Simple whitening concept",
                "body_md":(
                    "Your ‘gentle care’ thread comes through—welcoming without feeling clinical.\n\n"
                    "Small thought: an AI-made, lo-fi clip that walks through the whitening kit—steady close-ups, "
                    "calm captions about sensitivity and timing. No faces—just a reassuring pace.\n\n"
                    "Mind if I send two mini concepts?"
                )
            })
        }
    ]
    for fs in fewshots:
        messages.append({"role":"user","content":fs["user"]})
        messages.append({"role":"assistant","content":fs["assistant"]})

    messages.append({"role":"user","content":build_user_block(biz, angle_id, style)})

    resp = _call_model(messages, model_name, n=5)
    if not resp:
        return _fallback_rule_based(biz, subject, style)

    seen_fp = set()
    candidates = []
    for ch in resp.choices:
        txt = (ch.message.content or "").strip()
        data = _extract_json(txt)
        subj = (data.get("subject","") or "").strip()
        body = (data.get("body_md","") or "").strip()

        if not subj or len(subj.split()) > 7:
            subj = subject
        if not body:
            continue

        # length enforcement
        words = body.split()
        if len(words) > style.get("max_words",120):
            body = " ".join(words[:style.get("max_words",120)])

        # phrase diversity for AI/UGC
        if "ai-powered" in body.lower() or "ugc-style" in body.lower():
            body = re.sub(r"\bai[- ]powered\b.*?\bugc[- ]style\b", random.choice(AI_UGC_PHRASES), body, flags=re.I)
        if "ai-powered" in body.lower():
            body = re.sub(r"\bai[- ]powered\b", random.choice(AI_UGC_PHRASES), body, flags=re.I)
        if "ugc-style" in body.lower():
            body = re.sub(r"\bugc[- ]style\b", "UGC feel", body, flags=re.I)

        # optional softener
        if style.get("softener", False):
            body = f"{softener_line(biz)} {body}".strip()

        # optional mid-line break
        if style.get("linebreak_mid", False) and "\n\n" not in body:
            body = re.sub(r"([.!?])\s+", r"\1\n\n", body, count=1)

        # paragraph enforcement
        if style["paras"] == 1:
            body = re.sub(r"\n{2,}", " ", body).strip()
        else:
            if "\n\n" not in body:
                body = re.sub(r"([.!?])\s+", r"\1\n\n", body, count=1)

        # ensure low-friction question at end
        if not _ends_with_question(body):
            body = body.rstrip(". ") + "\n\n" + choose_cta()

        # normalize + safe lexical mutation
        body = lexical_mutate(normalize_text(body))

        if _violation(subj) or _violation(body):
            continue

        fp = ngram_fingerprint(body)
        if fp in seen_fp:
            continue
        seen_fp.add(fp)

        candidates.append({"subject": subj[:80], "body_md": body})

    if not candidates:
        return _fallback_rule_based(biz, subject, style)

    scored = sorted(((score_humanness(c, biz), c) for c in candidates), key=lambda x: x[0], reverse=True)
    best = scored[0][1]

    if os.getenv("DEBUG_EMAILS","").strip() == "1":
        print("\n---- EMAIL CANDIDATES (unique) ----")
        for s, c in scored:
            print(f"Score {s:.2f} | Subject: {c['subject']}\n{c['body_md']}\n")
        print("---- SELECTED ----")
        print(best)

    return best
