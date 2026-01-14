from __future__ import annotations

from typing import Dict, List, Any, Optional

import os
import json
import random
import time
import re

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


# ==============================================================================
# Global system text (can be overridden by DB profiles via pr.SYSTEM_TEXT)
# ==============================================================================

SYSTEM_TEXT: str = """
You are an assistant that writes short, human cold emails for a single recipient.

Rules:
- Output ONLY a JSON object with two keys: "subject" and "body_md".
- "subject": 3–7 words, specific, non-clickbait, no emojis.
- "body_md": 70–130 words, 1–2 short paragraphs.
- Keep the tone calm, clear, and human. No hype, no hard sell.
- Reference the business naturally (not in a robotic way).
- Avoid generic openings like "I noticed", "I saw", or "I came across".
- End with a low-friction question (e.g., asking if they'd like a small idea or example).

Do NOT include:
- salutations like "Hi NAME" or signatures.
- links, emojis, or bullet lists.
- any text outside the JSON.
""".strip()


# ==============================================================================
# Deterministic scorer (NO LLMs)
# ==============================================================================

def _score_email_candidate(subject: str, body_md: str) -> tuple[float, list[str]]:
    reasons: list[str] = []
    subject = (subject or "").strip()
    body = (body_md or "").strip()

    score = 1.0

    if not subject:
        score -= 0.25
        reasons.append("missing_subject")

    if not body:
        score -= 0.60
        reasons.append("missing_body")
        return max(0.0, min(1.0, score)), reasons

    qcount = body.count("?")
    if qcount == 0:
        score -= 0.10
        reasons.append("no_question")
    elif qcount > 1:
        score -= 0.10
        reasons.append("multiple_questions")

    if len(subject) > 120:
        score -= 0.05
        reasons.append("subject_too_long")

    if len(body) < 180:
        score -= 0.10
        reasons.append("body_too_short")

    if len(body) > 900:
        score -= 0.10
        reasons.append("body_too_long")

    low = (subject + " " + body).lower()
    spam_terms = [
        "guarantee", "guaranteed", "risk-free", "act now", "limited time",
        "free money", "earn $", "double your", "no obligation",
    ]
    hits = [t for t in spam_terms if t in low]
    if hits:
        score -= 0.10
        reasons.append("spam_terms:" + ",".join(hits[:3]))

    if low.count("!") >= 2:
        score -= 0.05
        reasons.append("too_many_exclamations")

    return max(0.0, min(1.0, score)), reasons


# ==============================================================================
# Small helpers
# ==============================================================================

def _observation_from(biz: Dict[str, Any]) -> str:
    name = (biz.get("BusinessName") or "").strip()
    niche = (biz.get("Niche") or "").strip()
    tagline = (biz.get("Tagline") or "").strip()
    city = (biz.get("City") or "").strip()

    if tagline:
        return f"Your line “{tagline}” sets a really clear tone."
    if name and niche and city:
        return f"{name} comes across as a thoughtful {niche.lower()} brand in {city}."
    if name and niche:
        return f"{name} feels like a focused {niche.lower()} brand."
    if name:
        return f"{name} has a distinct, coherent presence."
    return "Your brand has a clear visual identity."


def _choose_subject(biz: Dict[str, Any]) -> str:
    name = (biz.get("BusinessName") or "").strip()
    base = name or "your brand"
    options = [
        f"Small video idea for {base}",
        f"A tiny concept for {base}",
        "A simple visual idea",
        "A short content thought",
    ]
    return random.choice(options)


def _fallback_rule_based(biz: Dict[str, Any]) -> Dict[str, str]:
    observation = _observation_from(biz)
    subject = _choose_subject(biz)

    body = (
        f"{observation}\n\n"
        "I had a small idea for a short, quiet video: a few simple close-ups that show your product or service "
        "in use, with gentle pacing and subtle sound. Nothing loud or salesy—just a moment that feels like your brand.\n\n"
        "If that kind of piece could be useful, I can sketch a couple of tiny directions?"
    )

    score, reasons = _score_email_candidate(subject, body)

    return {
        "subject": subject,
        "body_md": body.strip(),
        "score": score,
        "score_reasons": reasons,
    }


def _normalize_model_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return "gpt-4o-mini"
    return name


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return {}
        return {}


# ==============================================================================
# OpenAI caller
# ==============================================================================

def _call_model(messages: List[Dict[str, Any]], model_name: str, n: int = 3):
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key or OpenAI is None:
        return None

    model_name = _normalize_model_name(model_name)
    client = OpenAI(api_key=api_key)

    backoff = 1.0
    for _ in range(4):
        try:
            return client.chat.completions.create(
                model=model_name,
                messages=messages,
                n=n,
                temperature=0.95,
                top_p=0.96,
                presence_penalty=0.2,
                frequency_penalty=0.1,
                max_tokens=340,
            )
        except Exception as e:
            msg = str(e)
            if any(x in msg for x in ("insufficient_quota", "RateLimit", "429")):
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            return None
    return None


# ==============================================================================
# Drafting
# ==============================================================================

def _build_user_prompt(biz: Dict[str, Any], angle_id: str) -> str:
    name = biz.get("BusinessName") or ""
    niche = biz.get("Niche") or ""
    city = biz.get("City") or ""
    website = biz.get("Website") or ""
    tagline = biz.get("Tagline") or ""
    products = biz.get("Products") or []

    lines = ["Business context:"]
    if name:
        lines.append(f"- Name: {name}")
    if niche:
        lines.append(f"- Niche: {niche}")
    if city:
        lines.append(f"- City: {city}")
    if website:
        lines.append(f"- Website: {website}")
    if tagline:
        lines.append(f"- Tagline: {tagline}")
    if products:
        lines.append(f"- Products/Services: {', '.join(map(str, products))}")

    lines.append(f"\nAngle ID: {angle_id or 'default'}")
    lines.append(
        "\nOutput JSON only, like:\n"
        '{\n  "subject": "Simple subject",\n  "body_md": "Single or two short paragraphs..."\n}'
    )

    return "\n".join(lines)


def draft_email(
    biz: Dict[str, Any],
    angle_id: str,
    model_name: str = "gpt-4o-mini",
    lead_id: Optional[int] = None,
    profile_id: Optional[int] = None,
    style_seed: Optional[str] = None,
    **_kwargs: Any,
) -> Dict[str, Any]:
    _ = lead_id
    _ = profile_id
    _ = style_seed

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key or OpenAI is None:
        return _fallback_rule_based(biz)

    system_text = SYSTEM_TEXT.strip()
    user_prompt = _build_user_prompt(biz, angle_id)

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": user_prompt},
    ]

    resp = _call_model(messages, model_name, n=3)
    if not resp or not getattr(resp, "choices", None):
        return _fallback_rule_based(biz)

    candidates: List[Dict[str, Any]] = []
    for ch in resp.choices:
        content = getattr(ch.message, "content", "") or ""
        obj = _extract_json(content)
        subj = (obj.get("subject") or "").strip()
        body = (obj.get("body_md") or "").strip()
        if subj and body:
            candidates.append({"subject": subj, "body_md": body})

    if not candidates:
        return _fallback_rule_based(biz)

    best = max(candidates, key=lambda c: len(c["body_md"]))
    subj = best["subject"].strip()[:120]
    body = best["body_md"].strip()

    bad_starts = ("I noticed", "I saw", "I came across")
    for bad in bad_starts:
        if body.startswith(bad):
            parts = re.split(r"(?<=[.!?])\s+", body, maxsplit=1)
            if len(parts) == 2:
                body = parts[1].lstrip()
            break

    score, reasons = _score_email_candidate(subj, body)

    return {
        "subject": subj,
        "body_md": body,
        "score": score,
        "score_reasons": reasons,
    }
