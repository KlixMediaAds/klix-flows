import json
from typing import Any, Dict, Optional

from klix.brain_gateway import brain_gateway


SYSTEM_PROMPT = """
You are a reply classification engine for a B2B cold email system called Klix OS.

Your job:
- Read the email reply.
- Decide what kind of reply it is.
- Return a STRICT JSON object with fields described below.
- Do NOT include any explanation text, only JSON.

Fields (all lowercase keys):

- category: one of:
    "INTERESTED"
    "OBJECTION"
    "NOT_INTERESTED"
    "OOO"
    "UNSUBSCRIBE"
    "QUERY"

- sub_category: short free-text refinement, for example:
    "pricing"
    "timing"
    "already_have_solution"
    "not_a_fit"
    "hire_internally"
    "spam_complaint"
    "vacation_ooo"
    "auto_reply"

- sentiment: one of:
    "positive"
    "neutral"
    "negative"

- interest_score: number from 0 to 1 (float).
    0.0 = no interest at all
    1.0 = extremely interested / wants to move forward

- next_action: one of:
    "book_call"
    "send_case_study"
    "send_followup_email"
    "clarify_requirements"
    "stop"
    "none"

Rules:
- If they explicitly ask to stop or unsubscribe, category = "UNSUBSCRIBE", next_action = "stop".
- If it is clearly an out-of-office, category = "OOO".
- If they seem interested but have questions or concerns, category = "OBJECTION" or "QUERY" depending on tone.
- If they are clearly not interested, category = "NOT_INTERESTED" and interest_score close to 0.

Return example:

{
  "category": "OBJECTION",
  "sub_category": "pricing",
  "sentiment": "neutral",
  "interest_score": 0.4,
  "next_action": "send_followup_email"
}
""".strip()


def _safe_default() -> Dict[str, Any]:
    return {
        "category": "QUERY",
        "sub_category": "unknown",
        "sentiment": "neutral",
        "interest_score": 0.3,
        "next_action": "send_followup_email",
    }


def _parse_json_maybe(text: str) -> Dict[str, Any]:
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to extract JSON between braces
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        snippet = text[start : end + 1]
        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            pass

    return _safe_default()


def classify_reply(
    raw_email: str,
    subject: Optional[str] = None,
    email_send_id: Optional[int] = None,
    lead_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Call BrainGateway to classify a single reply.

    Returns a dict with keys:
        category, sub_category, sentiment, interest_score, next_action

    Telemetry:
    - context_type   = "reply_classification"
    - email_send_id  = (optional) email_sends.id
    - lead_id        = (optional) leads.id
    """
    prompt = f"Subject: {subject or ''}\n\nEmail reply:\n{raw_email}"

    output_text = brain_gateway.generate(
        prompt=prompt,
        system=SYSTEM_PROMPT,
        model="gpt-4.1-mini",
        context_type="reply_classification",
        email_send_id=email_send_id,
        lead_id=lead_id,
    )

    data = _parse_json_maybe(output_text)

    # Normalize + fill defaults
    base = _safe_default()
    base.update({k: v for k, v in data.items() if k in base})

    # Clamp interest_score
    try:
        score = float(base.get("interest_score", 0.3))
        if score < 0.0:
            score = 0.0
        if score > 1.0:
            score = 1.0
        base["interest_score"] = score
    except Exception:
        base["interest_score"] = 0.3

    # Uppercase category for consistency
    if isinstance(base.get("category"), str):
        base["category"] = base["category"].upper()

    # Lowercase sentiment
    if isinstance(base.get("sentiment"), str):
        base["sentiment"] = base["sentiment"].lower()

    return base
