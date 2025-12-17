#!/usr/bin/env python
"""
classify_replies_once.py

Batch classifier for email_replies.

Goals:
- Safely classify replies using:
    1) Heuristics for obvious patterns:
       - Instantly-style warmup replies
       - Domain spam complaints (Report domain: …)
       - Unsubscribes
       - OOO / auto-replies
    2) LLM-based classifier when available:
       - Uses klix.reply_classifier.classify_reply
- Never crash if OpenAI / LLM pipeline is unavailable.
- Always update:
       - category
       - sub_category
       - sentiment
       - interest_score
       - next_action
- (Era 1.9.6) Also populate warmup / noise flags:
       - is_warmup
       - noise_type

This script is **idempotent**: it only touches rows where category IS NULL.

Telemetry (Era 1.9.5+):
- When LLM is used, llm_calls is written via BrainGateway with:
    context_type   = "reply_classification"
    email_send_id  = email_replies.email_send_id (when non-null)
"""

import os
import re
import sys
from typing import Any, Dict, Optional

from sqlalchemy import text

# Ensure project root is on sys.path so `klix` package can be imported
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from klix.brain_gateway import db_session  # uses normalized DB engine

# Best-effort import of LLM classifier; if it fails, we run heuristic-only.
try:
    from klix.reply_classifier import classify_reply as llm_classify_reply
except Exception as e:  # noqa: F841
    llm_classify_reply = None
    print("⚠️ LLM reply classifier unavailable; running in heuristic-only mode.")

# Warmup / noise detector (Era 1.9.6)
from klix.intel.warmup_detector import (
    WarmupDetectionInput,
    detect_warmup_from_row,
)


# --------------------------------------------------------------------
# Heuristic detectors (for category / sentiment / next_action)
# --------------------------------------------------------------------


def _is_warmup_style(subject: str, body: str) -> bool:
    """
    Detect Instantly-style warmup replies by pattern, **without** hardcoding IDs.

    Observed pattern:
      "Jess - still hiring? | DTSHV8V 7QMG3D1"
      "New customers? | YD3WJ4P 7QMG3D1"

    Generalized rule:
      - After a " | " in the subject, we see **two or more** code-like tokens:
        [A-Z0-9]{5,10} [A-Z0-9]{5,10} ...
    """
    subject = subject or ""
    body = body or ""

    # 1) Look at the part after " | " in the subject.
    if "|" in subject:
        tail = subject.split("|", 1)[1].strip()
        tokens = tail.split()
        code_tokens = [t for t in tokens if re.fullmatch(r"[A-Z0-9]{5,10}", t)]
        if len(code_tokens) >= 2:
            return True

    # 2) Extra safety: if both subject and body are full of short all-caps codes,
    #    treat as warmup too. This is conservative and only triggers if we see
    #    multiple code-looking tokens.
    combined = f"{subject} {body}"
    codes = re.findall(r"\b[A-Z0-9]{5,10}\b", combined)
    if len(codes) >= 4 and "|" in subject:
        return True

    return False


def _heuristic_classify(subject: str, body: str) -> Optional[Dict[str, Any]]:
    """
    Pure heuristic classifier.

    Returns a dict with the full reply classification, or None
    if we want to defer to the LLM classifier.
    """
    subject = subject or ""
    body = body or ""
    text_all = f"{subject}\n{body}"
    lower = text_all.lower()

    # 1) Instantly-style warmup replies → kill all interest
    if _is_warmup_style(subject, body):
        return {
            "category": "NOT_INTERESTED",
            "sub_category": "warmup_reply",
            "sentiment": "neutral",
            "interest_score": 0.0,
            "next_action": "none",
        }

    # 2) Domain spam / abuse complaints (Report domain...)
    if "report domain" in lower and "submitter:" in lower:
        # These should always halt sending on that address / thread.
        return {
            "category": "NOT_INTERESTED",
            "sub_category": "spam_complaint",
            "sentiment": "negative",
            "interest_score": 0.0,
            "next_action": "stop",
        }

    # 3) Generic spam reports
    spam_keywords = [
        "spam report",
        "spam complaint",
        "abuse report",
        "email abuse report",
    ]
    if any(k in lower for k in spam_keywords):
        return {
            "category": "NOT_INTERESTED",
            "sub_category": "spam_report",
            "sentiment": "negative",
            "interest_score": 0.0,
            "next_action": "stop",
        }

    # 4) Unsubscribe / stop requests
    unsub_phrases = [
        "unsubscribe",
        "remove me from your list",
        "remove me from this list",
        "please remove me",
        "do not contact me",
        "don't contact me",
        "stop emailing me",
        "stop sending me emails",
        "stop sending emails",
    ]
    if any(p in lower for p in unsub_phrases):
        return {
            "category": "UNSUBSCRIBE",
            "sub_category": "unsubscribe_request",
            "sentiment": "negative",
            "interest_score": 0.0,
            "next_action": "stop",
        }

    # 5) Out-of-office / auto-replies
    ooo_phrases = [
        "out of office",
        "out-of-office",
        "automatic reply",
        "auto reply",
        "autoreply",
        "i am currently away from the office",
        "i am away from the office",
    ]
    if any(p in lower for p in ooo_phrases):
        return {
            "category": "OOO",
            "sub_category": "auto_reply",
            "sentiment": "neutral",
            "interest_score": 0.0,
            "next_action": "none",
        }

    # 6) Clear "not interested" language
    not_interested_phrases = [
        "not interested",
        "no thanks",
        "no thank you",
        "we're all set",
        "we are all set",
        "we are good",
        "we're good",
        "already have a provider",
        "already have this covered",
        "we handle this internally",
        "we do this in house",
        "we do this in-house",
    ]
    if any(p in lower for p in not_interested_phrases):
        return {
            "category": "NOT_INTERESTED",
            "sub_category": "not_a_fit",
            "sentiment": "negative",
            "interest_score": 0.0,
            "next_action": "stop",
        }

    # 7) Obvious positive / interest language
    positive_phrases = [
        "sounds interesting",
        "this looks interesting",
        "let's talk",
        "lets talk",
        "can we talk",
        "book a call",
        "schedule a call",
        "set up a call",
        "can you send more info",
        "send more information",
        "send me more details",
    ]
    if any(p in lower for p in positive_phrases):
        return {
            "category": "INTERESTED",
            "sub_category": "positive_response",
            "sentiment": "positive",
            "interest_score": 0.7,
            "next_action": "book_call",
        }

    # 8) Fallback: nothing obvious → let the LLM handle it if available.
    return None


# --------------------------------------------------------------------
# Main per-reply classification
# --------------------------------------------------------------------


def classify_one_reply(
    reply_id: int,
    subject: str,
    body: str,
    email_send_id: Optional[int],
    lead_id: Optional[int],
) -> Dict[str, Any]:
    """
    Classify a single reply.

    Priority:
      1) Heuristics (warmup, domain complaint, unsubscribe, etc.)
      2) LLM-based classifier if available (klix.reply_classifier)
      3) Fallback generic classification if everything else fails.
    """
    # First, try heuristics
    heuristic = _heuristic_classify(subject, body)
    if heuristic is not None:
        return heuristic

    # Second, try the LLM classifier if it's available
    if llm_classify_reply is not None:
        try:
            return llm_classify_reply(
                body,
                subject=subject,
                email_send_id=email_send_id,
                lead_id=lead_id,
            )
        except Exception as e:
            print(f"  !! LLM classification failed for reply {reply_id}: {e!r}")

    # Final fallback: generic neutral query
    return {
        "category": "QUERY",
        "sub_category": "uncertain",
        "sentiment": "neutral",
        "interest_score": 0.5,
        "next_action": "send_followup_email",
    }


# --------------------------------------------------------------------
# Batch runner
# --------------------------------------------------------------------


def main(limit: int = 50) -> None:
    """
    Classify up to `limit` unclassified email_replies.

    Safe to run multiple times; it skips rows with category already set.
    Also populates is_warmup / noise_type based on klix.intel.warmup_detector.
    """
    print(f"🔍 Fetching up to {limit} unclassified replies...")

    with db_session() as session:
        rows = session.execute(
            text(
                """
                SELECT
                    r.id,
                    r.subject,
                    r.body,
                    r.email_send_id,
                    s.lead_id,
                    r.from_email,
                    r.to_email,
                    r.provider_message_id
                FROM email_replies r
                LEFT JOIN email_sends s
                    ON s.id = r.email_send_id
                WHERE r.category IS NULL
                ORDER BY r.id DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).fetchall()

        if not rows:
            print("✅ No unclassified replies found.")
            return

        print(f"Found {len(rows)} replies to classify.")

        for row in rows:
            reply_id = row.id
            subject = row.subject or ""
            body = row.body or ""
            email_send_id = row.email_send_id
            lead_id = row.lead_id
            from_email = getattr(row, "from_email", None)
            to_email = getattr(row, "to_email", None)
            provider_message_id = getattr(row, "provider_message_id", None)

            print(f"\n🧠 Classifying reply id={reply_id}, subject={subject[:80]!r}...")

            # 1) Warmup / noise detection (side-effect free)
            warmup_input = WarmupDetectionInput(
                subject=subject,
                body=body,
                from_address=from_email,
                to_address=to_email,
                provider_message_id=provider_message_id,
            )
            warmup_result = detect_warmup_from_row(warmup_input)
            print(
                f"  → warmup_detector: is_warmup={warmup_result.is_warmup}, "
                f"noise_type={warmup_result.noise_type}, reason={warmup_result.reason}"
            )

            # 2) Category / sentiment / action classification
            try:
                result = classify_one_reply(
                    reply_id,
                    subject,
                    body,
                    email_send_id,
                    lead_id,
                )
                print("  → classifier result:", result)

                session.execute(
                    text(
                        """
                        UPDATE email_replies
                        SET
                            category      = :category,
                            sub_category  = :sub_category,
                            sentiment     = :sentiment,
                            interest_score = :interest_score,
                            next_action   = :next_action,
                            is_warmup     = :is_warmup,
                            noise_type    = :noise_type
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": reply_id,
                        "category": result["category"],
                        "sub_category": result["sub_category"],
                        "sentiment": result["sentiment"],
                        "interest_score": float(result["interest_score"]),
                        "next_action": result["next_action"],
                        "is_warmup": bool(warmup_result.is_warmup),
                        "noise_type": warmup_result.noise_type,
                    },
                )

            except Exception as e:
                print(f"  !! Error classifying reply {reply_id}: {e}")

        print("\n✅ Done classifying this batch.")


if __name__ == "__main__":
    # Optional: allow `python scripts/classify_replies_once.py 100`
    if len(sys.argv) > 1:
        try:
            limit_arg = int(sys.argv[1])
        except ValueError:
            print("Usage: python scripts/classify_replies_once.py [limit]")
            sys.exit(1)
    else:
        limit_arg = 50

    main(limit=limit_arg)
