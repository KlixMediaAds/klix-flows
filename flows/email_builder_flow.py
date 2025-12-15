from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from prefect import flow, get_run_logger
from sqlalchemy import text

from klix.db import engine

# -----------------------------
# Config
# -----------------------------

DEFAULT_LIMIT = int(os.getenv("EMAIL_BUILDER_LIMIT", "200"))

# Prompt spine (required by send_queue_v2)
DEFAULT_PROMPT_ANGLE_ID = os.getenv("COLD_DEFAULT_PROMPT_ANGLE_ID", "site-copy-hook").strip()

# Verification gating
ALLOW_RISKY = os.getenv("EMAIL_BUILDER_ALLOW_RISKY", "false").lower() == "true"
VALID_STATUSES = {"valid"}
RISKY_STATUSES = {"risky"}

# -----------------------------
# SQL
# -----------------------------

SELECT_CANDIDATES_SQL = """
SELECT
    l.id AS lead_id,
    l.email,
    l.company,
    l.website,
    l.email_verification_status
FROM leads l
WHERE l.email IS NOT NULL
  AND l.email <> ''
  AND l.email_verification_status IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM email_sends s
        WHERE s.lead_id = l.id
          AND s.send_type = 'cold'
  )
ORDER BY l.id DESC
LIMIT :limit
"""
# Important: email_sends has UNIQUE (lead_id, send_type)
# We upsert, but we do NOT overwrite sent/failed rows.
UPSERT_SEND_SQL = """
INSERT INTO email_sends (
    lead_id,
    send_type,
    status,
    subject,
    body,
    to_email,
    prompt_angle_id,
    created_at,
    updated_at
)
VALUES (
    :lid,
    'cold',
    'queued',
    :subj,
    :body,
    :to_email,
    :prompt_angle_id,
    :ts,
    :ts
)
ON CONFLICT (lead_id, send_type)
DO UPDATE
SET
    status = 'queued',
    subject = EXCLUDED.subject,
    body = EXCLUDED.body,
    to_email = EXCLUDED.to_email,
    prompt_angle_id = EXCLUDED.prompt_angle_id,
    updated_at = EXCLUDED.updated_at
WHERE email_sends.status IN ('queued','held')
"""

@flow(name="email-builder")
def email_builder_flow(limit: int = DEFAULT_LIMIT) -> int:
    """
    Build cold emails and enqueue them into email_sends.

    HARD GUARANTEES:
    - Verification gate enforced (valid only; risky optional).
    - prompt_angle_id is ALWAYS set for cold sends (required by send_queue_v2).
    - We NEVER overwrite sent/failed rows (only refresh queued/held).
    """
    logger = get_run_logger()

    if not DEFAULT_PROMPT_ANGLE_ID:
        raise RuntimeError("COLD_DEFAULT_PROMPT_ANGLE_ID is empty; prompt spine is required.")

    if limit <= 0:
        logger.info("email_builder_flow: limit <= 0, nothing to do.")
        return 0

    with engine.begin() as conn:
        leads = conn.execute(text(SELECT_CANDIDATES_SQL), {"limit": limit}).mappings().all()

    if not leads:
        logger.info("email_builder_flow: no candidate leads found.")
        return 0

    eligible: List[Dict[str, Any]] = []
    blocked_invalid = 0
    blocked_risky = 0
    blocked_unknown = 0

    for l in leads:
        status = (l.get("email_verification_status") or "").strip().lower()
        if status in VALID_STATUSES:
            eligible.append(l)
        elif status in RISKY_STATUSES:
            if ALLOW_RISKY:
                eligible.append(l)
            else:
                blocked_risky += 1
        elif status == "invalid":
            blocked_invalid += 1
        else:
            blocked_unknown += 1

    logger.info(
        "email_builder_flow: verification gate | eligible=%s invalid=%s risky_blocked=%s unknown=%s allow_risky=%s",
        len(eligible),
        blocked_invalid,
        blocked_risky,
        blocked_unknown,
        ALLOW_RISKY,
    )

    if not eligible:
        logger.info("email_builder_flow: no leads passed verification gate.")
        return 0

    queued_or_refreshed = 0
    now_ts = datetime.now(timezone.utc)

    with engine.begin() as conn:
        for lead in eligible:
            lead_id = lead["lead_id"]
            to_email = (lead.get("email") or "").strip()
            company = (lead.get("company") or "").strip()
            website = (lead.get("website") or "").strip()

            ref = company or website or "your site"
            subject = "Quick question about your website"
            body = (
                "Hi there,\n\n"
                f"I was reviewing {ref} and noticed something worth flagging.\n\n"
                "Open to a quick note?\n\n"
                "- Josh"
            )

            res = conn.execute(
                text(UPSERT_SEND_SQL),
                {
                    "lid": lead_id,
                    "subj": subject,
                    "body": body,
                    "to_email": to_email,
                    "prompt_angle_id": DEFAULT_PROMPT_ANGLE_ID,
                    "ts": now_ts,
                },
            )

            if (res.rowcount or 0) > 0:
                queued_or_refreshed += 1

    logger.info(
        "email_builder_flow: queued_or_refreshed=%s prompt_angle_id=%s",
        queued_or_refreshed,
        DEFAULT_PROMPT_ANGLE_ID,
    )
    return queued_or_refreshed


if __name__ == "__main__":
    email_builder_flow()
