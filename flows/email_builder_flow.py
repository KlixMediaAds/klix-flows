from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from prefect import flow, get_run_logger
from sqlalchemy import text

from klix.db import engine

# ==============================================================================
# Config
# ==============================================================================

DEFAULT_LIMIT = int(os.getenv("EMAIL_BUILDER_LIMIT", "200"))

# Prompt spine fallback (send_queue_v2 requires prompt_angle_id; builder is profile-first but will fall back)
DEFAULT_PROMPT_ANGLE_ID = os.getenv("COLD_DEFAULT_PROMPT_ANGLE_ID", "site-copy-hook").strip()

# Verification gating
ALLOW_RISKY = os.getenv("EMAIL_BUILDER_ALLOW_RISKY", "false").lower() == "true"
VALID_STATUSES = {"valid"}
RISKY_STATUSES = {"risky"}

# Builder identity (for telemetry/debug)
WRITTEN_BY = os.getenv("EMAIL_BUILDER_WRITTEN_BY", "email_builder_flow").strip() or "email_builder_flow"

# ==============================================================================
# SQL
# ==============================================================================

SELECT_CANDIDATES_SQL = """
SELECT
    l.id AS lead_id,
    l.email,
    l.company,
    l.website,
    l.email_verification_status,
    l.meta,
    l.source_details
FROM leads l
WHERE l.email IS NOT NULL
  AND btrim(l.email) <> ''
  AND l.email_verification_status = 'valid'
  AND (
      -- brand new: no cold row exists
      NOT EXISTS (
          SELECT 1
          FROM email_sends s
          WHERE s.lead_id = l.id
            AND s.send_type = 'cold'
      )
      OR
      -- retry surface: prior cold attempt failed
      EXISTS (
          SELECT 1
          FROM email_sends s
          WHERE s.lead_id = l.id
            AND s.send_type = 'cold'
            AND s.status = 'failed'
            AND coalesce(s.error, '') !~* '(denylisted|denylist|bounce|queue_gate: missing prompt_angle_id|missing prompt spine|suppressed_recipient: internal_address)'
      )
    )
ORDER BY l.id DESC
LIMIT :limit
"""

# Important: email_sends has UNIQUE (lead_id, send_type)
# We upsert, but we do NOT overwrite sent rows.
UPSERT_SEND_SQL = """
INSERT INTO email_sends (
    lead_id,
    send_type,
    status,
    subject,
    body,
    to_email,
    prompt_angle_id,
    prompt_profile_id,
    model_name,
    generation_style_seed,
    written_by,
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
    :prompt_profile_id,
    :model_name,
    :generation_style_seed,
    :written_by,
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
    prompt_profile_id = EXCLUDED.prompt_profile_id,
    model_name = EXCLUDED.model_name,
    generation_style_seed = EXCLUDED.generation_style_seed,
    written_by = EXCLUDED.written_by,
    error = NULL,
    provider_message_id = NULL,
    sent_at = NULL,
    updated_at = EXCLUDED.updated_at
WHERE email_sends.status IN ('queued','held','failed')
"""

COUNT_ACTIVE_PROFILES_SQL = """
SELECT
  count(*) AS total,
  sum(CASE WHEN is_active = true THEN 1 ELSE 0 END) AS active,
  sum(CASE WHEN is_active = true AND weight > 0 THEN 1 ELSE 0 END) AS active_weight_gt0
FROM email_prompt_profiles
"""

# ==============================================================================
# Helpers
# ==============================================================================


def _extract_from_meta(meta: Any) -> Dict[str, Any]:
    """
    Safely extract common builder fields from leads.meta.

    meta can be:
      - dict (json object)
      - list (json array)
      - None / other
    """
    if not isinstance(meta, dict):
        return {}

    out: Dict[str, Any] = {}
    for k in ("city", "niche", "site_title", "tagline", "website", "services", "products", "name"):
        if k in meta and meta.get(k) is not None:
            out[k] = meta.get(k)

    products = out.get("services") or out.get("products")
    if isinstance(products, list):
        out["products"] = products
    elif isinstance(products, str) and products.strip():
        out["products"] = [products.strip()]

    for k in ("city", "niche", "site_title", "tagline", "website", "name"):
        v = out.get(k)
        if v is not None and not isinstance(v, str):
            out[k] = str(v)

    return out


def _lead_for_builder(row: Dict[str, Any]) -> Dict[str, Any]:
    lead = dict(row)
    extracted = _extract_from_meta(lead.get("meta"))

    company = (lead.get("company") or "").strip()
    website = (lead.get("website") or "").strip()

    if not company and extracted.get("name"):
        company = (extracted.get("name") or "").strip()
    if not website and extracted.get("website"):
        website = (extracted.get("website") or "").strip()

    lead["company"] = company
    lead["website"] = website
    lead["city"] = (extracted.get("city") or "").strip()
    lead["niche"] = (extracted.get("niche") or "").strip()
    lead["site_title"] = (extracted.get("site_title") or "").strip()
    lead["tagline"] = (extracted.get("tagline") or "").strip()

    products = extracted.get("products")
    lead["products"] = products if isinstance(products, list) else []

    return lead


def _preflight_profiles(logger) -> None:
    with engine.begin() as conn:
        row = conn.execute(text(COUNT_ACTIVE_PROFILES_SQL)).mappings().first()

    total = int((row or {}).get("total") or 0)
    active = int((row or {}).get("active") or 0)
    active_w = int((row or {}).get("active_weight_gt0") or 0)

    logger.info(
        "email_builder_flow: prompt_profiles preflight | total=%s active=%s active_weight_gt0=%s",
        total,
        active,
        active_w,
    )

    if active_w <= 0:
        raise RuntimeError(
            "No active email_prompt_profiles found (is_active=true AND weight>0). "
            "Email builder is profile-first; activate at least one prompt profile."
        )


# ==============================================================================
# Flow
# ==============================================================================


@flow(name="email-builder")
def email_builder_flow(limit: int = DEFAULT_LIMIT) -> int:
    """
    Build cold emails and enqueue them into email_sends.

    HARD GUARANTEES:
    - Verification gate enforced (valid only; risky optional).
    - prompt_angle_id is ALWAYS set for cold sends (required by send_queue_v2).
    - We NEVER overwrite sent rows (only refresh queued/held/failed).
    - If profiles are misconfigured (0 active weight), we hard-fail early.
    """
    logger = get_run_logger()

    if limit <= 0:
        logger.info("email_builder_flow: limit <= 0, nothing to do.")
        return 0

    _preflight_profiles(logger)

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

    from klix.email_builder.main import build_email_for_lead

    queued_or_refreshed = 0
    build_errors = 0
    now_ts = datetime.now(timezone.utc)

    with engine.begin() as conn:
        for raw in eligible:
            lead_id = raw["lead_id"]
            to_email = (raw.get("email") or "").strip()

            if not to_email or "@" not in to_email:
                logger.warning("email_builder_flow: skipping lead_id=%s due to missing/invalid email", lead_id)
                continue

            lead_for_prompt = _lead_for_builder(dict(raw))

            try:
                (
                    subject,
                    body_text,
                    _body_html,
                    _send_type_guess,
                    prompt_profile_id,
                    prompt_angle_id,
                    style_seed,
                ) = build_email_for_lead(lead_for_prompt)
            except Exception as e:
                build_errors += 1
                logger.error("email_builder_flow: build failed lead_id=%s err=%s", lead_id, str(e))
                continue

            subject = (subject or "").strip()
            body_text = (body_text or "").strip()
            prompt_angle_id = (prompt_angle_id or "").strip() or DEFAULT_PROMPT_ANGLE_ID

            if not prompt_angle_id:
                build_errors += 1
                logger.error("email_builder_flow: missing prompt_angle_id after build for lead_id=%s", lead_id)
                continue

            if not subject or not body_text:
                build_errors += 1
                logger.error("email_builder_flow: missing subject/body after build for lead_id=%s", lead_id)
                continue

            res = conn.execute(
                text(UPSERT_SEND_SQL),
                {
                    "lid": lead_id,
                    "subj": subject,
                    "body": body_text,
                    "to_email": to_email,
                    "prompt_angle_id": prompt_angle_id,
                    "prompt_profile_id": str(prompt_profile_id) if prompt_profile_id else None,
                    "model_name": None,
                    "generation_style_seed": str(style_seed) if style_seed else None,
                    "written_by": WRITTEN_BY,
                    "ts": now_ts,
                },
            )

            if (res.rowcount or 0) > 0:
                queued_or_refreshed += 1

    logger.info(
        "email_builder_flow: queued_or_refreshed=%s build_errors=%s prompt_angle_id_fallback=%s",
        queued_or_refreshed,
        build_errors,
        DEFAULT_PROMPT_ANGLE_ID,
    )
    return queued_or_refreshed


if __name__ == "__main__":
    email_builder_flow()
