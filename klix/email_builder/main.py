from __future__ import annotations
from typing import Tuple, Optional, Dict, Any, List

import os
import html
import random
import secrets

from sqlalchemy import create_engine, text

# Low-level AI email generator
from klix.email_builder.lib import prompt as pr


# ============================================================================
# Basic helpers
# ============================================================================

FRIENDLY_DOMAINS = {"gmail.com", "klixads.org"}


def _domain(email: str) -> str:
    email = (email or "").strip()
    return email.split("@", 1)[1].lower() if "@" in email else ""


def _to_html(md: str) -> str:
    """
    Very small MD→HTML: paragraphs + line breaks (no markdown library).
    """
    if not md:
        return ""
    safe = html.escape(md).replace("\r\n", "\n").replace("\r", "\n")
    parts: List[str] = []
    for chunk in safe.split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.append("<p>" + chunk.replace("\n", "<br>") + "</p>")
    return "".join(parts) or ("<p>" + safe + "</p>")


def _engine():
    """
    Lightweight SQLAlchemy engine initializer.

    Assumes DATABASE_URL has already been normalized
    (no 'postgresql+psycopg') as per your telemetry spine migration.
    """
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        return None
    try:
        return create_engine(url, pool_pre_ping=True)
    except Exception:
        return None


def _oldest_queued_for_lead(lead_id: int | str | None) -> Optional[Dict[str, Any]]:
    """
    Legacy helper: fetch {'subject', 'body'} for the oldest queued email
    for this lead. Kept for reference but no longer used by the builder.

    We intentionally do NOT reuse older queued content anymore; every call
    to build_email_for_lead generates a fresh email via prompt profiles.
    """
    if not lead_id:
        return None
    eng = _engine()
    if not eng:
        return None

    try:
        with eng.begin() as c:
            row = (
                c.execute(
                    text(
                        """
                        select subject, body
                          from email_sends
                         where status = 'queued' and lead_id = :lid
                         order by id asc
                         limit 1
                        """
                    ),
                    {"lid": lead_id},
                )
                .mappings()
                .first()
            )
            return dict(row) if row else None
    except Exception:
        return None


def _pick_prompt_profile() -> Optional[Dict[str, Any]]:
    """
    Pick an active prompt profile using its weight for selection.

    Returns a dict with:
        {id, name, angle_id, system_text, model_name, weight}
    or None if nothing is active or the DB is unreachable.
    """
    eng = _engine()
    if not eng:
        return None

    try:
        with eng.begin() as c:
            rows = (
                c.execute(
                    text(
                        """
                        select id, name, angle_id, system_text, model_name, weight
                          from email_prompt_profiles
                         where is_active = true
                           and weight > 0
                        """
                    )
                )
                .mappings()
                .all()
            )
    except Exception:
        return None

    if not rows:
        return None

    weighted: List[Dict[str, Any]] = []
    for row in rows:
        w = float(row.get("weight") or 0.0)
        if w <= 0:
            continue
        # Simple scaling so fractional weights still have impact
        copies = max(1, int(round(w * 10)))
        weighted.extend([dict(row)] * copies)

    if not weighted:
        # Fallback: just pick the first active row
        return dict(rows[0])

    return random.choice(weighted)


def _biz_from_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw lead row into the 'biz' dict expected by pr.draft_email.

    Supports both legacy and newer lead shapes:
      - business_name / BusinessName
      - company / Company
      - city / City
      - niche / Niche
      - website / Website
      - site_title / SiteTitle
      - tagline / Tagline
      - products / Products
    """
    business_name = (
        lead.get("business_name")
        or lead.get("BusinessName")
        or lead.get("company")
        or lead.get("Company")
        or ""
    )

    city = lead.get("city") or lead.get("City") or ""
    niche = lead.get("niche") or lead.get("Niche") or ""
    website = lead.get("website") or lead.get("Website") or ""
    site_title = lead.get("site_title") or lead.get("SiteTitle") or business_name
    tagline = lead.get("tagline") or lead.get("Tagline") or ""
    products = lead.get("products") or lead.get("Products") or []

    return {
        "BusinessName": business_name,
        "City": city,
        "Niche": niche,
        "Website": website,
        "SiteTitle": site_title or business_name,
        "Tagline": tagline,
        "Products": products,
    }


# ============================================================================
# Public API used by flows.email_builder_flow
# ============================================================================


def build_email_for_lead(
    lead: Dict[str, Any],
    prompt_profile: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, str, Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Core builder used by flows.email_builder_flow.

    Returns:
        subject: str
        body_text: str
        body_html: str
        send_type_guess: Optional[str]        (e.g., 'cold' / 'friendly')
        prompt_profile_id: Optional[str]      (UUID / label from email_prompt_profiles)
        prompt_angle_id: Optional[str]        (e.g., 'site-copy-hook')
        style_seed: Optional[str]             (e.g., '8a709a2a')

    Behavior (new, profile-first):
    - ALWAYS generate a fresh email; we no longer reuse existing queued rows.
    - Select an active prompt profile from email_prompt_profiles using weight.
    - Use the profile's angle_id, system_text, and model_name as the single
      source of truth for style/voice.
    - If no active profiles are found, we raise a RuntimeError so the calling
      flow can surface the problem instead of silently falling back.
    """
    # Derive lead_id and email (for send_type_guess)
    lead_id = (
        lead.get("lead_id")
        or lead.get("id")
        or lead.get("ID")
        or None
    )
    email = (
        lead.get("email")
        or lead.get("Email")
        or lead.get("to")
        or lead.get("To")
        or ""
    )

    # 1) Build the business dict for prompt logic
    biz = _biz_from_lead(lead)

    # 2) Choose prompt profile (DB-backed), unless explicitly provided
    if prompt_profile is not None:
        profile = dict(prompt_profile)
    else:
        profile = _pick_prompt_profile()

    if not profile:
        # Hard fail: we don't want to silently revert to a single hard-coded angle.
        raise RuntimeError(
            "No active email_prompt_profiles found (is_active=true, weight>0). "
            "Email builder is profile-first; configure at least one active profile."
        )

    profile_id: Optional[str] = None
    if profile.get("id") is not None:
        profile_id = str(profile["id"])

    angle_id = profile.get("angle_id")
    if not angle_id:
        # Emergency fallback: keep this extremely rare and visible.
        angle_id = "site-copy-hook"
        print(
            "[EMAIL_BUILDER] WARNING: prompt profile has no angle_id; "
            "falling back to 'site-copy-hook'. Fix email_prompt_profiles.angle_id."
        )

    model_name = profile.get("model_name") or os.getenv("MODEL_NAME", "gpt-4o-mini")
    system_text = profile.get("system_text")

    # 3) If the profile defines system_text, inject it into the prompt lib.
    #    This leverages the profile as the ONLY voice/style definition.
    if system_text:
        # This assumes pr.SYSTEM_TEXT is used by the low-level prompt engine.
        pr.SYSTEM_TEXT = system_text

    # 4) Call the prompt engine (may internally handle OpenAI / scoring / minor fallback)
    email_obj = pr.draft_email(biz, angle_id=angle_id, model_name=model_name)

    subject = (email_obj.get("subject") or "").strip()
    body_md = (email_obj.get("body_md") or "").strip()

    if not subject:
        subject = "Tiny video idea"
    if not body_md:
        body_md = (
            "Had a small idea for a no-face, product-focused visual that could fit your brand. "
            "If you're open to a quick peek, I can share it in 1–2 lines."
        )

    # 5) Decide on send_type_guess
    send_type_guess = "friendly" if _domain(email) in FRIENDLY_DOMAINS else "cold"

    # 6) Generate a style_seed for logging / analytics (8 hex chars)
    style_seed = secrets.token_hex(4)

    # 7) Convert to HTML with a small MD→HTML adapter
    body_text = body_md
    body_html = _to_html(body_md)

    # Enforce a reasonable subject length cap
    subject = subject[:120]

    return (
        subject,
        body_text,
        body_html,
        send_type_guess,
        profile_id,
        angle_id,
        style_seed,
    )
