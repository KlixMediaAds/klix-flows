from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict

from prefect import flow, get_run_logger
from sqlalchemy import text

from klix.db import engine
from klix.email_verifier import EmailVerifier, EmailVerificationResult


BATCH_SIZE_DEFAULT = int(os.getenv("EMAIL_VERIFIER_BATCH_SIZE", "200"))
MAX_AGE_DAYS_DEFAULT = int(os.getenv("EMAIL_VERIFIER_MAX_AGE_DAYS", "30"))


def _build_verifier() -> EmailVerifier:
    """
    Factory for the EmailVerifier.

    In Phase 1 we use the mock syntax-only verifier. Later, this is where you
    would pass a real provider API key from env, e.g. EMAIL_VERIFIER_API_KEY.
    """
    api_key = os.getenv("EMAIL_VERIFIER_API_KEY")
    source = os.getenv("EMAIL_VERIFIER_SOURCE", "mock-syntax")
    return EmailVerifier(api_key=api_key, source=source)


@flow(name="email-verifier")
def email_verifier_flow(
    limit: int = BATCH_SIZE_DEFAULT,
    max_age_days: int = MAX_AGE_DAYS_DEFAULT,
) -> int:
    """
    Verify up to `limit` leads whose emails are unverified or stale.

    Phase 1 is tag-only:
    - We update leads.email_verification_* fields
    - We DO NOT gate sending yet (that will be Phase 2)
    """
    logger = get_run_logger()
    verifier = _build_verifier()

    if limit <= 0:
        logger.info("email_verifier_flow: limit <= 0, nothing to do.")
        return 0

    cutoff_ts = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    # Fetch candidate leads in a single query.
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, email
                FROM leads
                WHERE email IS NOT NULL
                  AND email <> ''
                  AND (
                        email_verification_status IS NULL
                     OR email_verification_checked_at IS NULL
                     OR email_verification_checked_at < :cutoff
                  )
                ORDER BY id
                LIMIT :limit
                """
            ),
            {"cutoff": cutoff_ts, "limit": limit},
        ).mappings().all()

    if not rows:
        logger.info("email_verifier_flow: no leads require verification.")
        return 0

    logger.info(f"email_verifier_flow: loaded {len(rows)} leads for verification.")

    # Run verification in Python land.
    updates = []
    counts: Dict[str, int] = {}
    now_ts = datetime.now(timezone.utc)

    for row in rows:
        lead_id = row["id"]
        email = row["email"]

        result: EmailVerificationResult = verifier.verify(email)
        status = result.status or "risky"

        counts[status] = counts.get(status, 0) + 1

        updates.append(
            {
                "id": lead_id,
                "status": status,
                "source": result.source,
                "score": result.score,
                "checked_at": now_ts,
            }
        )

    # Apply updates in a single transaction.
    with engine.begin() as conn:
        for u in updates:
            conn.execute(
                text(
                    """
                    UPDATE leads
                    SET
                      email_verification_status = :status,
                      email_verification_source = :source,
                      email_verification_score = :score,
                      email_verification_checked_at = :checked_at
                    WHERE id = :id
                    """
                ),
                u,
            )

    total_updated = len(updates)
    logger.info(
        "email_verifier_flow: updated %s leads (breakdown: %s)",
        total_updated,
        counts,
    )

    return total_updated


if __name__ == "__main__":
    # Simple manual run helper (no deployment logic here).
    # Useful for quick tests from the CLI:
    #   python -m flows.email_verifier_flow
    email_verifier_flow()
