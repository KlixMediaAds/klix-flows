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
    api_key = os.getenv("EMAIL_VERIFIER_API_KEY")
    source = os.getenv("EMAIL_VERIFIER_SOURCE", "dns-mx")

    timeout_s = float(os.getenv("EMAIL_VERIFIER_TIMEOUT_S", "3.0"))
    lifetime_s = float(os.getenv("EMAIL_VERIFIER_LIFETIME_S", "5.0"))

    return EmailVerifier(
        api_key=api_key,
        source=source,
        timeout_seconds=timeout_s,
        lifetime_seconds=lifetime_s,
    )


@flow(name="email-verifier")
def email_verifier_flow(
    limit: int = BATCH_SIZE_DEFAULT,
    max_age_days: int = MAX_AGE_DAYS_DEFAULT,
) -> int:
    logger = get_run_logger()
    verifier = _build_verifier()

    cutoff_ts = datetime.now(timezone.utc) - timedelta(days=max_age_days)

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

    updates = []
    counts: Dict[str, int] = {}
    now_ts = datetime.now(timezone.utc)

    for row in rows:
        result: EmailVerificationResult = verifier.verify(row["email"])
        status = result.status or "risky"

        counts[status] = counts.get(status, 0) + 1

        updates.append(
            {
                "id": row["id"],
                "status": status,
                "source": result.source,
                "score": result.score,
                "checked_at": now_ts,
            }
        )

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

    logger.info(
        "email_verifier_flow: updated %s leads (breakdown: %s)",
        len(updates),
        counts,
    )

    return len(updates)


if __name__ == "__main__":
    email_verifier_flow()
