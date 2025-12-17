from __future__ import annotations

"""
validate_leads_flow.py

ERA 1.6 — Cold Drip Autopilot

Purpose:
    - Take leads that are "new" or unset and mark them as "ready".
    - This is the automated version of the manual UPDATE we used to run.
    - Designed to be safe, idempotent, and Prefect-friendly.

Behavior:
    - If limit is None:
        UPDATE all leads WHERE status IS NULL OR lower(status) = 'new' → status = 'ready'.
    - If limit is set:
        UPDATE only up to `limit` leads, prioritizing those with the oldest
        discovered_at/created_at timestamps.

Notes:
    - Uses klix.db.engine for normalized DATABASE_URL handling.
    - Can be wired into a higher-level orchestrator that:
        * runs lead_finder when ready-count < threshold
        * then runs validate_leads
        * then runs email_builder and send_queue_v2
"""

from typing import Optional

from prefect import flow, get_run_logger
from sqlalchemy import text

from klix.db import engine


@flow(name="validate_leads")
def validate_leads(limit: Optional[int] = None) -> int:
    """
    Mark NEW / new / NULL-status leads as 'ready'.

    Returns:
        int: number of leads updated.
    """
    log = get_run_logger()

    if limit is not None and limit <= 0:
        log.info("validate_leads: limit <= 0; nothing to do.")
        return 0

    updated_ids: list[int] = []

    with engine.begin() as conn:
        if limit is None:
            # Simple bulk update of all NEW/new/NULL leads.
            log.info("validate_leads: updating ALL NEW/new/NULL leads to 'ready'.")
            result = conn.execute(
                text(
                    """
                    UPDATE leads
                    SET status = 'ready'
                    WHERE status IS NULL
                       OR lower(status) = 'new'
                    RETURNING id;
                    """
                )
            )
            updated_ids = [row.id for row in result.fetchall()]
        else:
            # More careful: update only a limited batch, oldest first.
            log.info(
                "validate_leads: updating up to %s NEW/new/NULL leads to 'ready'.",
                limit,
            )
            result = conn.execute(
                text(
                    """
                    WITH candidate_ids AS (
                        SELECT id
                        FROM leads
                        WHERE status IS NULL
                           OR lower(status) = 'new'
                        ORDER BY
                            discovered_at NULLS LAST,
                            created_at   NULLS LAST,
                            id
                        LIMIT :limit
                    )
                    UPDATE leads
                    SET status = 'ready'
                    FROM candidate_ids
                    WHERE leads.id = candidate_ids.id
                    RETURNING leads.id;
                    """
                ),
                {"limit": limit},
            )
            updated_ids = [row.id for row in result.fetchall()]

    updated_count = len(updated_ids)
    log.info("validate_leads: marked %s leads as 'ready'.", updated_count)

    return updated_count


if __name__ == "__main__":
    # Local smoke test:
    #   python -m flows.validate_leads_flow
    count = validate_leads()
    print(f"validate_leads main(): updated {count} leads.")
