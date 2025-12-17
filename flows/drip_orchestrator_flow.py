from __future__ import annotations

import os
from typing import Optional

from prefect import flow, get_run_logger
from sqlalchemy import create_engine, text

from .lead_finder_flow import lead_finder
from .validate_leads_flow import validate_leads
from .email_builder_flow import email_builder
from .send_queue_v2 import send_queue_v2_flow


def _engine():
    """
    Simple SQLAlchemy engine helper using DATABASE_URL.
    Mirrors the pattern in email_builder_flow.
    """
    url = os.environ["DATABASE_URL"].replace("+psycopg", "")
    return create_engine(url, pool_pre_ping=True)


def _count_leads_without_sends() -> int:
    """
    Count leads that:
      - have a non-empty email
      - do NOT yet have any email_sends row

    This matches email_builder's notion of a "fresh" lead.
    """
    eng = _engine()
    with eng.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT count(*) AS c
                  FROM leads l
                 WHERE COALESCE(l.email, '') <> ''
                   AND NOT EXISTS (
                        SELECT 1
                          FROM email_sends s
                         WHERE s.lead_id = l.id
                    )
                """
            )
        ).first()
    return int(row[0]) if row else 0


@flow(name="klix_drip_orchestrator")
def klix_drip_orchestrator(
    min_candidates: int = 10,
    lead_finder_batch: int = 50,
    builder_limit: int = 20,
    send_batch_size: int = 5,
    allow_weekend: bool = True,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Hybrid pipeline (Option C):

    1) Check how many leads are "fresh" (no email_sends yet).
    2) If below min_candidates:
         - Run lead_finder(count=lead_finder_batch) to top up Neon.
    3) Run validate_leads() to mark NEW/NULL as 'ready'.
    4) Run email_builder(limit=builder_limit) to queue emails.
    5) Run send_queue_v2_flow(batch_size=send_batch_size) to send.

    Returns:
        int: number of emails actually sent by send_queue_v2_flow.
    """
    logger = get_run_logger()

    # A) Check current pipeline headroom
    current = _count_leads_without_sends()
    logger.info(
        "drip_orchestrator: builder candidates (no email_sends yet) = %s",
        current,
    )

    if current < min_candidates:
        logger.info(
            "drip_orchestrator: candidates below min (%s < %s); "
            "running lead_finder(count=%s).",
            current,
            min_candidates,
            lead_finder_batch,
        )
        added = lead_finder(count=lead_finder_batch)
        logger.info("drip_orchestrator: lead_finder added %s leads", added)
    else:
        logger.info(
            "drip_orchestrator: enough candidates (%s >= %s); skipping lead_finder.",
            current,
            min_candidates,
        )

    # B) Normalize lead statuses (NEW/NULL -> ready)
    updated = validate_leads()
    logger.info(
        "drip_orchestrator: validate_leads() marked %s leads as 'ready'.",
        updated,
    )

    # C) Build emails into email_sends (status='queued')
    logger.info(
        "drip_orchestrator: invoking email_builder(limit=%s).",
        builder_limit,
    )
    email_builder(limit=builder_limit)

    # D) Send from the queue using v2 engine
    logger.info(
        "drip_orchestrator: invoking send_queue_v2_flow(batch_size=%s, "
        "allow_weekend=%s, dry_run=%s).",
        send_batch_size,
        allow_weekend,
        dry_run,
    )
    sent = send_queue_v2_flow(
        batch_size=send_batch_size,
        allow_weekend=allow_weekend,
        dry_run=dry_run,
    )
    logger.info("drip_orchestrator: send_queue_v2_flow sent=%s.", sent)
    return sent


if __name__ == "__main__":
    # Local smoke test:
    #   python -m flows.drip_orchestrator_flow
    result = klix_drip_orchestrator()
    print(f"klix_drip_orchestrator main() sent={result}")
