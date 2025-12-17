import os
from typing import Dict

import psycopg2
from prefect import flow, get_run_logger


def _get_conn():
    """
    Return a psycopg2 connection using DATABASE_URL from the env.

    DATABASE_URL is in SQLAlchemy form:
      postgresql+psycopg://...

    For psycopg2 we strip the '+psycopg' bit:
      postgresql://...
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in the environment")

    clean_url = db_url.replace("postgresql+psycopg", "postgresql")
    return psycopg2.connect(clean_url)


def get_fuel_metrics() -> Dict[str, int]:
    """
    Core fuel metrics for the cold engine.

    eligible_leads:
      leads with a *valid* email that have never had a sent cold/friendly.

    queued_cold:
      queued rows that are cold.

    queued_total:
      all queued rows (cold + friendly).

    queued_sql_candidates:
      queued cold rows (mirrors claim SQL at the DB level; cooldown/caps are enforced later).
    """
    eligible_sql = """
        SELECT count(*) AS eligible_leads
        FROM leads l
        WHERE coalesce(l.email, '') <> ''
          AND coalesce(l.email_verification_status,'') = 'valid'
          AND NOT EXISTS (
            SELECT 1
              FROM email_sends s
             WHERE s.lead_id = l.id
               AND s.status = 'sent'
               AND s.send_type IN ('cold', 'friendly')
          );
    """

    queued_cold_sql = """
        SELECT count(*) AS queued_cold
        FROM email_sends
        WHERE status = 'queued'
          AND send_type = 'cold';
    """

    queued_total_sql = """
        SELECT count(*) AS queued_total
        FROM email_sends
        WHERE status = 'queued';
    """

    queued_candidates_sql = """
        SELECT count(*) AS queued_sql_candidates
        FROM email_sends
        WHERE status = 'queued'
          AND send_type = 'cold';
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(eligible_sql)
            eligible = (cur.fetchone() or [0])[0] or 0

            cur.execute(queued_cold_sql)
            queued_cold = (cur.fetchone() or [0])[0] or 0

            cur.execute(queued_total_sql)
            queued_total = (cur.fetchone() or [0])[0] or 0

            cur.execute(queued_candidates_sql)
            queued_candidates = (cur.fetchone() or [0])[0] or 0

    return {
        "eligible_leads": int(eligible),
        "queued_cold": int(queued_cold),
        "queued_total": int(queued_total),
        "queued_sql_candidates": int(queued_candidates),
    }


@flow(name="lead-fuel-gauge")
def lead_fuel_gauge_flow() -> Dict[str, int]:
    logger = get_run_logger()
    metrics = get_fuel_metrics()
    logger.info(
        "lead_fuel_gauge: eligible=%s queued_cold=%s queued_total=%s queued_sql_candidates=%s",
        metrics["eligible_leads"],
        metrics["queued_cold"],
        metrics["queued_total"],
        metrics["queued_sql_candidates"],
    )
    return metrics


if __name__ == "__main__":
    print(lead_fuel_gauge_flow())
