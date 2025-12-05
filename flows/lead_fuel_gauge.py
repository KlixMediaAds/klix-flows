import os
from typing import Dict

from prefect import flow, get_run_logger
import psycopg2


def _get_conn():
    """
    Return a psycopg2 connection using DATABASE_URL from the env.

    DATABASE_URL is in SQLAlchemy form:
      postgresql+psycopg://...

    For psycopg2 we just strip the '+psycopg' bit:
      postgresql://...
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in the environment")

    clean_url = db_url.replace("postgresql+psycopg", "postgresql")
    return psycopg2.connect(clean_url)


def get_fuel_metrics() -> Dict[str, int]:
    """
    Return the two core fuel metrics for the cold engine:

    - eligible_leads:
        leads that have an email, are NOT tagged as invalid, and have NEVER
        had a successfully sent cold/friendly email_sends row.

        Concretely:
          coalesce(email, '') <> ''
          AND (email_verification_status IS NULL OR email_verification_status <> 'invalid')
          AND NOT EXISTS (
            SELECT 1
              FROM email_sends s
             WHERE s.lead_id = l.id
               AND s.status = 'sent'
               AND s.send_type IN ('cold', 'friendly')
          )

      This allows us to:
        - Include brand-new leads (no sends at all).
        - Re-eligible any lead that only has failed/cancelled/test sends.
        - Exclude leads that already got a real sent cold/friendly.
        - Exclude leads we now know are syntactically invalid.

    - queued_cold:
        email_sends rows that are queued cold emails.
    """
    eligible_sql = """
        SELECT count(*) AS eligible_leads
        FROM leads l
        WHERE coalesce(l.email, '') <> ''
          AND (l.email_verification_status IS NULL OR l.email_verification_status <> 'invalid')
          AND NOT EXISTS (
            SELECT 1
              FROM email_sends s
             WHERE s.lead_id = l.id
               AND s.status = 'sent'
               AND s.send_type IN ('cold', 'friendly')
          );
    """

    queued_sql = """
        SELECT count(*) AS queued_cold
        FROM email_sends
        WHERE status = 'queued'
          AND send_type = 'cold';
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(eligible_sql)
            eligible_row = cur.fetchone()
            eligible = eligible_row[0] if eligible_row else 0

            cur.execute(queued_sql)
            queued_row = cur.fetchone()
            queued = queued_row[0] if queued_row else 0

    return {
        "eligible_leads": int(eligible or 0),
        "queued_cold": int(queued or 0),
    }


@flow(name="lead-fuel-gauge")
def lead_fuel_gauge_flow() -> Dict[str, int]:
    """
    Prefect flow wrapper around get_fuel_metrics().

    Logs the metrics for telemetry and returns them for programmatic use.
    """
    logger = get_run_logger()
    metrics = get_fuel_metrics()
    logger.info(
        "lead_fuel_gauge: eligible_leads=%s queued_cold=%s",
        metrics["eligible_leads"],
        metrics["queued_cold"],
    )
    return metrics


if __name__ == "__main__":
    # Allow quick ad-hoc runs via:
    #   PYTHONPATH=. python -m flows.lead_fuel_gauge
    metrics = lead_fuel_gauge_flow()
    print(
        f"eligible_leads={metrics['eligible_leads']} queued_cold={metrics['queued_cold']}"
    )
