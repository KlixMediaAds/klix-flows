from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from prefect import flow, get_run_logger

from .lead_fuel_gauge import get_fuel_metrics
from .validate_leads_flow import validate_leads
from .email_builder_flow import email_builder_flow as email_builder
from .send_queue_v2 import send_queue_v2_flow
from .lead_engine_v2_flow import lead_engine_v2


# -----------------------------
# Thresholds / knobs (env-tuned)
# -----------------------------
MIN_ELIGIBLE_LEADS = int(os.getenv("MIN_ELIGIBLE_LEADS", "300"))
MIN_QUEUED_COLD = int(os.getenv("MIN_QUEUED_COLD", "150"))
BUILDER_LIMIT = int(os.getenv("BUILDER_LIMIT", "60"))

SEND_BATCH_SIZE = int(os.getenv("SEND_BATCH_SIZE", "10"))
ALLOW_WEEKEND = os.getenv("ALLOW_WEEKEND", "true").strip().lower() in ("1", "true", "yes", "y")

DRY_RUN_ENV = os.getenv("DRY_RUN", "").strip().lower()
DRY_RUN: Optional[bool] = None
if DRY_RUN_ENV in ("1", "true", "yes", "y"):
    DRY_RUN = True
elif DRY_RUN_ENV in ("0", "false", "no", "n"):
    DRY_RUN = False


def _run_email_verifier_deployment(logger) -> Dict[str, Any]:
    """Run the email-verifier Prefect deployment (deployment-driven verification)."""
    try:
        from prefect.deployments import run_deployment  # type: ignore
    except Exception as e:
        logger.warning("supervisor: could not import prefect.deployments.run_deployment: %s", e)
        return {"ok": 0, "error": f"import_failed:{e}"}

    try:
        fr = run_deployment("email-verifier/email-verifier", parameters={})
        return {
            "ok": 1,
            "flow_run_id": getattr(fr, "id", None),
            "name": getattr(fr, "name", None),
        }
    except Exception as e:
        logger.warning("supervisor: email-verifier deployment run failed: %s", e)
        return {"ok": 0, "error": str(e)}


@flow(name="cold-pipeline-supervisor")
def cold_pipeline_supervisor() -> Dict[str, int]:
    logger = get_run_logger()

    metrics = get_fuel_metrics()
    eligible = int(metrics.get("eligible_leads", 0) or 0)
    queued = int(metrics.get("queued_cold", 0) or 0)

    logger.info("supervisor: start fuel metrics eligible=%s queued_cold=%s", eligible, queued)

    # 1) Intake (at most once per cycle)
    if eligible < MIN_ELIGIBLE_LEADS:
        logger.info(
            "supervisor: eligible(%s) < MIN_ELIGIBLE_LEADS(%s); running Lead Engine v2 ingest.",
            eligible,
            MIN_ELIGIBLE_LEADS,
        )

        intake_summary = lead_engine_v2()

        # Loud warning on total_inserted==0
        try:
            totals = (intake_summary or {}).get("totals") or {}
            total_inserted = int(totals.get("total_inserted", 0) or 0)
            if total_inserted == 0:
                logger.warning(
                    json.dumps(
                        {
                            "event": "supervisor_intake_warning",
                            "reason": "total_inserted_zero",
                            "lead_engine_v2_run_id": (intake_summary or {}).get("run_id"),
                            "intake_classification": (intake_summary or {}).get("intake_classification"),
                            "counts_by_source": (intake_summary or {}).get("counts_by_source"),
                            "last_successful_insert_ts": (intake_summary or {}).get("last_successful_insert_ts"),
                        },
                        sort_keys=True,
                    )
                )
        except Exception as e:
            logger.warning("supervisor: failed to emit intake warning payload: %s", e)

        ver = _run_email_verifier_deployment(logger)
        logger.info("supervisor: email_verifier invoked: %s", ver)

        updated = validate_leads()
        logger.info("supervisor: validate_leads() updated %s leads.", updated)

        metrics = get_fuel_metrics()
        eligible = int(metrics.get("eligible_leads", 0) or 0)
        queued = int(metrics.get("queued_cold", 0) or 0)
        logger.info("supervisor: after intake, eligible=%s queued_cold=%s", eligible, queued)

    else:
        updated = validate_leads()
        logger.info("supervisor: eligible >= MIN_ELIGIBLE_LEADS; validate_leads() updated %s leads.", updated)

        metrics = get_fuel_metrics()
        eligible = int(metrics.get("eligible_leads", 0) or 0)
        queued = int(metrics.get("queued_cold", 0) or 0)

    # 2) Builder top-up
    if queued < MIN_QUEUED_COLD and eligible > 0:
        logger.info(
            "supervisor: queued_cold(%s) < MIN_QUEUED_COLD(%s) and eligible(%s) > 0; running email_builder(limit=%s).",
            queued,
            MIN_QUEUED_COLD,
            eligible,
            BUILDER_LIMIT,
        )
        email_builder(limit=BUILDER_LIMIT)

        metrics = get_fuel_metrics()
        eligible = int(metrics.get("eligible_leads", 0) or 0)
        queued = int(metrics.get("queued_cold", 0) or 0)
        logger.info("supervisor: after email_builder, eligible=%s queued_cold=%s", eligible, queued)

    else:
        logger.info(
            "supervisor: no email_builder needed (queued_cold=%s, eligible=%s, MIN_QUEUED_COLD=%s).",
            queued,
            eligible,
            MIN_QUEUED_COLD,
        )

    # 3) Revenue continuity (always attempt send; idempotent if empty)
    logger.info(
        "supervisor: invoking send_queue_v2_flow(batch_size=%s, allow_weekend=%s, dry_run=%s).",
        SEND_BATCH_SIZE,
        ALLOW_WEEKEND,
        DRY_RUN,
    )

    sent = send_queue_v2_flow(batch_size=SEND_BATCH_SIZE, allow_weekend=ALLOW_WEEKEND, dry_run=DRY_RUN)
    logger.info("supervisor: send_queue_v2_flow sent=%s.", sent)

    metrics = get_fuel_metrics()
    logger.info(
        "supervisor: final fuel metrics eligible=%s queued_cold=%s",
        metrics.get("eligible_leads"),
        metrics.get("queued_cold"),
    )
    return metrics


if __name__ == "__main__":
    final_metrics = cold_pipeline_supervisor()
    print(
        "cold_pipeline_supervisor done: "
        f"eligible_leads={final_metrics.get('eligible_leads')} "
        f"queued_cold={final_metrics.get('queued_cold')}"
    )
