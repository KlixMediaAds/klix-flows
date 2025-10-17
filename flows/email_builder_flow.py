from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client

SEND_QUEUE_DEPLOYMENT = "send_queue/send-queue"

try:
    from klix.email_builder.main import build_for_new_leads
except Exception:
    build_for_new_leads = None

@flow(name="email_builder")
async def email_builder(max_leads: int = 100, send_type: str = "friendly_then_cold") -> int:
    log = get_run_logger()
    if not build_for_new_leads:
        log.warning("klix.email_builder.main.build_for_new_leads not found; noop run.")
        return 0
    queued = build_for_new_leads(limit=max_leads, mode=send_type)
    log.info(f"Queued {queued} emails")
    if queued > 0:
        async with get_client() as client:
            await client.create_flow_run_from_deployment(
                name=SEND_QUEUE_DEPLOYMENT,
                parameters={},  # use deployment defaults
            )
        log.info(f"Triggered send_queue because queued={queued}")
    return queued
