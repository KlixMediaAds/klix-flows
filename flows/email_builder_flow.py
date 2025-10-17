from prefect import flow, get_run_logger
from klix.email_builder.main import build_for_new_leads

@flow(name="email_builder")
def email_builder(max_leads: int = 100, send_type: str = "friendly_then_cold"):
    log = get_run_logger()
    m = build_for_new_leads(limit=max_leads, mode=send_type)
    log.info(f"Queued {m} emails")
