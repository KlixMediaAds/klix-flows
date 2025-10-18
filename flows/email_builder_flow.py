from __future__ import annotations
from prefect import flow, get_run_logger
from klix.email_builder.main import build_for_new_leads

@flow(name="email_builder")
def email_builder(
    limit: int = 25,
    mode: str = "friendly",
    # legacy params still present in the deployment; accept but ignore
    max_leads: int = 100,
    send_type: str = "friendly_then_cold",
) -> int:
    log = get_run_logger()
    n = build_for_new_leads(limit=limit, mode=mode)
    log.info(f"Queued {n} emails into Neon.email_sends (status='queued').")
    return n

if __name__ == "__main__":
    print(email_builder(5, "friendly"))
