from __future__ import annotations
from typing import Optional, Dict
from prefect import flow, get_run_logger
from klix.email_builder.main import build_for_new_leads

@flow(name="email_builder")
def email_builder(
    limit: int = 25,
    mode: str = "friendly",
    # accept legacy deployment params but ignore them:
    max_leads: int = 100,
    send_type: str = "friendly_then_cold",
    # accept old 'extras' param (optional) so server validation/execution is happy
    extras: Optional[Dict] = None,
) -> int:
    log = get_run_logger()
    n = build_for_new_leads(limit=limit, mode=mode)
    log.info(f"Queued {n} emails into Neon.email_sends (status='queued').")
    return n

if __name__ == "__main__":
    print(email_builder(5, "friendly"))
