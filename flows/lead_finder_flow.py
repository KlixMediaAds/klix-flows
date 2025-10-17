from prefect import flow, get_run_logger
from klix.lead_finder.main import find_leads

@flow(name="lead_finder")
def lead_finder(count: int = 100):
    log = get_run_logger()
    n = find_leads(target=count)
    log.info(f"Inserted {n} leads")
