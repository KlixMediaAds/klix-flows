from prefect import flow, get_run_logger

def _import_runner(candidates):
    for mod, attr in candidates:
        try:
            m = __import__(mod, fromlist=[attr])
            return getattr(m, attr)
        except Exception:
            continue
    raise ImportError(str(candidates))

LEADS = [("klix.lead_finder.entry","run"),("klix.lead_finder.main","run"),("klix.lead_finder","run")]
BUILD = [("klix.email_builder.entry","run"),("klix.email_builder.main","run"),("klix.email_builder","run")]

@flow(name="daily_intake")
def daily_intake(batch_size: int = 100):
    log = get_run_logger()
    lf = _import_runner(LEADS)
    eb = _import_runner(BUILD)
    log.info(f"lead_finder(limit={batch_size})"); lf(limit=batch_size)
    log.info(f"email_builder(limit={batch_size})"); eb(limit=batch_size)
    log.info("daily_intake finished.")
