from prefect import flow, get_run_logger

def _import_runner(candidates):
    for mod, attr in candidates:
        try:
            m = __import__(mod, fromlist=[attr])
            return getattr(m, attr)
        except Exception:
            continue
    raise ImportError(str(candidates))

LEADS_RUN_CANDIDATES = [
    ("klix.lead_finder.entry", "run"),
    ("klix.lead_finder.main", "run"),
    ("klix.lead_finder", "run"),
]
BUILDER_RUN_CANDIDATES = [
    ("klix.email_builder.entry", "run"),
    ("klix.email_builder.main", "run"),
    ("klix.email_builder", "run"),
]

@flow(name="daily_intake")
def daily_intake(batch_size: int = 100):
    log = get_run_logger()
    lead_run  = _import_runner(LEADS_RUN_CANDIDATES)
    build_run = _import_runner(BUILDER_RUN_CANDIDATES)

    log.info(f"Running lead_finder(limit={batch_size})")
    lead_run(limit=batch_size)

    log.info(f"Running email_builder(limit={batch_size})")
    build_run(limit=batch_size)

    log.info("daily_intake finished.")
