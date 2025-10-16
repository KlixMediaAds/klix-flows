import os
from prefect import flow, get_run_logger
from sqlalchemy import create_engine, text

# your sender brain (adjust import if your path differs)
from klix.email_sender.app.sender_engine.execute import tick

PER_RUN_BUDGET = int(os.getenv("PER_RUN_BUDGET", "8"))
JITTER_MAX_S   = float(os.getenv("JITTER_MAX_S", "1.5"))
LOCK_KEY       = int(os.getenv("PG_LOCK_KEY", "8421581"))

@flow(name="send_queue")
def send_queue():
    log = get_run_logger()
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    with engine.begin() as con:
        got_lock = con.execute(text("select pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar()
        if not got_lock:
            log.info("Another tick is running; exiting.")
            return
        try:
            stats = tick(per_run_budget=PER_RUN_BUDGET, jitter_s=JITTER_MAX_S)
            log.info(f"Tick finished: {stats}")
        finally:
            con.execute(text("select pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
