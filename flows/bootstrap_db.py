from prefect import flow
from flows.db import ensure_tables

@flow(name="bootstrap_db")
def bootstrap_db():
    ensure_tables()
    return "ok"

if __name__ == "__main__":
    bootstrap_db()
