from prefect import flow, task
from flows.db import engine
from flows.schema import Base

@task
def create_tables():
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created (or already exist).")

@flow
def bootstrap_db():
    create_tables()

if __name__ == "__main__":
    bootstrap_db()
