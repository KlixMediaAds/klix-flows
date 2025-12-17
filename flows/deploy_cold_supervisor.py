from prefect import flow

from flows.cold_supervisor import cold_pipeline_supervisor


if __name__ == "__main__":
    """
    Deploy the cold_pipeline_supervisor flow to Prefect.

    Usage:
        PYTHONPATH=. python flows/deploy_cold_supervisor.py
    """
    flow_from_source = cold_pipeline_supervisor.from_source(
        source=".",
        entrypoint="flows/cold_supervisor.py:cold_pipeline_supervisor",
    )

    flow_from_source.deploy(
        name="cold-pipeline-supervisor",
        work_pool_name="klix-managed",
        work_queue_name="cold-supervisor",
        cron="*/30 13-21 * * 1-5",  # every 30 min during send window, Mon–Fri
        tags=["cold", "supervisor"],
        parameters={},
    )
