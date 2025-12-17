from prefect.client.schemas.schedules import CronSchedule
from flows.runbook_daily import runbook_daily_flow


if __name__ == "__main__":
    runbook_daily_flow.deploy(
        name="runbook-daily",
        work_pool_name="klix-managed",
        tags=["governance", "runbook"],
        schedule=CronSchedule(
            cron="0 15 * * 1-5",  # 3 PM ET on weekdays
            timezone="America/Toronto",
        ),
        description=(
            "Daily 3PM ET governance runbook: env sanity, send window, "
            "deployments, inbox health, angles, fuel, queue states, caps."
        ),
        # Required by Prefect 3 deploy() to avoid the remote storage check.
        # This is a placeholder; your klix-managed process worker will just
        # run the flow from /opt/klix/klix-flows as usual.
        image="klix/runbook-daily:placeholder",
    )
