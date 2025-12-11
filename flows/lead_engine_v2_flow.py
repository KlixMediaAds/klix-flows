from prefect import flow, get_run_logger

from klix.lead_engine_v2.ingest_flow import run_ingest


@flow(name="lead-engine-v2")
def lead_engine_v2() -> None:
    """
    Prefect flow wrapper for Lead Engine v2.

    It delegates the core work to `klix.lead_engine_v2.ingest_flow.run_ingest()`
    and logs per-source metrics.
    """
    logger = get_run_logger()
    logger.info("Lead Engine v2 flow started.")

    counts_by_source = run_ingest()

    # Structured logging for future health checks / dashboards.
    total_inserted = 0
    for source, counts in counts_by_source.items():
        fetched = counts.get("fetched", 0)
        normalized = counts.get("normalized", 0)
        inserted = counts.get("inserted", 0)
        deduped = counts.get("deduped", 0)

        total_inserted += inserted

        logger.info(
            "lead_engine_v2_source_done",
            extra={
                "source": source,
                "fetched": fetched,
                "normalized": normalized,
                "inserted": inserted,
                "deduped": deduped,
            },
        )

    logger.info(
        "lead_engine_v2_run_complete",
        extra={
            "total_sources": len(counts_by_source),
            "total_inserted": total_inserted,
        },
    )


if __name__ == "__main__":
    # Allow local/manual runs via: python flows/lead_engine_v2_flow.py
    lead_engine_v2()

