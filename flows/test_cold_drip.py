from prefect import flow, get_run_logger


@flow(name="test-cold-drip-flow")
def test_cold_drip_flow() -> None:
    """
    Minimal plumbing-test flow for klix-managed / cold-drip.

    This should:
      - Import with zero side effects
      - Not touch DATABASE_URL, SMTP, or any KLIX internals
      - Just log a clear message so we can see it in the worker logs.
    """
    logger = get_run_logger()
    logger.info("TEST FLOW: hello from klix-managed / cold-drip! 🚀")


if __name__ == "__main__":
    # Optional: allow ad-hoc local runs if you ever want to test quickly:
    test_cold_drip_flow()
