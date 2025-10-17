from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client

SEND_QUEUE_DEPLOYMENT = "send_queue/send-queue"

def _discover_builder():
    """Find a callable named build_for_new_leads anywhere under klix.email_builder.*"""
    try:
        import pkgutil, importlib, inspect
        import klix.email_builder as pkg
        for modinfo in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
            try:
                m = importlib.import_module(modinfo.name)
            except Exception:
                continue
            fn = getattr(m, "build_for_new_leads", None)
            if callable(fn):
                return fn
    except Exception:
        pass
    # common fallbacks
    try:
        from klix.email_builder.main import build_for_new_leads as fn; return fn
    except Exception:
        pass
    try:
        from klix.email_builder.builder import build_for_new_leads as fn; return fn
    except Exception:
        pass
    return None

@flow(name="email_builder")
async def email_builder(max_leads: int = 100, send_type: str = "friendly_then_cold") -> int:
    log = get_run_logger()
    build_for_new_leads = _discover_builder()
    if not build_for_new_leads:
        log.warning("Could not locate build_for_new_leads in klix.email_builder.*; noop run.")
        return 0
    queued = build_for_new_leads(limit=max_leads, mode=send_type)
    log.info(f"Queued {queued} emails")
    if queued and queued > 0:
        async with get_client() as client:
            await client.create_flow_run_from_deployment(
                name=SEND_QUEUE_DEPLOYMENT,
                parameters={},  # deployment defaults
            )
        log.info(f"Triggered send_queue because queued={queued}")
    return queued
