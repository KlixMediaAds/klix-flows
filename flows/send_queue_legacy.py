from prefect import flow, get_run_logger

_impl = None
try:
    from klix.email_sender.send_queue import send_queue as _impl
except Exception:
    try:
        from klix.email_sender.main import send_queue as _impl
    except Exception:
        _impl = None

@flow(name="send_queue")
def send_queue(batch_size: int = 25, allow_weekend: bool = False) -> int:
    log = get_run_logger()
    if _impl is None:
        log.error("No send_queue implementation found in klix.email_sender.*; noop.")
        return 0
    try:
        return int(_impl(batch_size=batch_size, allow_weekend=allow_weekend))
    except TypeError:
        res = _impl()
        try:
            return int(res)
        except Exception:
            return 0
