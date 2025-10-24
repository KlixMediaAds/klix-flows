from prefect import flow
from flows.send_queue import send_queue as _send_queue
@flow(name="send-queue")
def send_queue_flow(batch_size: int = 25, allow_weekend: bool = True, dry_run: bool = False):
    import os
    os.environ["SEND_LIVE"] = "0" if dry_run else "1"
    return _send_queue(batch_size=batch_size, allow_weekend=allow_weekend)
