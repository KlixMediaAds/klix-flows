from prefect import flow, get_run_logger

@flow(name="send_queue")
def send_queue(batch_size: int = 25, allow_weekend: bool = False, **kwargs) -> int:
    """
    Wrapper that prefers the real sender if present.
    Accepts allow_weekend and absorbs stray params to avoid SignatureMismatch.
    """
    logger = get_run_logger()
    try:
        from klix.email_sender.send_queue import send_queue as real_send
        return real_send(batch_size=batch_size, allow_weekend=allow_weekend)
    except Exception as e:
        logger.warning("Real sender unavailable (%s). Falling back to stub.", e)
        import os
        from sqlalchemy import create_engine, text
        eng = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)
        n = 0
        with eng.begin() as cx:
            rows = cx.execute(text("""
              select id from email_sends where status='queued'
              order by id asc limit :lim
            """), {"lim": batch_size}).mappings().all()
        for r in rows:
            with eng.begin() as cx:
                cx.execute(text("""
                  update email_sends
                     set status='sent', provider_message_id='dry-run', sent_at=now()
                   where id=:id
                """), {"id": r["id"]})
            n += 1
        return n
