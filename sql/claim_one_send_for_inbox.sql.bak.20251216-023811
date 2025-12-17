UPDATE email_sends e
SET status     = 'sending',
    inbox_id   = %(inbox_id)s,
    updated_at = NOW()
WHERE e.id = (
    SELECT id
    FROM email_sends
    WHERE status = 'queued'
      AND send_type = 'cold'
    ORDER BY created_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING e.*;
