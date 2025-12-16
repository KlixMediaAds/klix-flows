UPDATE email_sends e
SET status     = 'sending',
    inbox_id   = %(inbox_id)s,
    updated_at = NOW()
WHERE e.id = (
    SELECT id
    FROM email_sends
    WHERE status = 'queued'
      AND send_type = 'cold'
      AND prompt_angle_id IS NOT NULL
      AND to_email IS NOT NULL
      AND subject  IS NOT NULL
      AND body     IS NOT NULL
    ORDER BY created_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING e.*;
