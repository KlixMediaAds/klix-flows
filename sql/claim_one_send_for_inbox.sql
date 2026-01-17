UPDATE email_sends e
SET
  status     = 'sending',
  inbox_id   = %(inbox_id)s,
  updated_at = NOW()
WHERE e.id = (
  SELECT es.id
  FROM email_sends es
  WHERE es.status = 'queued'
    AND es.inbox_id IS NULL
    AND es.send_type IN ('cold_v2','cold')
    AND es.prompt_angle_id IS NOT NULL
    AND es.to_email IS NOT NULL
    AND es.subject  IS NOT NULL
    AND es.body     IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM inboxes i
      WHERE i.inbox_id = %(inbox_id)s
        AND i.active = true
        AND i.paused = false
    )
  ORDER BY es.created_at ASC, es.id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING e.*;
