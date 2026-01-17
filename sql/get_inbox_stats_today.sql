WITH s AS (
  SELECT
    e.inbox_id,
    COUNT(*) FILTER (
      WHERE e.status = 'sent'
        AND e.sent_at::date = CURRENT_DATE
    ) AS sent_today,
    MAX(e.sent_at) FILTER (
      WHERE e.status = 'sent'
    ) AS last_sent_at
  FROM email_sends e
  GROUP BY e.inbox_id
)
SELECT
  i.inbox_id,
  i.email_address,
  i.domain,
  i.daily_cap,
  i.active,
  i.paused,
  i.last_used_at,
  COALESCE(s.sent_today, 0) AS sent_today,
  s.last_sent_at,
  i.provider_type,
  i.provider_config
FROM inboxes i
LEFT JOIN s ON s.inbox_id = i.inbox_id
WHERE i.active = true
  AND i.paused = false;
