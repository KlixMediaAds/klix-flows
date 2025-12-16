-- ERA 1.9.6 Warmup Filtering & Signal Hygiene v1
-- Noise analytics slice: 7d warmup vs real replies per inbox.

CREATE OR REPLACE VIEW v_reply_noise_stats_7d AS
SELECT
    i.inbox_id,
    i.email_address AS inbox_email,
    i.domain,
    i.provider,
    i.provider_type,
    COUNT(*) FILTER (WHERE r.is_warmup = TRUE) AS warmups_7d,
    COUNT(*) FILTER (WHERE r.is_warmup = FALSE OR r.is_warmup IS NULL) AS real_replies_7d,
    COUNT(*) AS total_replies_7d
FROM email_replies r
LEFT JOIN inboxes i
    ON r.to_email = i.email_address
WHERE r.received_at >= NOW() - INTERVAL '7 days'
GROUP BY
    i.inbox_id,
    i.email_address,
    i.domain,
    i.provider,
    i.provider_type;
