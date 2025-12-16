-- ERA 1.9.6 Warmup Filtering & Signal Hygiene v1
-- View normalization: ensure analytics views exclude warmup replies by default.

CREATE OR REPLACE VIEW v_email_performance_basic AS
WITH latest_reply AS (
    SELECT DISTINCT ON (email_replies.email_send_id)
        email_replies.email_send_id,
        email_replies.id,
        email_replies.received_at,
        email_replies.category,
        email_replies.sub_category,
        email_replies.sentiment,
        email_replies.interest_score,
        email_replies.next_action,
        email_replies.is_human_reviewed
    FROM email_replies
    WHERE email_replies.email_send_id IS NOT NULL
      AND (email_replies.is_warmup = FALSE OR email_replies.is_warmup IS NULL)
    ORDER BY email_replies.email_send_id, email_replies.received_at DESC NULLS LAST
), reply_agg AS (
    SELECT
        email_replies.email_send_id,
        COUNT(*) AS reply_count,
        MIN(email_replies.received_at) AS first_reply_received_at,
        MAX(email_replies.received_at) AS last_reply_received_at
    FROM email_replies
    WHERE email_replies.email_send_id IS NOT NULL
      AND (email_replies.is_warmup = FALSE OR email_replies.is_warmup IS NULL)
    GROUP BY email_replies.email_send_id
)
SELECT
    s.id AS email_send_id,
    s.lead_id,
    s.inbox_id,
    i.email_address AS from_email,
    s.to_email,
    s.subject,
    s.status,
    s.send_type,
    s.created_at,
    s.sent_at,
    COALESCE(s.sent_at, s.created_at) AS effective_sent_at,
    s.prompt_profile_id,
    s.prompt_angle_id,
    s.template_id,
    s.variant,
    s.segment_tag,
    s.psychographic_tag,
    s.experiment_id,
    s.model_name,
    s.generation_style_seed,
    ra.reply_count,
    ra.reply_count > 0 AS has_reply,
    ra.first_reply_received_at,
    ra.last_reply_received_at,
    lr.category AS last_reply_category,
    lr.sub_category AS last_reply_sub_category,
    lr.sentiment AS last_reply_sentiment,
    lr.interest_score AS last_reply_interest_score,
    lr.next_action AS last_reply_next_action,
    lr.is_human_reviewed AS last_reply_is_human_reviewed
FROM email_sends s
LEFT JOIN inboxes i
    ON s.inbox_id = i.inbox_id
LEFT JOIN reply_agg ra
    ON s.id = ra.email_send_id
LEFT JOIN latest_reply lr
    ON s.id = lr.email_send_id;

-- NOTE:
-- v_profile_angle_stats_7d and v_inbox_reply_health_7d already build on
-- v_email_performance_basic, so they automatically inherit the warmup filter.
