-- ERA 1.5.1 Telemetry Spine v1
-- Additive-only migration: email_sends/replies, llm_calls, interactions flag,
-- warmup_analytics, send_events.

-- 1.1 email_sends: prompt / variance metadata
ALTER TABLE email_sends
    ADD COLUMN IF NOT EXISTS template_id            TEXT,
    ADD COLUMN IF NOT EXISTS prompt_profile_id      UUID,
    ADD COLUMN IF NOT EXISTS prompt_angle_id        TEXT,
    ADD COLUMN IF NOT EXISTS generation_style_seed  TEXT,
    ADD COLUMN IF NOT EXISTS body_hash              TEXT,
    ADD COLUMN IF NOT EXISTS subject_hash           TEXT;

-- 1.2 email_replies: governor-ready reply signals
ALTER TABLE email_replies
    ADD COLUMN IF NOT EXISTS category          TEXT,   -- INTERESTED / OBJECTION / NOT_INTERESTED / OOO / UNSUBSCRIBE / OTHER
    ADD COLUMN IF NOT EXISTS sub_category      TEXT,
    ADD COLUMN IF NOT EXISTS sentiment         TEXT,   -- positive / neutral / negative
    ADD COLUMN IF NOT EXISTS interest_score    INTEGER,
    ADD COLUMN IF NOT EXISTS next_action       TEXT,   -- book_call / follow_up / stop / etc.
    ADD COLUMN IF NOT EXISTS is_human_reviewed BOOLEAN DEFAULT FALSE;

-- 1.3 llm_calls: LLM telemetry spine
CREATE TABLE IF NOT EXISTS llm_calls (
    id            BIGSERIAL PRIMARY KEY,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    context_type  TEXT,         -- 'email_personalization', 'reply_classification', etc.
    model_name    TEXT,
    input_text    TEXT,
    output_text   TEXT,
    email_send_id BIGINT REFERENCES email_sends(id),
    lead_id       BIGINT,
    success       BOOLEAN,
    latency_ms    INTEGER
);

-- 1.4 interactions: warmup vs real replies flag
ALTER TABLE interactions
    ADD COLUMN IF NOT EXISTS is_warmup BOOLEAN DEFAULT FALSE;

-- 1.5 warmup_analytics: minimal warmup store
CREATE TABLE IF NOT EXISTS warmup_analytics (
    email_address TEXT PRIMARY KEY,
    health_score  INTEGER,
    raw_json      JSONB
);

-- 1.6 send_events: v2 engine event log
CREATE TABLE IF NOT EXISTS send_events (
    id             BIGSERIAL PRIMARY KEY,
    inbox_id       UUID REFERENCES inboxes(inbox_id),
    email_send_id  BIGINT REFERENCES email_sends(id),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    event_type     TEXT,    -- 'candidate_selected', 'send_success', 'send_error', 'skip_cap', etc.
    message        TEXT
);
