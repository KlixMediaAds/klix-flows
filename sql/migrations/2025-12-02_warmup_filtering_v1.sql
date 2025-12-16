-- ERA 1.9.6 Warmup Filtering & Signal Hygiene v1
-- Additive-only migration: email_replies noise flags.

-- 1.1 email_replies: warmup / noise labelling
ALTER TABLE email_replies
    ADD COLUMN IF NOT EXISTS is_warmup  BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS noise_type TEXT;  -- 'warmup' | 'auto_reply' | 'bounce' | 'out_of_office' | 'other_noise'

