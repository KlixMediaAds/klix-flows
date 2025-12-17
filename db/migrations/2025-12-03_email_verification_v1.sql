-- Email verification v1: add verification metadata to leads

ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS email_verification_status TEXT,
  ADD COLUMN IF NOT EXISTS email_verification_source TEXT,
  ADD COLUMN IF NOT EXISTS email_verification_score NUMERIC,
  ADD COLUMN IF NOT EXISTS email_verification_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_leads_email_verif_status
  ON leads (email_verification_status);

CREATE INDEX IF NOT EXISTS idx_leads_email_verif_checked_at
  ON leads (email_verification_checked_at DESC);
