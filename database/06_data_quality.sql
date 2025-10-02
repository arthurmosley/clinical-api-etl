CREATE TABLE IF NOT EXISTS data_quality_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID NOT NULL REFERENCES etl_jobs(id) ON DELETE CASCADE,
  rule_name TEXT NOT NULL,
  severity  TEXT NOT NULL CHECK (severity IN ('info','warn','error')),
  affected_rows INT NOT NULL,
  created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dqr_job ON data_quality_reports(job_id);