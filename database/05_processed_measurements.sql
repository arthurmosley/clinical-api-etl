CREATE TABLE IF NOT EXISTS processed_measurements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  study_id TEXT NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  measurement_type TEXT NOT NULL,
  measured_at TIMESTAMP NOT NULL,

  value_num NUMERIC(14,4),
  value_text TEXT,
  unit TEXT,
  quality_score NUMERIC(3,2) CHECK (quality_score BETWEEN 0 AND 1),

  raw_row_id UUID NOT NULL REFERENCES staging.clinical_measurements(id) ON DELETE CASCADE,
  job_id UUID NOT NULL REFERENCES etl_jobs(id) ON DELETE CASCADE,
  created_at TIMESTAMP DEFAULT now(),

  CONSTRAINT uq_pm_obs UNIQUE (study_id, participant_id, measurement_type, measured_at, site_id),

  FOREIGN KEY (study_id, participant_id) REFERENCES participants(study_id, participant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_pm_study_type_time
  ON processed_measurements(study_id, measurement_type, measured_at DESC);
CREATE INDEX IF NOT EXISTS ix_pm_participant_time
  ON processed_measurements(study_id, participant_id, measured_at DESC);
CREATE INDEX IF NOT EXISTS ix_pm_low_quality
  ON processed_measurements(quality_score) WHERE quality_score < 0.95;
