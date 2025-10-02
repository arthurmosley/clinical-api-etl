CREATE TABLE IF NOT EXISTS measurement_aggregations (
  id BIGSERIAL PRIMARY KEY,
  grain TEXT NOT NULL DEFAULT 'daily',
  day DATE NOT NULL,
  study_id TEXT NOT NULL,
  participant_id TEXT,
  site_id TEXT,
  measurement_type TEXT NOT NULL,
  cnt BIGINT NOT NULL,
  cnt_num BIGINT,
  avg_num NUMERIC(14,4),
  min_num NUMERIC(14,4),
  max_num NUMERIC(14,4),
  first_ts TIMESTAMP,
  last_ts  TIMESTAMP,
  job_id UUID NOT NULL REFERENCES etl_jobs(id) ON DELETE CASCADE,

  CONSTRAINT uq_ma_daily UNIQUE (grain, day, study_id, participant_id, site_id, measurement_type)
);

CREATE INDEX IF NOT EXISTS ix_ma_study_type_day
  ON measurement_aggregations(study_id, measurement_type, day DESC);
CREATE INDEX IF NOT EXISTS ix_ma_participant_day
  ON measurement_aggregations(study_id, participant_id, day DESC);