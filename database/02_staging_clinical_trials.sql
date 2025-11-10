-- Clinical Data ETL Pipeline Database Schema
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(10) NOT NULL, -- slightly higher than what we're expecting this allows for studyIDs to go up to: 99,999.
    participant_id VARCHAR(7) NOT NULL, -- largest single study had 162k participants. Allows: 999,999
    measurement_type VARCHAR(20) NOT NULL,
    value varchar(10) NOT NULL,
    unit VARCHAR(10),
    "timestamp" TIMESTAMPTZ NOT NULL,
    site_id VARCHAR(25) NOT NULL, -- site_id is a string so corresponds to a name in input data. Allow 25 character max on the
    quality_score varchar(10),
    processed_at TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now(),

    -- new columns
    job_id UUID NOT NULL REFERENCES etl_jobs(id) ON DELETE CASCADE,
    source_filename TEXT NOT NULL,
    row_num INT NOT NULL,

    CONSTRAINT ux_raw_job_file_row UNIQUE (job_id, source_filename, row_num)
);

CREATE TABLE IF NOT EXISTS staging.rejections (
  job_id UUID NOT NULL,
  staging_row_id UUID NOT NULL,
  reason_code TEXT NOT NULL,
  detail TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  unique (job_id, staging_row_id, reason_code)
);

-- Basic indexes (candidate should optimize)
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_id 
    ON staging.clinical_measurements(study_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_id 
    ON staging.clinical_measurements(participant_id);

CREATE INDEX IF NOT EXISTS ix_raw_study_type_time
  ON staging.clinical_measurements(study_id, measurement_type, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS ix_raw_participant_time
  ON staging.clinical_measurements(participant_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS ix_raw_ts_brin
  ON staging.clinical_measurements USING BRIN("timestamp");