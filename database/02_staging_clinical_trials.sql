-- Clinical Data ETL Pipeline Database Schema
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    value TEXT NOT NULL,
    unit VARCHAR(20),
    "timestamp" TIMESTAMP NOT NULL,
    site_id VARCHAR(50) NOT NULL,
    quality_score NUMERIC(3,2) CHECK (quality_score BETWEEN 0 and 1),
    processed_at TIMESTAMP DEFAULT now(),
    created_at TIMESTAMP DEFAULT now(),

    -- new columns
    job_id UUID NOT NULL REFERENCES etl_jobs(id) ON DELETE CASCADE,
    source_filename TEXT NOT NULL,
    row_num INT NOT NULL
);

-- Basic indexes (candidate should optimize)
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_id 
    ON staging.clinical_measurements(study_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_id 
    ON staging.clinical_measurements(participant_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_timestamp 
    ON staging.clinical_measurements("timestamp");

CREATE INDEX IF NOT EXISTS ix_raw_study_type_time
  ON staging.clinical_measurements(study_id, measurement_type, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS ix_raw_participant_time
  ON staging.clinical_measurements(participant_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS ix_raw_ts_brin
  ON staging.clinical_measurements USING BRIN("timestamp");