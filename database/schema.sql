-- Clinical Data ETL Pipeline Database Schema
CREATE SCHEMA IF NOT EXISTS staging;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ETL Jobs tracking table
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    study_id VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- TODO: Candidate to implement
-- Expected tables to be designed by candidate:
-- - clinical_measurements (raw data)
-- - processed_measurements (transformed data)
-- - participants
-- - studies
-- - data_quality_reports
-- - measurement_aggregations

-- Sample basic table structure (candidate should enhance)
CREATE TABLE IF NOT EXISTS staging.clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    value TEXT NOT NULL,
    unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL,
    site_id VARCHAR(50) NOT NULL,
    quality_score NUMERIC(3,2) CHECK (quality_score BETWEEN 0 and 1),
    processed_at TIMESTAMP DEFAULT now(),
    created_at TIMESTAMP DEFAULT now(),
    job_id UUID NOT NULL REFERENCES etl_jobs(id) ON DELETE CASCADE,
    source_filename TEXT NOT NULL,
    row_num INT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_job_file_row
    ON staging.clinical_measurements(job_id, source_filename, row_num)

CREATE INDEX IF NOT EXISTS ix_raw_study_type_time
  ON staging.clinical_measurements(study_id, measurement_type, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS ix_raw_participant_time
  ON staging.clinical_measurements(participant_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS ix_raw_ts_brin
  ON staging.clinical_measurements USING BRIN("timestamp");

-- Basic indexes (candidate should optimize)
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_id 
    ON staging.clinical_measurements(study_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_id 
    ON staging.clinical_measurements(participant_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_timestamp 
    ON staging.clinical_measurements(timestamp);

-- ETL Jobs indexes
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_created_at ON etl_jobs(created_at);
