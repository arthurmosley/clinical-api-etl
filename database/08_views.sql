CREATE OR REPLACE VIEW v_study_quality AS
SELECT study_id, AVG(quality_score) AS avg_quality
FROM processed_measurements
GROUP BY study_id;

CREATE OR REPLACE VIEW v_glucose_trend AS
SELECT study_id, participant_id, DATE(measured_at) AS day, AVG(value_num) AS avg_glucose
FROM processed_measurements
WHERE measurement_type='glucose' AND value_num IS NOT NULL
GROUP BY study_id, participant_id, DATE(measured_at);

CREATE OR REPLACE VIEW v_counts_by_site AS
SELECT study_id, site_id, measurement_type, COUNT(*) AS cnt
FROM processed_measurements
GROUP BY study_id, site_id, measurement_type;

CREATE OR REPLACE VIEW v_low_quality AS
SELECT *
FROM processed_measurements
WHERE quality_score IS NOT NULL AND quality_score < 0.95;

CREATE OR REPLACE VIEW v_recent_30d AS
SELECT *
FROM processed_measurements
WHERE measured_at >= now() - interval '30 days';

CREATE OR REPLACE VIEW v_participants_per_study AS
SELECT study_id, COUNT(*) AS participants
FROM participants
GROUP BY study_id;
