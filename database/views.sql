/*CREATE OR REPLACE VIEW v_study_quality AS
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
GROUP BY study_id;*/

CREATE OR REPLACE VIEW v_unify_fact_tables AS
SELECT study_id, site_id, participant_id, measurement_type_id, quality_score,
measured_at, value_num, unit_id, quality_score
FROM facts.scalar_facts
UNION ALL
SELECT study_id, quality_score
FROM facts.bp_facts;

SELECT * FROM v_unify_fact_tables

-- Which studies have the highest data quality scores?

DROP VIEW IF EXISTS v_data_quality_by_study;
CREATE OR REPLACE VIEW v_data_quality_by_study AS
SELECT study_id, AVG(quality_score) AS avg_quality
FROM v_unify_fact_tables
GROUP BY study_id;

SELECT * FROM v_data_quality_by_study
ORDER BY avg_quality DESC;

-- What are the glucose trends for a specific participant over time?

-- How do measurement counts compare across different research sites?
SELECT s.study_name, si.site_name, COUNT(*) AS measurements
FROM (
    SELECT site_id, measured_at FROM facts.scalar_facts
    UNION ALL
    SELECT site_id, measured_at FROM facts.bp_facts
) f
JOIN dims.sites si ON si.id = f.site_id
JOIN dims.studies s ON s.id = si.study_id
GROUP BY s.study_name, si.site_name
ORDER BY measurements DESC

-- Which measurements have quality scores below our threshold?

-- What clinical data was collected in the last 30 days?

-- How many participants are enrolled in each study?

-- What's the average BMI for participants in a specific study?