
SELECT * FROM dims.studies

SELECT * FROM dims.sites

SELECT * FROM dims.participants
SELECT COUNT(*) FROM dims.participants
SELECT * FROM staging.clinical_measurements
SET enable_nestloop=0;SELECT 'postgresql' AS dbms,t.table_catalog,t.table_schema,t.table_name,c.column_name,c.ordinal_position,c.data_type,c.character_maximum_length,n.constraint_type,k2.table_schema,k2.table_name,k2.column_name FROM information_schema.tables t NATURAL LEFT JOIN information_schema.columns c LEFT JOIN(information_schema.key_column_usage k NATURAL JOIN information_schema.table_constraints n NATURAL LEFT JOIN information_schema.referential_constraints r)ON c.table_catalog=k.table_catalog AND c.table_schema=k.table_schema AND c.table_name=k.table_name AND c.column_name=k.column_name LEFT JOIN information_schema.key_column_usage k2 ON k.position_in_unique_constraint=k2.ordinal_position AND r.unique_constraint_catalog=k2.constraint_catalog AND r.unique_constraint_schema=k2.constraint_schema AND r.unique_constraint_name=k2.constraint_name WHERE t.TABLE_TYPE='BASE TABLE' AND t.table_schema NOT IN('information_schema','pg_catalog');

SELECT * FROM dims.measurement_types
SELECT * FROM dims.units
SELECT current_database(), current_user;
SELECT * FROM dims.units
SELECT * FROM staging.rejections
SELECT * FROM facts.scalar_facts
SELECT COUNT(*) FROM facts.scalar_facts
SELECT COUNT(*) FROM facts.bp_facts
  AND dm.id IS NULL;
````                                                                            
-- rows in QC view for this job

SELECT
  ds.id AS study_id, si.id AS site_id, p.id AS participant_id, dm.id AS measurement_type_id,
  qc."timestamp"::timestamptz AS measured_at,   -- or qc.measured_at
  qc.value_num::numeric(4,1)  AS value_num,
  dm.unit                     AS unit_id,
  qc.quality_score::numeric(3,2) AS quality_score
FROM staging.quality_control qc
JOIN dims.studies ds  ON ds.study_name  = qc.study_id
JOIN dims.sites   si  ON si.study_id    = ds.id  AND si.site_name = qc.site_id
JOIN dims.participants p ON p.study_id  = ds.id  AND p.site_id = si.id
                        AND p.participant_name = qc.participant_id
JOIN dims.measurement_types dm ON dm.measurement = qc.measurement_type
WHERE dm.measurement <> 'blood_pressure'
  AND qc.value_num IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM staging.rejections r
      WHERE r.job_id = qc.job_id AND r.staging_row_id = qc.staging_row_id
  )
LIMIT 5;


SELECT * FROM staging.quality_control
SELECT * FROM staging.clinical_measurements
SELECT * FROM dims.units


    CREATE OR REPLACE VIEW quality_control AS
      SELECT
        s.id AS staging_row_id,
        s.job_id,
        s.source_filename,
        s.row_num,
        s.study_id,
        s.site_id,
        s.participant_id,
        s.measurement_type,
        s.unit,
        s."timestamp",
        s.quality_score::numeric(3,2) AS quality_score,
        s.measurement_type = 'blood_pressure' as is_bp,
        s.measurement_type <> 'blood_pressure' AS is_scalar,
        CASE WHEN s.value ~ '^[0-9]+(\.[0-9]+)?$' THEN s.value::numeric END AS value_num,
        CASE WHEN s.value ~ '^[0-9]{2,3}/[0-9]{2,3}$'
          THEN split_part(s.value,'/',1)::smallint END AS systolic,
        CASE WHEN s.value ~ '^[0-9]{2,3}/[0-9]{2,3}$'
          THEN split_part(s.value,'/',2)::smallint END AS diastolic
      FROM staging.clinical_measurements s