CREATE SCHEMA IF NOT EXISTS facts;

CREATE TABLE IF NOT EXISTS facts.scalar_facts(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    study_id SMALLINT NOT NULL REFERENCES dims.studies(id),
    site_id SMALLINT NOT NULL REFERENCES dims.sites(id),
    participant_id INT NOT NULL REFERENCES dims.participants(id),
    measurement_type_id SMALLINT NOT NULL REFERENCES dims.measurement_types(id),
    measured_at TIMESTAMPTZ NOT NULL, -- captured at and processed at probably?
    value_num NUMERIC(4,1) NOT NULL,
    unit_id SMALLINT NOT NULL REFERENCES dims.units(id),
    quality_score NUMERIC (3,2) NOT NULL,
    UNIQUE (study_id, site_id, participant_id, measured_at, measurement_type_id)
);

CREATE TABLE IF NOT EXISTS facts.bp_facts(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    study_id SMALLINT NOT NULL REFERENCES dims.studies(id),
    site_id SMALLINT NOT NULL REFERENCES dims.sites(id),
    participant_id INT NOT NULL REFERENCES dims.participants(id),
    measured_at TIMESTAMPTZ NOT NULL,
    diastolic SMALLINT NOT NULL,
    systolic SMALLINT NOT NULL,
    quality_score NUMERIC (3,2) NOT NULL,
    UNIQUE (study_id, site_id, participant_id, measured_at)
);

INSERT INTO facts.bp_facts(
    study_id,
    site_id,
    participant_id,
    measured_at,
    diastolic,
    systolic,
    quality_score)
SELECT
    ds.id,
    dsi.id,
    dp.id,
    s.processed_at,
    SPLIT_PART(s.value, '/', 1)::SMALLINT,
    SPLIT_PART(s.value, '/', 2)::SMALLINT,
    s.quality_score::numeric(3,2)
FROM staging.clinical_measurements s
    JOIN dims.studies ds ON ds.study_name = s.study_id
    JOIN dims.sites dsi ON dsi.study_id = ds.id
        AND dsi.site_name = s.site_id
    JOIN dims.participants dp ON dp.study_id = ds.id
        AND dp.site_id = dsi.id
        AND dp.participant_name = s.participant_id
    JOIN dims.measurement_types dm ON dm.measurement = s.measurement_type
WHERE dm.measurement = 'blood_pressure'
    AND s.measurement_type = 'blood_pressure'
ON CONFLICT (study_id, site_id, participant_id, measured_at) DO NOTHING;