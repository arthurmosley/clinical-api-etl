CREATE SCHEMA IF NOT EXISTS facts;

/*CREATE TABLE IF NOT EXISTS facts.scalar_facts(
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
    participant_id INT NOT NULL REFERENCES dims.participants(id),
    diastolic SMALLINT NOT NULL,
    systolic SMALLINT NOT NULL,
    quality_score NUMERIC (3,2) NOT NULL,
    study_id SMALLINT NOT NULL REFERENCES dims.studies(id),
    site_id SMALLINT NOT NULL REFERENCES dims.sites(id),
    measured_at TIMESTAMPTZ NOT NULL,
    UNIQUE (study_id, site_id, participant_id, measured_at)
);*/


CREATE TABLE IF NOT EXISTS facts.measurement_events (
  id BIGSERIAL PRIMARY KEY,
  study_id SMALLINT NOT NULL REFERENCES dims.studies(id),
  site_id  SMALLINT NOT NULL REFERENCES dims.sites(id),
  participant_id INT NOT NULL REFERENCES dims.participants(id),
  measured_at TIMESTAMPTZ NOT NULL,
  quality_score NUMERIC(3,2) NOT NULL,
  -- prevent duplicate events for a participant at a site/time
  UNIQUE (study_id, site_id, participant_id, measured_at)
);

CREATE TABLE IF NOT EXISTS facts.measurement_values (
  event_id BIGINT NOT NULL REFERENCES facts.measurement_events(id) ON DELETE CASCADE,
  measurement_type_id SMALLINT NOT NULL REFERENCES dims.measurement_types(id),
  component TEXT,                 -- NULL for scalar; 'systolic'/'diastolic' for BP
  unit_id SMALLINT NOT NULL REFERENCES dims.units(id),
  value_num NUMERIC(12,4) NOT NULL,
  PRIMARY KEY (event_id, measurement_type_id, COALESCE(component, ''))
);