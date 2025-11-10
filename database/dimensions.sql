CREATE SCHEMA IF NOT EXISTS dims;

CREATE TABLE IF NOT EXISTS dims.studies (
  id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  study_name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dims.sites (
  id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  site_name TEXT NOT NULL,
  study_id SMALLINT NOT NULL REFERENCES dims.studies(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (study_id, site_name),
  UNIQUE (study_id, id)
);

CREATE TABLE IF NOT EXISTS dims.participants (
  id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  participant_name TEXT NOT NULL,
  study_id SMALLINT NOT NULL REFERENCES dims.studies(id) ON DELETE CASCADE,
  site_id SMALLINT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (study_id, participant_name),
  FOREIGN KEY (study_id, site_id) REFERENCES dims.sites(study_id, id)
);

CREATE TABLE IF NOT EXISTS dims.units (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    unit TEXT NOT NULL,
    UNIQUE (unit)
);

CREATE TABLE IF NOT EXISTS dims.measurement_types (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    measurement TEXT NOT NULL,
    unit SMALLINT NOT NULL REFERENCES dims.units(id),
    UNIQUE (measurement, unit)
);