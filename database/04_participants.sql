CREATE TABLE IF NOT EXISTS participants (
  study_id TEXT NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL,
  site_id TEXT,
  created_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (study_id, participant_id)
);

CREATE INDEX IF NOT EXISTS ix_participants_site
  ON participants(study_id, site_id);
  