CREATE TABLE IF NOT EXISTS studies (
  study_id TEXT PRIMARY KEY,
  name TEXT,
  created_at TIMESTAMP DEFAULT now()
);