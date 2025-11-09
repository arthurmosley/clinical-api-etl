import os
from sqlalchemy import create_engine, text
from typing import Dict, Iterable, List, Mapping, Any, Optional

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)

def engine_execute(sql: str, mapped_values: Mapping[str, Any]) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), mapped_values)

def batch_engine_execute(sql: str, rows: Iterable[Mapping[str, Any]]) -> None:
    batch = list(rows)
    if not batch:
        return
    with engine.begin() as conn:
        conn.execute(text(sql), batch)

def fetch_one(sql: str, params: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    with engine.begin() as conn:
        row = conn.execute(text(sql), params).mappings().first()
        return dict(row) if row else None

def fetch_job(job_id: str) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT id, filename, study_id, status, created_at, updated_at, completed_at, error_message
      FROM etl_jobs WHERE id = :id
    """
    return fetch_one(sql, {"id": job_id})
    

def mark_status(job_id: str, status: str, message: Optional[str] = None) -> None:
    engine_execute(
        """UPDATE etl_jobs
              SET status=:s, updated_at=NOW(),
                  completed_at=CASE WHEN :s IN ('completed','failed') THEN NOW() ELSE completed_at END,
                  error_message=:m
            WHERE id=:j""",
        {"s": status, "m": message, "j": job_id},
    )

def insert_staging_rows(rows: List[Dict[str, Any]]) -> None:
    """
    Insert pre-assigned raw UUIDs so we can reference them in processed_measurements.raw_row_id.
    """
    sql = """
    INSERT INTO staging.clinical_measurements(
      id, job_id, source_filename, row_num,
      study_id, participant_id, measurement_type, value, unit, "timestamp",
      site_id, quality_score
    )
    VALUES(
      :id, :job_id, :source_filename, :row_num,
      :study_id, :participant_id, :measurement_type, :value, :unit, :timestamp,
      :site_id, :quality_score
    )
    ON CONFLICT (job_id, source_filename, row_num) DO NOTHING
    """
    batch_engine_execute(sql, rows)

def upsert_job(job_id: str, filename: str, study_id: str | None) -> None:
    sql = """
    INSERT INTO etl_jobs (id, filename, study_id, status, created_at, updated_at, completed_at, error_message)
    VALUES (:id, :fn, :sid, 'running', NOW(), NOW(), NULL, NULL)
    ON CONFLICT (id) DO UPDATE
    SET status='running', updated_at=NOW(), error_message=NULL
    """
    engine_execute(sql, {"id": job_id, "fn": filename, "sid": study_id})

# ON CONFLICT I DO WANNA DO SOMETHING.

def upsert_studies(job_id: str) -> None:
  engine_execute(
      """INSERT INTO dims.studies(study_name)
            SELECT DISTINCT study_id
            FROM staging.clinical_measurements
            WHERE job_id=:j
          ON CONFLICT DO NOTHING""",
      {"j": job_id},
  )

# TODO: FINISH THESE UPSERTS. AFTER WE DO JOINS INTO THE PROCESSING TABLE FROM THERE WE CREATE FACT TABLES FOR ANALYSIS

def upsert_sites(job_id: str) -> None:
  engine_execute(
    """INSERT INTO dims.sites(study_id, site_name)
          SELECT DISTINCT ds.id, s.site_id
          FROM staging.clinical_measurements s
          JOIN dims.studies ds ON ds.study_name = s.study_id
          WHERE job_id=:j
        ON CONFLICT (study_id, site_name) DO NOTHING""",
    {"j": job_id},
  )

def upsert_participants(job_id: str) -> None:
  engine_execute(
    """INSERT INTO dims.participants(study_id, participant_name, site_id)
          SELECT DISTINCT ds.id, s.participant_id, dsi.id
          FROM staging.clinical_measurements s
          JOIN dims.studies ds ON ds.study_name = s.study_id
          JOIN dims.sites dsi ON dsi.study_id = ds.id
          AND dsi.site_name = s.site_id
          WHERE job_id=:j
        ON CONFLICT DO NOTHING""",
    {"j": job_id},
  )

def upsert_units(job_id: str) -> None:
  engine_execute(
    """INSERT INTO dims.units(unit)
          SELECT DISTINCT unit
          FROM staging.clinical_measurements
          WHERE job_id=:j
        ON CONFLICT DO NOTHING""",
    {"j": job_id},
  )

def upsert_measurement_types(job_id: str) -> None:
  engine_execute(
    """INSERT INTO dims.measurement_types(measurement, unit)
          SELECT DISTINCT measurement_type, du.id
          FROM staging.clinical_measurements s
          JOIN dims.units du ON du.unit = s.unit
          WHERE job_id=:j
        ON CONFLICT DO NOTHING""",
    {"j": job_id},
  )

def upsert_dims(job_id: str) -> None:
  upsert_studies(job_id)
  upsert_sites(job_id)
  upsert_participants(job_id)
  upsert_units(job_id)
  upsert_measurement_types(job_id)

