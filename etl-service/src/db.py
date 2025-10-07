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

def upsert_dims(job_id: str) -> None:
    engine_execute(
        """INSERT INTO studies(study_id)
             SELECT DISTINCT study_id
               FROM staging.clinical_measurements
              WHERE job_id=:j
           ON CONFLICT DO NOTHING""",
        {"j": job_id},
    )
    engine_execute(
        """INSERT INTO participants(study_id, participant_id, site_id)
             SELECT DISTINCT study_id, participant_id, site_id
               FROM staging.clinical_measurements
              WHERE job_id=:j
           ON CONFLICT (study_id, participant_id)
           DO UPDATE SET site_id=EXCLUDED.site_id""",
        {"j": job_id},
    )

def insert_processed_rows(rows: List[Dict[str, Any]]) -> None:
    sql = """
    INSERT INTO processed_measurements(
      study_id, participant_id, site_id, measurement_type, measured_at,
      value_num, value_text, unit, quality_score, raw_row_id, job_id
    )
    VALUES(
      :study_id, :participant_id, :site_id, :measurement_type, :measured_at,
      :value_num, :value_text, :unit, :quality_score, :raw_row_id, :job_id
    )
    ON CONFLICT ON CONSTRAINT uq_pm_obs DO NOTHING
    """
    batch_engine_execute(sql, rows)

def insert_quality_counts(rows: List[Dict[str, Any]]) -> None:
    sql = """
    INSERT INTO data_quality_reports(job_id, rule_name, severity, affected_rows)
    VALUES (:job_id, :rule_name, :severity, :affected_rows)
    """
    batch_engine_execute(sql, rows)


def upsert_aggregation_rows(rows: List[Dict[str, Any]]) -> None:
    sql = """
    INSERT INTO measurement_aggregations(
      study_id, participant_id, site_id, measurement_type,
      cnt, avg_num, min_num, max_num, job_id
    )
    VALUES(
      :study_id, :participant_id, :site_id, :measurement_type,
      :cnt, :avg_num, :min_num, :max_num, :job_id
    )
    ON CONFLICT (study_id, participant_id, site_id, measurement_type)
    DO UPDATE SET
      cnt     = EXCLUDED.cnt,
      avg_num = EXCLUDED.avg_num,
      min_num = LEAST(measurement_aggregations.min_num, EXCLUDED.min_num),
      max_num = GREATEST(measurement_aggregations.max_num, EXCLUDED.max_num)
    """
    batch_engine_execute(sql, rows)
