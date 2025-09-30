import os
from sqlalchemy import create_engine, text
from typing import Mapping, Any, Optional

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

def engine_execute(sql: str, mapped_values: Mapping[str, Any]) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), mapped_values)

def upsert_job(job_id: str, filename: str, study_id: str | None) -> None:
    sql = """
    INSERT INTO etl_jobs (id, filename, study_id, status, created_at, updated_at, completed_at, error_message)
    VALUES (:id, :fn, :sid, 'running', NOW(), NOW(), NULL, NULL)
    """
    engine_execute(sql, {"id": job_id, "fn": filename, "sid": study_id})

def fetch_job(job_id: str) -> Optional[dict]:
    sql = """
    "SELECT id, status, error_message 
    FROM etl_jobs 
    WHERE id=:id"
    """
    with engine.begin() as conn:
        row = conn.execute(text(
        ), {"id": job_id}).mappings().first()
    return dict(row) if row else None

def complete_job(job_id: str) -> None:
    sql = """
    UPDATE etl_jobs 
    SET status='completed', completed_at=NOW(), updated_at=NOW(), error_message=NULL
    WHERE id=:id
    """
    engine_execute(sql, {"id": job_id})

def fail_job(job_id: str, msg: str) -> None:
    sql = """
    UPDATE etl_jobs 
    SET status='failed', updated_at=NOW(), error_message=:message 
    WHERE id=:id
    """
    engine_execute(sql, {"id": job_id, "message": msg})