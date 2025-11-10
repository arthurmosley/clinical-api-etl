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

def upsert_units() -> None:
  engine_execute(
    """
    INSERT INTO dims.units(unit) VALUES
      ('mg/dL'), ('kg'), ('cm'), ('bpm'), ('mmHg')
    ON CONFLICT (unit) DO NOTHING;
    """,
    {},
  )

def upsert_measurement_types() -> None:
  engine_execute(
    """
    INSERT INTO dims.measurement_types(measurement, unit)
    SELECT m.measurement, u.id
    FROM (VALUES
        ('glucose','mg/dL'),
        ('cholesterol','mg/dL'),
        ('weight','kg'),
        ('height','cm'),
        ('heart_rate','bpm'),
        ('blood_pressure','mmHg')
    ) AS m(measurement, unit_name)
    JOIN dims.units u ON u.unit = m.unit_name
    ON CONFLICT (measurement, unit) DO UPDATE
      SET unit = EXCLUDED.unit;
    """,
    {},
  )

def upsert_dims(job_id: str) -> None:
  upsert_studies(job_id)
  upsert_sites(job_id)
  upsert_participants(job_id)
  upsert_units()
  upsert_measurement_types()
  
def create_QC_view() -> None:
  engine_execute(
    """
    CREATE OR REPLACE VIEW staging.quality_control AS
      SELECT
        s.id AS staging_row_id,
        s.job_id,
        s.source_filename,
        s.row_num,
        s.study_id,
        s.site_id,
        s.participant_id,
        lower(btrim(s.measurement_type)) AS measurement_type,
        s.unit,
        s."timestamp"::TIMESTAMPTZ AS measured_at,
        s.quality_score::numeric AS quality_score,
        s.measurement_type = 'blood_pressure' AS is_bp,
        s.measurement_type <> 'blood_pressure' AS is_scalar,
        CASE WHEN s.value ~ '^[0-9]+(\.[0-9]+)?$' 
          THEN s.value::numeric END AS value_num,
        CASE WHEN s.value ~ '^[0-9]{2,3}/[0-9]{2,3}$'
          THEN split_part(s.value,'/',1)::smallint END AS systolic,
        CASE WHEN s.value ~ '^[0-9]{2,3}/[0-9]{2,3}$'
          THEN split_part(s.value,'/',2)::smallint END AS diastolic
      FROM staging.clinical_measurements s
      """, {}
    )

def upsert_rejected_scalar(job_id: str) -> None:
  engine_execute(
    """
    INSERT INTO staging.rejections(
        job_id, 
        staging_row_id,
        reason_code,
        detail)
    SELECT qc.job_id, qc.staging_row_id, 'non numeric scalar', qc.value_num::TEXT
    FROM staging.quality_control qc
    WHERE job_id = :j AND qc.is_scalar AND qc.value_num IS NULL;
    """, {"j": job_id}
  )
  
def upsert_rejected_bp(job_id: str) -> None:
  engine_execute(
    """
    INSERT INTO staging.rejections(
        job_id, 
        staging_row_id,
        reason_code,
        detail)
    SELECT qc.job_id, qc.staging_row_id, 'invalid bp', qc.value_num::TEXT
    FROM staging.quality_control qc
    WHERE job_id = :j AND qc.is_bp AND (qc.systolic IS NULL OR qc.diastolic IS NULL);
    """, {"j": job_id}
  )
  
def upsert_rejected_unit(job_id: str) -> None:
  engine_execute(
    """
    INSERT INTO staging.rejections(job_id, staging_row_id, reason_code, detail)
    SELECT job_id, staging_row_id, 'unknown measurement type', measurement_type
    FROM staging.quality_control
    WHERE job_id = :j
      AND measurement_type NOT IN ('glucose','cholesterol','weight','height','blood_pressure', 'heart_rate');
    """, {"j": job_id}
  )
  
def upsert_rejected_quality_score(job_id: str) -> None:
  engine_execute(
    """
    INSERT INTO staging.rejections(job_id, staging_row_id, reason_code, detail)
    SELECT job_id, staging_row_id, 'malformed quality score', quality_score
    FROM staging.quality_control
    WHERE job_id = :j
      AND quality_score IS NULL
      OR (quality_score > 1 OR quality_score < 0);
    """, {"j": job_id}
  )
  
def upsert_rejected(job_id: str) -> None:
  upsert_rejected_scalar(job_id)
  upsert_rejected_bp(job_id)
  upsert_rejected_unit(job_id)
  upsert_rejected_quality_score(job_id)

def upsert_scalar_facts(job_id: str) -> None:
  engine_execute(
      """
      INSERT INTO facts.scalar_facts (
          study_id, 
          site_id, 
          participant_id, 
          measurement_type_id, 
          measured_at, 
          value_num, 
          unit_id, 
          quality_score)
      SELECT 
          dstudy.id,
          dsites.id,
          dparts.id,
          dm.id,
          qc.measured_at,
          qc.value_num::numeric(4,1),
          dm.unit,
          qc.quality_score::numeric(3,2)
      FROM staging.quality_control qc
          JOIN dims.studies dstudy ON dstudy.study_name = qc.study_id
          JOIN dims.sites dsites 
              ON dsites.study_id = dstudy.id 
              AND dsites.site_name = qc.site_id
          JOIN dims.participants dparts 
              ON dparts.study_id = dstudy.id
              AND dparts.site_id = dsites.id
              AND dparts.participant_name = qc.participant_id
          JOIN dims.measurement_types dm ON dm.measurement = qc.measurement_type
      WHERE qc.job_id=:j
          AND dm.measurement <> 'blood_pressure' -- all but blood pressure for measurement types
          AND qc.measurement_type <> 'blood_pressure' -- keep only numeric values
          AND qc.value_num IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM staging.rejections r
            WHERE r.job_id = qc.job_id
            AND r.staging_row_id = qc.staging_row_id
          )
      ON CONFLICT (study_id, site_id, participant_id, measured_at, measurement_type_id) DO NOTHING;""",
    {"j": job_id}
  )
   
def upsert_bp_facts(job_id: str) -> None:
  engine_execute(
    """
    INSERT INTO facts.bp_facts(
    study_id,
    site_id,
    participant_id,
    measured_at,
    systolic,
    diastolic,
    quality_score)
    SELECT
        ds.id,
        dsi.id,
        dp.id,
        qc.measured_at,
        qc.systolic,
        qc.diastolic,
        qc.quality_score::numeric(3,2)
    FROM staging.quality_control qc
        JOIN dims.studies ds ON ds.study_name = qc.study_id
        JOIN dims.sites dsi ON dsi.study_id = ds.id
            AND dsi.site_name = qc.site_id
        JOIN dims.participants dp ON dp.study_id = ds.id
            AND dp.site_id = dsi.id
            AND dp.participant_name = qc.participant_id
        JOIN dims.measurement_types dm ON dm.measurement = qc.measurement_type
    WHERE qc.job_id=:j
        AND dm.measurement = 'blood_pressure'
        AND qc.measurement_type = 'blood_pressure'
        AND qc.systolic IS NOT NULL AND qc.diastolic IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM staging.rejections r
          WHERE r.job_id = qc.job_id
          AND r.staging_row_id = qc.staging_row_id
        )
    ON CONFLICT (study_id, site_id, participant_id, measured_at) DO NOTHING;
    """, {"j": job_id}
)
