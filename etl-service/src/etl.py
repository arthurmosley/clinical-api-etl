from typing import Any, Dict
import pandas as pd
from sqlalchemy import text
from db import engine, complete_job, fail_job
from state import set_progress, jobs
import logging

# set of required columns from csv data source.
REQUIRED = {"study_id", "participant_id", "measurement_type", "value", "unit", "timestamp","site_id"}

log = logging.getLogger("etl")
logging.basicConfig(level=logging.INFO)

def logging_wrapper(job_id: str, prog: int, msg: str):
    set_progress(job_id, prog, msg)
    log.info("job=%s progress=%d message=%s", job_id, prog, msg)

def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def validate_schema(df: pd.DataFrame) -> None:
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")
    
def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    # create a copy to not impact original dataframe being passed in.
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True).dt.tz_convert(None)
    # cleaning non-valid timestamp entries from dataset.
    out = out.dropna(subset=["timestamp"])
    for c in ["study_id","participant_id","measurement_type","unit","site_id","value"]:
        out[c] = out[c].astype(str).str.strip()
    out["measurement_type"] = out["measurement_type"].str.lower()
    return out

def apply_quality_rules(df: pd.DataFrame) -> pd.DataFrame:
    if "quality_score" in df.columns:
        df = df.assign(quality_score=pd.to_numeric(df["quality_score"], errors="coerce"))
        # we know we have all numeric values now or we have flagged them as not a number
        # next we filter out what we don't want.
        df = df[(df["quality_score"].isna()) | ((df["quality_score"]>=0) & (df["quality_score"]<=1))]
    # TODO: What do I want to do about duplicates? I'm not sure yet. subset=["1", "2"...]
    return df.drop_duplicates()

def load_rows(df: pd.DataFrame) -> int:
    COLUMNS = ["study_id", "participant_id", "measurement_type", "value", "unit", "timestamp","site_id", "quality_score"]
    rows = df.reindex(columns=COLUMNS).to_dict(orient="records")
    if not rows: return 0
    sql = text("""
      INSERT INTO clinical_measurements
        (study_id, participant_id, measurement_type, value, unit, timestamp, site_id, quality_score)
      VALUES
        (:study_id, :participant_id, :measurement_type, :value, :unit, :timestamp, :site_id, :quality_score)
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)

def process_job(job_id: str, path: str) -> None:
    try:
        logging_wrapper(job_id, 10, "reading csv")                                                                                                                                
        df = read_csv(path)

        logging_wrapper(job_id, 25, "validating schema")            
        validate_schema(df)

        logging_wrapper(job_id, 40, "typing + cleaning")
        df = normalize_types(df)

        logging_wrapper(job_id, 55, "applying quality rules")
        df = apply_quality_rules(df)

        logging_wrapper(job_id, 70, "loading to db")
        inserted = load_rows(df)

        logging_wrapper(job_id, 90, f"finalizing ({inserted} rows)")
        complete_job(job_id)
        logging_wrapper(job_id, 100, "completed")
        jobs.pop(job_id, None)
    except Exception as e:        
        fail_job(job_id, str(e))
        logging_wrapper(job_id, 100, f"failed: {e}")