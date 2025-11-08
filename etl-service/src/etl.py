from decimal import Decimal, InvalidOperation
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import pandas as pd
from sqlalchemy import text
from db import (mark_status,
                insert_staging_rows, 
                upsert_dims)
from state import set_progress
import logging

# rules
REQ_UNIT = {"glucose","cholesterol","weight","height","blood_pressure"}
RANGES = {
    "glucose": (Decimal("40"), Decimal("400")),
    "cholesterol": (Decimal("50"), Decimal("400")),
    "weight": (Decimal("1"), Decimal("400")),
    "height": (Decimal("30"), Decimal("300")),
    "heart_rate": (Decimal("20"), Decimal("240")),
    "blood_pressure_1": (Decimal("50"), Decimal("250")),
    "blood_pressure_2": (Decimal("30"), Decimal("200"))
}

# set of required columns from csv data source.
REQUIRED = {"study_id", "participant_id", "measurement_type", "value", "unit", "timestamp","site_id"}

def to_decimal(s: Optional[str]) -> Optional[Decimal]:
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None
    
def parse_bp(s: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not s:
        return None, None
    parts = str(s).split("/")
    if len(parts) != 2:
        return None, None
    try:
        p1 = int(parts[0].strip())
        p2 = int(parts[1].strip())
    except ValueError:
        return None, None
    if 50 <= p1 <= 250 and 30 <= p2 <= 200:
        return p1, p2
    return None, None

def read_csv_to_df(csv_path: str) -> pd.DataFrame:
    """
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    if "quality_score" not in df.columns:
        df["quality_score"] = ""
    df["unit"] = df["unit"].astype(str).str.strip()
    if df["study_id"].str.strip().eq("").any():
        raise ValueError("study_id is required for all rows and cannot be blank")        
    return df

def stage_dataframe(job_id: str, filename: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign raw UUIDs + row numbers, insert to staging, return df with raw_id & row_num.
    """
    df = df.copy()
    df["raw_id"] = [str(uuid4()) for _ in range(len(df))]
    df["row_num"] = range(1, len(df) + 1)

    rows: List[Dict] = [
        dict(
            id=r.raw_id,
            job_id=job_id,
            source_filename=filename,
            row_num=int(r.row_num),
            study_id=r.study_id,
            participant_id=r.participant_id,
            measurement_type=r.measurement_type,
            value=r.value,
            unit=(r.unit if r.unit != "" else None),
            timestamp=r.timestamp,
            site_id=r.site_id,
            quality_score=None if r.quality_score in ("", "null") else float(r.quality_score),
        )
        for r in df.itertuples(index=False)
    ]
    insert_staging_rows(rows)
    return df

def upsert_dimensions_for_job(job_id: str) -> None:
    upsert_dims(job_id)

def process_job(job_id: str, csv_path: str) -> None:
    try:
        filename = os.path.basename(csv_path)

        mark_status(job_id, "running", "reading csv")
        set_progress(job_id, 10, "reading csv")
        df = read_csv_to_df(csv_path)

        mark_status(job_id, "running", "staging rows")
        set_progress(job_id, 30, "staging rows")
        staged_df = stage_dataframe(job_id, filename, df)

        mark_status(job_id, "running", "upserting dimensions")
        set_progress(job_id, 45, "upserting dimensions")
        upsert_dimensions_for_job(job_id)

        mark_status(job_id, "running", "processed")
        set_progress(job_id, 65, "building processed")
        #processed = build_processed_rows(job_id, staged_df)
        #insert_processed(processed)

        mark_status(job_id, "running", "quality checks")
        set_progress(job_id, 75, "quality checks")
        #qc = compute_quality_counts(job_id, df)
        #insert_quality(qc)

        mark_status(job_id, "running", "aggregations")
        set_progress(job_id, 90, "aggregations")
        #aggs = build_aggs_from_processed(job_id, processed)
        #upsert_aggs(aggs)

        mark_status(job_id, "completed", None)
        set_progress(job_id, 100, "completed", "completed")
    except Exception as e:
        mark_status(job_id, "failed", str(e))
        set_progress(job_id, 100, f"failed: {e}")
        