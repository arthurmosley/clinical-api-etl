from decimal import Decimal, InvalidOperation
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import pandas as pd
from sqlalchemy import text
from db import (mark_status,
                insert_staging_rows, 
                upsert_dims, 
                insert_processed_rows, 
                insert_quality_counts,
                upsert_aggregation_rows)
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
    # Maybe change how missing is calculated
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

def build_processed_rows(job_id: str, staged_df: pd.DataFrame) -> List[Dict]:
    """
    Transform staged rows to processed rows. One row per observation.
    Blood pressure 'S/D' becomes two observations.
    """
    processed: List[Dict] = []
    for r in staged_df.itertuples(index=False):
        study = r.study_id
        pid   = r.participant_id
        site  = r.site_id
        mtype = r.measurement_type
        ts    = r.timestamp
        unit  = r.unit if r.unit != "" else None
        q     = None if r.quality_score in ("", "null") else float(r.quality_score)
        raw_id = r.raw_id

        n = to_decimal(r.value)
        if mtype == "blood_pressure":
            sys, dia = parse_bp(r.value)
            if sys is not None and dia is not None:
                processed.append(dict(
                    study_id=study, participant_id=pid, site_id=site,
                    measurement_type="blood_pressure_systolic", measured_at=ts,
                    value_num=Decimal(sys), value_text=None, unit="mmHg",
                    quality_score=q, raw_row_id=raw_id, job_id=job_id
                ))
                processed.append(dict(
                    study_id=study, participant_id=pid, site_id=site,
                    measurement_type="blood_pressure_diastolic", measured_at=ts,
                    value_num=Decimal(dia), value_text=None, unit="mmHg",
                    quality_score=q, raw_row_id=raw_id, job_id=job_id
                ))
                continue
        if n is not None:
            processed.append(dict(
                study_id=study, participant_id=pid, site_id=site,
                measurement_type=mtype, measured_at=ts,
                value_num=n, value_text=None, unit=unit,
                quality_score=q, raw_row_id=raw_id, job_id=job_id
            ))
        else:
            processed.append(dict(
                study_id=study, participant_id=pid, site_id=site,
                measurement_type=mtype, measured_at=ts,
                value_num=None, value_text=r.value, unit=unit,
                quality_score=q, raw_row_id=raw_id, job_id=job_id
            ))
    return processed

def insert_processed(processed: List[Dict]) -> None:
    insert_processed_rows(processed)

def compute_quality_counts(job_id: str, df: pd.DataFrame) -> List[Dict]:
    """
    - Missing unit for required types
    - Malformed blood pressure
    - Numeric out of range (per RANGES)
    """
    out: List[Dict] = []

    # Missing unit
    miss = df[df["measurement_type"].isin(list(REQ_UNIT)) & (df["unit"].str.strip() == "")]
    if not miss.empty:
        out.append(dict(job_id=job_id, rule_name="missing_unit_required", severity="warn",
                        affected_rows=int(miss.shape[0])))

    # incorrect BP
    is_bp = df["measurement_type"].eq("blood_pressure")
    bad_bp = 0
    # checking for bad blood pressure values. Basic check.
    for v in df.loc[is_bp, "value"]:
        s, d = parse_bp(v)
        if s is None or d is None:
            bad_bp += 1
    if bad_bp:
        out.append(dict(job_id=job_id, rule_name="malformed_blood_pressure", severity="error",
                        affected_rows=bad_bp))

    # Numeric out of range
    df_num = df.copy()
    # creating new column in the dataframe to ultimately store in the DB that is numeric.
    df_num["value_num"] = [to_decimal(x) for x in df_num["value"]]
    num_oor = 0
    for mtype, (low, high) in RANGES.items():
        sel = df_num[(df_num["measurement_type"] == mtype) & df_num["value_num"].notna()]
        if sel.empty:
            continue
        vals = sel["value_num"].astype(float)
        num_oor += int(((vals < float(low)) | (vals > float(high))).sum())
    if num_oor:
        out.append(dict(job_id=job_id, rule_name="numeric_out_of_range", severity="warn",
                        affected_rows=num_oor))
    return out

def insert_quality(rows: List[Dict]) -> None:
    insert_quality_counts(rows)

def build_aggs_from_processed(job_id: str, processed: List[Dict]) -> List[Dict]:
    """
    Rollup for numeric processed rows using pandas groupby.
    """
    if not processed:
        return []
    fdf = pd.DataFrame(processed)
    fdf_num = fdf[fdf["value_num"].notna()].copy()
    if fdf_num.empty:
        return []
    fdf_num["day"] = pd.to_datetime(fdf_num["measured_at"], utc=True).dt.date
    g = fdf_num.groupby(["study_id","participant_id","site_id","measurement_type"], dropna=False)
    aggs = g["value_num"].agg(cnt="count", avg_num="mean", min_num="min", max_num="max").reset_index()
    out = [
        dict(
            study_id=row.study_id,
            participant_id=row.participant_id,
            site_id=row.site_id,
            measurement_type=row.measurement_type,
            cnt=int(row.cnt),
            avg_num=float(row.avg_num),
            min_num=float(row.min_num),
            max_num=float(row.max_num),
            job_id=job_id,
        )
        for row in aggs.itertuples(index=False)
    ]
    return out

def upsert_aggs(rows: List[Dict]) -> None:
    upsert_aggregation_rows(rows)

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
        processed = build_processed_rows(job_id, staged_df)
        insert_processed(processed)

        mark_status(job_id, "running", "quality checks")
        set_progress(job_id, 75, "quality checks")
        qc = compute_quality_counts(job_id, df)
        insert_quality(qc)

        mark_status(job_id, "running", "aggregations")
        set_progress(job_id, 90, "aggregations")
        aggs = build_aggs_from_processed(job_id, processed)
        upsert_aggs(aggs)

        mark_status(job_id, "completed", None)
        set_progress(job_id, 100, "completed", "completed")
    except Exception as e:
        mark_status(job_id, "failed", str(e))
        set_progress(job_id, 100, f"failed: {e}")
        