from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
from db import upsert_job, fetch_job, engine
from etl import process_job
from state import jobs
from pathlib import Path
import uvicorn

app = FastAPI(title="Clinical Data ETL Service", version="1.0.0")
DATA_DIR = Path("/app/data").resolve()

class ETLJobRequest(BaseModel):
    jobId: str
    filename: str
    studyId: Optional[str] = None

class ETLJobResponse(BaseModel):
    jobId: str
    status: str
    message: str

class ETLJobStatus(BaseModel):
    jobId: str
    status: str
    progress: Optional[int] = None
    message: Optional[str] = None

def valid_path(name: str) -> Path:
    p = (DATA_DIR / name).resolve()
    if not p.is_file():
        raise HTTPException(status_code=400, detail="Not a file.")
    return p

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "etl"}

@app.post("/__test__/reset")
async def reset_db():
    with engine.begin() as conn:
        with open("/app/database/dev/reset.sql") as f:
            conn.execute(text(f.read()))
    return {"status":"reset"}

@app.post("/jobs", response_model=ETLJobStatus)
async def submit_job(req: ETLJobRequest, background_tasks: BackgroundTasks):
    """
    Submit a new ETL job for processing
    """
    path = valid_path(req.filename)
    
    upsert_job(req.jobId, req.filename, req.studyId)
    # In memory storage
    jobs[req.jobId] = {
        "jobId": req.jobId,
        "filename": req.filename,
        "studyId": req.studyId,
        "status": "running", 
        "progress": 0, 
        "message": "starting"}
    background_tasks.add_task(process_job, req.jobId, str(path))

    return ETLJobStatus(
        jobId=req.jobId,
        status="running",
        progress=0,
        message="starting")

@app.get("/jobs/{job_id}/status", response_model=ETLJobStatus)
async def get_job_status(job_id: str):
    """
    Get the current status of an ETL job
    """
    if job_id in jobs:
        job = jobs[job_id]
        return ETLJobStatus(
            jobId=job_id,
            status=job["status"],
            progress=job.get("progress"),
            message=job.get("message"))
    row = fetch_job(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return ETLJobStatus(
        jobId=job_id,
        status=row["status"],
        message=row["error_message"])

@app.get("/jobs/{job_id}")
async def get_job_details(job_id: str):
    """
    Get detailed information about an ETL job
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return jobs[job_id]

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
