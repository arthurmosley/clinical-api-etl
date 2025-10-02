from typing import Dict, Any

# In-memory job storage (for demo purposes)
# In production, this would use a proper database or job queue
jobs: Dict[str, Dict[str, Any]] = {}

def set_progress(job_id: str, prog: int, msg: str) -> None:
    j = jobs.setdefault(job_id, {"status": "running"})
    j["progress"] = prog
    j["message"] = msg