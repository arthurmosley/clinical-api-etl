from fastapi.testclient import TestClient
from main import app
from uuid import uuid4

client = TestClient(app)

def test_submit_job():
    job_id = str(uuid4())
    res = client.post("/jobs", json={
        "jobId": job_id,
        "filename": "sample_study001.csv"
    })
    assert res.status_code == 200
    assert res.json()["status"] == "running"

