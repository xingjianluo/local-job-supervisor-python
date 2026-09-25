"""HTTP-level tests: root endpoint unchanged, job API end to end."""

import os
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("JOB_STATE_DIR", str(tmp_path))
    # main reads JOB_STATE_DIR at import time; reload to pick up the temp dir.
    import importlib

    import main

    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def test_root_endpoint_unchanged(client):
    assert client.get("/").json() == {"Hello": "World"}


def test_submit_poll_and_succeed(client):
    r = client.post("/jobs", json={"duration_ms": 100, "memory_mb": 16, "procs": 2})
    assert r.status_code == 201, r.text
    job = r.json()
    assert job["status"] in ("queued", "running")
    for _ in range(200):
        job = client.get(f"/jobs/{job['job_id']}").json()
        if job["status"] == "succeeded":
            break
        time.sleep(0.05)
    assert job["status"] == "succeeded" and job["reason"] == "completed"


def test_reject_reasons_are_distinguishable(client):
    r = client.post("/jobs", json={"duration_ms": -1, "memory_mb": 16})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "invalid_params"
    r = client.post("/jobs", json={"duration_ms": 10, "memory_mb": 10**9})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "memory_exceeds_limit"
    r = client.post("/jobs", json={"duration_ms": 10, "memory_mb": 16, "procs": 99})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "process_count_exceeds_limit"


def test_cancel_unknown_job_is_404(client):
    assert client.post("/jobs/no-such-job/cancel").status_code == 404
    assert client.get("/jobs/no-such-job").status_code == 404


def test_cancel_running_job_via_api(client):
    r = client.post("/jobs", json={"duration_ms": 30000, "memory_mb": 16})
    job_id = r.json()["job_id"]
    for _ in range(200):
        if client.get(f"/jobs/{job_id}").json()["status"] == "running":
            break
        time.sleep(0.05)
    first = client.post(f"/jobs/{job_id}/cancel").json()
    second = client.post(f"/jobs/{job_id}/cancel").json()
    assert first["status"] == second["status"] == "cancelled"
    assert first["reason"] == "cancelled"
    assert first["finished_at"] == second["finished_at"]
