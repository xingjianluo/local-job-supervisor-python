"""HTTP API behaviour, including that the root endpoint is unchanged."""

from __future__ import annotations

import os
import tempfile

_d = tempfile.mkdtemp(prefix="jobsupervisor-api-test-")
os.environ["JOB_STATE_FILE"] = os.path.join(_d, "job_state.json")
os.environ["JOB_MAX_CONCURRENCY"] = "2"
os.environ["JOB_MAX_MEMORY_MB_PER_JOB"] = "128"
os.environ["JOB_MAX_MEMORY_MB_TOTAL"] = "256"
os.environ["JOB_TIMEOUT_GRACE_MS"] = "400"

import pytest
from fastapi.testclient import TestClient

import main
from tests.conftest import wait_for


@pytest.fixture()
def client():
    with TestClient(main.app) as c:
        yield c


def test_root_endpoint_unchanged(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"Hello": "World"}


def test_submit_get_list_and_success(client):
    resp = client.post("/jobs", json={
        "job_type": "sleep",
        "constraints": {"duration_ms": 100, "memory_mb": 0, "max_procs": 0},
    })
    assert resp.status_code == 201
    job = resp.json()
    assert job["state"] == "queued"
    assert job["constraints"]["duration_ms"] == 100

    assert wait_for(lambda: client.get(f"/jobs/{job['job_id']}").json()["state"] == "succeeded")
    final = client.get(f"/jobs/{job['job_id']}").json()
    assert final["reason"] == "completed"
    assert final["exit_code"] == 0
    transitions = [(h["from"], h["to"]) for h in final["history"]]
    assert transitions == [(None, "queued"), ("queued", "running"), ("running", "succeeded")]

    listing = client.get("/jobs").json()
    assert any(j["job_id"] == job["job_id"] for j in listing)


def test_submit_invalid_params_rejected(client):
    resp = client.post("/jobs", json={
        "job_type": "sleep", "constraints": {"duration_ms": -5},
    })
    assert resp.status_code == 400
    assert resp.json()["detail"]["reason"] == "invalid_params"


def test_submit_resource_limit_rejected(client):
    resp = client.post("/jobs", json={
        "job_type": "alloc", "constraints": {"duration_ms": 100, "memory_mb": 129},
    })
    assert resp.status_code == 400
    assert resp.json()["detail"]["reason"] == "resource_limit"


def test_submit_insufficient_resources_rejected(client):
    for _ in range(2):
        resp = client.post("/jobs", json={
            "job_type": "alloc", "constraints": {"duration_ms": 2000, "memory_mb": 128},
        })
        assert resp.status_code == 201
    resp = client.post("/jobs", json={
        "job_type": "alloc", "constraints": {"duration_ms": 100, "memory_mb": 1},
    })
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "insufficient_resources"
    # cleanup: cancel the two blockers
    for j in client.get("/jobs").json():
        if j["state"] in ("queued", "running"):
            client.post(f"/jobs/{j['job_id']}/cancel")


def test_cancel_flow_and_idempotency(client):
    resp = client.post("/jobs", json={
        "job_type": "sleep", "constraints": {"duration_ms": 5000},
    })
    job_id = resp.json()["job_id"]
    assert wait_for(lambda: client.get(f"/jobs/{job_id}").json()["state"] == "running")

    first = client.post(f"/jobs/{job_id}/cancel")
    assert first.status_code == 200
    assert first.json()["state"] == "cancelled"
    assert first.json()["reason"] == "cancelled"

    second = client.post(f"/jobs/{job_id}/cancel")  # repeat cancel: no side effects
    assert second.status_code == 200
    assert second.json()["state"] == "cancelled"
    history = second.json()["history"]
    assert sum(1 for h in history if h["to"] == "cancelled") == 1


def test_cancel_and_get_nonexistent_job(client):
    assert client.post("/jobs/nope/cancel").status_code == 404
    assert client.get("/jobs/nope").status_code == 404


def test_abnormal_exit_via_api(client):
    resp = client.post("/jobs", json={
        "job_type": "fail", "constraints": {"duration_ms": 50},
    })
    job_id = resp.json()["job_id"]
    assert wait_for(lambda: client.get(f"/jobs/{job_id}").json()["state"] == "failed")
    final = client.get(f"/jobs/{job_id}").json()
    assert final["reason"] == "exit_nonzero"
    assert final["exit_code"] == 3
