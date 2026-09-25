"""FastAPI entrypoint: exposes the local job supervisor over HTTP.

The root endpoint behaviour is unchanged. All job state lives in a local
JSON file (JOB_STATE_FILE, default ./job_state.json); no external
services, credentials or network access are required.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from jobsupervisor.models import (
    JobConstraints,
    JobType,
    RejectReason,
    SubmissionRejected,
    SupervisorConfig,
)
from jobsupervisor.supervisor import Supervisor


def _config_from_env() -> SupervisorConfig:
    env = os.environ
    return SupervisorConfig(
        max_concurrency=int(env.get("JOB_MAX_CONCURRENCY", "4")),
        max_queue_size=int(env.get("JOB_MAX_QUEUE_SIZE", "16")),
        max_memory_mb_total=int(env.get("JOB_MAX_MEMORY_MB_TOTAL", "512")),
        max_memory_mb_per_job=int(env.get("JOB_MAX_MEMORY_MB_PER_JOB", "256")),
        max_procs_per_job=int(env.get("JOB_MAX_PROCS_PER_JOB", "4")),
        max_duration_ms=int(env.get("JOB_MAX_DURATION_MS", "60000")),
        timeout_grace_ms=int(env.get("JOB_TIMEOUT_GRACE_MS", "1000")),
        state_file=env.get("JOB_STATE_FILE", "job_state.json"),
    )


supervisor = Supervisor(_config_from_env())


@asynccontextmanager
async def lifespan(app: FastAPI):
    supervisor.recover()
    yield
    supervisor.shutdown()


app = FastAPI(lifespan=lifespan)


class ConstraintsIn(BaseModel):
    duration_ms: int = Field(default=100)
    memory_mb: int = Field(default=0)
    max_procs: int = Field(default=0)


class SubmitIn(BaseModel):
    job_type: JobType
    constraints: ConstraintsIn = Field(default_factory=ConstraintsIn)


_REJECT_STATUS = {
    RejectReason.INVALID_PARAMS: 400,
    RejectReason.RESOURCE_LIMIT: 400,
    RejectReason.INSUFFICIENT_RESOURCES: 409,
    RejectReason.QUEUE_FULL: 409,
}


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/jobs", status_code=201)
def submit_job(body: SubmitIn):
    c = body.constraints
    try:
        rec = supervisor.submit(
            body.job_type,
            JobConstraints(
                duration_ms=c.duration_ms,
                memory_mb=c.memory_mb,
                max_procs=c.max_procs,
            ),
        )
    except SubmissionRejected as exc:
        raise HTTPException(
            status_code=_REJECT_STATUS[exc.reason],
            detail={"reason": exc.reason.value, "message": exc.detail},
        )
    return rec.to_dict()


@app.get("/jobs")
def list_jobs():
    return [rec.to_dict() for rec in supervisor.list_jobs()]


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    rec = supervisor.get(job_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="job not found")
    return rec.to_dict()


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    rec = supervisor.cancel(job_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="job not found")
    return rec.to_dict()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
