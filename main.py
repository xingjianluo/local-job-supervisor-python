import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.models import JobSpec, Limits
from app.store import JobStore
from app.supervisor import JobRejectedError, JobSupervisor

logging.basicConfig(level=logging.INFO)

STATE_DIR = Path(os.environ.get("JOB_STATE_DIR", ".job-state"))


def _limits_from_env() -> Limits:
    def int_env(name: str, default: int) -> int:
        raw = os.environ.get(name)
        return int(raw) if raw not in (None, "") else default

    return Limits(
        max_concurrent=int_env("JOB_MAX_CONCURRENT", Limits.max_concurrent),
        max_memory_mb=int_env("JOB_MAX_MEMORY_MB", Limits.max_memory_mb),
        max_procs_per_job=int_env("JOB_MAX_PROCS_PER_JOB", Limits.max_procs_per_job),
        max_queue_size=int_env("JOB_MAX_QUEUE_SIZE", Limits.max_queue_size),
        cancel_grace_period_ms=int_env(
            "JOB_CANCEL_GRACE_PERIOD_MS", Limits.cancel_grace_period_ms
        ),
        cancel_kill_wait_ms=int_env(
            "JOB_CANCEL_KILL_WAIT_MS", Limits.cancel_kill_wait_ms
        ),
    )


class SubmitRequest(BaseModel):
    duration_ms: int = Field(description="simulated run duration per child process")
    memory_mb: int = Field(description="reserved memory accounted against the budget")
    procs: int = Field(default=1, description="number of derived child processes")
    timeout_ms: int | None = Field(default=None)
    exit_code: int = Field(default=0, description="exit code children end with")


@asynccontextmanager
async def lifespan(app: FastAPI):
    supervisor = JobSupervisor(JobStore(STATE_DIR / "jobs.json"), _limits_from_env())
    await supervisor.start()
    app.state.supervisor = supervisor
    yield
    await supervisor.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/jobs", status_code=201)
async def submit_job(req: SubmitRequest, request: Request):
    supervisor: JobSupervisor = request.app.state.supervisor
    spec = JobSpec(
        duration_ms=req.duration_ms,
        memory_mb=req.memory_mb,
        procs=req.procs,
        timeout_ms=req.timeout_ms,
        exit_code=req.exit_code,
    )
    try:
        job = await supervisor.submit(spec)
    except JobRejectedError as exc:
        raise HTTPException(
            status_code=422,
            detail={"reason": exc.reason.value, "detail": exc.detail},
        ) from exc
    return job.to_dict()


@app.get("/jobs")
def list_jobs(request: Request):
    supervisor: JobSupervisor = request.app.state.supervisor
    return [job.to_dict() for job in supervisor.list_jobs()]


@app.get("/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    supervisor: JobSupervisor = request.app.state.supervisor
    job = supervisor.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_dict()


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, request: Request):
    supervisor: JobSupervisor = request.app.state.supervisor
    job = await supervisor.cancel(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_dict()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
