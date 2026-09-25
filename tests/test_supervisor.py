"""Tests for the local job supervisor.

Each test drives the supervisor through asyncio.run so no external
services, credentials, or network access are required.
"""

import asyncio
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.models import JobSpec, JobStatus, Limits, RejectReason, TerminationReason
from app.store import JobStore
from app.supervisor import JobRejectedError, JobSupervisor

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

TIGHT = Limits(max_concurrent=2, max_memory_mb=64, max_procs_per_job=4, max_queue_size=8)


def make_supervisor(tmp_path: Path, limits: Limits = TIGHT) -> JobSupervisor:
    return JobSupervisor(JobStore(tmp_path / "jobs.json"), limits)


def run(coro):
    return asyncio.run(coro)


async def wait_terminal(sup: JobSupervisor, job_id: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = sup.get(job_id)
        if job.is_terminal:
            return job
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} did not finish: {sup.get(job_id).to_dict()}")


def child_process_count() -> int:
    """Count live python child processes spawned by this test process."""
    out = subprocess.run(
        ["ps", "--ppid", str(os.getpid()), "-o", "comm="],
        capture_output=True, text=True, check=True,
    ).stdout
    return sum(1 for line in out.splitlines() if "python" in line)


# --------------------------------------------------------------------- #
# concurrency limit
# --------------------------------------------------------------------- #

def test_concurrency_limit_never_exceeded(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        peak = 0
        jobs = [await sup.submit(JobSpec(duration_ms=300, memory_mb=8)) for _ in range(6)]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            running = sum(1 for j in sup.list_jobs() if j.status == JobStatus.RUNNING)
            peak = max(peak, running)
            if all(j.is_terminal for j in sup.list_jobs()):
                break
            await asyncio.sleep(0.01)
        assert all(j.status == JobStatus.SUCCEEDED for j in sup.list_jobs())
        await sup.stop()
        return peak

    peak = run(scenario())
    assert 0 < peak <= TIGHT.max_concurrent


def test_memory_budget_never_exceeded(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)  # budget 64 MB, jobs take 32 MB each
        await sup.start()
        peak = 0
        for _ in range(4):
            await sup.submit(JobSpec(duration_ms=300, memory_mb=32))
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            peak = max(peak, sup._running_memory_mb)
            if all(j.is_terminal for j in sup.list_jobs()):
                break
            await asyncio.sleep(0.01)
        await sup.stop()
        return peak

    peak = run(scenario())
    assert 0 < peak <= TIGHT.max_memory_mb


# --------------------------------------------------------------------- #
# rejection with distinguishable reasons
# --------------------------------------------------------------------- #

def test_reject_invalid_params(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        with pytest.raises(JobRejectedError) as exc:
            await sup.submit(JobSpec(duration_ms=0, memory_mb=8))
        assert exc.value.reason == RejectReason.INVALID_PARAMS
        with pytest.raises(JobRejectedError) as exc:
            await sup.submit(JobSpec(duration_ms=10, memory_mb=8, procs=0))
        assert exc.value.reason == RejectReason.INVALID_PARAMS
        await sup.stop()

    run(scenario())


def test_reject_resource_over_limit(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        with pytest.raises(JobRejectedError) as exc:
            await sup.submit(JobSpec(duration_ms=10, memory_mb=TIGHT.max_memory_mb + 1))
        assert exc.value.reason == RejectReason.MEMORY_EXCEEDS_LIMIT
        with pytest.raises(JobRejectedError) as exc:
            await sup.submit(
                JobSpec(duration_ms=10, memory_mb=8, procs=TIGHT.max_procs_per_job + 1)
            )
        assert exc.value.reason == RejectReason.PROCESS_COUNT_EXCEEDS_LIMIT
        # rejected jobs leave no record behind
        assert sup.list_jobs() == []
        await sup.stop()

    run(scenario())


# --------------------------------------------------------------------- #
# cancellation
# --------------------------------------------------------------------- #

def test_duplicate_cancel_is_idempotent_and_releases_children(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        while sup.get(job.job_id).status != JobStatus.RUNNING:
            await asyncio.sleep(0.01)
        before = child_process_count()
        assert before >= 2

        first, second = await asyncio.gather(sup.cancel(job.job_id), sup.cancel(job.job_id))
        third = await sup.cancel(job.job_id)
        assert first.status == second.status == third.status == JobStatus.CANCELLED
        assert first.reason == TerminationReason.CANCELLED
        assert first.finished_at == third.finished_at  # no state rewrite

        await asyncio.sleep(0.2)
        assert child_process_count() == 0  # no leaked derived processes
        assert not sup._children
        await sup.stop()

    run(scenario())


def test_cancel_terminal_and_unknown_jobs_have_no_side_effects(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=50, memory_mb=8))
        await wait_terminal(sup, job.job_id)
        snapshot = sup.get(job.job_id).to_dict()
        again = await sup.cancel(job.job_id)
        assert again.to_dict() == snapshot  # terminal job untouched
        assert await sup.cancel("does-not-exist") is None
        await sup.stop()

    run(scenario())


def test_cancel_queued_job(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path, Limits(max_concurrent=1, max_memory_mb=64))
        await sup.start()
        blocker = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        queued = await sup.submit(JobSpec(duration_ms=10, memory_mb=8))
        while sup.get(blocker.job_id).status != JobStatus.RUNNING:
            await asyncio.sleep(0.01)
        cancelled = await sup.cancel(queued.job_id)
        assert cancelled.status == JobStatus.CANCELLED
        await asyncio.sleep(0.2)
        assert sup.get(queued.job_id).status == JobStatus.CANCELLED  # never runs
        await sup.cancel(blocker.job_id)
        await sup.stop()

    run(scenario())


# --------------------------------------------------------------------- #
# abnormal exit and timeout
# --------------------------------------------------------------------- #

def test_abnormal_exit_recorded_with_reason(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=50, memory_mb=8, exit_code=3))
        done = await wait_terminal(sup, job.job_id)
        assert done.status == JobStatus.FAILED
        assert done.reason == TerminationReason.EXIT_CODE
        assert "3" in done.detail
        await sup.stop()

    run(scenario())


def test_timeout_kills_children_and_records_reason(tmp_path):
    async def scenario():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(
            JobSpec(duration_ms=30000, memory_mb=8, procs=2, timeout_ms=200)
        )
        done = await wait_terminal(sup, job.job_id)
        assert done.status == JobStatus.TIMEOUT
        assert done.reason == TerminationReason.TIMEOUT
        await asyncio.sleep(0.2)
        assert child_process_count() == 0
        await sup.stop()

    run(scenario())


# --------------------------------------------------------------------- #
# restart recovery
# --------------------------------------------------------------------- #

def test_restart_recovery(tmp_path):
    single = Limits(max_concurrent=1, max_memory_mb=64, max_procs_per_job=4)

    async def crash_running_job():
        sup = make_supervisor(tmp_path, single)
        await sup.start()
        done_job = await sup.submit(JobSpec(duration_ms=50, memory_mb=8))
        await wait_terminal(sup, done_job.job_id)
        running = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        queued = await sup.submit(JobSpec(duration_ms=50, memory_mb=8))
        while sup.get(running.job_id).status != JobStatus.RUNNING:
            await asyncio.sleep(0.01)
        assert sup.get(queued.job_id).status == JobStatus.QUEUED
        # simulate a crash: kill children, drop the loop without persisting
        for proc in sup._children.get(running.job_id, []):
            proc.kill()
        sup._dispatcher.cancel()
        return done_job.job_id, running.job_id, queued.job_id

    done_id, running_id, queued_id = run(crash_running_job())

    async def recover():
        sup = make_supervisor(tmp_path, single)
        await sup.start()
        # completed job is recognized, not re-executed
        done = sup.get(done_id)
        assert done.status == JobStatus.SUCCEEDED
        assert done.reason == TerminationReason.COMPLETED
        # interrupted running job gets a traceable terminal state
        interrupted = sup.get(running_id)
        assert interrupted.status == JobStatus.FAILED
        assert interrupted.reason == TerminationReason.SUPERVISOR_RESTART
        # queued job is not lost: it runs after recovery
        finished = await wait_terminal(sup, queued_id)
        assert finished.status == JobStatus.SUCCEEDED
        await sup.stop()

    run(recover())
