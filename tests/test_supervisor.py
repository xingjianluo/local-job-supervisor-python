"""Core supervisor behaviour: limits, cancellation, failures, recovery."""

from __future__ import annotations

import logging
import threading
import time

import pytest

from jobsupervisor.models import (
    JobConstraints,
    JobState,
    JobType,
    RejectReason,
    SubmissionRejected,
    SupervisorConfig,
    TERMINAL_STATES,
)
from jobsupervisor.supervisor import Supervisor
from tests.conftest import wait_for, worker_pids


def _all_terminal(sup: Supervisor) -> bool:
    jobs = sup.list_jobs()
    return bool(jobs) and all(j.state in TERMINAL_STATES for j in jobs)


def test_concurrency_limit_never_exceeded(supervisor, caplog):
    caplog.set_level(logging.INFO, logger="jobsupervisor.supervisor")
    peak = 0
    stop = threading.Event()

    def sampler():
        nonlocal peak
        while not stop.is_set():
            running = sum(1 for j in supervisor.list_jobs() if j.state is JobState.RUNNING)
            peak = max(peak, running)
            time.sleep(0.005)

    t = threading.Thread(target=sampler)
    t.start()
    records = [
        supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=300))
        for _ in range(5)
    ]
    assert wait_for(lambda: _all_terminal(supervisor))
    stop.set()
    t.join()

    assert peak <= 2, f"global concurrency limit violated: peak={peak}"
    assert all(supervisor.get(r.job_id).state is JobState.SUCCEEDED for r in records)
    # audit log carries job id, constraints and transitions
    text = caplog.text
    for r in records:
        assert r.job_id in text
    assert "constraints(duration_ms=300" in text
    assert "transition queued -> running" in text
    assert "transition running -> succeeded" in text


def test_duplicate_and_concurrent_cancel_is_idempotent(supervisor):
    rec = supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=5000))
    assert wait_for(lambda: supervisor.get(rec.job_id).state is JobState.RUNNING)

    threads = [threading.Thread(target=supervisor.cancel, args=(rec.job_id,)) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    again = supervisor.cancel(rec.job_id)  # cancel of terminal job: no-op

    final = supervisor.get(rec.job_id)
    assert final.state is JobState.CANCELLED
    assert final.reason == "cancelled"
    assert again.state is JobState.CANCELLED
    cancellations = [h for h in final.history if h["to"] == JobState.CANCELLED.value]
    assert len(cancellations) == 1, "duplicate cancel must not transition twice"


def test_cancel_queued_job(supervisor):
    blocker = supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=800))
    blocker2 = supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=800))
    queued = supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=100))
    assert supervisor.get(queued.job_id).state is JobState.QUEUED
    supervisor.cancel(queued.job_id)
    assert supervisor.get(queued.job_id).state is JobState.CANCELLED
    supervisor.cancel(blocker.job_id)
    supervisor.cancel(blocker2.job_id)


def test_cancel_nonexistent_job_returns_none(supervisor):
    assert supervisor.cancel("does-not-exist") is None


def test_invalid_params_rejected(supervisor):
    with pytest.raises(SubmissionRejected) as exc:
        supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=0))
    assert exc.value.reason is RejectReason.INVALID_PARAMS
    with pytest.raises(SubmissionRejected) as exc:
        supervisor.submit(JobType.SLEEP, JobConstraints(duration_ms=100, memory_mb=-1))
    assert exc.value.reason is RejectReason.INVALID_PARAMS


def test_per_job_resource_limit_rejected(supervisor):
    with pytest.raises(SubmissionRejected) as exc:
        supervisor.submit(JobType.ALLOC, JobConstraints(duration_ms=100, memory_mb=129))
    assert exc.value.reason is RejectReason.RESOURCE_LIMIT
    with pytest.raises(SubmissionRejected) as exc:
        supervisor.submit(JobType.SPAWN, JobConstraints(duration_ms=100, max_procs=4))
    assert exc.value.reason is RejectReason.RESOURCE_LIMIT


def test_insufficient_total_memory_rejected(supervisor):
    supervisor.submit(JobType.ALLOC, JobConstraints(duration_ms=2000, memory_mb=128))
    supervisor.submit(JobType.ALLOC, JobConstraints(duration_ms=2000, memory_mb=128))
    with pytest.raises(SubmissionRejected) as exc:
        supervisor.submit(JobType.ALLOC, JobConstraints(duration_ms=100, memory_mb=1))
    assert exc.value.reason is RejectReason.INSUFFICIENT_RESOURCES


def test_queue_full_rejected(config):
    config = SupervisorConfig(**{**config.__dict__, "max_queue_size": 1, "max_concurrency": 1})
    sup = Supervisor(config)
    sup.recover()
    try:
        first = sup.submit(JobType.SLEEP, JobConstraints(duration_ms=2000))
        assert wait_for(lambda: sup.get(first.job_id).state is JobState.RUNNING)
        sup.submit(JobType.SLEEP, JobConstraints(duration_ms=100))   # queued (full)
        with pytest.raises(SubmissionRejected) as exc:
            sup.submit(JobType.SLEEP, JobConstraints(duration_ms=100))
        assert exc.value.reason is RejectReason.QUEUE_FULL
    finally:
        sup.shutdown()


def test_abnormal_exit_recorded(supervisor, caplog):
    caplog.set_level(logging.INFO, logger="jobsupervisor.supervisor")
    rec = supervisor.submit(JobType.FAIL, JobConstraints(duration_ms=50))
    assert wait_for(lambda: supervisor.get(rec.job_id).state in TERMINAL_STATES)
    final = supervisor.get(rec.job_id)
    assert final.state is JobState.FAILED
    assert final.reason == "exit_nonzero"
    assert final.exit_code == 3
    assert f"job={rec.job_id}" in caplog.text
    assert "transition running -> failed" in caplog.text


def test_timeout_recorded(supervisor):
    rec = supervisor.submit(JobType.HANG, JobConstraints(duration_ms=150))
    assert wait_for(lambda: supervisor.get(rec.job_id).state in TERMINAL_STATES)
    final = supervisor.get(rec.job_id)
    assert final.state is JobState.TIMEOUT
    assert final.reason == "timeout"


def test_cancel_releases_derived_processes(supervisor):
    rec = supervisor.submit(JobType.SPAWN, JobConstraints(duration_ms=5000, max_procs=3))
    assert wait_for(lambda: supervisor.get(rec.job_id).state is JobState.RUNNING)
    assert wait_for(lambda: len(worker_pids(rec.job_id)) >= 1)
    supervisor.cancel(rec.job_id)
    assert wait_for(lambda: supervisor.get(rec.job_id).state is JobState.CANCELLED)
    assert wait_for(lambda: not worker_pids(rec.job_id)), "derived children leaked"


def test_restart_recovery(config, caplog):
    caplog.set_level(logging.INFO, logger="jobsupervisor.supervisor")
    sup = Supervisor(config)
    sup.recover()
    done = sup.submit(JobType.SLEEP, JobConstraints(duration_ms=100))
    assert wait_for(lambda: sup.get(done.job_id).state is JobState.SUCCEEDED)
    # occupy both concurrency slots so the next job stays queued
    running = sup.submit(JobType.SLEEP, JobConstraints(duration_ms=8000))
    blocker = sup.submit(JobType.SLEEP, JobConstraints(duration_ms=8000))
    assert wait_for(lambda: sup.get(running.job_id).state is JobState.RUNNING)
    assert wait_for(lambda: sup.get(blocker.job_id).state is JobState.RUNNING)
    queued = sup.submit(JobType.SLEEP, JobConstraints(duration_ms=100))
    assert sup.get(queued.job_id).state is JobState.QUEUED
    sup.shutdown()  # simulates service restart: children killed, state kept

    sup2 = Supervisor(config)
    sup2.recover()
    try:
        # completed job is not re-executed
        finished = sup2.get(done.job_id)
        assert finished.state is JobState.SUCCEEDED
        assert finished.attempts == 1
        # interrupted running job is accounted for, not silently lost
        for jid in (running.job_id, blocker.job_id):
            interrupted = sup2.get(jid)
            assert interrupted.state is JobState.FAILED
            assert interrupted.reason == "interrupted_by_restart"
        # queued job resumes and completes
        assert wait_for(lambda: sup2.get(queued.job_id).state is JobState.SUCCEEDED)
        assert "interrupted_by_restart" in caplog.text
    finally:
        sup2.shutdown()
