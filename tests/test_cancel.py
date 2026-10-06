"""Strong-cancellation semantics tests.

Scenarios (each logs a SCENARIO/PASS banner so a run with
``pytest -o log_cli=true -o log_cli_level=INFO`` shows coverage and the
asserted values):

1. cancel while queued                 -> not_required, never executes
2. cancel at the RUNNING dispatch edge -> teardown within grace, no leak
3. cancel mid-run                      -> graceful result + timing fields
4. repeated/concurrent cancels         -> one teardown, identical result
5. escalation to force-kill            -> outcome=forced, procs_forced>0
6. anomalous teardown                  -> outcome=cleanup_failed, distinct
7. quota returned + queue order        -> followers scheduled, no breach
8. cancel in-flight keeps capacity     -> limits not breached mid-teardown
9. auditable result survives restart   -> persisted cancel_result
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
from pathlib import Path

from app.models import (
    CancelOutcome,
    JobSpec,
    JobStatus,
    Limits,
    TerminationReason,
)
from app.store import JobStore
from app.supervisor import JobSupervisor

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("cancel-tests")

# Tight, fast timing: 120 ms grace window, 250 ms post-force wait.
FAST = Limits(
    max_concurrent=2,
    max_memory_mb=64,
    max_procs_per_job=4,
    max_queue_size=8,
    cancel_grace_ms=120,
    cancel_force_wait_ms=250,
)


def run(coro):
    return asyncio.run(coro)


def make_supervisor(tmp_path: Path, limits: Limits = FAST) -> JobSupervisor:
    return JobSupervisor(JobStore(tmp_path / "jobs.json"), limits)


def scenario(title: str) -> None:
    log.info("==== SCENARIO: %s ====", title)


def passed(title: str, **facts: object) -> None:
    log.info("PASS [%s] asserted=%s", title, facts)


async def wait_status(sup: JobSupervisor, job_id: str, status: JobStatus) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        job = sup.get(job_id)
        if job.status == status:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"job {job_id} never reached {status}: {sup.get(job_id)}")


async def wait_running(sup: JobSupervisor, job_id: str) -> None:
    await wait_status(sup, job_id, JobStatus.RUNNING)


async def wait_children_ready(
    sup: JobSupervisor, job_id: str, count: int, dwell: float = 0.15
) -> None:
    """RUNNING is committed before children are spawned; wait until the
    derived processes are registered and their SIGTERM handler is installed.
    """
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        procs = sup._children.get(job_id)
        if procs is not None and len(procs) == count:
            break
        await asyncio.sleep(0.005)
    await asyncio.sleep(dwell)


def child_process_count() -> int:
    out = subprocess.run(
        ["ps", "--ppid", str(os.getpid()), "-o", "comm="],
        capture_output=True, text=True, check=True,
    ).stdout
    return sum(1 for line in out.splitlines() if "python" in line)


# --------------------------------------------------------------------- #
# 1. cancel while queued
# --------------------------------------------------------------------- #

def test_cancel_queued_is_immediate_and_auditable(tmp_path):
    title = "queued cancel: not_required, never runs, repeated cancel same result"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(
            tmp_path, Limits(max_concurrent=1, max_memory_mb=64,
                             cancel_grace_ms=120, cancel_force_wait_ms=250)
        )
        await sup.start()
        blocker = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        queued = await sup.submit(JobSpec(duration_ms=10, memory_mb=8))
        await wait_running(sup, blocker.job_id)

        t0 = time.time()
        cancelled = await sup.cancel(queued.job_id)
        elapsed = time.time() - t0
        again = await sup.cancel(queued.job_id)

        cr = cancelled.cancel_result
        assert cancelled.status == JobStatus.CANCELLED
        assert cancelled.reason == TerminationReason.CANCELLED
        assert cr is not None
        assert cr.outcome == CancelOutcome.NOT_REQUIRED
        assert cr.procs_total == 0
        assert cr.procs_remaining == 0
        assert cr.requested_at <= cr.completed_at
        assert 0 <= elapsed < 0.5  # queued cancel finishes immediately
        assert again.cancel_result == cr  # identical auditable result
        await asyncio.sleep(0.15)
        assert sup.get(queued.job_id).status == JobStatus.CANCELLED
        assert sup._running_count == 1  # blocker untouched
        passed(
            title, status="cancelled", outcome=cr.outcome.value,
            duration_ms=cr.duration_ms, procs_total=cr.procs_total,
        )
        await sup.cancel(blocker.job_id)
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 2. cancel at the exact QUEUED -> RUNNING edge
# --------------------------------------------------------------------- #

def test_cancel_at_running_start_edge(tmp_path):
    title = "cancel right at dispatch: bounded teardown, no process leaked"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=3))
        await wait_running(sup, job.job_id)  # cancel as early as possible
        result_job = await sup.cancel(job.job_id)

        cr = result_job.cancel_result
        assert result_job.status == JobStatus.CANCELLED
        assert cr is not None
        assert cr.outcome in (CancelOutcome.GRACEFUL, CancelOutcome.NOT_REQUIRED)
        assert cr.procs_remaining == 0
        if cr.procs_total > 0:
            assert cr.procs_graceful == cr.procs_total
        assert cr.duration_ms <= FAST.cancel_grace_ms + 500
        await asyncio.sleep(0.15)
        assert child_process_count() == 0
        assert sup._running_count == 0 and sup._running_memory_mb == 0
        passed(
            title, outcome=cr.outcome.value, procs_total=cr.procs_total,
            graceful=cr.procs_graceful, duration_ms=cr.duration_ms,
        )
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 3. cancel mid-run, graceful self-exit
# --------------------------------------------------------------------- #

def test_cancel_mid_run_graceful(tmp_path):
    title = "mid-run cancel: graceful, timings and process counts recorded"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(tmp_path)
        await sup.start()
        before = time.time()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_running(sup, job.job_id)
        await asyncio.sleep(0.2)  # definitely mid-run
        live = child_process_count()
        assert live >= 2

        result_job = await sup.cancel(job.job_id)
        cr = result_job.cancel_result
        assert result_job.status == JobStatus.CANCELLED
        assert result_job.reason == TerminationReason.CANCELLED
        assert result_job.finished_at is not None
        assert cr.outcome == CancelOutcome.GRACEFUL
        assert cr.procs_total == 2
        assert cr.procs_graceful == 2
        assert cr.procs_forced == 0
        assert cr.procs_remaining == 0
        assert cr.grace_ms == FAST.cancel_grace_ms
        assert before <= cr.requested_at <= time.time()
        assert cr.requested_at <= cr.completed_at
        assert cr.duration_ms < FAST.cancel_grace_ms  # died well before deadline
        assert abs((cr.completed_at - cr.requested_at) * 1000 - cr.duration_ms) < 50
        await asyncio.sleep(0.15)
        assert child_process_count() == 0
        assert sup._running_count == 0
        passed(
            title, outcome=cr.outcome.value, total=cr.procs_total,
            graceful=cr.procs_graceful, duration_ms=cr.duration_ms,
            requested_at=cr.requested_at,
        )
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 4. repeated and concurrent cancellation -> one result
# --------------------------------------------------------------------- #

def test_concurrent_cancels_share_single_result(tmp_path):
    title = "repeated/concurrent cancels: teardown once, same final result"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_running(sup, job.job_id)

        results = await asyncio.gather(*(sup.cancel(job.job_id) for _ in range(5)))
        after = await sup.cancel(job.job_id)
        serialized = [j.cancel_result.to_dict() for j in results]

        assert all(j.status == JobStatus.CANCELLED for j in results)
        assert all(d == serialized[0] for d in serialized)
        assert after.cancel_result.to_dict() == serialized[0]
        cr = results[0].cancel_result
        assert cr.procs_remaining == 0
        assert cr.procs_graceful + cr.procs_forced == cr.procs_total
        await asyncio.sleep(0.15)
        assert child_process_count() == 0
        passed(
            title, callers=6, outcome=cr.outcome.value,
            completed_at=cr.completed_at, procs_total=cr.procs_total,
        )
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 5. escalation to force-kill
# --------------------------------------------------------------------- #

def test_cancel_escalates_to_force_kill(tmp_path):
    title = "SIGTERM ignored: grace expires then force-kill, outcome=forced"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(
            JobSpec(duration_ms=30000, memory_mb=8, procs=2, ignore_sigterm=True)
        )
        await wait_running(sup, job.job_id)
        await asyncio.sleep(0.15)

        t0 = time.monotonic()
        result_job = await sup.cancel(job.job_id)
        wall = time.monotonic() - t0
        cr = result_job.cancel_result

        assert result_job.status == JobStatus.CANCELLED
        assert result_job.reason == TerminationReason.CANCELLED
        assert cr.outcome == CancelOutcome.FORCED
        assert cr.procs_total == 2
        assert cr.procs_graceful == 0
        assert cr.procs_forced == 2
        assert cr.procs_remaining == 0
        # grace window is honoured before escalation; total is still bounded
        assert wall >= FAST.cancel_grace_ms / 1000.0 - 0.05
        assert wall < (FAST.cancel_grace_ms + FAST.cancel_force_wait_ms) / 1000.0 + 1.0
        await asyncio.sleep(0.15)
        assert child_process_count() == 0
        assert sup._running_count == 0
        passed(
            title, outcome=cr.outcome.value, forced=cr.procs_forced,
            duration_ms=cr.duration_ms, wall_ms=round(wall * 1000),
        )
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 6. anomalous teardown: force-kill cannot remove a process
# --------------------------------------------------------------------- #

class _UnkillableSupervisor(JobSupervisor):
    """Simulates a derived process the platform cannot kill: SIGKILL is
    swallowed, so the cancellation must be reported as cleanup_failed."""

    def _force_stop(self, proc) -> None:  # noqa: ANN001
        return None


def test_cancel_cleanup_failure_is_distinguishable(tmp_path):
    title = "process survives SIGKILL: cleanup_failed, never a normal cancel"
    scenario(title)

    async def scenario_coro():
        sup = _UnkillableSupervisor(
            JobStore(tmp_path / "jobs.json"), FAST
        )
        await sup.start()
        job = await sup.submit(
            JobSpec(duration_ms=30000, memory_mb=8, procs=2, ignore_sigterm=True)
        )
        await wait_running(sup, job.job_id)
        await wait_children_ready(sup, job.job_id, 2)
        leaked = list(sup._children[job.job_id])  # refs survive runner cleanup

        result_job = await sup.cancel(job.job_id)
        cr = result_job.cancel_result

        assert result_job.status == JobStatus.CANCELLED
        assert result_job.reason == TerminationReason.CLEANUP_FAILED
        assert cr.outcome == CancelOutcome.CLEANUP_FAILED
        assert cr.procs_total == 2
        assert cr.procs_remaining == 2
        assert cr.procs_forced == 0
        assert "still alive" in (cr.detail or "")
        # repeated cancel keeps reporting the same anomaly, does not fake OK
        again = await sup.cancel(job.job_id)
        assert again.reason == TerminationReason.CLEANUP_FAILED
        assert again.cancel_result == cr

        # real reaping so the test process never leaks children
        for proc in leaked:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
        await asyncio.sleep(0.15)
        assert child_process_count() == 0
        passed(
            title, outcome=cr.outcome.value, remaining=cr.procs_remaining,
            reason=result_job.reason.value, detail=cr.detail,
        )
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 7 & 8. quota returned after cancel; in-flight cancel keeps limits
# --------------------------------------------------------------------- #

def test_cancel_releases_quota_and_preserves_queue_order(tmp_path):
    title = "quota returned on cancel; queued followers run in order"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(
            tmp_path, Limits(max_concurrent=1, max_memory_mb=64,
                             cancel_grace_ms=120, cancel_force_wait_ms=250)
        )
        await sup.start()
        blocker = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        follower_a = await sup.submit(JobSpec(duration_ms=50, memory_mb=8))
        follower_b = await sup.submit(JobSpec(duration_ms=50, memory_mb=8))
        await wait_running(sup, blocker.job_id)
        assert sup.get(follower_a.job_id).status == JobStatus.QUEUED
        assert sup.get(follower_b.job_id).status == JobStatus.QUEUED

        cancelled_blocker = await sup.cancel(blocker.job_id)
        assert cancelled_blocker.status == JobStatus.CANCELLED
        assert cancelled_blocker.cancel_result.procs_remaining == 0
        # The released slot is handed to the next queued job quickly; the
        # dispatcher may have reused it before we look, so poll briefly.
        started = False
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            count = sup._running_count
            mem = sup._running_memory_mb
            assert 0 <= count <= 1
            assert 0 <= mem <= 8
            if sup.get(follower_a.job_id).status == JobStatus.RUNNING:
                assert sup.get(follower_b.job_id).status == JobStatus.QUEUED
                started = True
                break
            await asyncio.sleep(0.005)
        assert started, "follower A never acquired the released slot"

        # follower A (earlier in queue) finishes, then B runs exactly once
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if sup.get(follower_a.job_id).status == JobStatus.SUCCEEDED:
                break
            await asyncio.sleep(0.02)
        await wait_running(sup, follower_b.job_id)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if sup.get(follower_b.job_id).status == JobStatus.SUCCEEDED:
                break
            await asyncio.sleep(0.02)
        assert sup.get(follower_a.job_id).status == JobStatus.SUCCEEDED
        assert sup.get(follower_b.job_id).status == JobStatus.SUCCEEDED
        assert sup._running_count == 0 and sup._running_memory_mb == 0
        assert child_process_count() == 0
        passed(
            title, order=[follower_a.job_id, follower_b.job_id],
            peak_concurrent=1, final_count=sup._running_count,
        )
        await sup.stop()

    run(scenario_coro())


def test_limits_not_breached_during_in_flight_force_cancel(tmp_path):
    title = "during forced teardown the slot stays reserved; follower waits"
    scenario(title)

    async def scenario_coro():
        sup = make_supervisor(
            tmp_path, Limits(max_concurrent=1, max_memory_mb=64,
                             cancel_grace_ms=400, cancel_force_wait_ms=400)
        )
        await sup.start()
        blocker = await sup.submit(
            JobSpec(duration_ms=30000, memory_mb=32, ignore_sigterm=True)
        )
        follower = await sup.submit(JobSpec(duration_ms=30000, memory_mb=32))
        await wait_running(sup, blocker.job_id)
        await wait_children_ready(sup, blocker.job_id, 1)

        cancel_task = asyncio.create_task(sup.cancel(blocker.job_id))
        await asyncio.sleep(0.15)  # teardown in flight (grace not elapsed)
        assert sup._running_count == 1
        assert sup._running_memory_mb == 32
        assert sup.get(follower.job_id).status == JobStatus.QUEUED

        result_job = await cancel_task
        assert result_job.cancel_result.outcome == CancelOutcome.FORCED
        await wait_running(sup, follower.job_id)  # slot handed over exactly once
        assert sup._running_count == 1 and sup._running_memory_mb == 32
        await sup.cancel(follower.job_id)
        await asyncio.sleep(0.1)
        assert sup._running_count == 0 and sup._running_memory_mb == 0
        assert child_process_count() == 0
        passed(
            title,
            in_flight_count=1, in_flight_memory=32,
            outcome=result_job.cancel_result.outcome.value,
        )
        await sup.stop()

    run(scenario_coro())


# --------------------------------------------------------------------- #
# 9. auditable cancellation survives supervisor restart
# --------------------------------------------------------------------- #

def test_cancel_result_persists_across_restart(tmp_path):
    title = "cancel_result survives restart; queued cancel persists too"
    scenario(title)

    async def first_life():
        single = Limits(
            max_concurrent=1, max_memory_mb=64, max_procs_per_job=4,
            max_queue_size=8, cancel_grace_ms=120, cancel_force_wait_ms=250,
        )
        sup = JobSupervisor(JobStore(tmp_path / "jobs.json"), single)
        await sup.start()
        running = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_running(sup, running.job_id)
        await sup.cancel(running.job_id)
        blocker = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        queued = await sup.submit(JobSpec(duration_ms=10, memory_mb=8))
        await wait_running(sup, blocker.job_id)
        assert sup.get(queued.job_id).status == JobStatus.QUEUED
        await sup.cancel(queued.job_id)
        await sup.cancel(blocker.job_id)
        await sup.stop()
        return running.job_id, queued.job_id

    running_id, queued_id = run(first_life())

    async def second_life():
        sup = make_supervisor(tmp_path)
        await sup.start()
        rj = sup.get(running_id)
        qj = sup.get(queued_id)
        assert rj.status == JobStatus.CANCELLED
        assert rj.cancel_result is not None
        assert rj.cancel_result.outcome == CancelOutcome.GRACEFUL
        assert rj.cancel_result.procs_graceful == 2
        assert qj.status == JobStatus.CANCELLED
        assert qj.cancel_result.outcome == CancelOutcome.NOT_REQUIRED
        # neither is re-executed after recovery
        await asyncio.sleep(0.2)
        assert sup.get(running_id).status == JobStatus.CANCELLED
        assert sup.get(queued_id).status == JobStatus.CANCELLED
        passed(
            title,
            running_outcome=rj.cancel_result.outcome.value,
            queued_outcome=qj.cancel_result.outcome.value,
            requested_at=rj.cancel_result.requested_at,
        )
        await sup.stop()

    run(second_life())
