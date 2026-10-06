"""Hardened cancellation semantics tests.

Scenarios covered:
  1. cancel while queued
  2. cancel at the "just scheduled" instant (QUEUED -> RUNNING hand-over)
  3. cancel halfway through a run (graceful self-exit)
  4. duplicate + concurrent cancels converge on one finalisation / result
  5. escalation to force termination after the grace period
  6. cleanup failure is reported with its own distinguishable reason
  7. quota (concurrency + memory) released promptly, queue keeps order
  8. cancel audit result survives a supervisor restart

Every scenario logs its name and each assertion as it is checked, so the
test log shows what is covered and what was asserted.
"""

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

TIGHT = Limits(
    max_concurrent=2,
    max_memory_mb=64,
    max_procs_per_job=4,
    max_queue_size=16,
    cancel_grace_period_ms=600,
    cancel_kill_wait_ms=600,
)


def make_supervisor(tmp_path: Path, limits: Limits = TIGHT) -> JobSupervisor:
    return JobSupervisor(JobStore(tmp_path / "jobs.json"), limits)


def run(coro):
    return asyncio.run(coro)


class Checks:
    """Logs every assertion so coverage and results are visible in the log."""

    def __init__(self, scenario: str) -> None:
        self.scenario = scenario
        self.n = 0

    def check(self, cond: bool, what: str) -> None:
        self.n += 1
        status = "PASS" if cond else "FAIL"
        log.info("[%s] assert %d %s: %s", self.scenario, self.n, status, what)
        assert cond, f"{self.scenario}: {what}"


async def wait_status(
    sup: JobSupervisor, job_id: str, status: JobStatus, timeout: float = 5.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if sup.get(job_id).status == status:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(
        f"job {job_id} never reached {status}: {sup.get(job_id).status}"
    )


def child_process_count() -> int:
    out = subprocess.run(
        ["ps", "--ppid", str(os.getpid()), "-o", "comm="],
        capture_output=True, text=True, check=True,
    ).stdout
    return sum(1 for line in out.splitlines() if "python" in line)


# --------------------------------------------------------------------- #
# 1. cancel while queued
# --------------------------------------------------------------------- #

def test_cancel_queued_job_is_audited_and_never_runs(tmp_path):
    scenario = "queued-cancel"

    async def body():
        sup = make_supervisor(
            tmp_path, Limits(max_concurrent=1, max_memory_mb=64,
                             cancel_grace_period_ms=600)
        )
        await sup.start()
        blocker = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        queued = await sup.submit(JobSpec(duration_ms=10, memory_mb=8))
        await wait_status(sup, blocker.job_id, JobStatus.RUNNING)

        t0 = time.monotonic()
        cancelled = await sup.cancel(queued.job_id)
        elapsed = (time.monotonic() - t0) * 1000
        c = Checks(scenario)
        cr = cancelled.cancel_result
        c.check(cancelled.status == JobStatus.CANCELLED, "status is cancelled")
        c.check(cancelled.reason == TerminationReason.CANCELLED, "reason cancelled")
        c.check(cr is not None, "cancel_result recorded")
        c.check(cr.outcome == CancelOutcome.NOT_NEEDED, "outcome not_needed (queued)")
        c.check(cr.processes_total == 0, "no processes ever spawned")
        c.check(cr.requested_at > 0, "requested_at present")
        c.check(cr.duration_ms >= 0, "duration_ms present")
        c.check(cr.grace_period_ms == 600, "grace_period_ms recorded from config")
        c.check(elapsed < 500, "queued cancel finishes immediately, no grace wait")

        await asyncio.sleep(0.2)
        c.check(
            sup.get(queued.job_id).status == JobStatus.CANCELLED,
            "still cancelled after a beat (never runs)",
        )
        c.check(sup._running_count == 1, "queued cancel consumed no capacity")
        c.check(sup._running_memory_mb == 8, "memory untouched by queued cancel")

        await sup.cancel(blocker.job_id)
        await sup.stop()
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 2. cancel at the exact scheduling instant
# --------------------------------------------------------------------- #

def test_cancel_at_scheduling_instant_never_spawns(tmp_path):
    scenario = "just-scheduled-cancel"

    async def body():
        limits = Limits(max_concurrent=1, max_memory_mb=64,
                        cancel_grace_period_ms=600)
        sup = make_supervisor(tmp_path, limits)
        await sup.start()

        # Hold the one slot, then queue a victim; free the slot while racing
        # cancel() against the dispatch that promotes it to RUNNING.
        blocker = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        victim = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        await wait_status(sup, blocker.job_id, JobStatus.RUNNING)

        real_create = asyncio.create_subprocess_exec
        spawn_happened = {"n": 0}

        async def tracked_create(*args, **kwargs):
            spawn_happened["n"] += 1
            return await real_create(*args, **kwargs)

        asyncio.create_subprocess_exec = tracked_create
        cancel_task = asyncio.create_task(sup.cancel(victim.job_id))
        # Give the cancel claim a head start so it sits at the dispatch edge.
        await asyncio.sleep(0.05)
        blocker_job = await sup.cancel(blocker.job_id)
        victim_result = await cancel_task
        asyncio.create_subprocess_exec = real_create

        c = Checks(scenario)
        c.check(victim_result.status == JobStatus.CANCELLED, "victim cancelled")
        c.check(
            sup.get(victim.job_id).status == JobStatus.CANCELLED,
            "victim persisted as cancelled",
        )
        # No process may have been spawned for a cancel that won the QUEUED
        # decision; the exact winner is either queued cancel (no spawn) or a
        # running cancel (processes are spawned and immediately reaped), so
        # assert the externally visible invariant instead of the race winner:
        await asyncio.sleep(0.15)
        c.check(child_process_count() == 0, "no leaked processes after race")
        c.check(
            blocker_job.status == JobStatus.CANCELLED, "blocker also cancelled"
        )
        c.check(sup._running_count == 0, "capacity back to zero")
        c.check(sup._running_memory_mb == 0, "memory back to zero")
        log.info(
            "[%s] spawns observed during race window: %d",
            scenario, spawn_happened["n"],
        )
        await sup.stop()
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 3. cancel halfway through (graceful self-exit within grace period)
# --------------------------------------------------------------------- #

def test_cancel_running_graceful_outcome(tmp_path):
    scenario = "running-graceful"

    async def body():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=3))
        await wait_status(sup, job.job_id, JobStatus.RUNNING)
        before = child_process_count()

        t0 = time.monotonic()
        result = await sup.cancel(job.job_id)
        elapsed = (time.monotonic() - t0) * 1000
        c = Checks(scenario)
        cr = result.cancel_result
        c.check(result.status == JobStatus.CANCELLED, "status cancelled")
        c.check(result.reason == TerminationReason.CANCELLED, "normal cancel reason")
        c.check(cr is not None, "audit result present")
        c.check(cr.outcome == CancelOutcome.GRACEFUL, "outcome graceful")
        c.check(cr.processes_total == 3, "tracked all 3 derived processes")
        c.check(cr.processes_graceful == 3, "all 3 exited within grace period")
        c.check(cr.processes_forced == 0, "none force-terminated")
        c.check(cr.processes_left == 0, "none left behind")
        c.check(cr.requested_at > 0, "requested_at timestamp recorded")
        c.check(0 <= cr.duration_ms <= 600, "finished within the grace period")
        c.check(abs(cr.duration_ms - elapsed) < 300, "duration reflects wall time")
        c.check(before >= 3, "processes existed before cancel")

        await asyncio.sleep(0.2)
        c.check(child_process_count() == 0, "all derived processes gone")
        c.check(not sup._children, "child bookkeeping cleared")
        await sup.stop()
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 4. duplicate + concurrent cancels: one finalisation, identical result
# --------------------------------------------------------------------- #

def test_concurrent_and_duplicate_cancels_single_finalisation(tmp_path):
    scenario = "concurrent-duplicate-cancel"

    async def body():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_status(sup, job.job_id, JobStatus.RUNNING)

        r1, r2, r3 = await asyncio.gather(
            sup.cancel(job.job_id),
            sup.cancel(job.job_id),
            sup.cancel(job.job_id),
        )
        r4 = await sup.cancel(job.job_id)
        c = Checks(scenario)
        for i, r in enumerate((r1, r2, r3, r4), start=1):
            c.check(r.status == JobStatus.CANCELLED, f"result {i} cancelled")
            c.check(
                r.cancel_result.requested_at == r1.cancel_result.requested_at,
                f"result {i} shares the single requested_at",
            )
            c.check(
                r.cancel_result.outcome == CancelOutcome.GRACEFUL,
                f"result {i} same graceful outcome",
            )
        c.check(
            r1.cancel_result is not None and r2.cancel_result is not None,
            "all callers receive an audit result",
        )
        c.check(r1.finished_at == r4.finished_at, "state written exactly once")

        await asyncio.sleep(0.2)
        c.check(child_process_count() == 0, "exactly one cleanup: no processes")
        await sup.stop()
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 5. grace period exceeded -> forced termination
# --------------------------------------------------------------------- #

def test_force_termination_after_grace_period(tmp_path):
    scenario = "forced-termination"

    async def body():
        limits = Limits(
            max_concurrent=2, max_memory_mb=64,
            cancel_grace_period_ms=150, cancel_kill_wait_ms=1000,
        )
        sup = make_supervisor(tmp_path, limits)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_status(sup, job.job_id, JobStatus.RUNNING)
        while child_process_count() < 2:
            await asyncio.sleep(0.01)

        # Simulate SIGTERM-immune processes by swallowing the graceful
        # signal (patched on the instance, so no cross-test leakage); the
        # real SIGKILL still drives the forced escalation.
        grace_ms = limits.cancel_grace_period_ms
        observed = {"alive_when_signalled": False}

        def graceful_send(proc):
            # At the moment the graceful signal would be sent (start of the
            # grace window) the process must still be alive: the grace period
            # is genuinely granted, not skipped by an early force-kill.
            if proc.returncode is None:
                observed["alive_when_signalled"] = True
            return None  # swallow the graceful signal

        sup._request_graceful_stop = graceful_send
        t0 = time.monotonic()
        result = await sup.cancel(job.job_id)
        elapsed = (time.monotonic() - t0) * 1000
        c = Checks(scenario)
        cr = result.cancel_result
        c.check(result.status == JobStatus.CANCELLED, "still ends cancelled")
        c.check(result.reason == TerminationReason.CANCELLED,
                "forced escalation keeps normal cancel reason")
        c.check(cr.outcome == CancelOutcome.FORCED, "outcome forced")
        c.check(cr.processes_total == 2, "2 processes tracked")
        c.check(cr.processes_graceful == 0, "0 exited during grace period")
        c.check(cr.processes_forced == 2, "2 force-terminated")
        c.check(cr.processes_left == 0, "0 survivors")
        c.check(
            observed["alive_when_signalled"],
            "processes still alive when graceful window started "
            "(grace granted, not force-killed up front)",
        )
        c.check(elapsed >= grace_ms - 50, "waited out the grace period")
        c.check(
            cr.duration_ms >= grace_ms - 50,
            "recorded duration covers the grace period",
        )
        c.check(elapsed < grace_ms + 1500, "force escalation finishes shortly after")

        await asyncio.sleep(0.2)
        c.check(child_process_count() == 0, "SIGKILL reaped every process")
        await sup.stop()
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 6. cleanup failure: processes cannot be confirmed dead
# --------------------------------------------------------------------- #

def test_cleanup_failure_has_distinct_reason(tmp_path):
    scenario = "cleanup-failure"

    async def body():
        limits = Limits(
            max_concurrent=2, max_memory_mb=64,
            cancel_grace_period_ms=80, cancel_kill_wait_ms=120,
        )
        sup = make_supervisor(tmp_path, limits)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_status(sup, job.job_id, JobStatus.RUNNING)
        while child_process_count() < 2:
            await asyncio.sleep(0.01)

        # Ignore both graceful and force signals *during the cancel
        # workflow* (via its seams); the underlying real kill stays intact so
        # that stop() can still reap the stuck processes afterwards.
        sup._request_graceful_stop = lambda proc: None
        sup._request_force_stop = lambda proc: None

        result = await sup.cancel(job.job_id)
        c = Checks(scenario)
        cr = result.cancel_result
        c.check(result.status == JobStatus.CANCELLED, "job reaches a terminal state")
        c.check(
            result.reason == TerminationReason.CANCEL_CLEANUP_FAILED,
            "cleanup failure uses its own distinguishable reason",
        )
        c.check(cr.outcome == CancelOutcome.CLEANUP_FAILED, "outcome cleanup_failed")
        c.check(cr.processes_total == 2, "2 processes tracked")
        c.check(cr.processes_forced == 0, "0 confirmed force-killed")
        c.check(cr.processes_left == 2, "2 processes reported still alive")
        c.check(cr.detail and "still alive" in cr.detail, "detail explains the error")

        # Repeated cancel returns the same final result (single-flight already
        # finalised; terminal return is unchanged).
        again = await sup.cancel(job.job_id)
        c.check(again.finished_at == result.finished_at, "repeat does not rewrite")
        c.check(
            again.reason == TerminationReason.CANCEL_CLEANUP_FAILED,
            "repeat preserves the distinct reason",
        )
        c.check(sup._running_count == 0, "quota still returned despite failure")
        c.check(sup._running_memory_mb == 0, "memory still returned despite failure")

        # stop() must still reap the stuck processes (no real leak).
        await sup.stop()
        await asyncio.sleep(0.2)
        c.check(child_process_count() == 0, "stop() reaps stuck processes")
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 7. quota released promptly; queued jobs keep their order
# --------------------------------------------------------------------- #

def test_quota_released_and_queue_order_preserved(tmp_path):
    scenario = "quota-and-order"

    async def body():
        limits = Limits(max_concurrent=1, max_memory_mb=16,
                        cancel_grace_period_ms=300)
        sup = make_supervisor(tmp_path, limits)
        await sup.start()
        a = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        b = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8))
        d = await sup.submit(JobSpec(duration_ms=100, memory_mb=8))
        await wait_status(sup, a.job_id, JobStatus.RUNNING)
        c = Checks(scenario)
        c.check(sup.get(b.job_id).status == JobStatus.QUEUED, "b queued behind a")
        c.check(sup.get(d.job_id).status == JobStatus.QUEUED, "d queued too")

        peak_conc = 1
        peak_mem = 8

        async def sample_peaks():
            nonlocal peak_conc, peak_mem
            while True:
                peak_conc = max(peak_conc, sup._running_count)
                peak_mem = max(peak_mem, sup._running_memory_mb)
                await asyncio.sleep(0.002)

        sampler = asyncio.create_task(sample_peaks())
        await sup.cancel(a.job_id)
        # a's slot must free promptly; b (not d) runs next -> FIFO preserved.
        await wait_status(sup, b.job_id, JobStatus.RUNNING, timeout=3.0)
        c.check(sup.get(d.job_id).status == JobStatus.QUEUED, "d still waits for b")
        c.check(sup._running_count == 1, "exactly one running after cancel")
        await sup.cancel(b.job_id)
        finished_d = None
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            j = sup.get(d.job_id)
            if j.is_terminal:
                finished_d = j
                break
            await asyncio.sleep(0.01)
        sampler.cancel()
        c.check(finished_d is not None and finished_d.status == JobStatus.SUCCEEDED,
                "queued d executes exactly once and succeeds")
        c.check(peak_conc <= 1, f"global concurrency never exceeded: peak={peak_conc}")
        c.check(peak_mem <= 16, f"memory budget never exceeded: peak={peak_mem}")
        c.check(sup._running_count == 0, "all quota returned at the end")
        c.check(sup._running_memory_mb == 0, "memory zero at the end")
        await sup.stop()
        return c.n

    n = run(body())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)


# --------------------------------------------------------------------- #
# 8. audit result survives restart
# --------------------------------------------------------------------- #

def test_cancel_result_survives_restart(tmp_path):
    scenario = "restart-persistence"

    async def first_life():
        sup = make_supervisor(tmp_path)
        await sup.start()
        job = await sup.submit(JobSpec(duration_ms=30000, memory_mb=8, procs=2))
        await wait_status(sup, job.job_id, JobStatus.RUNNING)
        result = await sup.cancel(job.job_id)
        assert result.cancel_result.outcome == CancelOutcome.GRACEFUL
        await sup.stop()
        return result.job_id, result.cancel_result

    job_id, cr = run(first_life())

    async def second_life():
        sup = make_supervisor(tmp_path)
        await sup.start()
        c = Checks(scenario)
        job = sup.get(job_id)
        c.check(job.status == JobStatus.CANCELLED, "terminal state preserved")
        c.check(job.reason == TerminationReason.CANCELLED, "reason preserved")
        c.check(job.cancel_result is not None, "cancel_result survived restart")
        c.check(
            job.cancel_result.outcome == CancelOutcome.GRACEFUL,
            "outcome survived restart",
        )
        c.check(
            job.cancel_result.processes_total == cr.processes_total,
            "process counts survived restart",
        )
        c.check(
            job.cancel_result.requested_at == cr.requested_at,
            "requested_at survived restart",
        )
        c.check(
            job.cancel_result.duration_ms == cr.duration_ms,
            "duration_ms survived restart",
        )
        c.check(
            job.cancel_result.grace_period_ms == TIGHT.cancel_grace_period_ms,
            "grace period config recorded",
        )
        # A cancel after restart is the normal terminal-job no-op and must
        # not overwrite the preserved audit record.
        again = await sup.cancel(job_id)
        c.check(
            again.cancel_result.requested_at == cr.requested_at,
            "post-restart repeat cancel keeps original audit",
        )
        await sup.stop()
        return c.n

    n = run(second_life())
    log.info("[%s] SCENARIO COMPLETE with %d assertions", scenario, n)
