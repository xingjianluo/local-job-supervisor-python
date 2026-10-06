"""Job supervisor: bounded scheduling, lifecycle, cancellation, recovery.

Cancellation model (hardened):

* A cancel can be requested at any point (queued / just scheduled / halfway
  through). Requests are single-flight per job: concurrent and repeated
  cancels converge on the same finalisation and receive the same result.
* Running jobs first receive a graceful stop signal and get a configurable
  grace period; processes still alive afterwards are force-terminated.
* Every cancellation that finalises a job leaves an auditable
  ``CancelResult`` (outcome, timings, per-stage process counts), persisted
  with the job so it survives duplicate calls and restarts.
* A cleanup that cannot confirm process death is reported separately as
  ``cancel_cleanup_failed`` rather than disguised as a normal cancellation.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time

from .models import (
    ALLOWED_TRANSITIONS,
    CancelOutcome,
    CancelResult,
    Job,
    JobSpec,
    JobStatus,
    Limits,
    RejectReason,
    TerminationReason,
)
from .store import JobStore

logger = logging.getLogger("supervisor")

_CHILD_PROGRAM = (
    "import time,sys;"
    "time.sleep(float(sys.argv[1]));"
    "sys.exit(int(sys.argv[2]))"
)


class JobRejectedError(Exception):
    """Raised when a submission is rejected; carries a distinguishable reason."""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class JobSupervisor:
    def __init__(self, store: JobStore, limits: Limits | None = None) -> None:
        self.store = store
        self.limits = limits or Limits()
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._capacity = asyncio.Condition()
        self._running_count = 0
        self._running_memory_mb = 0
        self._children: dict[str, list[asyncio.subprocess.Process]] = {}
        self._dispatcher: asyncio.Task | None = None
        self._runners: set[asyncio.Task] = set()
        self._job_tasks: dict[str, asyncio.Task] = {}
        # Per-job coordination for cancellation hand-over.
        self._job_locks: dict[str, asyncio.Lock] = {}
        self._proc_ready: dict[str, asyncio.Event] = {}
        # job_id -> future resolving to the final job; existence means
        # "a cancellation owns this job". Single-flight coordination point.
        self._cancelling: dict[str, asyncio.Future[Job]] = {}
        self._cancel_tasks: set[asyncio.Task] = set()
        # Jobs whose scheduler capacity has already been returned.
        self._capacity_released: set[str] = set()
        self._started = False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Load persisted jobs and recover unfinished ones."""
        jobs = self.store.load()
        for job in jobs:
            if job.status == JobStatus.RUNNING:
                # The previous process died; its children are gone with it.
                self._transition(
                    job,
                    JobStatus.FAILED,
                    reason=TerminationReason.SUPERVISOR_RESTART,
                    detail="supervisor restarted while job was running",
                )
            elif job.status == JobStatus.QUEUED:
                # Not lost: re-enqueue so it still runs exactly once.
                self._queue.put_nowait(job.job_id)
        self._dispatcher = asyncio.create_task(self._dispatch_loop())
        self._started = True

    async def stop(self) -> None:
        if self._dispatcher is not None:
            self._dispatcher.cancel()
            self._dispatcher = None
        # Cancel any in-flight cancellation workflows first so they do not
        # fight the direct process cleanup below.
        for task in list(self._cancel_tasks):
            task.cancel()
        if self._cancel_tasks:
            await asyncio.gather(*self._cancel_tasks, return_exceptions=True)
        for task in list(self._runners):
            task.cancel()
        if self._runners:
            await asyncio.gather(*self._runners, return_exceptions=True)
        # Never leak derived child processes (includes stuck ones left by a
        # cleanup_failed cancellation).
        for procs in self._children.values():
            for proc in procs:
                await self._hard_kill(proc)
        self._children.clear()
        self._started = False

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #

    async def submit(self, spec: JobSpec) -> Job:
        self._validate(spec)
        job = Job(job_id=Job.new_id(), spec=spec)
        self.store.save(job)
        logger.info(
            "job=%s submitted spec(duration_ms=%d memory_mb=%d procs=%d timeout_ms=%s)",
            job.job_id, spec.duration_ms, spec.memory_mb, spec.procs, spec.timeout_ms,
        )
        self._queue.put_nowait(job.job_id)
        return job

    async def cancel(self, job_id: str) -> Job | None:
        """Idempotent, single-flight cancellation with an auditable result.

        - unknown job  -> None (caller maps to 404), no side effects
        - terminal job -> returned unchanged, no side effects
        - queued job   -> marked CANCELLED immediately (nothing to stop)
        - running job  -> graceful stop within the configured grace period,
                          force-termination afterwards; the returned job
                          carries a persisted CancelResult. Concurrent and
                          repeated callers join the same finalisation and get
                          the same result.
        """
        job = self.store.get(job_id)
        if job is None:
            return None
        if job.is_terminal:
            return job

        async with self._lock_for(job_id):
            job = self.store.get(job_id)
            if job.is_terminal:
                return job
            fut = self._cancelling.get(job_id)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._cancelling[job_id] = fut
                task = asyncio.create_task(self._cancel_once(job_id, fut))
                self._cancel_tasks.add(task)
                task.add_done_callback(self._cancel_tasks.discard)

        return await fut

    def get(self, job_id: str) -> Job | None:
        return self.store.get(job_id)

    def list_jobs(self) -> list[Job]:
        return self.store.all()

    # ------------------------------------------------------------------ #
    # validation
    # ------------------------------------------------------------------ #

    def _validate(self, spec: JobSpec) -> None:
        if (
            spec.duration_ms <= 0
            or spec.memory_mb <= 0
            or spec.procs < 1
            or (spec.timeout_ms is not None and spec.timeout_ms <= 0)
            or not (0 <= spec.exit_code <= 255)
        ):
            raise JobRejectedError(
                RejectReason.INVALID_PARAMS,
                "duration_ms/memory_mb/procs must be positive, "
                "timeout_ms positive when set, exit_code in [0, 255]",
            )
        if spec.memory_mb > self.limits.max_memory_mb:
            raise JobRejectedError(
                RejectReason.MEMORY_EXCEEDS_LIMIT,
                f"memory_mb={spec.memory_mb} exceeds limit {self.limits.max_memory_mb}",
            )
        if spec.procs > self.limits.max_procs_per_job:
            raise JobRejectedError(
                RejectReason.PROCESS_COUNT_EXCEEDS_LIMIT,
                f"procs={spec.procs} exceeds limit {self.limits.max_procs_per_job}",
            )
        queued = sum(
            1 for j in self.store.all() if j.status == JobStatus.QUEUED
        )
        if queued >= self.limits.max_queue_size:
            raise JobRejectedError(
                RejectReason.QUEUE_FULL,
                f"queue is full ({self.limits.max_queue_size})",
            )

    # ------------------------------------------------------------------ #
    # scheduling
    # ------------------------------------------------------------------ #

    async def _dispatch_loop(self) -> None:
        while True:
            job_id = await self._queue.get()
            job = self.store.get(job_id)
            if job is None or job.status != JobStatus.QUEUED:
                continue  # cancelled while queued, or recovered-then-cancelled
            async with self._capacity:
                await self._capacity.wait_for(self._has_capacity_for(job))
                # State may have changed while waiting for capacity; a job
                # cancelled in the meantime must consume neither a slot nor
                # memory, and must never start running.
                job = self.store.get(job_id)
                if job is None or job.status != JobStatus.QUEUED:
                    self._capacity.notify_all()
                    continue
                self._running_count += 1
                self._running_memory_mb += job.spec.memory_mb
            task = asyncio.create_task(self._run(job))
            self._runners.add(task)
            self._job_tasks[job.job_id] = task
            task.add_done_callback(self._runners.discard)

    def _has_capacity_for(self, job: Job):
        def check() -> bool:
            return (
                self._running_count < self.limits.max_concurrent
                and self._running_memory_mb + job.spec.memory_mb
                <= self.limits.max_memory_mb
            )

        return check

    # ------------------------------------------------------------------ #
    # execution
    # ------------------------------------------------------------------ #

    async def _run(self, job: Job) -> None:
        self._proc_ready.setdefault(job.job_id, asyncio.Event())
        started_running = False
        try:
            async with self._lock_for(job.job_id):
                # Atomic with cancel's claim registration: either the job
                # starts running, or a queued cancel wins and nothing is
                # ever spawned for it.
                if job.status != JobStatus.QUEUED or job.job_id in self._cancelling:
                    return
                self._transition(job, JobStatus.RUNNING)
            started_running = True
            job.started_at = time.time()
            self.store.save(job)
            await self._execute(job)
        finally:
            self._job_tasks.pop(job.job_id, None)
            async with self._lock_for(job.job_id):
                claimed = job.job_id in self._cancelling
                if not claimed:
                    # Natural finish: the runner itself returns the quota and
                    # drops the child bookkeeping.
                    self._children.pop(job.job_id, None)
                    await self._return_capacity(job)
                elif not started_running:
                    # Dispatched, then cancelled while still queued: the
                    # queued-cancel workflow never accounts capacity, so the
                    # slot acquired by the dispatcher is returned here.
                    await self._return_capacity(job)
                # Otherwise a running cancellation owns process cleanup and
                # capacity release; releasing early could breach limits
                # while the old processes are still dying.
            self._proc_ready.pop(job.job_id, None)

    async def _execute(self, job: Job) -> None:
        spec = job.spec
        procs: list[asyncio.subprocess.Process] = []
        # Register the (growing) process list immediately so a cancellation
        # arriving during spawn can see and clean up partial children.
        self._children[job.job_id] = procs
        ready = self._proc_ready.setdefault(job.job_id, asyncio.Event())
        spawn_error: OSError | None = None
        try:
            for _ in range(spec.procs):
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-c", _CHILD_PROGRAM,
                    str(spec.duration_ms / 1000.0), str(spec.exit_code),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                procs.append(proc)
        except OSError as exc:
            spawn_error = exc
        finally:
            # Whether spawn fully succeeded, partially, or failed, the
            # process list is now final: let a waiting cancellation proceed.
            ready.set()

        if spawn_error is not None:
            if await self._try_finish(
                job, JobStatus.FAILED,
                reason=TerminationReason.SPAWN_FAILED, detail=str(spawn_error),
            ):
                for proc in procs:
                    await self._hard_kill(proc)
            # If a cancel won the race it owns the partial children.
            return

        timeout = spec.timeout_ms / 1000.0 if spec.timeout_ms else None
        try:
            await asyncio.wait_for(
                asyncio.gather(*(p.wait() for p in procs)), timeout=timeout
            )
        except (asyncio.TimeoutError, TimeoutError):
            if await self._try_finish(
                job, JobStatus.TIMEOUT, reason=TerminationReason.TIMEOUT
            ):
                for proc in procs:
                    await self._hard_kill(proc)
            # Cancel winner owns the kill when the race was lost.
            return
        except asyncio.CancelledError:
            # Supervisor shutdown: direct process cleanup happens in stop().
            raise
        if not await self._try_finish(
            job,
            JobStatus.FAILED if any(p.returncode != 0 for p in procs)
            else JobStatus.SUCCEEDED,
            reason=TerminationReason.EXIT_CODE
            if any(p.returncode != 0 for p in procs)
            else TerminationReason.COMPLETED,
            detail=(
                f"child exited with code "
                f"{next(p.returncode for p in procs if p.returncode != 0)}"
                if any(p.returncode != 0 for p in procs)
                else None
            ),
        ):
            return  # cancel owns the job; do not rewrite its terminal state

    # ------------------------------------------------------------------ #
    # cancellation workflow
    # ------------------------------------------------------------------ #

    async def _cancel_once(self, job_id: str, fut: asyncio.Future[Job]) -> None:
        """The one task that actually finalises a cancellation for a job."""
        requested_at = time.time()
        started = time.monotonic()
        grace_ms = self.limits.cancel_grace_period_ms
        try:
            job = self.store.get(job_id)
            if job is None or job.is_terminal:
                fut.set_result(job)
                return

            if job.status == JobStatus.QUEUED:
                async with self._lock_for(job_id):
                    job = self.store.get(job_id)
                    if job is None or job.is_terminal:
                        fut.set_result(job)
                        return
                    result = CancelResult(
                        outcome=CancelOutcome.NOT_NEEDED,
                        requested_at=requested_at,
                        duration_ms=self._elapsed_ms(started),
                        grace_period_ms=grace_ms,
                    )
                    job.cancel_result = result
                    self._transition(
                        job, JobStatus.CANCELLED,
                        reason=TerminationReason.CANCELLED,
                    )
                logger.info(
                    "job=%s cancel finalised outcome=%s duration_ms=%d",
                    job_id, result.outcome.value, result.duration_ms,
                )
                fut.set_result(job)
                return

            # RUNNING (including "just scheduled": wait until children are
            # spawned and registered before deciding what to stop).
            ready = self._proc_ready.setdefault(job_id, asyncio.Event())
            await ready.wait()
            procs = list(self._children.get(job_id, []))
            total = len(procs)

            # Stage 1: graceful stop, bounded by the grace period.
            live = await self._graceful_stop(
                procs, self.limits.cancel_grace_period_ms / 1000.0
            )
            graceful = total - len(live)

            # Stage 2: escalate to force termination for survivors.
            forced = 0
            stuck: list[asyncio.subprocess.Process] = []
            if live:
                forced, stuck = await self._force(
                    live, self.limits.cancel_kill_wait_ms / 1000.0
                )

            if stuck:
                outcome = CancelOutcome.CLEANUP_FAILED
                reason = TerminationReason.CANCEL_CLEANUP_FAILED
                detail = (
                    f"{len(stuck)} derived process(es) still alive after "
                    f"grace period and force termination"
                )
            elif live:
                outcome = CancelOutcome.FORCED
                reason = TerminationReason.CANCELLED
                detail = None
            else:
                outcome = CancelOutcome.GRACEFUL
                reason = TerminationReason.CANCELLED
                detail = None

            result = CancelResult(
                outcome=outcome,
                requested_at=requested_at,
                duration_ms=self._elapsed_ms(started),
                processes_total=total,
                processes_graceful=graceful,
                processes_forced=forced,
                processes_left=len(stuck),
                grace_period_ms=grace_ms,
                detail=detail,
            )

            async with self._lock_for(job_id):
                job = self.store.get(job_id)
                if job is None or job.is_terminal:
                    fut.set_result(job)
                    return
                job.cancel_result = result
                self._transition(
                    job, JobStatus.CANCELLED, reason=reason, detail=detail
                )
                # Hand over quota/bookkeeping from the runner. The child
                # list is kept when processes are stuck so stop() can still
                # reap them; quota is always returned immediately.
                if not stuck:
                    self._children.pop(job_id, None)
                await self._return_capacity(job)

            logger.info(
                "job=%s cancel finalised outcome=%s procs=%d graceful=%d "
                "forced=%d left=%d duration_ms=%d",
                job_id, outcome.value, total, graceful, forced,
                len(stuck), result.duration_ms,
            )
            fut.set_result(job)
        except asyncio.CancelledError:
            # Supervisor shutdown; resolve waiters best-effort.
            if not fut.done():
                fut.set_result(self.store.get(job_id))
            raise
        except Exception:  # pragma: no cover - defensive, never strand callers
            logger.exception("job=%s cancellation workflow failed", job_id)
            if not fut.done():
                fut.set_result(self.store.get(job_id))

    async def _graceful_stop(
        self, procs: list[asyncio.subprocess.Process], grace_s: float
    ) -> list[asyncio.subprocess.Process]:
        """Ask every live process to stop; return those still alive after grace."""
        alive = [p for p in procs if p.returncode is None]
        for proc in alive:
            self._request_graceful_stop(proc)
        waiters = [asyncio.create_task(p.wait()) for p in alive]
        if waiters:
            _, pending = await asyncio.wait(waiters, timeout=grace_s)
            for task in pending:
                task.cancel()
            for task in waiters:
                if not task.done():
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        return [p for p in procs if p.returncode is None]

    async def _force(
        self, procs: list[asyncio.subprocess.Process], kill_wait_s: float
    ) -> tuple[int, list[asyncio.subprocess.Process]]:
        """Force-terminate live processes; return (killed count, survivors)."""
        alive = [p for p in procs if p.returncode is None]
        for proc in alive:
            self._request_force_stop(proc)
        waiters = [asyncio.create_task(p.wait()) for p in alive]
        if waiters:
            _, pending = await asyncio.wait(waiters, timeout=kill_wait_s)
            for task in pending:
                task.cancel()
            for task in waiters:
                if not task.done():
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        stuck = [p for p in alive if p.returncode is None]
        return len(alive) - len(stuck), stuck

    # Seams kept as methods so tests can simulate unresponsive processes.
    @staticmethod
    def _request_graceful_stop(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()

    @staticmethod
    def _request_force_stop(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #

    def _lock_for(self, job_id: str) -> asyncio.Lock:
        lock = self._job_locks.get(job_id)
        if lock is None:
            lock = asyncio.Lock()
            self._job_locks[job_id] = lock
        return lock

    @staticmethod
    def _elapsed_ms(started_monotonic: float) -> int:
        return max(0, int((time.monotonic() - started_monotonic) * 1000))

    async def _try_finish(
        self,
        job: Job,
        target: JobStatus,
        reason: TerminationReason,
        detail: str | None = None,
    ) -> bool:
        """Natural finalisation attempt.

        Returns False when a concurrent cancellation already owns the job;
        in that case the caller must not touch children or terminal state.
        """
        async with self._lock_for(job.job_id):
            if job.job_id in self._cancelling:
                return False
            if job.status != JobStatus.RUNNING:
                return False
            self._transition(job, target, reason=reason, detail=detail)
            return True

    async def _return_capacity(self, job: Job) -> None:
        """Give back concurrency and memory quota exactly once for a job."""
        async with self._capacity:
            if job.job_id in self._capacity_released:
                return
            self._capacity_released.add(job.job_id)
            self._running_count -= 1
            self._running_memory_mb -= job.spec.memory_mb
            self._capacity.notify_all()

    def _transition(
        self,
        job: Job,
        target: JobStatus,
        reason: TerminationReason | None = None,
        detail: str | None = None,
    ) -> None:
        if target not in ALLOWED_TRANSITIONS[job.status]:
            raise RuntimeError(
                f"illegal transition {job.status.value} -> {target.value} "
                f"for job {job.job_id}"
            )
        logger.info(
            "job=%s transition %s -> %s reason=%s",
            job.job_id, job.status.value, target.value,
            reason.value if reason else None,
        )
        job.status = target
        job.reason = reason
        job.detail = detail
        if target in (
            JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.TIMEOUT, JobStatus.CANCELLED
        ):
            job.finished_at = time.time()
        self.store.save(job)

    @staticmethod
    async def _hard_kill(proc: asyncio.subprocess.Process) -> None:
        """Unconditional best-effort kill used by timeout/spawn-fail/stop."""
        if proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=5)
