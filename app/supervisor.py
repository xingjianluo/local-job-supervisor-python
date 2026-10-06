"""Job supervisor: bounded scheduling, lifecycle, strong cancellation, recovery."""

from __future__ import annotations

import asyncio
import dataclasses
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
    "import signal,time,sys;"
    "signal.signal(signal.SIGTERM, signal.SIG_IGN) if sys.argv[3]=='1' else None;"
    "time.sleep(float(sys.argv[1]));"
    "sys.exit(int(sys.argv[2]))"
)


class JobRejectedError(Exception):
    """Raised when a submission is rejected; carries a distinguishable reason."""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclasses.dataclass
class _JobControl:
    """Per-job cancellation coordination.

    ``lock`` serialises cancels against state transitions so the teardown
    runs at most once; ``cancel_event`` kicks the runner; ``done_event`` is
    the single result every (repeated/concurrent) cancel waits on.
    """

    lock: asyncio.Lock = dataclasses.field(default_factory=asyncio.Lock)
    cancel_event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    done_event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    cancel_result: CancelResult | None = None
    request_monotonic: float | None = None


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
        self._controls: dict[str, _JobControl] = {}
        self._started = False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Load persisted jobs and recover unfinished ones."""
        jobs = self.store.load()
        for job in jobs:
            self._control_for(job.job_id)
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
        for task in list(self._runners):
            task.cancel()
        if self._runners:
            await asyncio.gather(*self._runners, return_exceptions=True)
        # Never leak derived child processes.
        for procs in list(self._children.values()):
            for proc in procs:
                if proc.returncode is None:
                    self._force_stop(proc)
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=1.0)
                    except (asyncio.TimeoutError, TimeoutError):
                        pass
        self._children.clear()
        self._started = False

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #

    async def submit(self, spec: JobSpec) -> Job:
        self._validate(spec)
        job = Job(job_id=Job.new_id(), spec=spec)
        self._control_for(job.job_id)
        self.store.save(job)
        logger.info(
            "job=%s submitted spec(duration_ms=%d memory_mb=%d procs=%d timeout_ms=%s)",
            job.job_id, spec.duration_ms, spec.memory_mb, spec.procs, spec.timeout_ms,
        )
        self._queue.put_nowait(job.job_id)
        return job

    async def cancel(self, job_id: str) -> Job | None:
        """Strong, idempotent cancellation.

        - unknown job -> None (caller maps to 404), unchanged behaviour
        - terminal job -> returned unchanged, no side effects
        - queued job -> cancelled at once (no process ever existed)
        - running job -> graceful request, grace period, then force-kill;
          returns only after every derived process is confirmed gone
          (or after an auditable cleanup_failed result)
        Repeated / concurrent cancels perform the teardown exactly once and
        all wait on the same final result.
        """
        job = self.store.get(job_id)
        if job is None:
            return None
        ctrl = self._control_for(job_id)
        queued_finished_now = False
        async with ctrl.lock:
            job = self.store.get(job_id)
            if job is None:
                return None
            if job.is_terminal:
                return job
            if job.cancel_requested_at is None:
                job.cancel_requested_at = time.time()
                ctrl.request_monotonic = time.monotonic()
                self.store.save(job)
                logger.info("job=%s cancel requested", job_id)
            ctrl.cancel_event.set()

            if job.status == JobStatus.QUEUED:
                # No derived process has existed; the teardown is complete
                # the moment the state is committed. The dispatcher skips
                # cancelled queue entries, so no slot will be consumed.
                result = self._make_result(
                    job,
                    ctrl,
                    outcome=CancelOutcome.NOT_REQUIRED,
                    procs_total=0,
                    procs_graceful=0,
                    procs_forced=0,
                    procs_remaining=0,
                    detail="cancelled while queued",
                )
                self._record_cancel_result(job, ctrl, result)
                self._transition(
                    job,
                    JobStatus.CANCELLED,
                    reason=TerminationReason.CANCELLED,
                    detail="cancelled while queued",
                )
                queued_finished_now = True
        if queued_finished_now:
            ctrl.done_event.set()
            return self.store.get(job_id)
        # RUNNING: the runner owns the teardown and the state transition;
        # every concurrent cancel waits on the same completion event.
        await ctrl.done_event.wait()
        return self.store.get(job_id)

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
                continue  # cancelled while queued
            ctrl = self._control_for(job_id)
            async with self._capacity:
                await self._capacity.wait_for(self._has_capacity_for(job))
                # Commit slot acquisition and the QUEUED -> RUNNING
                # transition atomically w.r.t. cancellation: a cancel either
                # sees QUEUED (no slot consumed) or RUNNING (slot held until
                # the runner's teardown releases it).
                async with ctrl.lock:
                    job = self.store.get(job_id)
                    if job is None or job.status != JobStatus.QUEUED:
                        self._capacity.notify_all()
                        continue
                    self._running_count += 1
                    self._running_memory_mb += job.spec.memory_mb
                    job.started_at = time.time()
                    self._transition(job, JobStatus.RUNNING)
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
        ctrl = self._control_for(job.job_id)
        try:
            try:
                await self._execute(job, ctrl)
            finally:
                self._children.pop(job.job_id, None)
        finally:
            self._job_tasks.pop(job.job_id, None)
            async with self._capacity:
                # Quota is returned only after teardown finished, so the
                # limits can never be exceeded and queued jobs proceed in
                # order as soon as cancellation completes.
                self._running_count -= 1
                self._running_memory_mb -= job.spec.memory_mb
                self._capacity.notify_all()
            # Release every waiting cancel, including shutdown-cancelled runs.
            ctrl.done_event.set()

    async def _execute(self, job: Job, ctrl: _JobControl) -> None:
        spec = job.spec
        procs: list[asyncio.subprocess.Process] = []
        try:
            for _ in range(spec.procs):
                if ctrl.cancel_event.is_set():
                    break  # cancel hit while still spawning
                procs.append(await self._spawn_child(job))
        except OSError as exc:
            logger.warning("job=%s spawn failed: %s", job.job_id, exc)
            for proc in procs:
                if proc.returncode is None:
                    self._force_stop(proc)
                    await self._wait_exit(proc)
            if ctrl.cancel_event.is_set():
                await self._cancel_reap(job, procs, ctrl)
                return
            async with ctrl.lock:
                if job.status == JobStatus.RUNNING:
                    self._transition(
                        job, JobStatus.FAILED,
                        reason=TerminationReason.SPAWN_FAILED, detail=str(exc),
                    )
            return
        self._children[job.job_id] = procs

        if ctrl.cancel_event.is_set():
            await self._cancel_reap(job, procs, ctrl)
            return

        all_exit = asyncio.gather(*(self._wait_exit(p) for p in procs))
        canceller = asyncio.ensure_future(ctrl.cancel_event.wait())
        timeout = spec.timeout_ms / 1000.0 if spec.timeout_ms else None
        try:
            try:
                await asyncio.wait_for(
                    asyncio.wait(
                        {all_exit, canceller}, return_when=asyncio.FIRST_COMPLETED
                    ),
                    timeout=timeout,
                )
            except (asyncio.TimeoutError, TimeoutError):
                if not canceller.done():
                    canceller.cancel()
                for proc in procs:
                    if proc.returncode is None:
                        self._force_stop(proc)
                        await self._wait_exit(proc)
                async with ctrl.lock:
                    if job.status == JobStatus.RUNNING:
                        self._transition(
                            job, JobStatus.TIMEOUT, reason=TerminationReason.TIMEOUT
                        )
                return

            if ctrl.cancel_event.is_set():
                await self._cancel_reap(job, procs, ctrl)
                return

            await all_exit  # all children have exited
            bad = [p.returncode for p in procs if p.returncode != 0]
            async with ctrl.lock:
                if job.status != JobStatus.RUNNING:
                    return  # cancel won the race; reap path handles the rest
                if bad:
                    self._transition(
                        job, JobStatus.FAILED,
                        reason=TerminationReason.EXIT_CODE,
                        detail=f"child exited with code {bad[0]}",
                    )
                else:
                    self._transition(
                        job, JobStatus.SUCCEEDED, reason=TerminationReason.COMPLETED
                    )
        finally:
            # Never leave un-retreated futures behind (runner cancellation on
            # shutdown cancels the in-flight waiters).
            for waiter in (all_exit, canceller):
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(all_exit, canceller, return_exceptions=True)

    async def _cancel_reap(
        self,
        job: Job,
        procs: list[asyncio.subprocess.Process],
        ctrl: _JobControl,
    ) -> None:
        """Two-phase teardown: graceful request within the grace period,
        then force-kill; an outcome that cannot be confirmed gone is
        reported distinctly as cleanup_failed."""
        grace = max(0.0, self.limits.cancel_grace_ms / 1000.0)
        force_wait = max(0.0, self.limits.cancel_force_wait_ms / 1000.0)
        started = ctrl.request_monotonic or time.monotonic()

        targets = [p for p in procs if p.returncode is None]
        procs_total = len(targets)
        for proc in targets:
            self._request_stop(proc)
        remaining_grace = max(0.0, grace - (time.monotonic() - started))
        await self._wait_some(targets, remaining_grace)
        graceful = [p for p in targets if p.returncode is not None]
        stuck = [p for p in targets if p.returncode is None]

        forced_procs: list[asyncio.subprocess.Process] = []
        leftovers: list[asyncio.subprocess.Process] = []
        if stuck:
            for proc in stuck:
                self._force_stop(proc)
            await self._wait_some(stuck, force_wait)
            forced_procs = [p for p in stuck if p.returncode is not None]
            leftovers = [p for p in stuck if p.returncode is None]

        if leftovers:
            outcome = CancelOutcome.CLEANUP_FAILED
            reason = TerminationReason.CLEANUP_FAILED
            detail = (
                f"{len(leftovers)} derived process(es) still alive after "
                f"grace {self.limits.cancel_grace_ms}ms and force-kill wait "
                f"{self.limits.cancel_force_wait_ms}ms"
            )
        elif stuck:
            outcome = CancelOutcome.FORCED
            reason = TerminationReason.CANCELLED
            detail = None
        elif procs_total == 0:
            # Reached RUNNING but no derived process had been spawned yet.
            outcome = CancelOutcome.NOT_REQUIRED
            reason = TerminationReason.CANCELLED
            detail = "cancelled before any derived process was spawned"
        else:
            outcome = CancelOutcome.GRACEFUL
            reason = TerminationReason.CANCELLED
            detail = None

        result = self._make_result(
            job,
            ctrl,
            outcome=outcome,
            procs_total=procs_total,
            procs_graceful=len(graceful),
            procs_forced=len(forced_procs),
            procs_remaining=len(leftovers),
            detail=detail,
        )
        async with ctrl.lock:
            job = self.store.get(job.job_id)
            self._record_cancel_result(job, ctrl, result)
            if job.status == JobStatus.RUNNING:
                self._transition(job, JobStatus.CANCELLED, reason=reason, detail=detail)
        logger.info(
            "job=%s cancel finished outcome=%s duration_ms=%d procs=%d "
            "graceful=%d forced=%d remaining=%d",
            job.job_id, result.outcome.value, result.duration_ms,
            result.procs_total, result.procs_graceful,
            result.procs_forced, result.procs_remaining,
        )

    # ------------------------------------------------------------------ #
    # process primitives (overridable in tests for fault injection)
    # ------------------------------------------------------------------ #

    async def _spawn_child(self, job: Job) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            sys.executable, "-I", "-c", _CHILD_PROGRAM,
            str(job.spec.duration_ms / 1000.0), str(job.spec.exit_code),
            "1" if job.spec.ignore_sigterm else "0",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

    def _request_stop(self, proc: asyncio.subprocess.Process) -> None:
        """Phase 1: ask the process to exit (SIGTERM)."""
        try:
            proc.terminate()
        except ProcessLookupError:
            pass

    def _force_stop(self, proc: asyncio.subprocess.Process) -> None:
        """Phase 2: force-kill (SIGKILL)."""
        try:
            proc.kill()
        except ProcessLookupError:
            pass

    async def _wait_exit(self, proc: asyncio.subprocess.Process) -> None:
        await proc.wait()

    async def _wait_some(
        self, procs: list[asyncio.subprocess.Process], timeout: float
    ) -> None:
        """Wait until all given processes exit or the timeout elapses."""
        pending = [p for p in procs if p.returncode is None]
        if not pending:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*(self._wait_exit(p) for p in pending)),
                timeout=timeout,
            )
        except (asyncio.TimeoutError, TimeoutError):
            pass

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #

    def _control_for(self, job_id: str) -> _JobControl:
        ctrl = self._controls.get(job_id)
        if ctrl is None:
            ctrl = _JobControl()
            self._controls[job_id] = ctrl
        return ctrl

    def _make_result(
        self,
        job: Job,
        ctrl: _JobControl,
        *,
        outcome: CancelOutcome,
        procs_total: int,
        procs_graceful: int,
        procs_forced: int,
        procs_remaining: int,
        detail: str | None,
    ) -> CancelResult:
        requested_at = job.cancel_requested_at or time.time()
        completed_at = time.time()
        grace_ms = (
            0 if outcome == CancelOutcome.NOT_REQUIRED
            else self.limits.cancel_grace_ms
        )
        return CancelResult(
            requested_at=requested_at,
            completed_at=completed_at,
            duration_ms=max(0, int(round((completed_at - requested_at) * 1000))),
            outcome=outcome,
            grace_ms=grace_ms,
            procs_total=procs_total,
            procs_graceful=procs_graceful,
            procs_forced=procs_forced,
            procs_remaining=procs_remaining,
            detail=detail,
        )

    def _record_cancel_result(
        self, job: Job, ctrl: _JobControl, result: CancelResult
    ) -> None:
        if ctrl.cancel_result is None:
            ctrl.cancel_result = result
        job.cancel_result = ctrl.cancel_result
        self.store.save(job)

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
