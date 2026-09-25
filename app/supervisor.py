"""Job supervisor: bounded scheduling, lifecycle, cancellation, recovery."""

from __future__ import annotations

import asyncio
import logging
import sys
import time

from .models import (
    ALLOWED_TRANSITIONS,
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
        for task in list(self._runners):
            task.cancel()
        if self._runners:
            await asyncio.gather(*self._runners, return_exceptions=True)
        # Never leak derived child processes.
        for procs in self._children.values():
            for proc in procs:
                await self._kill(proc)
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
        """Idempotent cancellation.

        - unknown job -> None (caller maps to 404)
        - terminal job -> returned unchanged, no side effects
        - queued job -> marked cancelled, dispatcher will skip it
        - running job -> children killed, marked cancelled
        """
        job = self.store.get(job_id)
        if job is None:
            return None
        if job.is_terminal:
            return job
        if job.status == JobStatus.QUEUED:
            self._transition(job, JobStatus.CANCELLED, reason=TerminationReason.CANCELLED)
            return job
        # RUNNING: kill derived processes, then wait for the runner to finish
        # so no child process or bookkeeping is left behind when cancel returns.
        self._transition(job, JobStatus.CANCELLED, reason=TerminationReason.CANCELLED)
        for proc in self._children.get(job_id, []):
            await self._kill(proc)
        task = self._job_tasks.get(job_id)
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        return job

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
        try:
            if job.status != JobStatus.QUEUED:
                return  # cancelled between dispatch and start
            self._transition(job, JobStatus.RUNNING)
            job.started_at = time.time()
            self.store.save(job)
            try:
                await self._execute(job)
            finally:
                self._children.pop(job.job_id, None)
        finally:
            self._job_tasks.pop(job.job_id, None)
            async with self._capacity:
                self._running_count -= 1
                self._running_memory_mb -= job.spec.memory_mb
                self._capacity.notify_all()

    async def _execute(self, job: Job) -> None:
        spec = job.spec
        procs: list[asyncio.subprocess.Process] = []
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
            for proc in procs:
                await self._kill(proc)
            self._transition(
                job, JobStatus.FAILED,
                reason=TerminationReason.SPAWN_FAILED, detail=str(exc),
            )
            return
        self._children[job.job_id] = procs
        timeout = spec.timeout_ms / 1000.0 if spec.timeout_ms else None
        try:
            await asyncio.wait_for(
                asyncio.gather(*(p.wait() for p in procs)), timeout=timeout
            )
        except (asyncio.TimeoutError, TimeoutError):
            for proc in procs:
                await self._kill(proc)
            if job.status == JobStatus.RUNNING:
                self._transition(
                    job, JobStatus.TIMEOUT, reason=TerminationReason.TIMEOUT
                )
            return
        except asyncio.CancelledError:
            for proc in procs:
                await self._kill(proc)
            raise
        if job.status != JobStatus.RUNNING:
            return  # cancelled while waiting
        bad = [p.returncode for p in procs if p.returncode != 0]
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

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #

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
    async def _kill(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        try:
            proc.kill()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except (asyncio.TimeoutError, TimeoutError):
            pass
