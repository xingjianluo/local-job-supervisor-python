"""Scheduling, execution, cancellation and restart recovery.

The supervisor runs every job as a local child process in its own process
group, enforces a global concurrency limit plus memory/proc/duration
limits, and persists every state transition so a restart can recover
without re-executing finished jobs or silently losing unfinished ones.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field

from .models import (
    JobConstraints,
    JobRecord,
    JobState,
    JobType,
    RejectReason,
    SubmissionRejected,
    SupervisorConfig,
    TERMINAL_STATES,
    TerminationReason,
)
from .store import JobStore

logger = logging.getLogger("jobsupervisor.supervisor")

_POLL_INTERVAL_S = 0.02


@dataclass
class _RunningJob:
    cancel_event: threading.Event = field(default_factory=threading.Event)
    abort_silent: bool = False  # set during shutdown: kill without transition
    proc: subprocess.Popen | None = None
    thread: threading.Thread | None = None


class Supervisor:
    """Bounded local job scheduler backed by local processes."""

    def __init__(self, config: SupervisorConfig) -> None:
        self.config = config
        self._store = JobStore(config.state_file)
        self._cond = threading.Condition()
        self._records: dict[str, JobRecord] = {}
        self._queue: deque[str] = deque()
        self._running: dict[str, _RunningJob] = {}
        self._stopping = False
        self._dispatcher: threading.Thread | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def recover(self) -> None:
        """Load persisted state and resume.

        - Terminal jobs are kept as-is and never re-executed.
        - Queued jobs are re-queued and will be dispatched.
        - Jobs recorded as RUNNING belong to the dead previous process;
          they are closed as FAILED/interrupted_by_restart so they are
          accounted for instead of silently lost.
        """
        with self._cond:
            self._records = self._store.load()
            for rec in self._records.values():
                if rec.state in TERMINAL_STATES:
                    continue
                if rec.state is JobState.QUEUED:
                    self._queue.append(rec.job_id)
                    logger.info(
                        "job=%s recovered as queued, re-queued", rec.job_id
                    )
                elif rec.state is JobState.RUNNING:
                    self._transition(
                        rec, JobState.FAILED,
                        reason=TerminationReason.INTERRUPTED_BY_RESTART.value,
                    )
            self._stopping = False
            self._dispatcher = threading.Thread(
                target=self._dispatch_loop, name="job-dispatcher", daemon=True
            )
            self._dispatcher.start()
            self._cond.notify_all()

    def shutdown(self) -> None:
        """Stop dispatching and kill running process groups.

        Running records are left in RUNNING state on disk (their child
        processes are killed so nothing is leaked); a subsequent recover()
        will close them as interrupted_by_restart.
        """
        with self._cond:
            self._stopping = True
            running = list(self._running.items())
            for _, rj in running:
                rj.abort_silent = True
                rj.cancel_event.set()
                self._kill_proc(rj.proc)
            self._cond.notify_all()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=5)
        for _, rj in running:
            if rj.thread is not None:
                rj.thread.join(timeout=5)

    # ------------------------------------------------------------------
    # submission / query / cancel
    # ------------------------------------------------------------------
    def submit(self, job_type: JobType, constraints: JobConstraints) -> JobRecord:
        with self._cond:
            self._validate(constraints)
            job_id = uuid.uuid4().hex[:12]
            rec = JobRecord(
                job_id=job_id,
                job_type=job_type,
                constraints=constraints,
                state=JobState.QUEUED,
                created_at=time.time(),
            )
            rec.history.append(
                {"from": None, "to": JobState.QUEUED.value, "at": rec.created_at, "reason": None}
            )
            self._records[job_id] = rec
            self._queue.append(job_id)
            self._store.save(self._records)
            logger.info(
                "job=%s submitted type=%s constraints(duration_ms=%d, memory_mb=%d, max_procs=%d)",
                job_id, job_type.value, constraints.duration_ms,
                constraints.memory_mb, constraints.max_procs,
            )
            self._cond.notify_all()
            return rec

    def _validate(self, c: JobConstraints) -> None:
        cfg = self.config
        if c.duration_ms <= 0 or c.memory_mb < 0 or c.max_procs < 0:
            raise SubmissionRejected(
                RejectReason.INVALID_PARAMS,
                "duration_ms must be > 0 and memory_mb/max_procs must be >= 0",
            )
        if c.duration_ms > cfg.max_duration_ms:
            raise SubmissionRejected(
                RejectReason.RESOURCE_LIMIT,
                f"duration_ms {c.duration_ms} exceeds per-job limit {cfg.max_duration_ms}",
            )
        if c.memory_mb > cfg.max_memory_mb_per_job:
            raise SubmissionRejected(
                RejectReason.RESOURCE_LIMIT,
                f"memory_mb {c.memory_mb} exceeds per-job limit {cfg.max_memory_mb_per_job}",
            )
        if c.max_procs > cfg.max_procs_per_job:
            raise SubmissionRejected(
                RejectReason.RESOURCE_LIMIT,
                f"max_procs {c.max_procs} exceeds per-job limit {cfg.max_procs_per_job}",
            )
        reserved = sum(
            self._records[jid].constraints.memory_mb
            for jid in list(self._queue) + list(self._running)
        )
        if reserved + c.memory_mb > cfg.max_memory_mb_total:
            raise SubmissionRejected(
                RejectReason.INSUFFICIENT_RESOURCES,
                f"reserved memory {reserved}MiB + {c.memory_mb}MiB exceeds total budget "
                f"{cfg.max_memory_mb_total}MiB",
            )
        if len(self._queue) >= cfg.max_queue_size:
            raise SubmissionRejected(
                RejectReason.QUEUE_FULL,
                f"queue is full ({cfg.max_queue_size} jobs)",
            )

    def get(self, job_id: str) -> JobRecord | None:
        with self._cond:
            return self._records.get(job_id)

    def list_jobs(self) -> list[JobRecord]:
        with self._cond:
            return list(self._records.values())

    def cancel(self, job_id: str) -> JobRecord | None:
        """Idempotent cancellation.

        - Unknown job: returns None (caller maps to 404).
        - Terminal job: returns the record unchanged (no side effects).
        - Queued job: transitions to CANCELLED immediately.
        - Running job: kills the whole process group; the runner thread
          finalizes the CANCELLED transition.
        """
        with self._cond:
            rec = self._records.get(job_id)
            if rec is None:
                return None
            if rec.state in TERMINAL_STATES:
                return rec
            if rec.state is JobState.QUEUED:
                try:
                    self._queue.remove(job_id)
                except ValueError:
                    pass
                self._transition(rec, JobState.CANCELLED,
                                 reason=TerminationReason.CANCELLED.value)
                self._cond.notify_all()
                return rec
            # RUNNING: signal the runner; exactly one transition happens there.
            rj = self._running.get(job_id)
            if rj is not None:
                rj.cancel_event.set()
                self._kill_proc(rj.proc)
                # Wait for the runner to finalize so callers observe a
                # stable terminal state right after cancel() returns.
                deadline = time.monotonic() + 10.0
                while rec.state not in TERMINAL_STATES and time.monotonic() < deadline:
                    self._cond.wait(timeout=0.1)
            return rec

    # ------------------------------------------------------------------
    # dispatcher
    # ------------------------------------------------------------------
    def _dispatch_loop(self) -> None:
        while True:
            with self._cond:
                while not self._stopping and not self._dispatchable():
                    self._cond.wait(timeout=0.5)
                if self._stopping:
                    return
                job_id = self._queue.popleft()
                rec = self._records[job_id]
                rj = _RunningJob()
                self._running[job_id] = rj
                self._transition(rec, JobState.RUNNING)
                rec.attempts += 1
                thread = threading.Thread(
                    target=self._run_job, args=(job_id, rj),
                    name=f"job-runner-{job_id}", daemon=True,
                )
                rj.thread = thread
                thread.start()

    def _dispatchable(self) -> bool:
        if not self._queue or len(self._running) >= self.config.max_concurrency:
            return False
        rec = self._records[self._queue[0]]
        reserved = sum(
            self._records[jid].constraints.memory_mb for jid in self._running
        )
        return reserved + rec.constraints.memory_mb <= self.config.max_memory_mb_total

    # ------------------------------------------------------------------
    # runner
    # ------------------------------------------------------------------
    def _run_job(self, job_id: str, rj: _RunningJob) -> None:
        rec = self._records[job_id]
        c = rec.constraints
        cmd = [
            sys.executable, "-m", "jobsupervisor.worker",
            "--job-id", job_id,
            "--type", rec.job_type.value,
            "--duration-ms", str(c.duration_ms),
            "--memory-mb", str(c.memory_mb),
            "--max-procs", str(c.max_procs),
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,  # own process group: cancellable as a unit
            )
        except OSError as exc:
            logger.warning("job=%s failed to start: %s", job_id, exc)
            with self._cond:
                self._finalize(rec, rj, JobState.FAILED,
                               TerminationReason.START_FAILED.value)
            return
        rj.proc = proc

        deadline = time.monotonic() + (c.duration_ms + self.config.timeout_grace_ms) / 1000.0
        while True:
            if rj.cancel_event.is_set():
                self._kill_proc(proc)
                proc.wait()
                with self._cond:
                    if rj.abort_silent:
                        return  # shutdown: leave record RUNNING on disk
                    self._finalize(rec, rj, JobState.CANCELLED,
                                   TerminationReason.CANCELLED.value)
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._kill_proc(proc)
                proc.wait()
                with self._cond:
                    if rj.abort_silent:
                        return
                    self._finalize(rec, rj, JobState.TIMEOUT,
                                   TerminationReason.TIMEOUT.value)
                return
            try:
                rc = proc.wait(timeout=min(_POLL_INTERVAL_S, remaining))
            except subprocess.TimeoutExpired:
                continue
            with self._cond:
                if rj.abort_silent:
                    return
                if rj.cancel_event.is_set():
                    # Killed by cancel(): the exit code is a side effect of
                    # our own SIGKILL, so the outcome is CANCELLED.
                    self._finalize(rec, rj, JobState.CANCELLED,
                                   TerminationReason.CANCELLED.value)
                    return
                if rc == 0:
                    self._finalize(rec, rj, JobState.SUCCEEDED,
                                   TerminationReason.COMPLETED.value, exit_code=0)
                else:
                    self._finalize(rec, rj, JobState.FAILED,
                                   TerminationReason.EXIT_NONZERO.value, exit_code=rc)
            return

    # ------------------------------------------------------------------
    # helpers (called with self._cond held unless noted)
    # ------------------------------------------------------------------
    def _finalize(
        self,
        rec: JobRecord,
        rj: _RunningJob,
        state: JobState,
        reason: str,
        exit_code: int | None = None,
    ) -> None:
        if rec.state in TERMINAL_STATES:
            return  # defensive: never transition twice
        self._transition(rec, state, reason=reason, exit_code=exit_code)
        self._running.pop(rec.job_id, None)
        self._cond.notify_all()

    def _transition(
        self,
        rec: JobRecord,
        new_state: JobState,
        reason: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        old = rec.state
        rec.state = new_state
        rec.reason = reason
        if exit_code is not None:
            rec.exit_code = exit_code
        now = time.time()
        if new_state is JobState.RUNNING:
            rec.started_at = now
        if new_state in TERMINAL_STATES:
            rec.finished_at = now
        rec.history.append(
            {"from": old.value, "to": new_state.value, "at": now, "reason": reason}
        )
        c = rec.constraints
        logger.info(
            "job=%s constraints(duration_ms=%d, memory_mb=%d, max_procs=%d) "
            "transition %s -> %s reason=%s exit_code=%s",
            rec.job_id, c.duration_ms, c.memory_mb, c.max_procs,
            old.value, new_state.value, reason, exit_code,
        )
        self._store.save(self._records)

    @staticmethod
    def _kill_proc(proc: subprocess.Popen | None) -> None:
        """Kill the whole process group so no derived child is left behind."""
        if proc is None or proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except OSError:
                pass
