"""Type definitions for the local job supervisor.

Only data types and the state-transition table live here; no runtime logic.
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field


class JobStatus(str, enum.Enum):
    """Lifecycle states of a job."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


TERMINAL_STATES: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.TIMEOUT, JobStatus.CANCELLED}
)

# Allowed state transitions. Anything not listed is illegal and must be rejected.
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.TIMEOUT, JobStatus.CANCELLED}
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.TIMEOUT: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


class RejectReason(str, enum.Enum):
    """Machine-distinguishable reasons why a submission is rejected."""

    INVALID_PARAMS = "invalid_params"
    MEMORY_EXCEEDS_LIMIT = "memory_exceeds_limit"
    PROCESS_COUNT_EXCEEDS_LIMIT = "process_count_exceeds_limit"
    QUEUE_FULL = "queue_full"


class TerminationReason(str, enum.Enum):
    """Why a job reached a terminal state."""

    COMPLETED = "completed"
    EXIT_CODE = "exit_code"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    # Cleanup itself failed (e.g. a derived process could not be reaped);
    # deliberately distinct from a normal cancellation.
    CANCEL_CLEANUP_FAILED = "cancel_cleanup_failed"
    SPAWN_FAILED = "spawn_failed"
    SUPERVISOR_RESTART = "supervisor_restart"


class CancelOutcome(str, enum.Enum):
    """How a cancellation actually finished.

    - GRACEFUL: all derived processes exited on their own within the grace
      period (never had to be force-killed).
    - FORCED: at least one process ignored the graceful signal and was
      force-terminated once the grace period elapsed.
    - CLEANUP_FAILED: even after force termination some processes could not
      be confirmed dead; this is a cleanup error, not a normal cancellation.
    - NOT_NEEDED: the job was still queued, so no process ever existed.
    """

    GRACEFUL = "graceful"
    FORCED = "forced"
    CLEANUP_FAILED = "cleanup_failed"
    NOT_NEEDED = "not_needed"


@dataclass
class JobSpec:
    """Constraints supplied at submission time."""

    duration_ms: int
    memory_mb: int
    procs: int = 1
    timeout_ms: int | None = None
    exit_code: int = 0


@dataclass(frozen=True)
class CancelResult:
    """Auditable record of one cancellation request that actually finalised a job.

    Fields:
        outcome: how the cleanup finished (see CancelOutcome).
        requested_at: wall-clock time (time.time()) when cancellation began.
        duration_ms: wall-clock milliseconds spent finishing the job.
        processes_total: derived processes the job had at cancel time
                         (0 for queued jobs).
        processes_graceful: processes that exited within the grace period
                            before force termination was attempted.
        processes_forced: processes that had to be force-terminated.
        processes_left: processes still not confirmed dead when cleanup gave
                        up; > 0 implies outcome == CLEANUP_FAILED.
        grace_period_ms: configured grace period in effect for this request.
        detail: human-readable extra context (e.g. cleanup error).
    """

    outcome: CancelOutcome
    requested_at: float
    duration_ms: int
    processes_total: int = 0
    processes_graceful: int = 0
    processes_forced: int = 0
    processes_left: int = 0
    grace_period_ms: int = 0
    detail: str | None = None

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome.value,
            "requested_at": self.requested_at,
            "duration_ms": self.duration_ms,
            "processes_total": self.processes_total,
            "processes_graceful": self.processes_graceful,
            "processes_forced": self.processes_forced,
            "processes_left": self.processes_left,
            "grace_period_ms": self.grace_period_ms,
            "detail": self.detail,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CancelResult":
        return cls(
            outcome=CancelOutcome(data["outcome"]),
            requested_at=data["requested_at"],
            duration_ms=data["duration_ms"],
            processes_total=data.get("processes_total", 0),
            processes_graceful=data.get("processes_graceful", 0),
            processes_forced=data.get("processes_forced", 0),
            processes_left=data.get("processes_left", 0),
            grace_period_ms=data.get("grace_period_ms", 0),
            detail=data.get("detail"),
        )


@dataclass
class Job:
    """Persistent job record."""

    job_id: str
    spec: JobSpec
    status: JobStatus = JobStatus.QUEUED
    reason: TerminationReason | None = None
    detail: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    cancel_result: CancelResult | None = None

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex[:12]

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "spec": {
                "duration_ms": self.spec.duration_ms,
                "memory_mb": self.spec.memory_mb,
                "procs": self.spec.procs,
                "timeout_ms": self.spec.timeout_ms,
                "exit_code": self.spec.exit_code,
            },
            "status": self.status.value,
            "reason": self.reason.value if self.reason else None,
            "detail": self.detail,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "cancel_result": self.cancel_result.to_dict() if self.cancel_result else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        spec = JobSpec(**data["spec"])
        cr = data.get("cancel_result")
        return cls(
            job_id=data["job_id"],
            spec=spec,
            status=JobStatus(data["status"]),
            reason=TerminationReason(data["reason"]) if data.get("reason") else None,
            detail=data.get("detail"),
            created_at=data["created_at"],
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            cancel_result=CancelResult.from_dict(cr) if cr else None,
        )


@dataclass(frozen=True)
class Limits:
    """Global scheduler limits."""

    max_concurrent: int = 4
    max_memory_mb: int = 1024
    max_procs_per_job: int = 8
    max_queue_size: int = 64
    # Cancellation hardening:
    # After a cancel is requested, derived processes get this long to exit by
    # themselves; once it elapses they are force-terminated. The whole
    # cancellation must finish shortly after this window.
    cancel_grace_period_ms: int = 5000
    # How long to wait for a single process to actually die after a kill
    # signal before declaring the cleanup itself failed.
    cancel_kill_wait_ms: int = 2000
