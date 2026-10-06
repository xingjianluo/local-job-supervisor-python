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
    SPAWN_FAILED = "spawn_failed"
    SUPERVISOR_RESTART = "supervisor_restart"
    # Cancellation was requested but derived processes could not be
    # confirmed gone; distinguishable from a clean cancellation.
    CLEANUP_FAILED = "cleanup_failed"


class CancelOutcome(str, enum.Enum):
    """How a cancellation actually finished.

    - GRACEFUL: every derived process exited on its own within the grace
      period (including jobs whose children had just exited).
    - FORCED: some processes ignored the graceful request and had to be
      force-killed after the grace period.
    - CLEANUP_FAILED: even after force-kill, some processes could not be
      confirmed gone; the cleanup is anomalous, not a normal cancellation.
    - NOT_REQUIRED: the job was cancelled while queued, so no derived
      process ever existed.
    """

    GRACEFUL = "graceful"
    FORCED = "forced"
    CLEANUP_FAILED = "cleanup_failed"
    NOT_REQUIRED = "not_required"


@dataclass(frozen=True)
class CancelResult:
    """Auditable result of a single cancellation; persisted with the job.

    Repeated and concurrent cancellations return the same result object;
    it survives a supervisor restart because it is stored with the job.
    """

    requested_at: float
    completed_at: float
    duration_ms: int
    outcome: CancelOutcome
    grace_ms: int
    procs_total: int
    procs_graceful: int
    procs_forced: int
    procs_remaining: int
    detail: str | None = None

    def to_dict(self) -> dict:
        return {
            "requested_at": self.requested_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "outcome": self.outcome.value,
            "grace_ms": self.grace_ms,
            "procs_total": self.procs_total,
            "procs_graceful": self.procs_graceful,
            "procs_forced": self.procs_forced,
            "procs_remaining": self.procs_remaining,
            "detail": self.detail,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CancelResult":
        return cls(
            requested_at=data["requested_at"],
            completed_at=data["completed_at"],
            duration_ms=data["duration_ms"],
            outcome=CancelOutcome(data["outcome"]),
            grace_ms=data["grace_ms"],
            procs_total=data["procs_total"],
            procs_graceful=data["procs_graceful"],
            procs_forced=data["procs_forced"],
            procs_remaining=data["procs_remaining"],
            detail=data.get("detail"),
        )


@dataclass
class JobSpec:
    """Constraints supplied at submission time."""

    duration_ms: int
    memory_mb: int
    procs: int = 1
    timeout_ms: int | None = None
    exit_code: int = 0
    # When true, derived processes ignore the graceful termination signal so
    # the forced-termination path can be exercised. Default false keeps the
    # existing submission behaviour unchanged.
    ignore_sigterm: bool = False


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
    # Timestamp of the first cancellation request; set while the job is
    # still QUEUED/RUNNING so an in-flight cancellation is observable.
    cancel_requested_at: float | None = None
    # Auditable result once cancellation finished; survives restarts.
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
                "ignore_sigterm": self.spec.ignore_sigterm,
            },
            "status": self.status.value,
            "reason": self.reason.value if self.reason else None,
            "detail": self.detail,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "cancel_requested_at": self.cancel_requested_at,
            "cancel_result": self.cancel_result.to_dict() if self.cancel_result else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        spec_data = dict(data["spec"])
        spec_data.setdefault("ignore_sigterm", False)  # tolerate older state files
        spec = JobSpec(**spec_data)
        return cls(
            job_id=data["job_id"],
            spec=spec,
            status=JobStatus(data["status"]),
            reason=TerminationReason(data["reason"]) if data.get("reason") else None,
            detail=data.get("detail"),
            created_at=data["created_at"],
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            cancel_requested_at=data.get("cancel_requested_at"),
            cancel_result=(
                CancelResult.from_dict(data["cancel_result"])
                if data.get("cancel_result")
                else None
            ),
        )


@dataclass(frozen=True)
class Limits:
    """Global scheduler limits and cancellation timing knobs."""

    max_concurrent: int = 4
    max_memory_mb: int = 1024
    max_procs_per_job: int = 8
    max_queue_size: int = 64
    # Grace period for a running job to shut itself down after a cancel
    # request before derived processes are force-killed.
    cancel_grace_ms: int = 2000
    # Extra time to wait after force-kill before the cleanup is declared
    # anomalous (cleanup_failed), i.e. derived processes are still around.
    cancel_force_wait_ms: int = 2000
