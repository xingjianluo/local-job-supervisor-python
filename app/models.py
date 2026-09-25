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


@dataclass
class JobSpec:
    """Constraints supplied at submission time."""

    duration_ms: int
    memory_mb: int
    procs: int = 1
    timeout_ms: int | None = None
    exit_code: int = 0


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
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        spec = JobSpec(**data["spec"])
        return cls(
            job_id=data["job_id"],
            spec=spec,
            status=JobStatus(data["status"]),
            reason=TerminationReason(data["reason"]) if data.get("reason") else None,
            detail=data.get("detail"),
            created_at=data["created_at"],
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
        )


@dataclass(frozen=True)
class Limits:
    """Global scheduler limits."""

    max_concurrent: int = 4
    max_memory_mb: int = 1024
    max_procs_per_job: int = 8
    max_queue_size: int = 64
