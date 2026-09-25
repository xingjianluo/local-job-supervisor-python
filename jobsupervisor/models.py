"""Type definitions for the job supervisor (skeleton)."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobState(str, enum.Enum):
    """Lifecycle states of a job."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


TERMINAL_STATES: frozenset[JobState] = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.TIMEOUT, JobState.CANCELLED}
)


class RejectReason(str, enum.Enum):
    """Distinguishable reasons why a submission is rejected."""

    INVALID_PARAMS = "invalid_params"
    RESOURCE_LIMIT = "resource_limit"
    INSUFFICIENT_RESOURCES = "insufficient_resources"
    QUEUE_FULL = "queue_full"


class TerminationReason(str, enum.Enum):
    """Distinguishable reasons why a job reached a terminal state."""

    COMPLETED = "completed"
    EXIT_NONZERO = "exit_nonzero"
    START_FAILED = "start_failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    INTERRUPTED_BY_RESTART = "interrupted_by_restart"


class JobType(str, enum.Enum):
    """Simulated short-job kinds executed as local processes."""

    SLEEP = "sleep"
    ALLOC = "alloc"
    FAIL = "fail"
    SPAWN = "spawn"
    HANG = "hang"


@dataclass(frozen=True)
class JobConstraints:
    """Input constraints attached to a job submission."""

    duration_ms: int = 100
    memory_mb: int = 0
    max_procs: int = 0


@dataclass
class JobRecord:
    """Persistent, auditable record of a single job."""

    job_id: str
    job_type: JobType
    constraints: JobConstraints
    state: JobState = JobState.QUEUED
    reason: str | None = None
    exit_code: int | None = None
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    attempts: int = 0
    history: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type.value,
            "constraints": {
                "duration_ms": self.constraints.duration_ms,
                "memory_mb": self.constraints.memory_mb,
                "max_procs": self.constraints.max_procs,
            },
            "state": self.state.value,
            "reason": self.reason,
            "exit_code": self.exit_code,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "attempts": self.attempts,
            "history": list(self.history),
        }

    @staticmethod
    def from_dict(data: dict) -> "JobRecord":
        c = data["constraints"]
        return JobRecord(
            job_id=data["job_id"],
            job_type=JobType(data["job_type"]),
            constraints=JobConstraints(
                duration_ms=int(c["duration_ms"]),
                memory_mb=int(c["memory_mb"]),
                max_procs=int(c["max_procs"]),
            ),
            state=JobState(data["state"]),
            reason=data.get("reason"),
            exit_code=data.get("exit_code"),
            created_at=float(data.get("created_at", 0.0)),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            attempts=int(data.get("attempts", 0)),
            history=list(data.get("history", [])),
        )


@dataclass(frozen=True)
class SupervisorConfig:
    """Global and per-job limits enforced by the supervisor."""

    max_concurrency: int = 4
    max_queue_size: int = 16
    max_memory_mb_total: int = 512
    max_memory_mb_per_job: int = 256
    max_procs_per_job: int = 4
    max_duration_ms: int = 60_000
    timeout_grace_ms: int = 1_000
    state_file: str = "job_state.json"


class SubmissionRejected(Exception):
    """Raised when a job submission is rejected with a typed reason."""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
