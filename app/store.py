"""Persistent job store backed by a single JSON file with atomic writes."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from .models import Job


class JobStore:
    """Keeps every job record in memory and mirrors it to disk atomically.

    The file is the source of truth across restarts: on load, completed jobs
    are recognized and unfinished jobs are visible to the recovery logic.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def load(self) -> list[Job]:
        with self._lock:
            if self.path.exists():
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                self._jobs = {item["job_id"]: Job.from_dict(item) for item in raw}
            return list(self._jobs.values())

    def save(self, job: Job) -> None:
        with self._lock:
            self._jobs[job.job_id] = job
            self._flush_locked()

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def all(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def _flush_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = [job.to_dict() for job in self._jobs.values()]
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)
