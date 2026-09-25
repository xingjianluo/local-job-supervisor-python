"""Persistent job state store backed by a local JSON file."""

from __future__ import annotations

import json
import os
import tempfile

from .models import JobRecord


class JobStore:
    """JSON-file backed store so job state survives service restarts.

    Writes are atomic (write-to-temp + rename) so a crash mid-write never
    leaves a corrupt state file.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> dict[str, JobRecord]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return {item["job_id"]: JobRecord.from_dict(item) for item in raw.get("jobs", [])}

    def save(self, records: dict[str, JobRecord]) -> None:
        payload = {"jobs": [rec.to_dict() for rec in records.values()]}
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".job_state.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
