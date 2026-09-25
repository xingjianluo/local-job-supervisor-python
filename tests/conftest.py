from __future__ import annotations

import os
import time

import pytest

from jobsupervisor.models import SupervisorConfig
from jobsupervisor.supervisor import Supervisor


@pytest.fixture()
def config(tmp_path):
    return SupervisorConfig(
        max_concurrency=2,
        max_queue_size=8,
        max_memory_mb_total=256,
        max_memory_mb_per_job=128,
        max_procs_per_job=3,
        max_duration_ms=10_000,
        timeout_grace_ms=400,
        state_file=str(tmp_path / "job_state.json"),
    )


@pytest.fixture()
def supervisor(config):
    sup = Supervisor(config)
    sup.recover()
    yield sup
    sup.shutdown()


def wait_for(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def worker_pids(job_id: str) -> list[int]:
    """PIDs of local worker processes (and their children) for a job id."""
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as fh:
                cmdline = fh.read().decode(errors="replace")
        except OSError:
            continue
        if job_id in cmdline:
            pids.append(int(entry))
    return pids
