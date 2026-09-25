"""Worker entrypoint executed as a local child process.

Simulates several kinds of short jobs purely with local resources:

- ``sleep``: idle for ``duration_ms`` then exit 0.
- ``alloc``: hold ``memory_mb`` of memory for ``duration_ms`` then exit 0.
- ``fail``: exit with a non-zero code (simulates abnormal exit).
- ``spawn``: derive ``max_procs`` child processes, wait for them, exit 0.
- ``hang``: run 10x longer than ``duration_ms`` (deterministically triggers
  the supervisor timeout).

Every log line carries the job id and the input constraints so test logs
are auditable. No network, credentials or external services are used.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

FAIL_EXIT_CODE = 3


def _log(job_id: str, message: str) -> None:
    print(f"[worker][job={job_id}] {message}", flush=True)


def _run_sleep(job_id: str, duration_ms: int, memory_mb: int, max_procs: int) -> int:
    _log(job_id, f"type=sleep constraints(duration_ms={duration_ms}, memory_mb={memory_mb}, max_procs={max_procs})")
    time.sleep(duration_ms / 1000.0)
    _log(job_id, "sleep done")
    return 0


def _run_alloc(job_id: str, duration_ms: int, memory_mb: int, max_procs: int) -> int:
    _log(job_id, f"type=alloc constraints(duration_ms={duration_ms}, memory_mb={memory_mb}, max_procs={max_procs})")
    block = bytearray(max(memory_mb, 0) * 1024 * 1024)
    for i in range(0, len(block), 4096):  # touch pages so memory is really resident
        block[i] = 1
    time.sleep(duration_ms / 1000.0)
    _log(job_id, f"alloc done, released {memory_mb}MiB")
    return 0


def _run_fail(job_id: str, duration_ms: int, memory_mb: int, max_procs: int) -> int:
    _log(job_id, f"type=fail constraints(duration_ms={duration_ms}, memory_mb={memory_mb}, max_procs={max_procs})")
    time.sleep(duration_ms / 1000.0)
    _log(job_id, f"exiting abnormally with code {FAIL_EXIT_CODE}")
    return FAIL_EXIT_CODE


def _run_spawn(job_id: str, duration_ms: int, memory_mb: int, max_procs: int) -> int:
    _log(job_id, f"type=spawn constraints(duration_ms={duration_ms}, memory_mb={memory_mb}, max_procs={max_procs})")
    children = []
    for index in range(max_procs):
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(%f)" % (duration_ms / 1000.0)]
        )
        children.append(child)
        _log(job_id, f"derived child {index} pid={child.pid}")
    for child in children:
        child.wait()
    _log(job_id, f"all {len(children)} derived children finished")
    return 0


def _run_hang(job_id: str, duration_ms: int, memory_mb: int, max_procs: int) -> int:
    _log(job_id, f"type=hang constraints(duration_ms={duration_ms}, memory_mb={memory_mb}, max_procs={max_procs})")
    time.sleep(duration_ms * 10 / 1000.0)
    _log(job_id, "hang done (should have been timed out)")
    return 0


_HANDLERS = {
    "hang": _run_hang,
    "sleep": _run_sleep,
    "alloc": _run_alloc,
    "fail": _run_fail,
    "spawn": _run_spawn,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="jobsupervisor.worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--type", required=True, choices=sorted(_HANDLERS))
    parser.add_argument("--duration-ms", type=int, default=100)
    parser.add_argument("--memory-mb", type=int, default=0)
    parser.add_argument("--max-procs", type=int, default=0)
    args = parser.parse_args(argv)
    return _HANDLERS[args.type](args.job_id, args.duration_ms, args.memory_mb, args.max_procs)


if __name__ == "__main__":
    raise SystemExit(main())
