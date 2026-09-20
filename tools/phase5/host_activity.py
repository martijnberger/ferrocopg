"""Sample host contention during diagnostics without recording process arguments."""

from __future__ import annotations

import json
import threading
import time

import psutil


def snapshot():
    processes, unavailable = {}, 0
    for proc in psutil.process_iter():
        try:
            with proc.oneshot():
                times = proc.cpu_times()
                created = proc.create_time()
                processes[(proc.pid, created)] = {
                    "pid": proc.pid,
                    "created": created,
                    "ppid": proc.ppid(),
                    "name": proc.name(),
                    "cpu_seconds": times.user + times.system,
                }
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            unavailable += 1
    return {
        "monotonic": time.monotonic(),
        "unix_time": time.time(),
        "cpu_times": psutil.cpu_times()._asdict(),
        "processes": processes,
        "unavailable": unavailable,
    }


def interval_record(before, after):
    elapsed = after["monotonic"] - before["monotonic"]
    if elapsed <= 0:
        raise ValueError("host-activity clock did not advance")
    active, new = [], []
    for key, current in after["processes"].items():
        previous = before["processes"].get(key)
        if previous is None:
            new.append(current)
            continue
        cpu = current["cpu_seconds"] - previous["cpu_seconds"]
        if cpu < 0:
            raise ValueError("process CPU time decreased")
        if cpu:
            active.append(
                {
                    **current,
                    "interval_cpu_seconds": cpu,
                    "one_core_percent": cpu / elapsed * 100,
                }
            )
    return {
        "unix_time": after["unix_time"],
        "elapsed_seconds": elapsed,
        "system_cpu_seconds": {
            key: value - before["cpu_times"][key]
            for key, value in after["cpu_times"].items()
        },
        "system_cpu_totals": after["cpu_times"],
        "active_processes": sorted(
            active, key=lambda row: (-row["interval_cpu_seconds"], row["pid"])
        ),
        "new_processes": new,
        "disappeared_processes": [
            value
            for key, value in before["processes"].items()
            if key not in after["processes"]
        ],
        "unavailable_processes": after["unavailable"],
    }


class HostActivity:
    """A diagnostic observer, not a pass/fail host-idleness classifier."""

    def __init__(self, path, interval=2.0):
        if interval <= 0:
            raise ValueError("positive host-activity interval required")
        self.path = path
        self.interval = interval
        self.stop = threading.Event()
        self.error = None
        self.thread = None

    def __enter__(self):
        self.output = self.path.open("x")
        try:
            self.previous = snapshot()
            self.write(
                {
                    "mode": "host-activity",
                    "interval_seconds": self.interval,
                    "initial_unix_time": self.previous["unix_time"],
                    "initial_cpu_totals": self.previous["cpu_times"],
                    "initial_unavailable_processes": self.previous["unavailable"],
                    "coordinator_pid": psutil.Process().pid,
                    "initial_processes": list(self.previous["processes"].values()),
                    "limitations": [
                        "sampling can miss processes that start and exit between snapshots",
                        "new and exited processes lack complete interval CPU accounting",
                        "process names and parent PIDs do not prove workload ownership",
                        "the observer adds overhead; its thread CPU time is recorded",
                        "observer thread CPU excludes the initial coordinating-thread snapshot",
                        "no automatic idle verdict; review all intervals and system CPU deltas",
                    ],
                }
            )
            self.thread = threading.Thread(target=self.collect, daemon=True)
            self.thread.start()
        except BaseException:
            self.output.close()
            raise
        return self

    def write(self, record):
        self.output.write(json.dumps(record) + "\n")
        self.output.flush()

    def collect(self):
        started = time.thread_time()
        samples = 0
        try:
            while True:
                finished = self.stop.wait(self.interval)
                current = snapshot()
                self.write(interval_record(self.previous, current))
                self.previous = current
                samples += 1
                if finished:
                    break
            self.write(
                {
                    "status": "completed",
                    "samples": samples,
                    "observer_thread_cpu_seconds": time.thread_time() - started,
                }
            )
        except BaseException as exc:
            self.error = exc

    def __exit__(self, exc_type, exc, traceback):
        self.stop.set()
        self.thread.join()
        self.output.close()
        if self.error is not None:
            raise RuntimeError("host-activity sampling failed") from self.error
