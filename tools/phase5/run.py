#!/usr/bin/env python3
"""Measure installed packages in isolated processes; enforce the Phase 5 contract."""

from __future__ import annotations

import argparse
import gc
import importlib
import importlib.metadata
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workloads import BENCHMARKS, SOAKS, workload

BACKENDS = ("rust", "python", "c")
SCHEMA = 1
PERFORMANCE_POLICY = "beta-2026-09-20"
PERFORMANCE_LIMITS = {"python": 1.15, "c": 1.50}


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def driver_for(backend: str) -> Any:
    driver = importlib.import_module("ferrocopg" if backend == "rust" else "psycopg")
    expected = "ferrocopg" if backend == "rust" else backend
    if driver.pq.__impl__ != expected:
        raise RuntimeError(f"requested {backend}, loaded {driver.pq.__impl__}")
    return driver


def snapshot(process: Any, observer: Any, app: str) -> dict[str, int]:
    gc.collect()
    objects = gc.get_objects()
    owned = sum(
        str(type(obj).__module__).split(".")[0]
        in ("ferrocopg", "psycopg", "psycopg_pool")
        for obj in objects
    )
    del objects
    return {
        "rss_bytes": process.memory_info().rss,
        "threads": process.num_threads(),
        "sockets": len(process.net_connections(kind="inet")),
        "fds": process.num_fds(),
        "driver_objects": owned,
        "sessions": observer.execute(
            "select count(*) from pg_stat_activity where application_name = %s", (app,)
        ).fetchone()[0],
    }


def growth_failures(samples: list[dict[str, int]]) -> list[str]:
    if len(samples) < 6:
        return ["at least six post-warmup resource samples are required"]
    failures = []
    # RSS can retain allocator arenas; bound both retained memory and late growth.
    limits = {
        "rss_bytes": 16 * 1024**2,
        "driver_objects": 32,
        "threads": 0,
        "sockets": 0,
        "fds": 0,
        "sessions": 0,
    }
    for key, allowance in limits.items():
        first = statistics.median(s[key] for s in samples[:3])
        last = statistics.median(s[key] for s in samples[-3:])
        if last - first > allowance:
            failures.append(f"{key} grew by {last - first:g} (limit {allowance})")
        if key == "sessions" and any(s[key] for s in samples):
            failures.append("server sessions survived workload cleanup")
        if (
            key in ("threads", "sockets", "fds")
            and max(s[key] for s in samples) > first
        ):
            failures.append(f"{key} exceeded its warmed cleanup baseline")
    return failures


def settle_snapshot(process: Any, observer: Any, app: str) -> dict[str, int]:
    deadline = time.monotonic() + 5
    previous = None
    while True:
        sample = snapshot(process, observer, app)
        if (
            sample == previous and sample["sessions"] == 0
        ) or time.monotonic() >= deadline:
            return sample
        previous = sample
        time.sleep(0.05)


def metadata(driver: Any, observer: Any, args: argparse.Namespace) -> dict[str, Any]:
    import psutil

    return {
        "revision": args.revision,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_logical": psutil.cpu_count(),
        "cpu_physical": psutil.cpu_count(logical=False),
        "memory_bytes": psutil.virtual_memory().total,
        "driver": driver.__version__,
        "driver_file": driver.__file__,
        "pool": importlib.metadata.version("psycopg-pool"),
        "psutil": psutil.__version__,
        "libpq": driver.pq.version(),
        "server": observer.execute("select version()").fetchone()[0],
        "server_settings": dict(
            observer.execute(
                "select name, setting from pg_settings where name in "
                "('shared_buffers','max_connections','ssl','fsync','synchronous_commit')"
            ).fetchall()
        ),
        "vendored_revision": getattr(driver, "__vendored_psycopg_revision__", None),
    }


def worker(args: argparse.Namespace) -> dict[str, Any]:
    import psutil

    driver = driver_for(args.backend)
    dsn = os.environ["PHASE5_DSN"]
    app = "phase5-" + uuid.uuid4().hex
    from importlib import import_module

    conninfo = import_module(driver.__name__ + ".conninfo")
    dsn = conninfo.make_conninfo(dsn, application_name=app)
    process = psutil.Process()
    # A separate official-Psycopg observer is excluded by its application name.
    import psycopg

    with psycopg.connect(
        os.environ["PHASE5_DSN"],
        autocommit=True,
        application_name="phase5-observer",
        connect_timeout=10,
    ) as observer:
        result: dict[str, Any] = {
            "schema": SCHEMA,
            "mode": args.mode,
            "backend": args.backend,
            "metadata": metadata(driver, observer, args),
            "failures": [],
        }
        if args.mode == "benchmark":
            result["workload"] = args.workload
            durations, cpu_samples, latencies = [], [], []
            with workload(driver, dsn, args.workload, args.rows) as item:
                for _ in range(args.warmup):
                    item.run()
                for _ in range(args.samples):
                    cpu_start = time.process_time_ns()
                    start = time.perf_counter_ns()
                    units = 0
                    for _ in range(args.iterations):
                        operation_start = time.perf_counter_ns()
                        units += item.run()
                        latencies.append(
                            (time.perf_counter_ns() - operation_start) / 1e6
                        )
                    elapsed = (time.perf_counter_ns() - start) / 1e9
                    cpu_samples.append((time.process_time_ns() - cpu_start) / 1e9)
                    durations.append(elapsed)
            result.update(
                {
                    "iterations": args.iterations,
                    "rows": args.rows,
                    "seconds": durations,
                    "cpu_seconds": cpu_samples,
                    "operation_latency_ms": latencies,
                    "latency_ms": {
                        "p50": statistics.median(latencies),
                        "p95": percentile(latencies, 0.95),
                        "p99": percentile(latencies, 0.99),
                    },
                    "units_per_second": units / statistics.median(durations),
                }
            )
        else:
            operations = {name: 0 for name in SOAKS}

            def epoch() -> None:
                for name in SOAKS:
                    with workload(driver, dsn, name, args.rows) as item:
                        for _ in range(args.iterations):
                            item.run()
                            operations[name] += 1

            for _ in range(3):
                epoch()
            resources = [settle_snapshot(process, observer, app)]
            start = time.monotonic()
            while time.monotonic() - start < args.seconds or len(resources) < 6:
                epoch()
                resources.append(settle_snapshot(process, observer, app))
            result.update(
                {
                    "elapsed_seconds": time.monotonic() - start,
                    "operations": operations,
                    "resources": resources,
                    "failures": growth_failures(resources),
                }
            )
        result["cleanup"] = settle_snapshot(process, observer, app)
        if result["cleanup"]["sessions"]:
            result["failures"].append("server sessions remain after cleanup")
        return result


def compare(results: list[dict[str, Any]]) -> dict[str, Any]:
    ratios, failures = {}, []
    for name in BENCHMARKS:
        rows = [r for r in results if r.get("workload") == name]
        group = {r["backend"]: r for r in rows}
        if len(rows) != len(BACKENDS) or set(group) != set(BACKENDS):
            failures.append(f"{name}: missing backend measurements")
            continue
        if any(r.get("failures") for r in group.values()):
            failures.append(f"{name}: worker failed")
            continue
        if any(
            len(r.get("seconds", [])) < 3
            or any(not math.isfinite(n) or n <= 0 for n in r["seconds"])
            for r in group.values()
        ):
            failures.append(f"{name}: invalid timing samples")
            continue
        medians = {b: statistics.median(r["seconds"]) for b, r in group.items()}
        ratios[name] = {b: medians["rust"] / medians[b] for b in ("python", "c")}
        for backend, limit in PERFORMANCE_LIMITS.items():
            ratio = ratios[name][backend]
            if ratio > limit:
                failures.append(f"{name}: rust/{backend}={ratio:.3f} exceeds {limit}")
    return {
        "performance_policy": PERFORMANCE_POLICY,
        "performance_limits": dict(PERFORMANCE_LIMITS),
        "ratios": ratios,
        "failures": failures,
    }


def child(
    args: argparse.Namespace, backend: str, name: str, output: Path
) -> dict[str, Any]:
    import psutil

    env = dict(os.environ, PSYCOPG_IMPL="python" if backend == "rust" else backend)
    env.pop("PSYCOPG_SOURCE_IMPL", None)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        args.mode,
        "--worker",
        "--backend",
        backend,
        "--workload",
        name if args.mode == "benchmark" else "parameterized",
        "--output",
        str(output),
        "--seconds",
        str(args.seconds),
        "--iterations",
        str(args.iterations),
        "--samples",
        str(args.samples),
        "--warmup",
        str(args.warmup),
        "--rows",
        str(args.rows),
        "--revision",
        args.revision,
    ]
    log = output.with_suffix(".log")
    with log.open("w") as stream:
        proc = subprocess.Popen(command, env=env, stdout=stream, stderr=stream)
        monitor = psutil.Process(proc.pid)
        start, peak_rss = time.monotonic(), 0
        timed_out = False
        while proc.poll() is None:
            try:
                peak_rss = max(peak_rss, monitor.memory_info().rss)
            except psutil.NoSuchProcess:
                pass
            if time.monotonic() - start > args.timeout:
                timed_out = True
                proc.kill()
                break
            time.sleep(0.02)
        code = proc.wait()
    if output.exists():
        result = json.loads(output.read_text())
    else:
        result = {"backend": backend, "workload": name, "failures": []}
    if code or timed_out:
        result["failures"].append(
            f"worker exit={code}; timeout={timed_out}; see {log.name}"
        )
    result["peak_rss_bytes"] = peak_rss
    output.write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("benchmark", "soak"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--seconds", type=float, default=1800)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=2400)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--backend", choices=BACKENDS, default="rust")
    parser.add_argument("--workload", choices=BENCHMARKS, default="parameterized")
    args = parser.parse_args()
    if not os.environ.get("PHASE5_DSN"):
        parser.error("set PHASE5_DSN to a dedicated test database with TLS enabled")
    if min(args.iterations, args.samples, args.rows, args.seconds, args.timeout) <= 0:
        parser.error("run sizes and timeouts must be positive")
    if args.samples < 3 or args.warmup < 1:
        parser.error("at least three samples and one warmup are required")
    if args.worker:
        try:
            result = worker(args)
        except Exception:
            traceback.print_exc()
            return 1
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        return bool(result["failures"])
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    names = BENCHMARKS if args.mode == "benchmark" else ("soak",)
    for index, name in enumerate(names):
        # Rotate order to avoid always giving one backend the cold server.
        order = BACKENDS[index % 3 :] + BACKENDS[: index % 3]
        for backend in order:
            print(f"{args.mode}: {name}/{backend}", flush=True)
            result = child(args, backend, name, args.output / f"{name}-{backend}.json")
            results.append(result)
    verdict = (
        compare(results)
        if args.mode == "benchmark"
        else {
            "failures": [f"{r['backend']}: {f}" for r in results for f in r["failures"]]
        }
    )
    report = {
        "schema": SCHEMA,
        "mode": args.mode,
        "revision": args.revision,
        "configuration": {k: v for k, v in vars(args).items() if k != "output"},
        "results": results,
        **verdict,
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(verdict, indent=2))
    return bool(verdict["failures"])


if __name__ == "__main__":
    raise SystemExit(main())
