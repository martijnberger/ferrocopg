#!/usr/bin/env python3
"""Sparse installed-package query timings for diagnosis, never acceptance."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any

from run import BACKENDS, driver_for, metadata

PHASES = ("cursor", "execute", "fetch", "release")
QUERY = "select %s::int + 1"
PARAMS = (41,)


def positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def measure(conn: Any, iterations: int, *, phased: bool) -> dict[str, Any]:
    totals = [0] * len(PHASES)
    cpu_start = time.process_time_ns()
    wall_start = time.perf_counter_ns()
    for _ in range(iterations):
        if not phased:
            if conn.execute(QUERY, PARAMS).fetchone() != (42,):
                raise AssertionError("incorrect query result")
            continue
        t0 = time.perf_counter_ns()
        cur = conn.cursor()
        t1 = time.perf_counter_ns()
        cur.execute(QUERY, PARAMS)
        t2 = time.perf_counter_ns()
        if cur.fetchone() != (42,):
            raise AssertionError("incorrect query result")
        t3 = time.perf_counter_ns()
        del cur
        t4 = time.perf_counter_ns()
        for index, elapsed in enumerate((t1 - t0, t2 - t1, t3 - t2, t4 - t3)):
            totals[index] += elapsed
    wall = time.perf_counter_ns() - wall_start
    cpu = time.process_time_ns() - cpu_start
    return {
        "wall_us": wall / iterations / 1000,
        "cpu_us": cpu / iterations / 1000,
        "phases_us": dict(zip(PHASES, (n / iterations / 1000 for n in totals)))
        if phased
        else None,
    }


def installed_files(distribution: str) -> dict[str, str]:
    """Fingerprint installed implementation files, not a claimed wheel checksum."""
    dist = importlib.metadata.distribution(distribution)
    return {
        str(file): hashlib.sha256(Path(dist.locate_file(file)).read_bytes()).hexdigest()
        for file in dist.files or ()
        if str(file).endswith((".py", ".so", ".pyd"))
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=BACKENDS, required=True)
    parser.add_argument(
        "--workload", choices=("prepared", "parameterized"), required=True
    )
    parser.add_argument("--revision", required=True)
    parser.add_argument("--iterations", type=positive, default=10000)
    parser.add_argument("--samples", type=positive, default=9)
    parser.add_argument("--warmup", type=positive, default=1000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Select before importing either driver, as in the acceptance workers.
    os.environ["PSYCOPG_IMPL"] = "python" if args.backend == "rust" else args.backend
    driver = driver_for(args.backend)
    distribution = "ferrocopg" if args.backend == "rust" else "psycopg"
    packages = {distribution: installed_files(distribution)}
    if args.backend == "c":
        packages["psycopg-c"] = installed_files("psycopg-c")
    samples: dict[str, list[dict[str, Any]]] = {"public": [], "phased": []}
    with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
        environment = metadata(driver, conn, args)
        conn.prepare_threshold = 0 if args.workload == "prepared" else None
        measure(conn, args.warmup, phased=False)
        measure(conn, args.warmup, phased=True)
        for sample in range(args.samples):
            # Alternate probe order without conflating the instrumented and public cases.
            for phased in (False, True) if sample % 2 == 0 else (True, False):
                samples["phased" if phased else "public"].append(
                    measure(conn, args.iterations, phased=phased)
                )
    report = {
        "schema": 1,
        "mode": "query-diagnostic",
        "acceptance_evidence": False,
        "backend": args.backend,
        "workload": args.workload,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "metadata": environment,
        "installed_file_sha256": packages,
        "samples": samples,
        "medians_us": {
            case: {
                key: statistics.median(row[key] for row in rows)
                for key in ("wall_us", "cpu_us")
            }
            for case, rows in samples.items()
        },
        "phase_medians_us": {
            phase: statistics.median(
                row["phases_us"][phase] for row in samples["phased"]
            )
            for phase in PHASES
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
