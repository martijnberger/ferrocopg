"""Isolate cursor lifetime and adapter overhead without changing production code."""

from __future__ import annotations

import argparse
import cProfile
import json
import os
import statistics
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

from query_profile import installed_files, positive
from run import driver_for, metadata

CASES = ("public-fresh", "public-reused", "adapter-fresh", "adapter-reused")
BOOKKEEPING_HELPERS = (
    "_refresh_client_encoding",
    "_refresh_session_timeout",
    "_update_transaction_state",
)


@contextmanager
def bookkeeping_control(conn, mode):
    """Ablate helper bodies only for this script's fixed autocommit SELECTs."""
    if mode == "normal":
        yield
        return
    if mode != "omit-helper-bodies":
        raise ValueError("unknown bookkeeping control")
    if (
        not conn.autocommit
        or conn.info.encoding != "utf-8"
        or conn._in_transaction
        or conn._transaction_failed
        or any(name in vars(conn) for name in BOOKKEEPING_HELPERS)
    ):
        raise ValueError(
            "bookkeeping control requires an unchanged UTF-8 idle connection"
        )
    for name in BOOKKEEPING_HELPERS:
        if not callable(getattr(conn, name)):
            raise ValueError(f"missing bookkeeping helper: {name}")

    def omitted(query, **kwargs):
        pass

    try:
        for name in BOOKKEEPING_HELPERS:
            setattr(conn, name, omitted)
        yield
    finally:
        for name in BOOKKEEPING_HELPERS:
            if name in vars(conn):
                delattr(conn, name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--backend", choices=("rust", "c"), default="rust")
    parser.add_argument(
        "--workload", choices=("constant", "parameterized"), required=True
    )
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--iterations", type=positive, default=10000)
    parser.add_argument("--samples", type=positive, default=9)
    parser.add_argument(
        "--bookkeeping",
        choices=("normal", "omit-helper-bodies"),
        default="normal",
        help="diagnostic ablation, not a compatible backend implementation",
    )
    args = parser.parse_args()
    if args.backend == "c" and args.case.startswith("adapter"):
        parser.error("adapter controls are Rust-specific diagnostic bypasses")
    if args.bookkeeping != "normal" and (
        args.backend != "rust" or not args.case.startswith("public")
    ):
        parser.error("bookkeeping ablation requires the Rust public API control")
    if (
        args.samples < 3
        or args.output.exists()
        or args.profile
        and args.profile.exists()
    ):
        parser.error("at least three samples and unused output/profile paths required")
    os.environ["PSYCOPG_IMPL"] = "python" if args.backend == "rust" else "c"
    os.environ.pop("PSYCOPG_SOURCE_IMPL", None)
    driver = driver_for(args.backend)
    query, params = (
        ("select 42::int4", None)
        if args.workload == "constant"
        else ("select %s::int4 + 1", (41,))
    )
    with (
        driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn,
        ExitStack() as controls,
    ):
        environment = metadata(driver, conn, args)
        controls.enter_context(bookkeeping_control(conn, args.bookkeeping))
        if args.case.startswith("adapter"):
            from ferrocopg._ferrocopg import NoTlsCursorAdapter
            from ferrocopg.rows import tuple_row

            def cursor():
                return NoTlsCursorAdapter(conn, row_factory=tuple_row)
        else:
            cursor = conn.cursor
        reused = cursor() if args.case.endswith("reused") else None

        def operation():
            cur = reused if reused is not None else cursor()
            cur.execute(query, params, prepare=True, binary=True)
            if cur.fetchone() != (42,):
                raise AssertionError("incorrect result")

        for _ in range(1000):
            operation()
        samples = []
        if args.profile:
            profiler = cProfile.Profile()
            profiler.enable()
            for _ in range(args.iterations):
                operation()
            profiler.disable()
            profiler.dump_stats(str(args.profile))
        else:
            for _ in range(args.samples):
                cpu = time.process_time_ns()
                wall = time.perf_counter_ns()
                for _ in range(args.iterations):
                    operation()
                samples.append(
                    {
                        "wall_us": (time.perf_counter_ns() - wall)
                        / args.iterations
                        / 1000,
                        "cpu_us": (time.process_time_ns() - cpu)
                        / args.iterations
                        / 1000,
                    }
                )
        if reused is not None:
            reused.close()
    distributions = (
        ["ferrocopg"] if args.backend == "rust" else ["psycopg", "psycopg-c"]
    )
    report = {
        "mode": "integration-profile" if args.profile else "integration-diagnostic",
        "acceptance_evidence": False,
        "case": args.case,
        "backend": args.backend,
        "bookkeeping": args.bookkeeping,
        "omitted_helper_bodies": list(BOOKKEEPING_HELPERS)
        if args.bookkeeping != "normal"
        else [],
        "workload": args.workload,
        "iterations": args.iterations,
        "warmup": 1000,
        "binary": True,
        "prepared": True,
        "metadata": environment,
        "installed_file_sha256": {
            name: installed_files(name) for name in distributions
        },
        "samples": samples,
        "medians_us": {
            key: statistics.median(row[key] for row in samples)
            for key in ("wall_us", "cpu_us")
        }
        if samples
        else None,
        "profile": str(args.profile) if args.profile else None,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
