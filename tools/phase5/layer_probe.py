"""Matched native/public query diagnostic, not a release benchmark or CPU profile."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from query_profile import installed_files, positive
from run import driver_for, metadata

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_LAYERS = ("binding", "python", "c", "rust")
LAYERS = ("libpq", "postgres", "postgres-wait", "session", *PUBLIC_LAYERS)
QUERY = "select 42::int4"
PARAM_QUERY = "select $1::int + 1"
CASES = ("constant", "prepared", "unprepared")


def validate_dsn(dsn: str) -> None:
    from psycopg.conninfo import conninfo_to_dict

    if conninfo_to_dict(dsn).get("sslmode") != "disable":
        raise ValueError("matched native probes require explicit sslmode=disable")


def parameter_protocol(cursor: Any) -> tuple[list[int], list[int]]:
    # Official execute need not retain its optional query cache after dumping.
    adapted = cursor._query if cursor._query is not None else cursor._tx
    return list(adapted.types), list(adapted.formats)


def public_probe(args: argparse.Namespace) -> dict[str, Any]:
    backend = "rust" if args.worker == "binding" else args.worker
    os.environ["PSYCOPG_IMPL"] = "python" if backend == "rust" else backend
    driver = driver_for(backend)
    validate_dsn(os.environ["PHASE5_DSN"])
    parameterized = args.case != "constant"
    prepared = args.case != "unprepared"
    binary = args.result_format == "binary"
    query_text = PARAM_QUERY if parameterized else QUERY
    expected = b"\0\0\0*" if binary else b"42"
    timings = []
    with ExitStack() as stack:
        if args.worker == "binding":
            from ferrocopg._rust import _ferrocopg as native

            with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as observer:
                environment = metadata(driver, observer, args)
            session = native.connect_no_tls_session(os.environ["PHASE5_DSN"])
            stack.callback(session.close)
            params = [(21, True, b"\0)")] if parameterized else []
            statement = (
                session.prepare_params(
                    query_text, [21] if parameterized else []
                ).statement_id
                if prepared
                else None
            )

            def query() -> None:
                result = (
                    session.run_prepared_params_format(statement, params, binary)
                    if prepared
                    else session.run_params_format(query_text, params, binary)
                )
                if result.row_count != 1 or result.get_value(0, 0) != expected:
                    raise AssertionError("incorrect query result")
        else:
            conn = stack.enter_context(
                driver.connect(os.environ["PHASE5_DSN"], autocommit=True)
            )
            environment = metadata(driver, conn, args)
            query_text = query_text.replace("$1", "%s")
            params = (41,) if parameterized else None
            # Check actual adaptation/result formats outside the measured loop.
            with conn.execute(
                query_text, params, prepare=prepared, binary=binary
            ) as check:
                if check.fetchone() != (42,):
                    raise AssertionError("incorrect setup query result")
                if (
                    check.pgresult.ftype(0) != 23
                    or check.pgresult.fformat(0) != int(binary)
                    or (parameterized and parameter_protocol(check) != ([21], [1]))
                ):
                    raise AssertionError("query parameter/result protocol mismatch")

            def query() -> None:
                if conn.execute(
                    query_text, params, prepare=prepared, binary=binary
                ).fetchone() != (42,):
                    raise AssertionError("incorrect query result")

        for _ in range(1000):
            query()
        for _ in range(args.samples):
            start = time.perf_counter_ns()
            for _ in range(args.iterations):
                query()
            timings.append((time.perf_counter_ns() - start) / args.iterations / 1000)
    distributions = ["ferrocopg"] if backend == "rust" else ["psycopg"]
    if args.worker == "c":
        distributions.append("psycopg-c")
    return {
        "backend": args.worker,
        "iterations": args.iterations,
        "warmup": 1000,
        "case": args.case,
        "result_format": args.result_format,
        "parameter_oids": [21] if parameterized else [],
        "wall_us": timings,
        "metadata": environment,
        "installed_file_sha256": {
            name: installed_files(name) for name in distributions
        },
    }


def validate(
    result: dict[str, Any],
    layer: str,
    iterations: int,
    samples: int,
    case: str | None = None,
    result_format: str | None = None,
) -> None:
    if (
        result["backend"] != layer
        or result["iterations"] != iterations
        or result["warmup"] != 1000
        or len(result["wall_us"]) != samples
        or any(not math.isfinite(n) or n <= 0 for n in result["wall_us"])
    ):
        raise ValueError(f"invalid {layer} diagnostic")
    if case is not None and (
        case not in CASES
        or result_format not in ("binary", "text")
        or result.get("case") != case
        or result.get("result_format") != result_format
        or result.get("parameter_oids") != ([21] if case != "constant" else [])
    ):
        raise ValueError(f"invalid {layer} query protocol")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-rust", type=Path)
    parser.add_argument("--native-libpq", type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=positive, default=10000)
    parser.add_argument("--samples", type=positive, default=9)
    parser.add_argument("--case", choices=CASES, default="constant")
    parser.add_argument("--result-format", choices=("binary", "text"), default="binary")
    parser.add_argument("--worker", choices=PUBLIC_LAYERS, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.worker:
        validate_dsn(os.environ["PHASE5_DSN"])
    if args.samples < 3:
        parser.error("at least three samples required")
    if args.output.exists():
        parser.error("output already exists; preserve previous attempts")
    if args.worker:
        args.output.write_text(json.dumps(public_probe(args), indent=2) + "\n")
        return 0
    if not args.native_rust or not args.native_libpq:
        parser.error("build and supply both native probe binaries first")
    args.output.mkdir(parents=True)
    report: dict[str, Any] = {
        "mode": "layer-diagnostic",
        "acceptance_evidence": False,
        "revision": args.revision,
        "python": sys.version,
        "platform": platform.platform(),
        "protocol": {
            "query": QUERY if args.case == "constant" else PARAM_QUERY,
            "prepared": args.case != "unprepared",
            "result_format": args.result_format,
            "in_flight": 1,
            "iterations": args.iterations,
            "samples": args.samples,
            "warmup": 1000,
            "tls": False,
        },
        "sha256": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                args.native_rust,
                args.native_libpq,
                ROOT / "Cargo.lock",
                ROOT / "crates/ferrocopg-postgres/examples/layer_probe.rs",
                ROOT / "tools/phase5/libpq_probe.c",
                Path(__file__),
            )
        },
        "orders": {},
        "failures": [],
    }
    if args.case != "constant" or args.result_format != "binary":
        report["protocol"].update(
            case=args.case,
            parameter_oids=[21] if args.case != "constant" else [],
            parameter_format="binary" if args.case != "constant" else None,
        )
    try:
        for order, layers in (("forward", LAYERS), ("reverse", LAYERS[::-1])):
            measurements = report["orders"][order] = {}
            for layer in layers:
                print(f"{order}: {layer}", flush=True)
                output = args.output / f"{order}-{layer}.json"
                if layer in PUBLIC_LAYERS:
                    command = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--worker",
                        layer,
                        "--revision",
                        args.revision,
                        "--output",
                        str(output),
                        "--iterations",
                        str(args.iterations),
                        "--samples",
                        str(args.samples),
                        "--case",
                        args.case,
                        "--result-format",
                        args.result_format,
                    ]
                else:
                    command = (
                        [str(args.native_libpq)]
                        if layer == "libpq"
                        else [str(args.native_rust), layer]
                    )
                    command += [
                        str(args.iterations),
                        str(args.samples),
                        args.case,
                        args.result_format,
                    ]
                env = dict(os.environ)
                env.pop("PSYCOPG_SOURCE_IMPL", None)
                result = subprocess.run(
                    command, env=env, capture_output=True, text=True, timeout=600
                )
                output.with_suffix(".log").write_text(result.stderr)
                if result.returncode:
                    raise RuntimeError(
                        f"{layer} failed with {result.returncode}; see {output.with_suffix('.log')}"
                    )
                if layer not in PUBLIC_LAYERS:
                    output.write_text(result.stdout)
                raw = json.loads(output.read_text())
                validate(
                    raw,
                    layer,
                    args.iterations,
                    args.samples,
                    args.case,
                    args.result_format,
                )
                measurements[layer] = {
                    "wall_us": raw["wall_us"],
                    "median_us": statistics.median(raw["wall_us"]),
                }
    except Exception as exc:
        report["failures"].append(str(exc))
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
