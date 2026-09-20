"""Separate installed-wheel latency, CPU, allocation, and syscall diagnostics."""

from __future__ import annotations

import argparse
import cProfile
import hashlib
import json
import math
import os
import platform
import pstats
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

from layer_probe import LAYERS, PUBLIC_LAYERS, QUERY
from layer_probe import validate as validate_layer
from protocol_probe import WORKLOADS as PROTOCOL_WORKLOADS
from protocol_probe import validate as validate_protocol
from query_profile import installed_files, positive
from repeat_benchmark import identity
from run import BACKENDS, driver_for, metadata
from workloads import BENCHMARKS, workload

WORKLOADS = (*BENCHMARKS, "pipeline")
KINDS = ("timing", "calls", "allocations", "syscalls")
WARMUP = 10
SAMPLES = 9
ROWS = 1000
SYSCALLS = (
    "%network,read,write,readv,writev,poll,ppoll,select,pselect6,"
    "epoll_wait,epoll_pwait,epoll_ctl,futex"
)


def iterations_for(kind, name):
    if name in ("parameterized", "prepared"):
        return 10000 if kind in ("timing", "calls") else 1000
    return 100 if kind == "timing" else 20


def parse_syscalls(raw):
    """Keep errors and counts, without equating syscalls with protocol messages."""
    counts = {}
    for line in raw.splitlines():
        fields = line.split()
        if not fields or not re.fullmatch(r"\d+\.\d+", fields[0]):
            continue
        if fields[-1] == "total":
            continue
        if len(fields) not in (5, 6):
            raise ValueError(f"unrecognized strace summary: {line}")
        name = fields[-1]
        count, errors = int(fields[3]), int(fields[4]) if len(fields) == 6 else 0
        seconds = float(fields[1])
        if (
            name in counts
            or count <= 0
            or not 0 <= errors <= count
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            raise ValueError(f"invalid strace counts: {line}")
        counts[name] = {
            "calls": count,
            "errors": errors,
            "instrumented_seconds": seconds,
        }
    if not counts:
        raise ValueError("strace produced no syscall counts")
    return counts


def worker(args):
    os.environ["PSYCOPG_IMPL"] = "python" if args.backend == "rust" else args.backend
    os.environ.pop("PSYCOPG_SOURCE_IMPL", None)
    tracer_pid = None
    if args.kind == "syscalls":
        tracer_pid = next(
            (
                int(line.split()[1])
                for line in Path("/proc/self/status").read_text().splitlines()
                if line.startswith("TracerPid:")
            ),
            0,
        )
        if not tracer_pid:
            raise RuntimeError("syscall workers must actually run under strace")
    driver = driver_for(args.backend)
    dsn = os.environ["PHASE5_DSN"]
    with driver.connect(dsn, autocommit=True) as observer:
        environment = metadata(driver, observer, args)
    samples = []
    artifact = None
    with workload(driver, dsn, args.workload, ROWS) as operation:
        for _ in range(WARMUP):
            operation.run()
        if args.kind == "timing":
            for _ in range(SAMPLES):
                cpu, wall = time.process_time_ns(), time.perf_counter_ns()
                for _ in range(args.iterations):
                    operation.run()
                samples.append(
                    {
                        "wall_seconds": (time.perf_counter_ns() - wall) / 1e9,
                        "cpu_seconds": (time.process_time_ns() - cpu) / 1e9,
                    }
                )
        elif args.kind == "calls":
            artifact = args.output.with_suffix(".pstats")
            profiler = cProfile.Profile(timer=time.thread_time)
            profiler.enable()
            try:
                for _ in range(args.iterations):
                    operation.run()
            finally:
                profiler.disable()
            profiler.dump_stats(str(artifact))
        elif args.kind == "allocations":
            import memray

            artifact = args.output.with_suffix(".bin")
            with memray.Tracker(
                artifact, native_traces=True, trace_python_allocators=True
            ):
                for _ in range(args.iterations):
                    operation.run()
        else:
            for _ in range(args.iterations):
                operation.run()
    distributions = ["ferrocopg"] if args.backend == "rust" else ["psycopg"]
    if args.backend == "c":
        distributions.append("psycopg-c")
    result = {
        "mode": "attribution",
        "acceptance_evidence": False,
        "kind": args.kind,
        "backend": args.backend,
        "workload": args.workload,
        "iterations": args.iterations,
        "warmup": WARMUP,
        "rows": ROWS,
        "samples": samples,
        "instrumented": args.kind != "timing",
        "metadata": environment,
        "installed_file_sha256": {
            name: installed_files(name) for name in distributions
        },
        "artifact": artifact.name if artifact else None,
    }
    if args.kind == "calls":
        stats = pstats.Stats(str(artifact))
        result["profile_scope"] = (
            "main-thread CPU only; native internals folded; not query latency"
        )
        result["functions"] = [
            {
                "filename": key[0],
                "line": key[1],
                "function": key[2],
                "calls": value[1],
                "self_cpu_seconds": value[2],
                "cumulative_cpu_seconds": value[3],
            }
            for key, value in sorted(
                stats.stats.items(), key=lambda item: item[1][3], reverse=True
            )
        ]
    elif args.kind == "allocations":
        result["profile_scope"] = (
            "all-thread Python and native allocator events during warmed operations; "
            "allocator layers are not distinct user-object counts; not query latency"
        )
        result["memray_version"] = memray.__version__
    elif args.kind == "syscalls":
        result["tracer_pid"] = tracer_pid
        result["profile_scope"] = (
            "whole child process including imports, setup, warmup, operations, and cleanup; "
            "syscall counts are not protocol round trips or ordinary latency"
        )
    return result


def validate_worker(result, revision, backend, name, kind):
    samples = result["samples"]
    artifact = result.get("artifact")
    suffix = {"calls": ".pstats", "allocations": ".bin"}.get(kind)
    if (
        result["mode"] != "attribution"
        or result["acceptance_evidence"] is not False
        or result["kind"] != kind
        or result["backend"] != backend
        or result["workload"] != name
        or result["iterations"] != iterations_for(kind, name)
        or result["warmup"] != WARMUP
        or result["rows"] != ROWS
        or result["metadata"]["revision"] != revision
        or result["instrumented"] is not (kind != "timing")
        or (
            suffix is not None
            and (
                not isinstance(artifact, str)
                or Path(artifact).name != artifact
                or not artifact.endswith(suffix)
            )
        )
        or (suffix is None and artifact is not None)
        or (kind == "calls" and not result.get("functions"))
        or (kind == "allocations" and result.get("memray_version") != "1.20.0")
        or (kind == "syscalls" and result.get("tracer_pid", 0) <= 0)
        or len(samples) != (SAMPLES if kind == "timing" else 0)
        or any(
            not math.isfinite(row[key]) or row[key] <= 0
            for row in samples
            for key in ("wall_seconds", "cpu_seconds")
        )
        or not result["installed_file_sha256"]
        or any(
            not files
            or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in files.values())
            for files in result["installed_file_sha256"].values()
        )
    ):
        raise ValueError("invalid attribution worker identity, scope, or samples")


def validate_allocations(result):
    """Check allocation events, not distinct objects or retained-memory growth."""
    for key in ("total_num_allocations", "total_bytes_allocated"):
        if type(result.get(key)) is not int or result[key] <= 0:
            raise ValueError("missing or invalid allocation totals")
    metadata = result.get("metadata", {})
    if (
        metadata.get("has_native_traces") is not True
        or metadata.get("trace_python_allocators") is not True
    ):
        raise ValueError("allocation capture lacks native or Python allocator traces")
    histogram = result.get("allocation_size_histogram", [])
    if not histogram or any(
        type(row.get("count")) is not int or row["count"] < 0 for row in histogram
    ):
        raise ValueError("invalid allocation histogram")
    if sum(row["count"] for row in histogram) != result["total_num_allocations"]:
        raise ValueError("allocation histogram does not match total")


def run_command(command, output, timeout=600):
    with output.open("w") as log:
        with subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        ) as process:
            try:
                status = process.wait(timeout=timeout)
            except BaseException:
                # A tracer or coordinator may own children; stop the entire owned group.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                raise
    if status:
        raise RuntimeError(f"command failed with {status}; see {output}")


def validate_layers(output, revision):
    summary = json.loads((output / "summary.json").read_text())
    if (
        summary["revision"] != revision
        or summary["mode"] != "layer-diagnostic"
        or summary["acceptance_evidence"] is not False
        or summary["failures"]
        or set(summary["orders"]) != {"forward", "reverse"}
        or len(summary["sha256"]) != 6
        or summary["protocol"]
        != {
            "query": QUERY,
            "prepared": True,
            "result_format": "binary",
            "in_flight": 1,
            "iterations": 10000,
            "samples": 9,
            "warmup": 1000,
            "tls": False,
        }
    ):
        raise ValueError("invalid layer diagnostic summary")
    environment = None
    fingerprints = {}
    for order in ("forward", "reverse"):
        if set(summary["orders"][order]) != set(LAYERS):
            raise ValueError("missing layer measurement")
        for layer in LAYERS:
            raw = json.loads((output / f"{order}-{layer}.json").read_text())
            validate_layer(raw, layer, 10000, 9)
            expected = {
                "wall_us": raw["wall_us"],
                "median_us": statistics.median(raw["wall_us"]),
            }
            if summary["orders"][order][layer] != expected:
                raise ValueError("inconsistent layer median")
            if layer in PUBLIC_LAYERS:
                if raw["metadata"]["revision"] != revision:
                    raise ValueError("wrong layer revision")
                current = environment_identity(raw)
                if environment is None:
                    environment = current
                elif environment != current:
                    raise ValueError("layer environment changed")
                backend = "rust" if layer == "binding" else layer
                files = raw["installed_file_sha256"]
                if fingerprints.setdefault(backend, files) != files:
                    raise ValueError("layer code changed")
    for path, digest in summary["sha256"].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
            raise ValueError("layer probe source or executable changed")
    return environment, fingerprints


def environment_identity(result):
    return {
        key: result["metadata"][key]
        for key in ("python", "platform", "machine", "server", "server_settings")
    }


def coordinate(args):
    if platform.system() != "Linux" or not shutil.which("strace"):
        raise ValueError(
            "the complete attribution coordinator requires Linux and strace"
        )
    if not args.wheel or not args.native_rust or not args.native_libpq:
        raise ValueError("supply the installed release wheel and both native probes")
    frozen = identity(args.wheel)
    args.output.mkdir()
    manifest = {
        "mode": "attribution",
        "acceptance_evidence": False,
        "revision": args.revision,
        "identity": frozen,
        "status": "running",
        "workers": [],
        "protocol_workers": [],
        "failures": [],
        "limitations": [
            "CPU profiles are main-thread only; allocator tracking includes background threads",
            "syscall traces include process setup/warmup/cleanup; protocol cycles are not general network round trips",
            "pipelining is throughput-oriented and must not be conflated with scalar query latency",
        ],
    }

    def save():
        (args.output / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )

    save()
    try:
        # Finish all ordinary measurements before importing any instrumenting profiler.
        run_command(
            [
                sys.executable,
                str(Path(__file__).with_name("layer_probe.py")),
                "--revision",
                args.revision,
                "--native-rust",
                str(args.native_rust),
                "--native-libpq",
                str(args.native_libpq),
                "--output",
                str(args.output / "layers"),
            ],
            args.output / "layers.log",
            timeout=1800,
        )
        sequence = [
            ("timing", order, backend, name)
            for order, backends in (("forward", BACKENDS), ("reverse", BACKENDS[::-1]))
            for name in WORKLOADS
            for backend in backends
        ] + [
            (kind, "instrumented", backend, name)
            for kind in KINDS[1:]
            for name in WORKLOADS
            for backend in BACKENDS
        ]
        environment, fingerprints = validate_layers(
            args.output / "layers", args.revision
        )
        for kind, order, backend, name in sequence:
            if identity(args.wheel) != frozen:
                raise ValueError("installed wheel, comparators, or host changed")
            stem = f"{kind}-{order}-{backend}-{name}"
            print(stem, flush=True)
            output = args.output / f"{stem}.json"
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                "--revision",
                args.revision,
                "--kind",
                kind,
                "--backend",
                backend,
                "--workload",
                name,
                "--iterations",
                str(iterations_for(kind, name)),
                "--output",
                str(output),
            ]
            trace = output.with_suffix(".strace")
            if kind == "syscalls":
                command = [
                    "strace",
                    "-f",
                    "-c",
                    "-e",
                    f"trace={SYSCALLS}",
                    "-o",
                    str(trace),
                    *command,
                ]
            run_command(command, output.with_suffix(".log"))
            result = json.loads(output.read_text())
            validate_worker(result, args.revision, backend, name, kind)
            current = environment_identity(result)
            if environment is None:
                environment = current
            elif environment != current:
                raise ValueError("worker machine/server changed")
            files = result["installed_file_sha256"]
            if fingerprints.setdefault(backend, files) != files:
                raise ValueError("worker installed code changed")
            entry = {
                "report": output.name,
                "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            }
            if result["artifact"]:
                artifact = args.output / result["artifact"]
                if artifact != output.with_suffix(artifact.suffix):
                    raise ValueError(
                        "worker names an unrelated instrumentation artifact"
                    )
                if not artifact.stat().st_size:
                    raise ValueError("empty instrumentation artifact")
                entry["artifact_sha256"] = hashlib.sha256(
                    artifact.read_bytes()
                ).hexdigest()
            if kind == "allocations":
                stats = output.with_suffix(".allocations.json")
                run_command(
                    [
                        sys.executable,
                        "-m",
                        "memray",
                        "stats",
                        "--json",
                        "-o",
                        str(stats),
                        str(output.with_suffix(".bin")),
                    ],
                    output.with_suffix(".stats.log"),
                )
                validate_allocations(json.loads(stats.read_text()))
                entry["allocation_summary"] = stats.name
                entry["allocation_summary_sha256"] = hashlib.sha256(
                    stats.read_bytes()
                ).hexdigest()
            elif kind == "syscalls":
                entry["syscalls"] = parse_syscalls(trace.read_text())
            manifest["workers"].append(entry)
            save()
        if len(manifest["workers"]) != len(sequence) or identity(args.wheel) != frozen:
            raise ValueError("incomplete or changed attribution run")
        for order, backends in (("forward", BACKENDS), ("reverse", BACKENDS[::-1])):
            for name in PROTOCOL_WORKLOADS:
                for backend in backends:
                    if identity(args.wheel) != frozen:
                        raise ValueError(
                            "installed code changed before protocol capture"
                        )
                    output = args.output / f"protocol-{order}-{backend}-{name}.json"
                    print(output.stem, flush=True)
                    run_command(
                        [
                            sys.executable,
                            str(Path(__file__).with_name("protocol_probe.py")),
                            "--revision",
                            args.revision,
                            "--backend",
                            backend,
                            "--workload",
                            name,
                            "--output",
                            str(output),
                        ],
                        output.with_suffix(".log"),
                    )
                    result = json.loads(output.read_text())
                    validate_protocol(result, args.revision, backend, name)
                    if (
                        environment_identity(result) != environment
                        or result["installed_file_sha256"] != fingerprints[backend]
                    ):
                        raise ValueError("protocol worker code or environment changed")
                    manifest["protocol_workers"].append(
                        {
                            "report": output.name,
                            "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                        }
                    )
                    save()
        if len(manifest["protocol_workers"]) != 2 * len(BACKENDS) * len(
            PROTOCOL_WORKLOADS
        ):
            raise ValueError("incomplete protocol captures")
        if identity(args.wheel) != frozen:
            raise ValueError("installed code changed during protocol capture")
        manifest["environment"] = environment
        manifest["artifact_sha256"] = {
            str(path.relative_to(args.output)): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sorted(args.output.rglob("*"))
            if path.is_file() and path.name != "manifest.json"
        }
        manifest["status"] = "completed"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["failures"].append(str(exc))
    save()
    return int(bool(manifest["failures"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--native-rust", type=Path)
    parser.add_argument("--native-libpq", type=Path)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--backend", choices=BACKENDS)
    parser.add_argument("--workload", choices=WORKLOADS)
    parser.add_argument("--kind", choices=KINDS)
    parser.add_argument("--iterations", type=positive)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or args.output.exists():
        parser.error("full revision and unused output path required")
    if args.worker:
        if not all((args.backend, args.workload, args.kind, args.iterations)):
            parser.error("worker requires backend, workload, kind, and iterations")
        for suffix in (".pstats", ".bin"):
            if args.output.with_suffix(suffix).exists():
                parser.error("preserve existing instrumentation artifacts")
        result = worker(args)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        return 0
    return coordinate(args)


if __name__ == "__main__":
    raise SystemExit(main())
