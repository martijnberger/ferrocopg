"""Region-scoped Linux interpreter API, scheduler, and poll counts, not timings."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import sysconfig
from pathlib import Path

from attribution import environment_identity, run_command
from layer_probe import parameter_protocol, validate_dsn
from query_profile import installed_files
from repeat_benchmark import identity
from run import driver_for, metadata

ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = {
    "query": "select %s::int + 1",
    "parameter_oids": [21],
    "parameter_format": "binary",
    "result_format": "binary",
    "prepared": True,
    "autocommit": True,
    "in_flight": 1,
}
SYMBOLS = (
    "PyEval_SaveThread",
    "PyEval_RestoreThread",
    "PyGILState_Ensure",
    "PyGILState_Release",
)
POLLS = (
    "poll",
    "ppoll",
    "select",
    "pselect6",
    "epoll_wait",
    "epoll_pwait",
    "epoll_pwait2",
)
COUNTERS = {
    "region_enter",
    "region_exit",
    "invalid_marker",
    "save_thread",
    "restore_thread",
    "ensure_locked",
    "ensure_unlocked",
    "ensure_unknown",
    "release_locked",
    "release_unlocked",
    "release_unknown",
    "wakeup",
    "wakeup_new",
    "switch_in",
    "switch_out",
    *(f"poll_{name}" for name in POLLS),
}


def safe_path(path):
    value = str(path.resolve())
    if not re.fullmatch(r"/[A-Za-z0-9_./+\-]+", value):
        raise ValueError("unsupported characters in BPF probe path")
    return value


def program(library, marker, polls):
    library, marker = safe_path(library), safe_path(marker)
    if not polls or not set(polls) <= set(POLLS) or len(set(polls)) != len(polls):
        raise ValueError("invalid available poll tracepoints")
    source = f"""
BEGIN {{ @owned[cpid] = 1; }}
tracepoint:sched:sched_process_fork /pid == cpid/
{{ @owned[args->child_pid] = 1; }}
tracepoint:sched:sched_process_exit /@owned[tid]/
{{ delete(@owned[tid]); }}
uprobe:{marker}:phase5_trace_start /pid == cpid/
{{
    if (@active) {{ @events["invalid_marker"] = count(); }}
    @active = 1;
    @events["region_enter"] = count();
}}
uprobe:{marker}:phase5_trace_stop /pid == cpid/
{{
    if (!@active) {{ @events["invalid_marker"] = count(); }}
    @active = 0;
    @events["region_exit"] = count();
}}
uprobe:{library}:PyEval_SaveThread /pid == cpid && @active/
{{ @events["save_thread"] = count(); }}
uretprobe:{library}:PyEval_RestoreThread /pid == cpid && @active/
{{ @events["restore_thread"] = count(); }}
uretprobe:{library}:PyGILState_Ensure /pid == cpid && @active/
{{
    if (retval == 0) {{ @events["ensure_locked"] = count(); }}
    else if (retval == 1) {{ @events["ensure_unlocked"] = count(); }}
    else {{ @events["ensure_unknown"] = count(); }}
}}
uprobe:{library}:PyGILState_Release /pid == cpid && @active/
{{
    if (arg0 == 0) {{ @events["release_locked"] = count(); }}
    else if (arg0 == 1) {{ @events["release_unlocked"] = count(); }}
    else {{ @events["release_unknown"] = count(); }}
}}
tracepoint:sched:sched_wakeup /@active && @owned[args->pid]/
{{ @events["wakeup"] = count(); }}
tracepoint:sched:sched_wakeup_new /@active && @owned[args->pid]/
{{ @events["wakeup_new"] = count(); }}
tracepoint:sched:sched_switch /@active/
{{
    if (@owned[args->prev_pid]) {{ @events["switch_out"] = count(); }}
    if (@owned[args->next_pid]) {{ @events["switch_in"] = count(); }}
}}
"""
    for poll in polls:
        source += f"""
tracepoint:syscalls:sys_enter_{poll} /pid == cpid && @active/
{{ @events["poll_{poll}"] = count(); }}
"""
    return (
        source
        + """
END { print(@events); clear(@events); clear(@owned); clear(@active); }
"""
    )


def parse_counts(raw):
    counts = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"@events\[([a-z0-9_]+)\]: ([0-9]+)", line)
        if not match or match[1] not in COUNTERS or match[1] in counts:
            raise ValueError(f"invalid or unexpected BPF output: {line}")
        counts[match[1]] = int(match[2])
    counts = {key: counts.get(key, 0) for key in sorted(COUNTERS)}
    if (
        counts["region_enter"] != 1
        or counts["region_exit"] != 1
        or counts["invalid_marker"]
        or counts["ensure_unknown"]
        or counts["release_unknown"]
        or not counts["save_thread"]
        or not counts["restore_thread"]
        or not sum(counts[f"poll_{name}"] for name in POLLS)
    ):
        raise ValueError("missing region, interpreter handoffs, or polling evidence")
    return counts


def validate_worker(report, backend, revision):
    if (
        report["mode"] != "handoff-worker"
        or report["backend"] != backend
        or report["iterations"] != 1000
        or report["warmup"] != 1000
        or report["acceptance_evidence"] is not False
        or report["metadata"]["revision"] != revision
        or report["protocol"] != PROTOCOL
    ):
        raise ValueError("invalid handoff worker identity or protocol")
    files = report["installed_file_sha256"]
    names = {"ferrocopg"} if backend == "rust" else {"psycopg"}
    if backend == "c":
        names.add("psycopg-c")
    if set(files) != names or any(
        not entries
        or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in entries.values())
        for entries in files.values()
    ):
        raise ValueError("invalid installed handoff code fingerprint")
    return environment_identity(report), files


def worker(args):
    os.environ["PSYCOPG_IMPL"] = "python" if args.worker == "rust" else args.worker
    driver = driver_for(args.worker)
    validate_dsn(os.environ["PHASE5_DSN"])
    if hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled():
        raise ValueError("this diagnostic requires a GIL-enabled interpreter")
    markers = ctypes.PyDLL(str(args.marker))
    for name in ("phase5_trace_start", "phase5_trace_stop"):
        function = getattr(markers, name)
        function.argtypes, function.restype = [], None
    with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
        environment = metadata(driver, conn, args)

        def query():
            cursor = conn.execute(
                "select %s::int + 1", (41,), prepare=True, binary=True
            )
            if cursor.fetchone() != (42,):
                raise AssertionError("incorrect handoff query result")
            return cursor

        check = query()
        if (
            parameter_protocol(check) != ([21], [1])
            or check.pgresult.ftype(0) != 23
            or check.pgresult.fformat(0) != 1
        ):
            raise AssertionError("handoff parameter/result protocol mismatch")
        check.close()
        del check
        for _ in range(1000):
            query()
        markers.phase5_trace_start()
        try:
            for _ in range(1000):
                query()
        finally:
            markers.phase5_trace_stop()
    names = ["ferrocopg"] if args.worker == "rust" else ["psycopg"]
    if args.worker == "c":
        names.append("psycopg-c")
    args.output.write_text(
        json.dumps(
            {
                "mode": "handoff-worker",
                "acceptance_evidence": False,
                "backend": args.worker,
                "iterations": 1000,
                "warmup": 1000,
                "metadata": environment,
                "installed_file_sha256": {
                    name: installed_files(name) for name in names
                },
                "protocol": PROTOCOL,
            },
            indent=2,
        )
        + "\n"
    )


def preflight(output):
    if sys.platform != "linux" or os.geteuid() != 0:
        raise ValueError("Linux root privileges required for this BPF diagnostic")
    library = Path(sysconfig.get_config_var("LIBDIR")) / sysconfig.get_config_var(
        "LDLIBRARY"
    )
    exports = subprocess.check_output(
        ["nm", "-D", "--defined-only", str(library)], text=True
    )
    available = {line.split()[-1] for line in exports.splitlines() if line.split()}
    if not set(SYMBOLS) <= available:
        raise ValueError("Python shared library lacks required exported GIL APIs")
    events = subprocess.check_output(["bpftrace", "-l", "tracepoint:*"], text=True)
    (output / "available-tracepoints.txt").write_text(events)
    available = set(events.splitlines())
    required = {
        f"tracepoint:sched:sched_{name}"
        for name in ("process_fork", "process_exit", "wakeup", "wakeup_new", "switch")
    }
    if not required <= available:
        raise ValueError("required scheduler tracepoints are unavailable")
    polls = [
        name for name in POLLS if f"tracepoint:syscalls:sys_enter_{name}" in available
    ]
    if not polls:
        raise ValueError("no supported poll tracepoints")
    run_command(["bpftrace", "--info"], output / "bpftrace-info.txt")
    run_command(["bpftrace", "--version"], output / "bpftrace-version.txt")
    return library, polls


def coordinate(args):
    args.output.mkdir()
    manifest = {
        "mode": "handoff-diagnostic",
        "acceptance_evidence": False,
        "revision": args.revision,
        "status": "running",
        "workers": [],
        "failures": [],
        "limitations": [
            "counts only the marked warmed loop, never ordinary latency",
            "explicit CPython API handoffs are not every interpreter GIL ownership switch",
            "save/release count entries; restore/ensure count successful returns",
            "scheduler wakeups are not GIL acquisitions or network round trips",
            "tracing perturbs scheduling; counts are instrumented observations",
            "only prepared binary scalar queries; not all workloads",
        ],
    }

    def save():
        (args.output / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )

    save()
    try:
        frozen = identity(args.wheel)
        manifest["identity"] = frozen
        library, polls = preflight(args.output)
        marker = args.output / "markers.so"
        run_command(
            [
                "cc",
                "-shared",
                "-fPIC",
                "-O2",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(ROOT / "tools/phase5/handoff_markers.c"),
                "-o",
                str(marker),
            ],
            args.output / "marker-build.log",
        )
        trace = args.output / "handoff.bt"
        trace.write_text(program(library, marker, polls))
        manifest["poll_tracepoints"] = polls
        manifest["python_library_sha256"] = hashlib.sha256(
            library.read_bytes()
        ).hexdigest()
        environments, fingerprints = [], {}
        for order, backends in (
            ("forward", ("python", "c", "rust")),
            ("reverse", ("rust", "c", "python")),
        ):
            for backend in backends:
                if identity(args.wheel) != frozen:
                    raise ValueError("installed wheel or comparator changed")
                name = f"{order}-{backend}"
                raw = args.output / f"{name}.json"
                counts_file = args.output / f"{name}-counts.txt"
                command = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    backend,
                    "--revision",
                    args.revision,
                    "--marker",
                    str(marker),
                    "--output",
                    str(raw),
                ]
                run_command(
                    [
                        "bpftrace",
                        "-q",
                        "-kk",
                        "-o",
                        str(counts_file),
                        "-c",
                        shlex.join(command),
                        str(trace),
                    ],
                    args.output / f"{name}.log",
                    timeout=300,
                )
                counts = parse_counts(counts_file.read_text())
                log = (args.output / f"{name}.log").read_text()
                if re.search(r"warning|error|lost.*events", log, re.IGNORECASE):
                    raise ValueError("tracer reported a warning, error, or lost events")
                report = json.loads(raw.read_text())
                environment, files = validate_worker(report, backend, args.revision)
                environments.append(environment)
                if not files or fingerprints.setdefault(backend, files) != files:
                    raise ValueError("installed worker code changed")
                if any(value != environments[0] for value in environments):
                    raise ValueError("handoff environment changed")
                manifest["workers"].append(
                    {"order": order, "backend": backend, "counts": counts}
                )
                save()
        if identity(args.wheel) != frozen:
            raise ValueError("installed code changed during instrumentation")
        manifest["status"] = "completed"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["failures"].append(str(exc))
    manifest["artifact_sha256"] = {
        str(p.relative_to(args.output)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(args.output.rglob("*"))
        if p.is_file() and p.name != "manifest.json"
    }
    save()
    return int(bool(manifest["failures"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--worker", choices=("python", "c", "rust"))
    parser.add_argument("--marker", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-f0-9]{40}", args.revision) or args.output.exists():
        parser.error("full revision and unused output path required")
    if args.worker:
        if not args.marker:
            parser.error("worker requires a marker library")
        worker(args)
        return 0
    if not args.wheel:
        parser.error("coordinator requires the installed wheel")
    return coordinate(args)


if __name__ == "__main__":
    raise SystemExit(main())
