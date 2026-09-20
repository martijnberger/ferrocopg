"""Attribution cannot turn incomplete or instrumented measurements into evidence."""

import contextlib
import copy
import hashlib
import json
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import attribution as a

REVISION = "a" * 40
TRACE = " 90.00 0.001000 10 100 2 recvfrom\n 10.00 0.000100 1 100 poll\n"


def result(kind="timing", backend="rust", name="prepared"):
    report = {
        "mode": "attribution",
        "acceptance_evidence": False,
        "kind": kind,
        "backend": backend,
        "workload": name,
        "iterations": a.iterations_for(kind, name),
        "warmup": a.WARMUP,
        "rows": a.ROWS,
        "samples": [dict(wall_seconds=1.0, cpu_seconds=0.5)] * a.SAMPLES
        if kind == "timing"
        else [],
        "instrumented": kind != "timing",
        "metadata": dict(
            revision=REVISION,
            python="test",
            platform="test",
            machine="test",
            server="test",
            server_settings={},
        ),
        "installed_file_sha256": {backend: {"native.so": "a" * 64}},
        "artifact": None,
    }
    if kind == "calls":
        report.update(artifact="capture.pstats", functions=[{"calls": 1}])
    elif kind == "allocations":
        report.update(artifact="capture.bin", memray_version="1.20.0")
    elif kind == "syscalls":
        report["tracer_pid"] = 123
    return report


def allocations():
    return {
        "total_num_allocations": 3,
        "total_bytes_allocated": 128,
        "allocation_size_histogram": [{"count": 1}, {"count": 2}],
        "metadata": {"has_native_traces": True, "trace_python_allocators": True},
    }


def layers(output):
    output.mkdir()
    sources = {}
    for i in range(6):
        path = output / f"source-{i}"
        path.write_text(str(i))
        sources[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    summary = {
        "mode": "layer-diagnostic",
        "acceptance_evidence": False,
        "revision": REVISION,
        "failures": [],
        "sha256": sources,
        "protocol": dict(
            query=a.QUERY,
            prepared=True,
            result_format="binary",
            in_flight=1,
            iterations=10000,
            samples=9,
            warmup=1000,
            tls=False,
        ),
        "orders": {},
    }
    for order in ("forward", "reverse"):
        summary["orders"][order] = {}
        for layer in a.LAYERS:
            backend = "rust" if layer == "binding" else layer
            raw = dict(
                result(backend=backend),
                backend=layer,
                iterations=10000,
                warmup=1000,
                wall_us=[20.0] * 9,
            )
            (output / f"{order}-{layer}.json").write_text(json.dumps(raw))
            summary["orders"][order][layer] = dict(wall_us=[20.0] * 9, median_us=20.0)
    (output / "summary.json").write_text(json.dumps(summary))
    return summary


class AttributionTests(unittest.TestCase):
    def test_allocation_window_excludes_setup_warmup_and_cleanup(self):
        active = False
        calls = []
        options = []

        @contextlib.contextmanager
        def tracker(path, **kwargs):
            nonlocal active
            options.append(kwargs)
            active = True
            try:
                yield
            finally:
                active = False

        @contextlib.contextmanager
        def workload(*args):
            calls.append(("setup", active))
            yield SimpleNamespace(run=lambda: calls.append(("run", active)))
            calls.append(("cleanup", active))

        args = SimpleNamespace(
            backend="rust",
            kind="allocations",
            workload="prepared",
            iterations=3,
            output=Path("capture.json"),
        )
        with (
            patch.dict(a.os.environ, PHASE5_DSN="test"),
            patch.dict(
                "sys.modules",
                memray=SimpleNamespace(Tracker=tracker, __version__="1.20.0"),
            ),
            patch.object(a, "driver_for", return_value=MagicMock()),
            patch.object(a, "metadata", return_value=result()["metadata"]),
            patch.object(a, "installed_files", return_value={"native.so": "a" * 64}),
            patch.object(a, "workload", side_effect=workload),
        ):
            report = a.worker(args)
        self.assertEqual(
            calls,
            [("setup", False)]
            + [("run", False)] * a.WARMUP
            + [("run", True)] * 3
            + [("cleanup", False)],
        )
        self.assertEqual(
            options, [dict(native_traces=True, trace_python_allocators=True)]
        )
        self.assertEqual(report["samples"], [])
        self.assertTrue(report["instrumented"])

    def test_worker_identity_and_scope(self):
        for kind in a.KINDS:
            report = result(kind)
            a.validate_worker(report, REVISION, "rust", "prepared", kind)
            changes = [
                ("acceptance_evidence", True),
                ("backend", "c"),
                ("iterations", 1),
                ("warmup", 0),
                ("rows", 1),
                ("instrumented", kind == "timing"),
                ("installed_file_sha256", {}),
            ]
            if kind == "timing":
                changes += [
                    ("samples", []),
                    ("artifact", "unrelated.bin"),
                    ("samples", [dict(wall_seconds=float("nan"), cpu_seconds=1)] * 9),
                ]
            else:
                changes += [("samples", [dict(wall_seconds=1, cpu_seconds=1)])]
            if kind in ("calls", "allocations"):
                changes += [("artifact", None), ("artifact", "../capture.bin")]
            if kind == "calls":
                changes += [("functions", [])]
            if kind == "allocations":
                changes += [("memray_version", "0")]
            if kind == "syscalls":
                changes += [("tracer_pid", 0)]
            for key, value in changes:
                with self.subTest(kind=kind, key=key), self.assertRaises(ValueError):
                    a.validate_worker(
                        dict(report, **{key: value}), REVISION, "rust", "prepared", kind
                    )

    def test_allocation_counts_and_capture_flags(self):
        a.validate_allocations(allocations())
        for key, value in (
            ("total_num_allocations", 2),
            ("total_bytes_allocated", 0),
            ("total_num_allocations", True),
            ("allocation_size_histogram", []),
            ("allocation_size_histogram", [{"count": -1}]),
            ("metadata", {"has_native_traces": True}),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                a.validate_allocations(dict(allocations(), **{key: value}))

    def test_syscall_counts_preserve_errors_and_reject_bad_traces(self):
        parsed = a.parse_syscalls(TRACE)
        self.assertEqual(parsed["recvfrom"]["errors"], 2)
        self.assertEqual(parsed["poll"]["calls"], 100)
        for text in (
            "",
            TRACE + TRACE,
            "90.00 0.001 1 0 poll",
            "90.00 0.001 1 1 2 poll",
            "90.00 0.001 poll",
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                a.parse_syscalls(text)

    def test_timeout_stops_owned_process_group(self):
        process = MagicMock(pid=123)
        process.wait.side_effect = [subprocess.TimeoutExpired("test", 1), 0, 0]
        manager = MagicMock()
        manager.__enter__.return_value = process
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(a.subprocess, "Popen", return_value=manager) as popen,
            patch.object(a.os, "killpg") as kill,
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                a.run_command(["test"], Path(directory) / "log", timeout=1)
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertEqual(kill.call_args_list[0].args, (123, signal.SIGTERM))
            self.assertEqual(kill.call_args_list[1].args, (123, signal.SIGKILL))

    def test_layer_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "layers"
            summary = layers(output)
            a.validate_layers(output, REVISION)
            bad = []
            item = copy.deepcopy(summary)
            item["orders"]["forward"].pop("session")
            bad.append(item)
            item = copy.deepcopy(summary)
            item["orders"]["forward"]["session"]["median_us"] = 10
            bad.append(item)
            item = copy.deepcopy(summary)
            item["protocol"]["tls"] = True
            bad.append(item)
            item = copy.deepcopy(summary)
            item["sha256"] = {}
            bad.append(item)
            for item in bad:
                (output / "summary.json").write_text(json.dumps(item))
                with self.assertRaises(ValueError):
                    a.validate_layers(output, REVISION)
            (output / "summary.json").write_text(json.dumps(summary))
            Path(next(iter(summary["sha256"]))).write_text("changed")
            with self.assertRaises(ValueError):
                a.validate_layers(output, REVISION)

    def coordinate(self, failure=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(
                wheel=root / "wheel.whl",
                native_rust=root / "rust",
                native_libpq=root / "libpq",
                revision=REVISION,
                output=root / "output",
            )

            def run(command, output, **kwargs):
                if "--kind" not in command and "memray" not in command:
                    layers(args.output / "layers")
                    return
                if "memray" in command:
                    Path(command[command.index("-o") + 1]).write_text(
                        json.dumps(allocations())
                    )
                    return
                kind, backend, name = (
                    command[command.index(flag) + 1]
                    for flag in ("--kind", "--backend", "--workload")
                )
                if failure == "worker":
                    raise RuntimeError("injected worker failure")
                report = result(kind, backend, name)
                path = Path(command[command.index("--output") + 1])
                if kind in ("calls", "allocations"):
                    artifact = path.with_suffix(
                        ".bin" if kind == "allocations" else ".pstats"
                    )
                    report["artifact"] = artifact.name
                    if failure != "missing-artifact":
                        artifact.write_bytes(b"test capture")
                if kind == "syscalls":
                    path.with_suffix(".strace").write_text(TRACE)
                path.write_text(json.dumps(report))

            with (
                patch.object(a.platform, "system", return_value="Linux"),
                patch.object(a.shutil, "which", return_value="strace"),
                patch.object(a, "identity", return_value={"wheel": "fixed"}),
                patch.object(a, "run_command", side_effect=run),
                patch("builtins.print"),
            ):
                status = a.coordinate(args)
            manifest = json.loads((args.output / "manifest.json").read_text())
            reports = [
                json.loads((args.output / entry["report"]).read_text())
                for entry in manifest["workers"]
            ]
            return status, manifest, reports

    def test_complete_run_keeps_timings_separate_and_hashes_all_artifacts(self):
        status, manifest, reports = self.coordinate()
        self.assertEqual(status, 0)
        self.assertEqual(manifest["status"], "completed")
        self.assertFalse(manifest["acceptance_evidence"])
        self.assertEqual(len(reports), 180)
        self.assertTrue(all(r["kind"] == "timing" for r in reports[:72]))
        self.assertTrue(all(r["kind"] != "timing" for r in reports[72:]))
        self.assertTrue(manifest["artifact_sha256"])

    def test_failed_workers_and_missing_captures_preserve_failed_manifest(self):
        for failure in ("worker", "missing-artifact"):
            with self.subTest(failure=failure):
                status, manifest, _ = self.coordinate(failure)
                self.assertEqual(status, 1)
                self.assertEqual(manifest["status"], "failed")
                self.assertTrue(manifest["failures"])
