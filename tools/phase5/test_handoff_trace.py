"""Reject incomplete marked regions and mismatched handoff diagnostics."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import handoff_trace as trace

REVISION = "a" * 40
COUNTS = "\n".join(
    f"@events[{key}]: {value}"
    for key, value in {
        "region_enter": 1,
        "region_exit": 1,
        "save_thread": 1000,
        "restore_thread": 1000,
        "poll_epoll_wait": 1000,
    }.items()
)


def worker(backend):
    names = ["ferrocopg"] if backend == "rust" else ["psycopg"]
    if backend == "c":
        names.append("psycopg-c")
    return {
        "mode": "handoff-worker",
        "acceptance_evidence": False,
        "backend": backend,
        "iterations": 1000,
        "warmup": 1000,
        "metadata": {
            "revision": REVISION,
            "python": "test",
            "platform": "test",
            "machine": "test",
            "server": "test",
            "server_settings": {},
        },
        "installed_file_sha256": {name: {"module.so": "b" * 64} for name in names},
        "protocol": copy.deepcopy(trace.PROTOCOL),
    }


class HandoffTraceTests(unittest.TestCase):
    def test_program_scope_and_supported_symbols(self):
        program = trace.program(
            Path("/tmp/libpython.so"), Path("/tmp/marker.so"), ["poll", "epoll_wait"]
        )
        for symbol in trace.SYMBOLS:
            self.assertIn(
                f"/tmp/libpython.so:{symbol} /pid == cpid && @active/", program
            )
        self.assertIn("@active && @owned[args->pid]", program)
        self.assertIn("delete(@owned[tid])", program)
        self.assertIn("BEGIN { @active = 0; @owned[cpid] = 1; }", program)
        self.assertIn("sys_enter_poll /pid == cpid && @active/", program)
        self.assertIn("sys_enter_epoll_wait /pid == cpid && @active/", program)
        self.assertNotIn("sys_enter_ppoll", program)
        self.assertIn("clear(@owned)", program)
        for polls in ([], ["invalid"], ["poll", "poll"]):
            with self.assertRaises(ValueError):
                trace.program(Path("/tmp/lib.so"), Path("/tmp/marker.so"), polls)
        with self.assertRaises(ValueError):
            trace.safe_path(Path('/tmp/unsafe";'))

    def test_counts_require_complete_region_and_real_events(self):
        counts = trace.parse_counts(COUNTS)
        self.assertEqual(counts["save_thread"], 1000)
        self.assertEqual(counts["wakeup"], 0)
        for raw in (
            "",
            COUNTS.replace("region_exit]: 1", "region_exit]: 2"),
            COUNTS.replace("save_thread]: 1000", "save_thread]: 0"),
            COUNTS.replace("poll_epoll_wait]: 1000", "poll_epoll_wait]: 0"),
            COUNTS + "\n@events[invalid_marker]: 1",
            COUNTS + "\n@events[ensure_unknown]: 1",
            COUNTS + "\n@events[save_thread]: 1000",
            COUNTS + "\n@events[unsupported]: 1",
            COUNTS + "\nWARNING: lost events",
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                trace.parse_counts(raw)

    def test_worker_protocol_and_installed_code_validation(self):
        for backend in ("python", "c", "rust"):
            trace.validate_worker(worker(backend), backend, REVISION)
        mutations = (
            lambda r: r.update(acceptance_evidence=True),
            lambda r: r.update(iterations=1),
            lambda r: r.update(warmup=1),
            lambda r: r["metadata"].update(revision="c" * 40),
            lambda r: r["protocol"].update(parameter_oids=[23]),
            lambda r: r["protocol"].update(prepared=False),
            lambda r: r.update(installed_file_sha256={}),
            lambda r: r["installed_file_sha256"]["ferrocopg"].update(
                {"module.so": "bad"}
            ),
        )
        for mutate in mutations:
            raw = worker("rust")
            mutate(raw)
            with self.assertRaises(ValueError):
                trace.validate_worker(raw, "rust", REVISION)

    def run_coordinator(self, root, *, fail=False):
        library = root / "libpython.so"
        library.write_bytes(b"python fixture")
        args = SimpleNamespace(
            output=root / "results", wheel=root / "wheel", revision=REVISION
        )
        calls = []

        def command(argv, log, timeout=600):
            log.write_text("")
            if argv[0] == "cc":
                Path(argv[-1]).write_bytes(b"marker fixture")
                return
            command = trace.shlex.split(argv[argv.index("-c") + 1])
            self.assertIn("-k", argv)
            self.assertNotIn("-kk", argv)
            backend = command[command.index("--worker") + 1]
            calls.append(backend)
            self.assertEqual(timeout, 300)
            Path(command[-1]).write_text(json.dumps(worker(backend)))
            Path(argv[argv.index("-o") + 1]).write_text(COUNTS)
            if fail and len(calls) == 2:
                log.write_text("WARNING: lost events\n")

        with (
            patch.object(trace, "identity", return_value={"frozen": True}),
            patch.object(trace, "preflight", return_value=(library, ["epoll_wait"])),
            patch.object(trace, "run_command", side_effect=command),
        ):
            status = trace.coordinate(args)
        return status, json.loads((args.output / "manifest.json").read_text()), calls

    def test_coordinator_keeps_both_orders_and_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            status, manifest, calls = self.run_coordinator(Path(tmp))
            self.assertEqual(status, 0)
            self.assertEqual(calls, ["python", "c", "rust", "rust", "c", "python"])
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(len(manifest["workers"]), 6)
            self.assertFalse(manifest["acceptance_evidence"])
            self.assertIn("handoff.bt", manifest["artifact_sha256"])
            self.assertNotIn("manifest.json", manifest["artifact_sha256"])

    def test_coordinator_preserves_partial_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            status, manifest, calls = self.run_coordinator(Path(tmp), fail=True)
            self.assertEqual(status, 1)
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(len(manifest["workers"]), 1)
            self.assertEqual(len(calls), 2)
            self.assertIn("forward-c-counts.txt", manifest["artifact_sha256"])

    def test_linux_requirement_is_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(trace.sys, "platform", "darwin"):
                with self.assertRaisesRegex(ValueError, "Linux root"):
                    trace.preflight(Path(tmp))
