"""The repeated gate must never cherry-pick workloads or stale verdicts."""

import copy
import json
import tempfile
import unittest
from itertools import count
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from repeat_benchmark import main, validate_report
from run import BACKENDS, BENCHMARKS, compare

REVISION = "a" * 40


class RepeatedBenchmarkTests(unittest.TestCase):
    def report(self):
        results = [
            {
                "backend": backend,
                "workload": workload,
                "seconds": [1.0] * 9,
                "failures": [],
                "iterations": 100,
                "rows": 1000,
                "metadata": {
                    "revision": REVISION,
                    "server": "PostgreSQL test",
                    "server_settings": {},
                    "python": "test",
                    "platform": "test",
                    "machine": "test",
                    "cpu_logical": 4,
                    "cpu_physical": 2,
                    "memory_bytes": 1000,
                },
            }
            for backend in BACKENDS
            for workload in BENCHMARKS
        ]
        return {
            "mode": "benchmark",
            "revision": REVISION,
            "results": results,
            "configuration": {
                "iterations": 100,
                "samples": 9,
                "rows": 1000,
                "warmup": 10,
            },
            **compare(results),
        }

    def test_complete_pass(self):
        self.assertFalse(validate_report(self.report(), REVISION, 0))

    def test_real_failure_is_retained(self):
        report = self.report()
        report["results"][0]["seconds"] = [2.0] * 9
        report.update(compare(report["results"]))
        self.assertTrue(validate_report(report, REVISION, 1))

    def run_repeated(self, reports, identities=None, activity_snapshots=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / "candidate.whl"
            wheel.touch()
            output = root / "results"
            pending = iter(reports)
            clock = count(1)

            def snapshot():
                tick = next(clock)
                return {
                    "monotonic": tick,
                    "unix_time": tick + 1000,
                    "cpu_times": {"user": tick, "idle": tick * 3},
                    "processes": {},
                    "unavailable": 0,
                }

            def run(command, **kwargs):
                report = next(pending)
                child = Path(command[command.index("--output") + 1])
                child.mkdir()
                (child / "report.json").write_text(json.dumps(report))
                return SimpleNamespace(returncode=int(bool(report["failures"])))

            with (
                patch(
                    "sys.argv",
                    [
                        "repeat_benchmark.py",
                        "--revision",
                        REVISION,
                        "--wheel",
                        str(wheel),
                        "--output",
                        str(output),
                    ],
                ),
                patch(
                    "repeat_benchmark.identity",
                    return_value={"wheel": "same"},
                    side_effect=identities,
                ),
                patch("repeat_benchmark.subprocess.run", side_effect=run) as process,
                patch(
                    "host_activity.snapshot",
                    side_effect=activity_snapshots or snapshot,
                ),
            ):
                status = main()
            summary = json.loads((output / "summary.json").read_text())
            for run in summary["runs"]:
                telemetry = [
                    json.loads(line)
                    for line in (output / run["host_activity"]).read_text().splitlines()
                ]
                self.assertEqual(telemetry[0]["mode"], "host-activity")
                self.assertEqual(telemetry[-1]["status"], "completed")
                self.assertEqual(telemetry[-1]["samples"], len(telemetry) - 2)
                self.assertGreaterEqual(telemetry[-1]["samples"], 1)
            return (
                status,
                summary,
                process.call_count,
            )

    def test_three_complete_passes_are_required(self):
        status, summary, count = self.run_repeated([self.report()] * 3)
        self.assertEqual(status, 0)
        self.assertTrue(summary["benchmark_gate_passed"])
        self.assertEqual(count, 3)
        self.assertEqual(len(summary["runs"]), 3)
        self.assertTrue(summary["host_observer"]["review_required"])
        self.assertIsNone(summary["host_observer"]["idle_verdict"])

    def test_missing_host_observation_cannot_pass(self):
        with patch(
            "repeat_benchmark.HostActivity",
            side_effect=RuntimeError("observer unavailable"),
        ):
            status, summary, count = self.run_repeated([self.report()] * 3)
        self.assertEqual(status, 1)
        self.assertFalse(summary["benchmark_gate_passed"])
        self.assertEqual(count, 0)
        self.assertEqual(summary["failures"], ["observer unavailable"])

    def test_failed_host_observation_cannot_pass(self):
        from test_host_activity import sample

        status, summary, count = self.run_repeated(
            [self.report()] * 3,
            activity_snapshots=[sample(10, []), RuntimeError("snapshot failed")],
        )
        self.assertEqual(status, 1)
        self.assertFalse(summary["benchmark_gate_passed"])
        self.assertEqual(count, 1)
        self.assertEqual(summary["failures"], ["host-activity sampling failed"])

    def test_failed_first_run_is_not_replaced_by_later_passes(self):
        failed = self.report()
        failed["results"][0]["seconds"] = [2.0] * 9
        failed.update(compare(failed["results"]))
        status, summary, count = self.run_repeated(
            [failed, self.report(), self.report()]
        )
        self.assertEqual(status, 1)
        self.assertFalse(summary["benchmark_gate_passed"])
        self.assertEqual(count, 3)
        self.assertTrue(summary["failures"][0].startswith("run-1:"))

    def test_changed_packages_or_server_cannot_pass(self):
        status, summary, count = self.run_repeated(
            [self.report()] * 3, identities=[{}, {}, {"changed": True}]
        )
        self.assertEqual(status, 1)
        self.assertEqual(count, 1)
        self.assertIn("identity changed", summary["failures"][0])
        changed = self.report()
        for result in changed["results"]:
            result["metadata"]["server"] = "different"
        status, summary, count = self.run_repeated([self.report(), changed])
        self.assertEqual(status, 1)
        self.assertEqual(count, 2)
        self.assertIn("server changed", summary["failures"][0])

    def test_incomplete_attempt_cannot_pass(self):
        status, summary, count = self.run_repeated([self.report()])
        self.assertEqual(status, 1)
        self.assertFalse(summary["benchmark_gate_passed"])
        self.assertEqual(len(summary["runs"]), 1)

    def test_incomplete_stale_or_inconsistent_report_is_rejected(self):
        base = self.report()
        for field, value in (
            ("revision", "b" * 40),
            ("mode", "soak"),
            ("performance_policy", "old-policy"),
            ("failures", ["hidden"]),
            ("ratios", {}),
        ):
            with self.subTest(field=field):
                report = copy.deepcopy(base)
                report[field] = value
                with self.assertRaises(ValueError):
                    validate_report(report, REVISION, 0)
        for mutation in ("missing", "duplicate", "revision", "samples", "size"):
            with self.subTest(mutation=mutation):
                report = copy.deepcopy(base)
                if mutation == "missing":
                    report["results"].pop()
                elif mutation == "duplicate":
                    report["results"].append(report["results"][0])
                elif mutation == "revision":
                    report["results"][0]["metadata"]["revision"] = "b" * 40
                elif mutation == "samples":
                    report["results"][0]["seconds"] = [1.0] * 3
                else:
                    report["configuration"]["iterations"] = 20
                with self.assertRaises(ValueError):
                    validate_report(report, REVISION, 0)
        with self.assertRaises(ValueError):
            validate_report(base, REVISION, 1)


if __name__ == "__main__":
    unittest.main()
