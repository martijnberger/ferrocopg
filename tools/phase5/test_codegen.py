"""Accounting checks for the optional, non-acceptance codegen experiment."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from codegen_compare import (
    ITERATIONS,
    ORDER,
    PROFILES,
    SAMPLES,
    WORKLOADS,
    build_environment,
    compare_queries,
    complete_measurement,
    query_measurement,
)
from run import BACKENDS, compare
from workloads import BENCHMARKS


class CodegenExperimentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name)

    def result(self, workload="prepared", duration=1.0):
        return {
            "metadata": {"revision": "revision"},
            "mode": "benchmark",
            "backend": "rust",
            "workload": workload,
            "iterations": ITERATIONS,
            "seconds": [duration] * SAMPLES,
            "cpu_seconds": [duration / 2] * SAMPLES,
            "failures": [],
        }

    def write(self, name, result):
        path = self.output / name
        path.write_text(json.dumps(result))
        return path

    def test_profiles_are_isolated_and_portable(self):
        with patch.dict(
            os.environ, {"PATH": "path", "CARGO_TARGET_DIR": "old"}, clear=True
        ):
            default = build_environment("default", self.output / "default")
            thin = build_environment("thin", self.output / "thin")
            self.assertEqual(os.environ["CARGO_TARGET_DIR"], "old")
        self.assertEqual(default["CARGO_PROFILE_RELEASE_LTO"], "false")
        self.assertEqual(default["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"], "16")
        self.assertEqual(thin["CARGO_PROFILE_RELEASE_LTO"], "thin")
        self.assertEqual(thin["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"], "1")
        self.assertNotEqual(default["CARGO_TARGET_DIR"], thin["CARGO_TARGET_DIR"])
        self.assertNotIn("RUSTFLAGS", thin)

    def test_inherited_codegen_overrides_are_rejected(self):
        for key in (
            "CARGO_PROFILE_RELEASE_OPT_LEVEL",
            "CARGO_PROFILE_RELEASE_LTO",
            "RUSTFLAGS",
            "CARGO_ENCODED_RUSTFLAGS",
            "CARGO_BUILD_RUSTFLAGS",
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS",
        ):
            with (
                self.subTest(key=key),
                patch.dict(os.environ, {key: "override"}, clear=True),
            ):
                with self.assertRaisesRegex(ValueError, "inherited codegen overrides"):
                    build_environment("default", self.output)

    def test_wrong_scope_and_failures_are_rejected(self):
        for key, value in (
            ("metadata", {"revision": "other"}),
            ("mode", "soak"),
            ("backend", "c"),
            ("workload", "parameterized"),
            ("failures", ["query failed"]),
            ("iterations", 10),
            ("seconds", [1.0]),
            ("cpu_seconds", [1.0]),
            ("seconds", [float("nan")] * SAMPLES),
            ("cpu_seconds", [0.0] * SAMPLES),
        ):
            with self.subTest(key=key, value=value):
                result = self.result()
                result[key] = value
                path = self.write("invalid.json", result)
                with self.assertRaisesRegex(ValueError, "invalid query measurement"):
                    query_measurement(path, "revision", "prepared")

    def test_comparison_preserves_each_order_not_an_average(self):
        self.assertEqual(
            ORDER, (("default", "a"), ("thin", "a"), ("thin", "b"), ("default", "b"))
        )
        for profile in PROFILES:
            for workload in WORKLOADS:
                for order in ("a", "b"):
                    duration = (
                        1.0 if profile == "default" else (0.9 if order == "a" else 1.1)
                    )
                    self.write(
                        f"{profile}-{workload}-{order}.json",
                        self.result(workload, duration),
                    )
        report = compare_queries(self.output, "revision")
        self.assertIs(report["release_acceptance"], False)
        self.assertEqual(len(report["pairs"]), 4)
        for workload in WORKLOADS:
            for order, ratio in (("a", 0.9), ("b", 1.1)):
                pair = report["pairs"][f"{workload}-{order}"]
                self.assertAlmostEqual(pair["thin_over_default"]["wall_us"], ratio)
                self.assertAlmostEqual(pair["thin_over_default"]["cpu_us"], ratio)

    def test_missing_pair_cannot_produce_a_summary(self):
        with self.assertRaises(FileNotFoundError):
            compare_queries(self.output, "revision")

    def complete_report(self):
        results = []
        for workload in BENCHMARKS:
            for backend in BACKENDS:
                result = self.result(workload, 2.0 if backend == "rust" else 1.0)
                result["backend"] = backend
                results.append(result)
        return {
            "revision": "revision",
            "mode": "benchmark",
            "results": results,
            **compare(results),
        }

    def test_complete_benchmark_keeps_failed_performance_gates(self):
        report = self.complete_report()
        path = self.write("complete.json", report)
        verdict = complete_measurement(path, "revision", 1)
        self.assertEqual(verdict["failures"], report["failures"])
        self.assertTrue(verdict["failures"])
        with self.assertRaisesRegex(ValueError, "inconsistent benchmark verdict"):
            complete_measurement(path, "revision", 0)

    def test_complete_benchmark_rejects_missing_or_failed_workers(self):
        for problem in ("missing", "failed", "revision"):
            with self.subTest(problem=problem):
                report = self.complete_report()
                if problem == "missing":
                    report["results"].pop()
                elif problem == "failed":
                    report["results"][0]["failures"] = ["worker failed"]
                else:
                    report["results"][0]["metadata"]["revision"] = "other"
                path = self.write("incomplete.json", report)
                with self.assertRaisesRegex(ValueError, "incomplete or failed"):
                    complete_measurement(path, "revision", 1)


if __name__ == "__main__":
    unittest.main()
