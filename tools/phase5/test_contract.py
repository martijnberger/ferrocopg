"""Tests for acceptance accounting, independent of database availability."""

import copy
import unittest
from unittest.mock import patch

from run import (
    BACKENDS,
    BENCHMARKS,
    PERFORMANCE_LIMITS,
    PERFORMANCE_POLICY,
    compare,
    growth_failures,
    percentile,
    settle_snapshot,
)


class ContractTests(unittest.TestCase):
    def results(self):
        return [
            {
                "backend": backend,
                "workload": name,
                "seconds": [1.0, 1.0, 1.0],
                "failures": [],
            }
            for name in BENCHMARKS
            for backend in BACKENDS
        ]

    def samples(self):
        return [
            dict(
                rss_bytes=1_000_000,
                driver_objects=10,
                threads=1,
                sockets=1,
                fds=4,
                sessions=0,
            )
            for _ in range(6)
        ]

    def test_exact_parity_passes(self):
        self.assertFalse(compare(self.results())["failures"])

    def test_python_regression_is_not_hidden_by_c_budget(self):
        results = self.results()
        results[0]["seconds"] = [1.150001] * 3
        self.assertIn("rust/python", compare(results)["failures"][0])

    def test_beta_limits_are_inclusive_and_recorded(self):
        for backend, limit in PERFORMANCE_LIMITS.items():
            with self.subTest(backend=backend):
                results = self.results()
                for row in results:
                    row["seconds"] = [limit if row["backend"] != backend else 1.0] * 3
                verdict = compare(results)
                self.assertFalse(verdict["failures"])
                self.assertEqual(verdict["performance_policy"], PERFORMANCE_POLICY)
                self.assertEqual(verdict["performance_limits"], PERFORMANCE_LIMITS)
                for row in results:
                    if row["backend"] == "rust":
                        row["seconds"] = [limit + 0.000001] * 3
                self.assertTrue(compare(results)["failures"])

    def test_invalid_samples_and_duplicate_workers_fail(self):
        for samples in (
            [],
            [1.0],
            [0.0] * 3,
            [-1.0] * 3,
            [float("nan")] * 3,
            [float("inf")] * 3,
        ):
            with self.subTest(samples=samples):
                results = self.results()
                results[0]["seconds"] = samples
                self.assertTrue(compare(results)["failures"])
        results = self.results()
        self.assertTrue(compare(results + [results[0]])["failures"])

    def test_c_budget_is_enforced(self):
        results = self.results()
        for row in results:
            if row["backend"] == "c":
                row["seconds"] = [0.5] * 3
        self.assertEqual(len(compare(results)["failures"]), len(BENCHMARKS))

    def test_missing_or_failed_worker_cannot_pass(self):
        results = self.results()
        self.assertTrue(compare(results[:-1])["failures"])
        results[0]["failures"] = ["timeout"]
        self.assertTrue(compare(results)["failures"])

    def test_small_resource_samples_do_not_prove_stability(self):
        self.assertTrue(growth_failures(self.samples()[:5]))

    def test_stable_cleanup_passes(self):
        self.assertFalse(growth_failures(self.samples()))

    def test_each_resource_leak_fails(self):
        for key in self.samples()[0]:
            with self.subTest(key=key):
                samples = copy.deepcopy(self.samples())
                for row in samples[-3:]:
                    row[key] += 20 * 1024**2
                self.assertTrue(growth_failures(samples))

    def test_live_sessions_fail_even_without_growth(self):
        samples = self.samples()
        for row in samples:
            row["sessions"] = 1
        self.assertTrue(growth_failures(samples))

    def test_settling_waits_for_server_cleanup(self):
        clean = self.samples()[0]
        closing = dict(clean, sessions=1)
        with (
            patch("run.snapshot", side_effect=[closing, closing, clean, clean]) as snap,
            patch("run.time.monotonic", return_value=0),
            patch("run.time.sleep") as sleep,
        ):
            self.assertEqual(settle_snapshot(None, None, "test"), clean)
        self.assertEqual(snap.call_count, 4)
        self.assertEqual(sleep.call_count, 3)

    def test_settling_does_not_hide_persistent_sessions(self):
        closing = dict(self.samples()[0], sessions=1)
        with (
            patch("run.snapshot", return_value=closing) as snap,
            patch("run.time.monotonic", side_effect=[0, 0, 1, 5]),
            patch("run.time.sleep"),
        ):
            sample = settle_snapshot(None, None, "test")
        self.assertEqual(snap.call_count, 3)
        self.assertEqual(sample["sessions"], 1)
        self.assertIn(
            "server sessions survived workload cleanup", growth_failures([sample] * 6)
        )

    def test_clean_settling_still_requires_stable_resources(self):
        clean = self.samples()[0]
        busy = dict(clean, threads=2)
        with (
            patch("run.snapshot", side_effect=[busy, clean, clean]) as snap,
            patch("run.time.monotonic", return_value=0),
            patch("run.time.sleep"),
        ):
            self.assertEqual(settle_snapshot(None, None, "test"), clean)
        self.assertEqual(snap.call_count, 3)

    def test_tail_percentile_does_not_round_down(self):
        self.assertEqual(percentile([1.0, 2.0, 3.0], 0.99), 3.0)


if __name__ == "__main__":
    unittest.main()
