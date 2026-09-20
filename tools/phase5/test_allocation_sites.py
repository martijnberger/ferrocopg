"""Profiler overhead is identified conservatively and never silently removed."""

import copy
import unittest
from types import SimpleNamespace

from allocation_sites import classify, summarize, validate


def stack(*names):
    return [(name, "native.c", 1) for name in names]


def record(allocator, size, *names):
    return SimpleNamespace(
        allocator=allocator,
        size=size,
        n_allocations=1,
        native_stack_trace=lambda: stack(*names),
        stack_trace=lambda: [("query", "driver.py", 10)],
    )


class AllocationSitesTests(unittest.TestCase):
    def test_conservative_native_classification(self):
        self.assertEqual(
            classify(
                stack(
                    "_PyFrame_MakeAndSetFrameObject",
                    "PyEval_GetFrame",
                    "call_profile_func.llvm.123",
                )
            ),
            "profile_frame_materialization",
        )
        self.assertEqual(
            classify(stack("force_instrument_lock_held.llvm.123")), "monitoring_setup"
        )
        self.assertEqual(classify([]), "unresolved_native_site")
        self.assertEqual(classify(stack("<unknown>")), "unresolved_native_site")
        for names in (
            ("_PyFrame_MakeAndSetFrameObject", "application_introspection"),
            ("PyType_GenericAlloc", "_TAIL_CALL_INSTRUMENTED_CALL"),
            ("PyEval_GetFrame",),
            ("call_profile_func_similar_name",),
        ):
            self.assertEqual(classify(stack(*names)), "other_instrumented_allocations")

    def fixture(self):
        return summarize(
            [
                record(
                    2,
                    200,
                    "_PyFrame_MakeAndSetFrameObject",
                    "PyEval_GetFrame",
                    "call_profile_func",
                ),
                record(2, 128, "force_instrument_lock_held"),
                record(6, 64, "driver_allocate"),
                record(6, 0, "driver_zero_size"),
                record(14, 4096),
                record(15, 4096),
                record(1, 0),
                record(5, 0),
            ],
            {"total_num_allocations": 5, "total_bytes_allocated": 4488},
        )

    def test_totals_retain_profiling_zero_size_and_mapping_allocations(self):
        result = self.fixture()
        self.assertEqual(
            result["classes"]["profile_frame_materialization"]["allocations"], 1
        )
        self.assertEqual(
            result["classes"]["other_instrumented_allocations"]["allocations"], 2
        )
        self.assertEqual(result["total_num_allocations"], 5)
        self.assertEqual(result["total_bytes_allocated"], 4488)

    def test_reject_unknown_allocator_and_inconsistent_analysis(self):
        with self.assertRaises(ValueError):
            summarize([record(99, 1)], {})
        result = self.fixture()
        for mutate in (
            lambda r: r["sites"].pop(),
            lambda r: r["classes"]["monitoring_setup"].update(bytes=0),
            lambda r: r["allocator_counts"].update({6: 0}),
            lambda r: r["sites"][0].update(classification="monitoring_setup"),
            lambda r: r.update(acceptance_evidence=True),
        ):
            broken = copy.deepcopy(result)
            mutate(broken)
            with self.assertRaises(ValueError):
                validate(broken, result)
