"""Incomplete, mismatched, or changed matrix runs are not valid diagnostics."""

import copy
import json
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import layer_matrix as matrix

REVISION = "a" * 40
SOURCES = {f"source-{i}": str(i) * 64 for i in range(6)}


def fixture(output, case, fmt):
    output.mkdir()
    summary = dict(
        mode="layer-diagnostic",
        acceptance_evidence=False,
        revision=REVISION,
        failures=[],
        protocol=matrix.protocol(case, fmt),
        sha256=SOURCES,
        orders={},
    )
    for order in ("forward", "reverse"):
        summary["orders"][order] = {}
        for layer in matrix.LAYERS:
            raw = dict(
                backend=layer,
                iterations=10000,
                warmup=1000,
                wall_us=[20.0] * 9,
                case=case,
                result_format=fmt,
                parameter_oids=[] if case == "constant" else [21],
            )
            if layer in matrix.PUBLIC_LAYERS:
                backend = "rust" if layer == "binding" else layer
                raw.update(
                    metadata=dict(
                        revision=REVISION,
                        python="test",
                        platform="test",
                        machine="test",
                        server="test",
                        server_settings={},
                    ),
                    installed_file_sha256={backend: {"native.so": "a" * 64}},
                )
            (output / f"{order}-{layer}.json").write_text(json.dumps(raw))
            summary["orders"][order][layer] = dict(
                wall_us=raw["wall_us"], median_us=20.0
            )
    (output / "summary.json").write_text(json.dumps(summary))
    return summary


class LayerMatrixTests(unittest.TestCase):
    def test_six_exact_protocols(self):
        self.assertEqual(len(matrix.MATRIX), 6)
        for case, fmt in matrix.MATRIX:
            with self.subTest(case=case, fmt=fmt), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "case"
                fixture(output, case, fmt)
                environment, files = matrix.validate_case(
                    output, REVISION, case, fmt, SOURCES
                )
                self.assertEqual(environment["server"], "test")
                self.assertEqual(set(files), {"rust", "python", "c"})
                expected = matrix.protocol(case, fmt)
                self.assertEqual(expected["prepared"], case != "unprepared")
                self.assertEqual(expected["result_format"], fmt)
                self.assertEqual(expected["in_flight"], 1)
                self.assertFalse(expected["tls"])
                if case != "constant":
                    self.assertEqual(expected["parameter_oids"], [21])
                    self.assertEqual(expected["parameter_format"], "binary")

    def test_summary_protocol_source_and_complete_medians_are_required(self):
        mutations = (
            lambda s: s.update(acceptance_evidence=True),
            lambda s: s.update(revision="b" * 40),
            lambda s: s["failures"].append("failed"),
            lambda s: s["protocol"].update(prepared=False),
            lambda s: s["protocol"].update(parameter_oids=[23]),
            lambda s: s["protocol"].update(result_format="binary"),
            lambda s: s["sha256"].update({"other-source": "a" * 64}),
            lambda s: s["orders"].pop("reverse"),
            lambda s: s["orders"]["forward"].pop("binding"),
            lambda s: s["orders"]["reverse"]["rust"].update(median_us=1),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "case"
                summary = copy.deepcopy(fixture(output, "prepared", "text"))
                mutate(summary)
                (output / "summary.json").write_text(json.dumps(summary))
                with self.assertRaises(ValueError):
                    matrix.validate_case(output, REVISION, "prepared", "text", SOURCES)

    def test_worker_protocol_revision_environment_and_code_must_match(self):
        mutations = (
            lambda r: r.update(case="unprepared"),
            lambda r: r.update(parameter_oids=[23]),
            lambda r: r.update(wall_us=[float("nan")] * 9),
            lambda r: r["metadata"].update(revision="b" * 40),
            lambda r: r["metadata"].update(server="different"),
            lambda r: r.update(installed_file_sha256={}),
            lambda r: r["installed_file_sha256"]["rust"].update(
                {"native.so": "b" * 64}
            ),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "case"
                fixture(output, "prepared", "text")
                p = output / "reverse-rust.json"
                raw = json.loads(p.read_text())
                mutate(raw)
                p.write_text(json.dumps(raw))
                with self.assertRaises(ValueError):
                    matrix.validate_case(output, REVISION, "prepared", "text", SOURCES)

    def coordinate(self, root, *, fail=False, drift=False):
        args = SimpleNamespace(
            revision=REVISION,
            wheel=Path("wheel"),
            native_rust=Path("rust"),
            native_libpq=Path("libpq"),
            output=root / "results",
        )
        calls = []

        def command(argv, log, timeout):
            case, fmt = (
                argv[argv.index("--case") + 1],
                argv[argv.index("--result-format") + 1],
            )
            calls.append((case, fmt))
            self.assertEqual(timeout, 1800)
            self.assertEqual(argv[argv.index("--iterations") + 1], "10000")
            self.assertEqual(argv[argv.index("--samples") + 1], "9")
            output = Path(argv[argv.index("--output") + 1])
            fixture(output, case, fmt)
            log.write_text("retained log\n")
            if fail and len(calls) == 2:
                raise RuntimeError("probe failed; preserve partial output")

        frozen = {"wheel": "frozen"}
        with (
            patch.object(matrix, "source_identity", return_value=SOURCES),
            patch.object(
                matrix,
                "identity",
                **(
                    {"side_effect": [frozen, frozen, frozen, {"wheel": "changed"}]}
                    if drift
                    else {"return_value": frozen}
                ),
            ),
            patch.object(matrix, "run_command", side_effect=command),
            patch.object(
                matrix, "HostActivity", side_effect=lambda p: nullcontext()
            ) as activity,
        ):
            status = matrix.coordinate(args)
        self.assertEqual(activity.call_count, len(calls))
        self.assertEqual(
            [call.args[0].name for call in activity.call_args_list],
            [f"{case}-{fmt}-host-activity.jsonl" for case, fmt in calls],
        )
        return status, json.loads((args.output / "manifest.json").read_text()), calls

    def test_coordinator_preserves_all_cases_and_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            status, manifest, calls = self.coordinate(Path(tmp))
            self.assertEqual(status, 0)
            self.assertEqual(calls, list(matrix.MATRIX))
            self.assertEqual(len(manifest["cases"]), 6)
            self.assertEqual(manifest["status"], "completed")
            self.assertFalse(manifest["acceptance_evidence"])
            self.assertEqual(len(manifest["artifact_sha256"]), 108)

    def test_coordinator_keeps_failure_and_detects_changed_install(self):
        for option, count in (("fail", 2), ("drift", 1)):
            with self.subTest(option=option), tempfile.TemporaryDirectory() as tmp:
                status, manifest, calls = self.coordinate(Path(tmp), **{option: True})
                self.assertEqual(status, 1)
                self.assertEqual(len(calls), count)
                self.assertEqual(manifest["status"], "failed")
                self.assertTrue(manifest["failures"])
                self.assertEqual(len(manifest["cases"]), 1)
