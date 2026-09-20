"""Reject malformed diagnostic measurements independently of a database."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import layer_probe
from layer_probe import parameter_protocol, validate, validate_dsn


class LayerProbeTests(unittest.TestCase):
    def test_parameter_protocol_uses_actual_dump_state_without_requiring_query_cache(
        self,
    ):
        adapted = SimpleNamespace(types=(21,), formats=[1])
        self.assertEqual(
            parameter_protocol(SimpleNamespace(_query=adapted)), ([21], [1])
        )
        self.assertEqual(
            parameter_protocol(SimpleNamespace(_query=None, _tx=adapted)), ([21], [1])
        )
        other = SimpleNamespace(types=(23,), formats=[0])
        self.assertEqual(
            parameter_protocol(SimpleNamespace(_query=other, _tx=adapted)), ([23], [0])
        )

    def test_worker_selects_backend_before_importing_dsn_parser(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "worker.json"
            with (
                patch(
                    "sys.argv",
                    [
                        "layer_probe",
                        "--worker",
                        "python",
                        "--revision",
                        "a" * 40,
                        "--output",
                        str(output),
                    ],
                ),
                patch.object(layer_probe, "public_probe", return_value={}) as worker,
                patch.object(layer_probe, "validate_dsn") as validate_connection,
            ):
                self.assertEqual(layer_probe.main(), 0)
                worker.assert_called_once()
                validate_connection.assert_not_called()

    def test_tls_cannot_silently_differ_between_native_and_public_paths(self):
        validate_dsn("host=127.0.0.1 sslmode=disable")
        for dsn in (
            "host=127.0.0.1",
            "host=127.0.0.1 sslmode=prefer",
            "sslmode=require",
        ):
            with self.subTest(dsn=dsn), self.assertRaises(ValueError):
                validate_dsn(dsn)

    def test_explicit_protocol_rejects_mismatched_case_format_and_parameter_oid(self):
        for case in ("constant", "prepared", "unprepared"):
            for fmt in ("binary", "text"):
                base = dict(
                    backend="postgres",
                    iterations=10,
                    warmup=1000,
                    wall_us=[20.0] * 3,
                    case=case,
                    result_format=fmt,
                    parameter_oids=[] if case == "constant" else [21],
                )
                validate(base, "postgres", 10, 3, case, fmt)
                for field, value in (
                    ("case", "other"),
                    ("result_format", "other"),
                    ("parameter_oids", [23]),
                ):
                    with self.subTest(case=case, fmt=fmt, field=field):
                        with self.assertRaises(ValueError):
                            validate(
                                dict(base, **{field: value}),
                                "postgres",
                                10,
                                3,
                                case,
                                fmt,
                            )

    def test_valid_measurement(self):
        validate(
            {
                "backend": "postgres",
                "iterations": 10000,
                "warmup": 1000,
                "wall_us": [20.0] * 9,
            },
            "postgres",
            10000,
            9,
        )

    def test_bad_identity_size_or_timing_fails(self):
        base = {
            "backend": "postgres",
            "iterations": 10000,
            "warmup": 1000,
            "wall_us": [20.0] * 9,
        }
        for key, value in (
            ("backend", "rust"),
            ("iterations", 10),
            ("warmup", 0),
            ("wall_us", [20.0]),
            ("wall_us", [0.0] * 9),
            ("wall_us", [float("nan")] * 9),
            ("wall_us", [float("inf")] * 9),
        ):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                validate(dict(base, **{key: value}), "postgres", 10000, 9)
