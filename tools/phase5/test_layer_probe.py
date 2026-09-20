"""Reject malformed diagnostic measurements independently of a database."""

import unittest

from layer_probe import validate


class LayerProbeTests(unittest.TestCase):
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
