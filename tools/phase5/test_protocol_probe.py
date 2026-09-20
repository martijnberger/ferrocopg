"""Wire diagnostics preserve stream framing and never export sensitive payloads."""

import copy
import json
import struct
import unittest

from protocol_probe import Decoder, bind_metadata, summarize, validate


def frame(kind, body=b""):
    return kind.encode() + struct.pack("!I", len(body) + 4) + body


def startup():
    body = (
        struct.pack("!I", 196608) + b"user\0secret-user\0password\0secret-password\0\0"
    )
    return struct.pack("!I", len(body) + 4) + body


def bind():
    return (
        b"private-portal\0private-statement\0"
        + struct.pack("!HHH", 1, 1, 1)
        + struct.pack("!i", 12)
        + b"secret-value"
        + struct.pack("!HH", 1, 0)
    )


class ProtocolProbeTests(unittest.TestCase):
    def test_fragmentation_coalescing_and_payload_redaction(self):
        wire = (
            startup()
            + frame("p", b"secret-password\0")
            + frame("B", bind())
            + frame("S")
        )
        expected = Decoder(True).feed(wire)
        for split in range(len(wire) + 1):
            decoder = Decoder(True)
            self.assertEqual(
                decoder.feed(wire[:split]) + decoder.feed(wire[split:]), expected
            )
            decoder.finish()
        decoder = Decoder(True)
        events = [event for byte in wire for event in decoder.feed(bytes([byte]))]
        decoder.finish()
        self.assertEqual(events, expected)
        encoded = json.dumps(events)
        for secret in (
            "secret-user",
            "secret-password",
            "secret-value",
            "private-portal",
            "private-statement",
        ):
            self.assertNotIn(secret, encoded)
        self.assertEqual(events[2]["parameter_count"], 1)
        self.assertEqual(events[2]["parameter_formats"], [1])
        self.assertEqual(events[2]["result_formats"], [0])

    def test_invalid_startup_lengths_and_truncated_frames(self):
        for wire in (
            struct.pack("!II", 8, 80877103),
            struct.pack("!II", 8, 80877104),
            struct.pack("!I", 3),
            struct.pack("!I", 100_000_000),
        ):
            with self.subTest(wire=wire), self.assertRaises(ValueError):
                Decoder(True).feed(wire)
        for wire in (
            b"\0",
            startup()[:-1],
            startup() + frame("Q", b"select 42\0")[:-1],
        ):
            decoder = Decoder(True)
            decoder.feed(wire)
            with self.assertRaises(ValueError):
                decoder.finish()
        with self.assertRaises(ValueError):
            Decoder(False).feed(frame("Z", b"bad"))

    def test_bind_null_default_formats_and_bad_fields(self):
        self.assertEqual(
            bind_metadata(b"\0\0" + struct.pack("!HHiH", 0, 1, -1, 0)),
            dict(parameter_count=1, parameter_formats=[], result_formats=[]),
        )
        for body in (
            b"missing terminator",
            bind()[:-1],
            bind() + b"extra",
            b"\0\0" + struct.pack("!HHH", 1, 9, 0) + b"\0\0",
            b"\0\0" + struct.pack("!HHiH", 0, 1, -2, 0),
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                bind_metadata(body)

    def test_cycles_exclude_startup_and_do_not_claim_round_trips(self):
        events = [
            dict(
                type="Z",
                direction="backend",
                bytes=6,
                connection=0,
                startup_complete=True,
            ),
            dict(type="S", direction="frontend", bytes=5, connection=0),
            dict(type="S", direction="frontend", bytes=5, connection=0),
            dict(type="Z", direction="backend", bytes=6, connection=0),
            dict(type="Z", direction="backend", bytes=6, connection=0),
        ]
        summary = summarize(events)
        self.assertEqual(summary["completed_query_cycles"], 2)
        self.assertEqual(summary["frontend_syncs"], 2)
        self.assertEqual(summary["message_counts"]["backend:Z"], 3)
        self.assertNotIn("round_trips", summary)

    def test_report_recomputes_each_operation(self):
        events = [dict(type="Z", direction="backend", bytes=6, connection=0)] * 3
        report = dict(
            mode="protocol-diagnostic",
            acceptance_evidence=False,
            instrumented=True,
            backend="rust",
            workload="prepared",
            metadata={"revision": "a" * 40},
            warmup=10,
            iterations=3,
            rows=1000,
            events=events,
            installed_file_sha256={"native": "b" * 64},
            probe_sha256="c" * 64,
            operations=[
                dict(index=i, begin=i, end=i + 1, **summarize(events[i : i + 1]))
                for i in range(3)
            ],
        )
        validate(report, "a" * 40, "rust", "prepared")
        for mutate in (
            lambda r: r.update(acceptance_evidence=True),
            lambda r: r["operations"].pop(),
            lambda r: r["operations"][0].update(completed_query_cycles=2),
            lambda r: r["operations"][1].update(begin=0),
            lambda r: r.update(instrumented=False),
        ):
            broken = copy.deepcopy(report)
            mutate(broken)
            with self.assertRaises(ValueError):
                validate(broken, "a" * 40, "rust", "prepared")
