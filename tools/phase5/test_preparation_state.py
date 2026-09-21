"""Differential transition tests for the native execution-boundary prototype."""

import random
import sys
import unittest
from types import SimpleNamespace


class PreparationStateTests(unittest.TestCase):
    def setUp(self):
        from ferrocopg._preparing import Prepare, PrepareManager
        from ferrocopg._rust._ferrocopg import NativePreparationState

        self.Prepare = Prepare
        self.reference = PrepareManager()
        self.native = NativePreparationState()

    def compare(self):
        ref = self.reference
        expected = {
            "counts": [(q, list(t), n) for (q, t), n in ref._counts.items()],
            "names": [(q, list(t), n) for (q, t), n in ref._names.items()],
            "to_flush": list(ref._to_flush),
            "next_name": ref._prepared_idx,
            "prepare_threshold": ref.prepare_threshold,
            "prepared_max": ref.prepared_max,
        }
        self.assertEqual(self.native.snapshot(), expected)

    def get(self, query, prepare=None):
        expected = self.reference.get(query, prepare)
        self.assertEqual(self.native.get(query.query, query.types, prepare), expected)
        self.compare()
        return expected

    def add(self, query, decision):
        prep, name = decision
        key = self.reference.maybe_add_to_cache(query, prep, name)
        self.assertEqual(
            self.native.add(query.query, query.types, prep, name), key is not None
        )
        self.compare()
        return key

    def validate(self, query, decision, results):
        prep, name = decision
        reference_results = [
            SimpleNamespace(status=status, command_status=command)
            for status, command in results
        ]
        expected_error = None
        try:
            self.reference.validate(
                self.reference.key(query), prep, name, reference_results
            )
        except Exception as exc:
            expected_error = type(exc)
        if expected_error:
            with self.assertRaises(expected_error):
                self.native.validate(query.query, query.types, prep, results)
        else:
            self.native.validate(query.query, query.types, prep, results)
        self.compare()

    def configure(self, threshold, maximum):
        for state in (self.reference, self.native):
            state.prepare_threshold = threshold
            state.prepared_max = maximum
        self.compare()

    def clear(self):
        self.assertEqual(self.native.clear(), self.reference.clear())
        self.compare()

    def pop_flush(self):
        if self.reference._to_flush:
            self.assertEqual(
                self.native.pop_flush(), self.reference._to_flush.popleft()
            )
        else:
            with self.assertRaises(IndexError):
                self.native.pop_flush()
        self.compare()

    def execute(self, query, prepare=None, results=((2, b"SELECT 1"),)):
        decision = self.get(query, prepare)
        if self.add(query, decision) is not None:
            self.validate(query, decision, results)
        return decision

    def test_threshold_types_lru_and_gradual_eviction(self):
        queries = [
            SimpleNamespace(query=b"select $1", types=(oid,))
            for oid in (21, 23, 20, 25, 17)
        ]
        self.configure(2, 3)
        for query in queries:
            for _ in range(4):
                self.execute(query)
        self.configure(0, 1)
        # A shrink does not eagerly evict all existing entries.
        self.execute(queries[-1])
        for query in queries:
            self.execute(query)
            self.pop_flush()
        self.configure(None, sys.maxsize)
        self.execute(queries[0], True)
        self.execute(queries[0], False)
        self.configure(-1, 0)
        self.execute(queries[0])

    def test_command_status_boundaries_and_result_validation(self):
        query = SimpleNamespace(query=b"select 42", types=())
        commands = [
            None,
            b"",
            b"SELECT 1",
            b"DROP",
            b"DROP TABLE",
            b"DROP_thing",
            b"DROP2",
            b"DROP\xff",
            b"ALTER TABLE",
            b"ROLLBACK TO",
            b"DISCARD ALL",
            b"discard all",
            b" ROLLBACK",
            b"ROLLBACK;",
        ]
        for status in (0, 1, 2, 3, 7, 10):
            for command in commands:
                with self.subTest(status=status, command=command):
                    self.clear()
                    self.configure(0, 2)
                    decision = self.get(query)
                    self.add(query, decision)
                    self.validate(query, decision, [(status, command)])
        for results in ([], [(2, b"SELECT 1"), (2, b"SELECT 1")]):
            decision = self.get(query, True)
            self.add(query, decision)
            self.validate(query, decision, results)

    def test_pipeline_reservation_discard_and_close_queue(self):
        first = SimpleNamespace(query=b"select 1", types=())
        second = SimpleNamespace(query=b"select 2", types=())
        self.configure(0, 0)
        reserved = self.get(first)
        self.add(first, reserved)
        self.get(first)  # Reservation is visible before result validation.
        self.reference._names.pop(self.reference.key(first), None)
        self.reference._counts.pop(self.reference.key(first), None)
        self.native.discard(first.query, first.types)
        self.compare()
        self.execute(first)
        self.execute(second)
        self.clear()  # No names: pending individual closes are retained.
        self.pop_flush()
        self.pop_flush()
        self.pop_flush()
        self.configure(0, 1)
        self.execute(first)
        self.execute(second)
        self.clear()  # Names exist: replace individual closes with close-all.
        self.pop_flush()
        self.pop_flush()

    def test_seeded_independent_transition_sequences(self):
        queries = [
            SimpleNamespace(query=f"select $1 /* {i} */".encode(), types=(oid,))
            for i in range(3)
            for oid in (0, 21, 23, 4294967295)
        ]
        for seed in range(10):
            rng = random.Random(seed)
            pending = []
            for step in range(500):
                with self.subTest(seed=seed, step=step):
                    action = rng.randrange(8)
                    query = rng.choice(queries)
                    if action <= 1:
                        decision = self.get(query, rng.choice((None, False, True)))
                        pending.append((query, decision))
                    elif action == 2 and pending:
                        self.add(*rng.choice(pending))
                    elif action == 3 and pending:
                        query, decision = rng.choice(pending)
                        results = rng.choice(
                            (
                                [],
                                [(1, b"ROLLBACK")],
                                [(2, b"SELECT 1")],
                                [(7, None)],
                                [(1, b"ALTER TABLE"), (2, b"SELECT 1")],
                            )
                        )
                        self.validate(query, decision, results)
                    elif action == 4:
                        self.configure(
                            rng.choice((None, -1, 0, 2, 5)),
                            rng.choice((-1, 0, 1, 3, sys.maxsize)),
                        )
                    elif action == 5:
                        self.clear()
                    elif action == 6:
                        self.pop_flush()
                    else:
                        self.execute(query, rng.choice((None, False, True)))
                    self.compare()

    def test_snapshots_are_detached_and_states_are_independent(self):
        from ferrocopg._rust._ferrocopg import NativePreparationState

        query = SimpleNamespace(query=b"select $1", types=(21,))
        self.execute(query, True)
        snapshot = self.native.snapshot()
        snapshot["names"][0][1].append(23)
        snapshot["names"].clear()
        snapshot["to_flush"].append(b"changed")
        self.compare()
        other = NativePreparationState()
        self.assertEqual(other.snapshot()["next_name"], 0)
        self.assertEqual(other.snapshot()["names"], [])
        for decision in (0, 4, 255):
            with self.assertRaises(ValueError):
                self.native.add(b"q", [], decision, b"name")
            with self.assertRaises(ValueError):
                self.native.validate(b"q", [], decision, [])
        self.compare()

    def test_prepared_keys_and_names_own_mutable_inputs(self):
        query = bytearray(b"select $1")
        types = [21]
        name = bytearray(b"owned_name")
        self.native.add(query, types, self.Prepare.SHOULD, name)
        query[:] = b"changed"
        types[:] = [23]
        name[:] = b"changed"
        self.assertEqual(
            self.native.snapshot()["names"], [(b"select $1", [21], b"owned_name")]
        )
        self.assertEqual(
            self.native.get(b"select $1", [21]), (self.Prepare.YES, b"owned_name")
        )
