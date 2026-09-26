"""Live controls for the coarse native execution operation, before public routing."""

import gc
import os
import struct
import subprocess
import sys
import threading
import time
import unittest
import weakref
from types import SimpleNamespace


@unittest.skipUnless(os.environ.get("PHASE5_DSN"), "requires installed wheel and DSN")
class ExecutionBoundaryTests(unittest.TestCase):
    def setUp(self):
        from ferrocopg._rust import _ferrocopg as native

        self.session = native.connect_session(os.environ["PHASE5_DSN"])
        self.addCleanup(self.session.close)

    def execute(self, query, values=(), types=(), formats=(), **kwargs):
        return self.session.execute_query(
            query, list(values), list(types), list(formats), **kwargs
        )

    def query(self, query, values=(), types=(), formats=(), **kwargs):
        outcome = self.execute(query, values, types, formats, **kwargs)
        if outcome.error is not None:
            raise outcome.error
        self.assertIsNotNone(outcome.result)
        return outcome.result

    def snapshot(self):
        return self.session.execution_preparation_snapshot().snapshot()

    def server_prepared_count(self):
        result = self.session.run_params_format(
            "select count(*) from pg_prepared_statements", [], False
        )
        return int(result.get_value(0, 0))

    def test_live_preparation_matches_reference_and_server_ownership(self):
        from ferrocopg._preparing import PrepareManager

        reference = PrepareManager()
        reference.prepare_threshold = 2
        reference.prepared_max = 3
        self.session.configure_execution(2, 3)
        for index in range(60):
            oid = 21 if index % 3 else 23
            value = str(index).encode()
            query = f"select $1::int8 as value_{index % 5}".encode()
            prepare = (None, True, False)[index % 3]
            pgq = SimpleNamespace(query=query, types=(oid,))
            decision, name = reference.get(pgq, prepare)
            result = self.query(query, [value], [oid], [0], prepare=prepare)
            self.assertEqual(result.get_value(0, 0), value)
            key = reference.maybe_add_to_cache(pgq, decision, name)
            if key is not None:
                reference.validate(
                    key,
                    decision,
                    name,
                    [SimpleNamespace(status=2, command_status=b"SELECT 1")],
                )
            reference._to_flush.clear()
            snapshot = self.snapshot()
            self.assertEqual(
                snapshot["counts"],
                [(q, list(t), n) for (q, t), n in reference._counts.items()],
            )
            self.assertEqual(
                snapshot["names"],
                [(q, list(t), n) for (q, t), n in reference._names.items()],
            )
            self.assertEqual(snapshot["next_name"], reference._prepared_idx)
            self.assertEqual(snapshot["to_flush"], [])
            self.assertEqual(self.server_prepared_count(), len(reference._names))

        detached = self.session.execution_preparation_snapshot()
        detached.clear()
        self.assertEqual(len(self.snapshot()["names"]), len(reference._names))
        self.session.clear_execution_prepared()
        self.assertEqual(self.server_prepared_count(), 0)
        self.assertEqual(self.snapshot()["names"], [])
        self.assertEqual(self.snapshot()["counts"], [])

    def test_settings_types_and_formats_remain_dynamic(self):
        query = b"select $1::int8"
        for oid, code, value in (
            (21, "!h", 42),
            (23, "!i", 40000),
            (20, "!q", 1 << 40),
        ):
            for binary in (False, True):
                result = self.query(
                    query,
                    [struct.pack(code, value)],
                    [oid],
                    [1],
                    prepare=True,
                    binary=binary,
                )
                self.assertEqual(result.wire_format, int(binary))
                expected = struct.pack("!q", value) if binary else str(value).encode()
                self.assertEqual(result.get_value(0, 0), expected)
        self.assertEqual(self.server_prepared_count(), 3)

        self.session.configure_execution(None, 1 << 20000)
        self.query(b"select 99", prepare=True)
        self.assertEqual(self.server_prepared_count(), 3)
        self.session.configure_execution(1 << 20000, 3)
        self.query(b"select 100")
        self.assertEqual(self.server_prepared_count(), 3)
        self.session.configure_execution(0, 0)
        self.query(b"select 101")
        # Resizing rotates gradually, just like the Python preparation manager.
        self.assertEqual(self.server_prepared_count(), 3)

    def test_owned_result_navigation_projection_and_cache_cycles(self):
        outcome = self.execute(b"select i::int4 from generate_series(1, 3) i")
        self.assertEqual(outcome._index, 0)
        self.assertEqual(outcome._pos, 0)
        self.assertIs(outcome.current_result, outcome.result)
        self.assertEqual(outcome.statusmessage, "SELECT 3")
        self.assertEqual(outcome.fetchone(), [b"1"])
        self.assertEqual(outcome.fetchall(), [[b"2"], [b"3"]])
        self.assertIsNone(outcome.fetchone())
        self.assertIsNone(outcome.nextset())
        self.assertIs(outcome.set_result(-1), outcome)
        self.assertEqual(outcome._pos, 0)
        self.assertEqual(list(outcome.results()), [outcome])
        with self.assertRaises(IndexError):
            outcome.set_result(1)
        pgresults = outcome.pgresults("utf-8", 0)
        self.assertIs(outcome.pgresults("utf-8", 0), pgresults)
        self.assertEqual(pgresults[0].command_status, b"SELECT 3")
        self.assertEqual(pgresults[0].get_value(2, 0), b"3")
        outcome.set_encoding("ascii")
        self.assertIsNot(outcome.pgresults("utf-8", 0), pgresults)
        self.session.close()
        self.assertEqual(outcome.fetchone(), [b"1"])

        class Owner:
            pass

        owner = Owner()
        reference = weakref.ref(owner)
        owner.outcome = outcome
        outcome.pgresults("utf-8", 0).append(owner)
        del owner, outcome
        gc.collect()
        self.assertIsNone(reference())

    def test_reserved_and_immediate_queries_share_reference_preparation_state(self):
        from ferrocopg._preparing import PrepareManager

        reference = PrepareManager()
        reference.prepare_threshold = 1
        reference.prepared_max = 3
        self.session.configure_execution(1, 3)

        def check_state():
            reference._to_flush.clear()
            actual = self.snapshot()
            self.assertEqual(
                actual["counts"],
                [(q, list(t), n) for (q, t), n in reference._counts.items()],
            )
            self.assertEqual(
                actual["names"],
                [(q, list(t), n) for (q, t), n in reference._names.items()],
            )
            self.assertEqual(actual["next_name"], reference._prepared_idx)

        for batch in range(12):
            queued = []
            for index in range(4):
                query = f"select $1::int8 as col_{batch % 5}".encode()
                pgq = SimpleNamespace(query=query, types=(23,))
                prepare = False if index == 3 else None
                decision, name = reference.get(pgq, prepare)
                key = reference.maybe_add_to_cache(pgq, decision, name)
                plan = self.session.reserve_execution(query, [23], prepare)
                queued.append((query, plan, decision, name, key, index))
                check_state()
            for query, plan, decision, name, key, index in queued:
                result = self.query(
                    query, [b"42"], [23], [0], reservation=plan, binary=bool(index % 2)
                )
                self.assertEqual(
                    result.get_value(0, 0),
                    struct.pack("!q", 42) if index % 2 else b"42",
                )
                if key is not None:
                    reference.validate(
                        key,
                        decision,
                        name,
                        [SimpleNamespace(status=2, command_status=b"SELECT 1")],
                    )
                check_state()
            self.assertEqual(self.server_prepared_count(), len(reference._names))
            # The non-pipeline path reuses the same statement and updates it once.
            pgq = SimpleNamespace(query=query, types=(23,))
            decision, name = reference.get(pgq)
            key = reference.maybe_add_to_cache(pgq, decision, name)
            self.query(query, [b"43"], [23], [0])
            if key is not None:
                reference.validate(
                    key,
                    decision,
                    name,
                    [SimpleNamespace(status=2, command_status=b"SELECT 1")],
                )
            check_state()

    def test_reservations_reject_wrong_session_signature_and_replay(self):
        from ferrocopg._rust import _ferrocopg as native

        query = b"select $1::int4"
        plan = self.session.reserve_execution(query, [23], True)
        before = self.snapshot()
        callbacks = []
        other = native.connect_session(os.environ["PHASE5_DSN"])
        self.addCleanup(other.close)
        with self.assertRaisesRegex(ValueError, "another session"):
            other.execute_query(
                query,
                [b"42"],
                [23],
                [0],
                reservation=plan,
                preflight=lambda: callbacks.append("wrong owner"),
            )
        with self.assertRaisesRegex(ValueError, "query or types"):
            self.execute(
                query,
                [b"42"],
                [21],
                [0],
                reservation=plan,
                preflight=lambda: callbacks.append("wrong types"),
            )
        with self.assertRaisesRegex(ValueError, "query or types"):
            self.execute(b"select 43", reservation=plan)
        self.assertEqual(callbacks, [])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.server_prepared_count(), 0)
        self.query(query, [b"42"], [23], [0], reservation=plan)
        outcome = self.execute(query, [b"42"], [23], [0], reservation=plan)
        self.assertIsInstance(outcome.error, ValueError)
        self.assertIn("already consumed", str(outcome.error))
        stale = self.session.reserve_execution(query, [23])
        self.session.clear_execution_prepared()
        outcome = self.execute(query, [b"42"], [23], [0], reservation=stale)
        self.assertIsNone(outcome.error)
        self.assertEqual(outcome.result.get_value(0, 0), b"42")
        self.assertEqual(self.server_prepared_count(), 0)
        self.assertEqual(self.snapshot()["names"], [])

    def test_failed_and_cancelled_reservations_release_pending_names(self):
        from ferrocopg import errors

        for query, values, types, expected in (
            (b"select (", [], [], errors.SyntaxError),
            (b"select 1 / $1::int4", [b"0"], [23], errors.DivisionByZero),
        ):
            plan = self.session.reserve_execution(query, types, True)
            result = self.execute(
                query, values, types, [0] * len(types), reservation=plan
            )
            self.assertIsInstance(result.error, expected)
            self.assertEqual(self.snapshot()["names"], [])
            self.assertEqual(self.server_prepared_count(), 0)
        plan = self.session.reserve_execution(b"select 42", [], True)
        dependent = self.session.reserve_execution(b"select 42", [])
        self.session.cancel_execution_reservation(plan)
        self.session.cancel_execution_reservation(plan)
        self.assertEqual(self.snapshot()["names"], [])
        outcome = self.execute(b"select 42", reservation=dependent)
        self.assertIsNone(outcome.error)
        self.assertEqual(self.server_prepared_count(), 0)
        self.query(b"select 42", prepare=True)
        reused = self.session.reserve_execution(b"select 42", [])
        self.session.cancel_execution_reservation(reused)
        self.assertEqual(self.server_prepared_count(), 1)
        self.assertEqual(len(self.snapshot()["names"]), 1)

    def test_reserved_queries_replan_after_eviction_and_ddl_without_replay(self):
        self.query(b"create temporary table reservation_eviction (i int)")
        self.session.configure_execution(0, 1)
        queued = []
        for value in range(6):
            query = f"insert into reservation_eviction values ({value}) returning i".encode()
            queued.append((query, self.session.reserve_execution(query, [], True)))
        for index, (query, plan) in enumerate(queued):
            self.assertEqual(
                self.query(query, reservation=plan).get_value(0, 0), str(index).encode()
            )
        self.session.clear_execution_prepared()
        result = self.query(
            b"select count(*), sum(i) from reservation_eviction", prepare=False
        )
        self.assertEqual(result.get_value(0, 0), b"6")
        self.assertEqual(result.get_value(0, 1), b"15")
        self.assertLessEqual(self.server_prepared_count(), 1)
        ddl = b"alter table reservation_eviction add column j int"
        query = b"select i from reservation_eviction order by i"
        ddl_plan = self.session.reserve_execution(ddl, [], True)
        query_plan = self.session.reserve_execution(query, [], True)
        self.query(ddl, reservation=ddl_plan)
        self.assertEqual(self.server_prepared_count(), 0)
        result = self.query(query, reservation=query_plan)
        self.assertEqual(
            [result.get_value(i, 0) for i in range(6)],
            [str(i).encode() for i in range(6)],
        )
        self.assertEqual(self.server_prepared_count(), 1)

    def test_reservation_is_one_shot_across_threads(self):
        self.query(b"create temporary table reservation_once (i int)")
        query = b"insert into reservation_once values (1) returning i"
        plan = self.session.reserve_execution(query, [], True)
        outcomes = []
        barrier = threading.Barrier(3)

        def execute():
            barrier.wait()
            outcomes.append(self.execute(query, reservation=plan))

        threads = [threading.Thread(target=execute) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(sum(outcome.result is not None for outcome in outcomes), 1)
        self.assertEqual(
            sum(isinstance(outcome.error, ValueError) for outcome in outcomes), 1
        )
        self.assertEqual(
            self.query(b"select count(*) from reservation_once").get_value(0, 0), b"1"
        )

    def test_policy_properties_preserve_other_setting_and_allow_preflight(self):
        huge = 1 << 20000
        self.session.execution_prepare_threshold = huge
        self.assertEqual(self.session.execution_prepare_threshold, huge)
        self.assertEqual(self.session.execution_prepared_max, 100)
        self.session.execution_prepared_max = -huge
        self.assertEqual(self.session.execution_prepare_threshold, huge)
        self.assertEqual(self.session.execution_prepared_max, -huge)
        self.session.execution_prepare_threshold = None
        self.assertEqual(self.session.execution_prepared_max, -huge)
        self.session.execution_prepared_max = 100
        callbacks = []

        def configure():
            self.session.execution_prepare_threshold = 2
            self.session.execution_prepared_max = 3
            callbacks.append(self.session.execution_prepare_threshold)
            callbacks.append(self.session.execution_prepared_max)

        self.query(b"select 42", preflight=configure)
        self.assertEqual(callbacks, [2, 3])
        self.assertEqual(self.snapshot()["prepare_threshold"], 2)
        self.assertEqual(self.snapshot()["prepared_max"], 3)

    def test_invalidation_and_failed_execution_release_statement_owners(self):
        from ferrocopg import errors

        self.query(b"select 1", prepare=True)
        self.query(b"create temporary table native_execution_test (i int)")
        self.query(b"/* comment */ alter table native_execution_test add column j int")
        self.assertEqual(self.snapshot()["names"], [])
        self.assertEqual(self.server_prepared_count(), 0)
        self.query(b"select 2", prepare=True)
        self.query(b"begin")
        self.query(b"rollback")
        self.assertEqual(self.server_prepared_count(), 0)
        for _ in range(3):
            outcome = self.execute(
                b"select 1 / $1::int4", [b"0"], [23], [0], prepare=True
            )
            self.assertIsInstance(outcome.error, errors.DivisionByZero)
            self.assertIsNone(outcome.result)
            self.assertEqual(self.server_prepared_count(), 0)
            outcome = self.execute(b"select (", prepare=True)
            self.assertIsInstance(outcome.error, errors.SyntaxError)
            self.assertEqual(self.server_prepared_count(), 0)
        self.query(b"-- empty", prepare=True)
        self.assertEqual(self.server_prepared_count(), 0)
        self.assertEqual(self.snapshot()["names"], [])
        self.assertEqual(self.query(b"select 42").get_value(0, 0), b"42")

    def test_outcomes_own_notices_errors_and_result_lifetimes(self):
        from ferrocopg import errors, pq

        first = self.execute(b"do $$ begin raise notice 'first'; end $$")
        failed = self.execute(
            b"do $$ begin raise notice 'before error'; raise exception 'failure'; end $$",
            prepare=True,
        )
        last = self.execute(b"select 42")
        primary = pq.DiagnosticField.MESSAGE_PRIMARY
        self.assertIsNone(first.error)
        self.assertIsInstance(failed.error, errors.RaiseException)
        self.assertIsNone(failed.result)
        self.assertEqual([n[primary] for n in first.notices], [b"first"])
        self.assertEqual([n[primary] for n in failed.notices], [b"before error"])
        self.assertEqual(last.notices, [])
        self.assertEqual(self.session.drain_notices(), [])
        self.session.close()
        self.assertEqual(last.result.get_value(0, 0), b"42")
        self.assertEqual(last.result.command_tag, "SELECT 1")
        self.assertEqual([n[primary] for n in first.notices], [b"first"])

        class Owner:
            pass

        owner = Owner()
        owner.outcome = failed
        failed.error.owner = owner
        ref = weakref.ref(owner)
        del owner, failed
        gc.collect()
        self.assertIsNone(ref(), "execution error cycles must be visible to GC")

    def test_preflight_rejects_invalid_input_without_executing(self):
        self.query(b"create temporary table native_preflight_test (i int)")
        before = self.snapshot()
        for values, types, formats, encoding in (
            ([b"1"], [], [0], "utf-8"),
            ([b"1"], [23], [], "utf-8"),
            ([b"1"], [23], [2], "utf-8"),
            ([b"1"], [23], [0], "latin1"),
        ):
            with self.assertRaises(ValueError):
                self.execute(
                    b"insert into native_preflight_test values ($1)",
                    values,
                    types,
                    formats,
                    encoding=encoding,
                )
            self.assertEqual(self.snapshot(), before)
        result = self.query(b"select count(*) from native_preflight_test")
        self.assertEqual(result.get_value(0, 0), b"0")

    def test_notifications_are_captured_only_when_requested(self):
        self.session.listen("native_execution_events")
        first = self.execute(b"notify native_execution_events, 'first'")
        self.assertIsNone(first.error)
        self.assertEqual(first.notifications, [])
        pending = self.session.drain_notifications()
        self.assertEqual(
            [(n.channel, n.payload) for n in pending],
            [("native_execution_events", "first")],
        )
        second = self.execute(
            b"notify native_execution_events, 'second'",
            capture_notifications=True,
        )
        self.assertIsNone(second.error)
        self.assertEqual(self.session.drain_notifications(), [])
        self.query(b"select 42")
        self.session.close()
        self.assertEqual(
            [(n.channel, n.payload) for n in second.notifications],
            [("native_execution_events", "second")],
        )

    def test_mutable_parameters_are_owned_before_io(self):
        import psycopg

        data = bytearray(b"old")
        failures = []
        pid = self.session.backend_pid()

        def change_after_query_starts():
            try:
                with psycopg.connect(
                    os.environ["PHASE5_DSN"], autocommit=True
                ) as observer:
                    deadline = time.monotonic() + 5
                    while not observer.execute(
                        "select state = 'active' and query like '%%pg_sleep%%' "
                        "from pg_stat_activity where pid = %s",
                        (pid,),
                    ).fetchone()[0]:
                        if time.monotonic() > deadline:
                            raise AssertionError("native query never started")
                        time.sleep(0.005)
                    data[:] = b"new"
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=change_after_query_starts)
        thread.start()
        try:
            result = self.query(
                b"select $1::bytea, pg_sleep(0.2)",
                [memoryview(data)],
                [17],
                [1],
                binary=True,
            )
        finally:
            thread.join(6)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(data, b"new")
        self.assertEqual(result.get_value(0, 0), b"old")

    def test_preflight_runs_after_snapshot_without_a_native_guard(self):
        data = bytearray(b"old")
        calls = []

        def preflight():
            data[:] = b"new"
            # Re-entering the low-level session would deadlock under its mutex.
            calls.append(self.query(b"select 41").get_value(0, 0))
            self.session.begin()
            self.session.configure_execution(0, 100)

        result = self.query(
            b"select $1::bytea",
            [memoryview(data)],
            [17],
            [1],
            binary=True,
            preflight=preflight,
        )
        self.assertEqual(calls, [b"41"])
        self.assertEqual(result.get_value(0, 0), b"old")
        self.assertEqual(result.transaction_status, ord("T"))
        self.assertEqual(self.server_prepared_count(), 1)
        self.session.rollback()

        before = self.snapshot()
        expected = RuntimeError("preflight failed")

        def fail():
            raise expected

        with self.assertRaises(RuntimeError) as caught:
            self.execute(b"select 42", preflight=fail)
        self.assertIs(caught.exception, expected)
        self.assertEqual(self.snapshot(), before)

    def test_cancelled_operation_keeps_notices_and_recovers(self):
        from ferrocopg import errors, pq

        import psycopg

        pid = self.session.backend_pid()
        cancel = self.session.cancel_handle()
        failures = []

        def cancel_after_query_starts():
            try:
                with psycopg.connect(
                    os.environ["PHASE5_DSN"], autocommit=True
                ) as observer:
                    deadline = time.monotonic() + 5
                    while not observer.execute(
                        "select state = 'active' and wait_event = 'PgSleep' "
                        "from pg_stat_activity where pid = %s",
                        (pid,),
                    ).fetchone()[0]:
                        if time.monotonic() > deadline:
                            raise AssertionError("native query never started sleeping")
                        time.sleep(0.005)
                    cancel.cancel_timeout(2)
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=cancel_after_query_starts)
        thread.start()
        try:
            outcome = self.execute(
                b"do $$ begin raise notice 'cancelling'; perform pg_sleep(10); end $$",
                prepare=True,
            )
        finally:
            thread.join(6)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertIsInstance(outcome.error, errors.QueryCanceled)
        self.assertIsNone(outcome.result)
        self.assertEqual(
            [n[pq.DiagnosticField.MESSAGE_PRIMARY] for n in outcome.notices],
            [b"cancelling"],
        )
        self.assertEqual(self.server_prepared_count(), 0)
        self.assertEqual(self.snapshot()["names"], [])
        self.assertEqual(self.query(b"select 42").get_value(0, 0), b"42")

    @unittest.skipIf(os.name == "nt", "requires POSIX signal delivery")
    def test_signal_error_and_notices_belong_to_the_interrupted_operation(self):
        code = """
import os
import signal
import threading
import time
import psycopg
from ferrocopg import pq
from ferrocopg._rust import _ferrocopg as native

class Interrupted(RuntimeError):
    pass

session = native.connect_session(os.environ["PHASE5_DSN"])
pid = session.backend_pid()
previous = signal.getsignal(signal.SIGINT)
try:
    with psycopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as observer:
        for index in range(3):
            session.configure_execution(5, 100)
            expected = Interrupted(index)
            def interrupt(signum, frame):
                session.execution_prepare_threshold = None
                session.execution_prepared_max = 101
                assert session.execution_prepare_threshold is None
                assert session.execution_prepared_max == 101
                raise expected
            signal.signal(signal.SIGINT, interrupt)
            queued, failures = [], []
            def send_signal_then_query():
                try:
                    deadline = time.monotonic() + 3
                    while not observer.execute(
                        "select state = 'active' and wait_event = 'PgSleep' "
                        "from pg_stat_activity where pid = %s", (pid,)
                    ).fetchone()[0]:
                        assert time.monotonic() < deadline, "query did not sleep"
                        time.sleep(0.005)
                    os.kill(os.getpid(), signal.SIGINT)
                    queued.append(session.execute_query(b"select 42", [], [], []))
                except BaseException as error:
                    failures.append(error)
            worker = threading.Thread(target=send_signal_then_query, daemon=True)
            worker.start()
            outcome = session.execute_query(
                b"do $$ begin raise notice 'interrupted'; perform pg_sleep(10); end $$",
                [], [], [], prepare=True,
            )
            worker.join(3)
            assert not worker.is_alive(), "queued operation did not finish"
            assert not failures, failures
            assert outcome.error is expected, "exception changed operation or identity"
            assert outcome.result is None
            primary = pq.DiagnosticField.MESSAGE_PRIMARY
            assert [n[primary] for n in outcome.notices] == [b"interrupted"]
            assert len(queued) == 1
            assert queued[0].error is None
            assert queued[0].notices == []
            assert queued[0].result.get_value(0, 0) == b"42"
            assert session.execution_preparation_snapshot().snapshot()["names"] == []
            assert session.execution_preparation_snapshot().snapshot()["prepare_threshold"] is None
finally:
    signal.signal(signal.SIGINT, previous)
    session.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
