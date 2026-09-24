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
                session.configure_execution(None, 100)
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
