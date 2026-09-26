"""Live regression tests for the staged wheel and official pool package."""

import gc
import importlib
import os
import struct
import subprocess
import sys
import unittest
import weakref
from unittest.mock import patch


@unittest.skipUnless(
    os.environ.get("PHASE5_DSN"), "requires an installed wheel and DSN"
)
class InstalledPoolTests(unittest.TestCase):
    def test_extended_results_keep_exact_server_outcomes(self):
        from ferrocopg import errors
        from ferrocopg._rust import _ferrocopg as native

        for prepared in (False, True):
            for binary in (False, True):
                with self.subTest(prepared=prepared, binary=binary):
                    session = native.connect_session(os.environ["PHASE5_DSN"])
                    retained = []
                    statements = {}

                    def run(query, tag, transaction=ord("I"), affected=0, params=()):
                        if prepared:
                            if query not in statements:
                                statements[query] = session.prepare_params(
                                    query, [value[0] for value in params]
                                )
                            result = session.run_prepared_params_format(
                                statements[query].statement_id, list(params), binary
                            )
                        else:
                            result = session.run_params_format(
                                query, list(params), binary
                            )
                        self.assertEqual(result.command_tag, tag)
                        self.assertEqual(result.transaction_status, transaction)
                        self.assertEqual(result.rows_affected, affected)
                        self.assertEqual(result.wire_format, int(binary))
                        retained.append((result, tag, transaction, affected))
                        return result

                    try:
                        run("-- only a comment", "")
                        run(
                            "select $1::int4",
                            "SELECT 1",
                            affected=1,
                            params=[(23, True, struct.pack("!i", 42))],
                        )
                        run(
                            "create temporary table outcome_test (i int)",
                            "CREATE TABLE",
                        )
                        run(
                            "with data as (select 42) insert into outcome_test "
                            "select * from data returning i",
                            "INSERT 0 1",
                            affected=1,
                        )
                        run(
                            "/* leading comment */ select i from outcome_test",
                            "SELECT 1",
                            affected=1,
                        )
                        run("select i from outcome_test where false", "SELECT 0")
                        run("begin", "BEGIN", ord("T"))
                        run("savepoint outcome_save", "SAVEPOINT", ord("T"))
                        if prepared:
                            for query in ("rollback to outcome_save", "commit"):
                                statements[query] = session.prepare_params(query, [])
                        with self.assertRaises(errors.DivisionByZero) as failure:
                            session.run_params_format("select 1 / 0", [], binary)
                        self.assertEqual(failure.exception.sqlstate, "22012")
                        run("rollback to outcome_save", "ROLLBACK", ord("T"))
                        run("commit", "COMMIT")
                        run("begin", "BEGIN", ord("T"))
                        with self.assertRaises(errors.DivisionByZero) as failure:
                            session.run_params_format("select 1 / 0", [], binary)
                        self.assertEqual(failure.exception.sqlstate, "22012")
                        # PostgreSQL rolls back an aborted transaction on COMMIT.
                        run("commit", "ROLLBACK")
                        run("truncate outcome_test", "TRUNCATE TABLE")
                        run("drop table outcome_test", "DROP TABLE")
                    finally:
                        session.close()

                    for result, tag, transaction, affected in retained:
                        self.assertEqual(result.command_tag, tag)
                        self.assertEqual(result.transaction_status, transaction)
                        self.assertEqual(result.rows_affected, affected)

    def test_execution_plan_keeps_value_types_and_result_formats_dynamic(self):
        import ferrocopg

        import psycopg

        for driver in (ferrocopg, psycopg):
            with self.subTest(driver=driver.__name__):
                with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                    query = "select %s::int8 as execution_plan_value"
                    with conn.cursor() as cur:
                        for value in (41, 40000, 1 << 40, 42):
                            for binary in (False, True):
                                cur.execute(
                                    query, (value,), prepare=True, binary=binary
                                )
                                self.assertEqual(cur.fetchone(), (value,))
                                self.assertEqual(cur.description[0].type_code, 20)
                                self.assertEqual(cur.pgresult.fformat(0), int(binary))
                    prepared = conn.execute(
                        "select parameter_types::text from pg_prepared_statements "
                        "where statement = %s",
                        ("select $1::int8 as execution_plan_value",),
                        prepare=False,
                    ).fetchall()
                    self.assertEqual(
                        sorted(row[0] for row in prepared),
                        ["{bigint}", "{integer}", "{smallint}"],
                    )

    def test_execution_result_survives_reentrant_loader_queries(self):
        import ferrocopg

        import psycopg

        for driver in (ferrocopg, psycopg):
            with self.subTest(driver=driver.__name__):
                Loader = importlib.import_module(f"{driver.__name__}.adapt").Loader
                with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                    calls = []

                    class QueryingLoader(Loader):
                        def load(self, data):
                            value = int(data)
                            calls.append(value)
                            return conn.execute(
                                "select %s::int4", (value + 100,), prepare=True
                            ).fetchone()[0]

                    with conn.cursor() as cur:
                        cur.adapters.register_loader(23, QueryingLoader)
                        cur.execute(
                            "select i::int4 from generate_series(41, 42) i",
                            prepare=True,
                        )
                        result = cur.pgresult
                        self.assertEqual(cur.fetchall(), [(141,), (142,)])
                        self.assertEqual(calls, [41, 42])
                        self.assertIs(cur.pgresult, result)
                        self.assertEqual(result.get_value(0, 0), b"41")
                        self.assertEqual(result.get_value(1, 0), b"42")
                        self.assertEqual(cur.rowcount, 2)
                        self.assertEqual(cur.rownumber, 2)
                    self.assertEqual(conn.execute("select 43").fetchone(), (43,))

    def test_parameter_buffers_are_snapshotted_before_preparation(self):
        import ferrocopg
        from ferrocopg.adapt import Dumper

        class Payload:
            pass

        for as_view in (False, True):
            with self.subTest(as_view=as_view):
                payload = bytearray(b"abcd")

                class MutableDumper(Dumper):
                    oid = 17
                    format = ferrocopg.pq.Format.BINARY

                    def dump(self, obj):
                        return memoryview(payload)[::2] if as_view else payload

                with ferrocopg.connect(
                    os.environ["PHASE5_DSN"], autocommit=True
                ) as conn:
                    conn.adapters.register_dumper(Payload, MutableDumper)
                    preflight = conn._ensure_transaction

                    def mutate_before_preflight():
                        payload[:] = b"xxxx"
                        return preflight()

                    # Native ownership must precede the transaction hook as
                    # well as preparation I/O, without a Python packet copy.
                    with patch.object(
                        conn, "_ensure_transaction", mutate_before_preflight
                    ):
                        cur = conn.execute(
                            "select %s::bytea", (Payload(),), prepare=True
                        )
                    self.assertEqual(cur.fetchone(), (b"ac" if as_view else b"abcd",))
                    self.assertEqual(payload, b"xxxx")
                    cur.execute("select %s::bytea", (Payload(),), prepare=True)
                    self.assertEqual(cur.fetchone(), (b"xx" if as_view else b"xxxx",))

    def test_native_path_removes_python_request_and_result_containers(self):
        import ferrocopg
        from ferrocopg import _ferrocopg as integration
        from ferrocopg._rust import _ferrocopg as native

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            with (
                patch.object(
                    integration._BoundParams,
                    "__new__",
                    side_effect=AssertionError("parameter packet"),
                ),
                patch.object(
                    integration.BackendResultCursor,
                    "__init__",
                    side_effect=AssertionError("result wrapper"),
                ),
            ):
                for query, params in (
                    ("select 42::int4", None),
                    ("select %s::int4", (42,)),
                ):
                    cur = conn.execute(query, params, prepare=True)
                    self.assertEqual(cur.fetchone(), (42,))
                    self.assertEqual(cur.statusmessage, "SELECT 1")
                    self.assertIsInstance(
                        integration._backend_cursor_adapter(cur)._result,
                        native.BackendExecutionOutcome,
                    )

    def test_transaction_preflight_errors_are_not_processed_twice(self):
        import ferrocopg

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            observed = []
            conn._session.error_handler = observed.append
            expected = ferrocopg.errors.DataError("preflight callback")

            def fail_directly():
                raise expected

            with patch.object(conn, "_ensure_transaction", fail_directly):
                with self.assertRaises(ferrocopg.errors.DataError) as caught:
                    conn.execute("select %s::int4", (42,), prepare=True)
            self.assertIs(caught.exception, expected)
            self.assertEqual(observed, [])
            self.assertEqual(str(expected), "preflight callback")

            def fail_from_command():
                conn._session.execute_params("select 1 / 0", [])

            with patch.object(conn, "_ensure_transaction", fail_from_command):
                with self.assertRaises(ferrocopg.errors.DivisionByZero) as caught:
                    conn.execute("select %s::int4", (42,), prepare=True)
            self.assertEqual(observed, [caught.exception])
            self.assertEqual(conn._prepared._names, {})
            self.assertEqual(conn.execute("select 42").fetchone(), (42,))

    def test_public_queries_use_one_native_preparation_owner(self):
        import ferrocopg

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            conn.prepare_threshold = 0
            native = conn._session._session
            with patch.object(
                conn._session,
                "prepare_bound",
                side_effect=AssertionError("legacy prepare"),
            ):
                cur = conn.execute("select %s::int4", (42,))
                self.assertEqual(cur.fetchone(), (42,))
                with conn.pipeline():
                    cursors = [conn.execute("select %s::int4", (i,)) for i in range(3)]
                self.assertEqual(
                    [cur.fetchone() for cur in cursors], [(0,), (1,), (2,)]
                )
            snapshot = native.execution_preparation_snapshot().snapshot()
            self.assertEqual(
                conn._prepared._names,
                {(q, tuple(t)): name for q, t, name in snapshot["names"]},
            )
            self.assertEqual(conn._prepared_ids, {})
            self.assertEqual(conn._prepared_statusmessages, {})
            detached = conn._prepared._names
            detached.clear()
            self.assertTrue(conn._prepared._names)
            conn.prepare_threshold = None
            conn.prepared_max = 17
            self.assertIsNone(native.execution_prepare_threshold)
            self.assertEqual(native.execution_prepared_max, 17)

    def test_public_pipeline_abort_cancels_unexecuted_native_reservations(self):
        import ferrocopg

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            with self.assertRaises(ferrocopg.errors.DivisionByZero):
                with conn.pipeline():
                    conn.execute("select 1 / 0", prepare=True)
                    conn.execute("select 42", prepare=True)
            snapshot = (
                conn._session._session.execution_preparation_snapshot().snapshot()
            )
            self.assertEqual(snapshot["names"], [])
            self.assertEqual(
                conn.execute(
                    "select count(*) from pg_prepared_statements", prepare=False
                ).fetchone(),
                (0,),
            )
            self.assertEqual(conn.execute("select 42", prepare=True).fetchone(), (42,))

    def test_notice_callbacks_control_notification_consumption(self):
        import ferrocopg

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            conn.execute("listen phase5_callback_policy")
            delivered = []

            def receive(notification):
                delivered.append(notification.payload)

            def change_policy(notice):
                if notice.message_primary == "enable":
                    conn.add_notify_handler(receive)
                elif notice.message_primary == "disable":
                    conn.remove_notify_handler(receive)

            conn.add_notice_handler(change_policy)
            for policy in ("enable", "disable"):
                conn.execute(
                    f"do $$ begin raise notice '{policy}'; "
                    f"perform pg_notify('phase5_callback_policy', '{policy}'); end $$",
                    prepare=True,
                )
            self.assertEqual(delivered, ["enable"])
            pending = list(conn.notifies(timeout=0, stop_after=1))
            self.assertEqual(
                [notification.payload for notification in pending], ["disable"]
            )

    def test_execution_schema_type_registry_snapshots(self):
        import ferrocopg

        import psycopg

        for driver in (ferrocopg, psycopg):
            with self.subTest(driver=driver.__name__):
                Loader = importlib.import_module(f"{driver.__name__}.adapt").Loader
                TypeInfo = importlib.import_module(f"{driver.__name__}.types").TypeInfo

                class NamedLoader(Loader):
                    format = driver.pq.Format.BINARY

                    def __init__(self, oid, context):
                        super().__init__(oid, context)
                        self.label = context.adapters.types[oid].name

                    def load(self, data):
                        return (self.label, int.from_bytes(data, "big", signed=True))

                with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                    conn.adapters.register_loader(23, NamedLoader)
                    conn.adapters.types.add(TypeInfo("schema_one", 23, 0))
                    before = conn.cursor(binary=True)
                    self.assertEqual(
                        conn.execute("select 41::int4", binary=True).fetchone(),
                        (("schema_one", 41),),
                    )
                    conn.adapters.types.add(TypeInfo("schema_two", 23, 0))
                    self.assertEqual(
                        conn.execute("select 42::int4", binary=True).fetchone(),
                        (("schema_two", 42),),
                    )
                    conn.adapters.types.clear()
                    conn.adapters.types.add(TypeInfo("schema_three", 23, 0))
                    self.assertEqual(
                        conn.execute("select 43::int4", binary=True).fetchone(),
                        (("schema_three", 43),),
                    )
                    before.execute("select 44::int4")
                    self.assertEqual(before.fetchone(), (("schema_one", 44),))
                    before.close()

    def test_execution_schema_constructor_mutation_is_local(self):
        import ferrocopg

        import psycopg

        for driver in (ferrocopg, psycopg):
            with self.subTest(driver=driver.__name__):
                adapt = importlib.import_module(f"{driver.__name__}.adapt")

                class Value:
                    pass

                class LocalLoader(adapt.Loader):
                    format = driver.pq.Format.BINARY

                    def load(self, data):
                        return ("local", int.from_bytes(data, "big", signed=True))

                class MutatingDumper(adapt.Dumper):
                    oid = 23
                    format = driver.pq.Format.BINARY

                    def __init__(self, cls, context):
                        super().__init__(cls, context)
                        context.adapters.register_loader(23, LocalLoader)

                    def dump(self, value):
                        return (42).to_bytes(4, "big", signed=True)

                with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                    conn.adapters.register_dumper(Value, MutatingDumper)
                    for _ in range(3):
                        self.assertEqual(
                            conn.execute(
                                "select %s", (Value(),), binary=True
                            ).fetchone(),
                            (("local", 42),),
                        )
                        self.assertEqual(
                            conn.execute("select 43::int4", binary=True).fetchone(),
                            (43,),
                        )

    def test_execution_schema_bounds_and_releases_registry_snapshots(self):
        import ferrocopg

        import psycopg

        for driver in (ferrocopg, psycopg):
            with self.subTest(driver=driver.__name__):
                TypeInfo = importlib.import_module(f"{driver.__name__}.types").TypeInfo
                references = []
                with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                    for i in range(64):
                        info = TypeInfo(f"schema_{i}", 23, 0)
                        references.append(weakref.ref(info))
                        conn.adapters.types.clear()
                        conn.adapters.types.add(info)
                        self.assertEqual(
                            conn.execute("select 42::int4", binary=True).fetchone(),
                            (42,),
                        )
                    gc.collect()
                    self.assertLessEqual(
                        sum(reference() is not None for reference in references), 32
                    )
                gc.collect()
                # The closed connection's public map still owns its last registration.
                self.assertTrue(
                    all(reference() is None for reference in references[:-1])
                )

    def test_execution_plan_adapter_snapshots_and_callback_lifetimes(self):
        import ferrocopg

        import psycopg

        for driver in (ferrocopg, psycopg):
            with self.subTest(driver=driver.__name__):
                Loader = importlib.import_module(f"{driver.__name__}.adapt").Loader
                contexts = []
                events = []

                class TaggedLoader(Loader):
                    format = driver.pq.Format.BINARY

                    def __init__(self, oid, context):
                        super().__init__(oid, context)
                        contexts.append(context)
                        events.append("init")

                    def load(self, data):
                        events.append("load")
                        return ("connection", int.from_bytes(data, "big", signed=True))

                class CursorLoader(TaggedLoader):
                    def load(self, data):
                        events.append("cursor load")
                        return ("cursor", int.from_bytes(data, "big", signed=True))

                with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                    before = conn.cursor(binary=True)
                    conn.adapters.register_loader(23, TaggedLoader)
                    first = conn.cursor(binary=True)
                    second = conn.cursor(binary=True)

                    before.execute("select 42::int4")
                    self.assertEqual(before.fetchone(), (42,))
                    self.assertEqual(events, [])

                    first.execute("select 43::int4")
                    self.assertEqual(first.fetchone(), (("connection", 43),))
                    second.execute("select 44::int4")
                    self.assertEqual(second.fetchone(), (("connection", 44),))
                    self.assertEqual(events, ["init", "load", "init", "load"])
                    self.assertIsNot(contexts[0], contexts[1])
                    self.assertIs(contexts[0].connection, conn)
                    self.assertIs(contexts[1].connection, conn)

                    first.execute("select 45::int4")
                    first.adapters.register_loader(23, CursorLoader)
                    self.assertEqual(first.fetchone(), (("cursor", 45),))
                    second.execute("select 46::int4")
                    self.assertEqual(second.fetchone(), (("connection", 46),))
                    before.execute("select 47::int4")
                    self.assertEqual(before.fetchone(), (47,))

                    before.close()
                    first.close()
                    second.close()

    def test_cursor_result_projection_identity_and_lifetime(self):
        import ferrocopg

        observed = []

        def factory(context):
            self.assertIs(context, cur)
            observed.append((context.pgresult.nfields, context.description[0].name))
            return tuple

        with ferrocopg.connect(
            os.environ["PHASE5_DSN"], autocommit=True, prepare_threshold=None
        ) as conn:
            cur = conn.cursor(row_factory=factory)
            self.assertIs(type(cur), ferrocopg.Cursor)
            self.assertIsNone(cur.pgresult)
            cur.execute("select i from generate_series(1, 3) i")
            self.assertEqual(observed, [(1, "i")])
            first = cur.pgresult
            self.assertEqual(cur.fetchone(), (1,))
            self.assertIs(cur.pgresult, first)
            cur.arraysize = 1
            self.assertEqual(cur.fetchmany(), [(2,)])
            self.assertIs(cur.pgresult, first)
            self.assertEqual(cur.rownumber, 2)
            cur.execute("select 4 as value; select 5 as value")
            self.assertIsNot(cur.pgresult, first)
            second = cur.pgresult
            self.assertEqual(cur.fetchone(), (4,))
            self.assertTrue(cur.nextset())
            self.assertEqual(cur.fetchone(), (5,))
            cur.set_result(0)
            self.assertIs(cur.pgresult, second)
            self.assertEqual(cur.fetchone(), (4,))
            with self.assertRaises(ferrocopg.errors.DivisionByZero):
                cur.execute("select 1 / 0")
            self.assertIsNone(cur.pgresult)
            cur.close()
            self.assertIsNone(cur.pgresult)
        self.assertEqual(first.get_value(2, 0), b"3")

    def test_cursor_projection_preserves_encoding_and_subclass_descriptors(self):
        import ferrocopg

        writes = []
        descriptor = ferrocopg.Cursor.pgresult

        class CustomCursor(ferrocopg.Cursor):
            @property
            def pgresult(self):
                return descriptor.__get__(self)

            @pgresult.setter
            def pgresult(self, result):
                writes.append(result)
                descriptor.__set__(self, result)

        with ferrocopg.connect(
            os.environ["PHASE5_DSN"], autocommit=True, prepare_threshold=None
        ) as conn:
            cur = conn.execute('select 42 as "name\u00e9"')
            conn.execute("set client_encoding to LATIN1")
            self.assertEqual(cur.pgresult.fname(0), "name\u00e9".encode())
            conn.execute("set client_encoding to UTF8")
            conn.cursor_factory = CustomCursor
            custom = conn.execute("select 43")
            self.assertIs(writes[-1], custom.pgresult)
            self.assertEqual(custom.pgresult.get_value(0, 0), b"43")
            self.assertEqual(custom.fetchone(), (43,))
            self.assertIs(writes[-1], custom.pgresult)
            custom.close()
            self.assertIsNone(writes[-1])

    def test_copy_preflight_preserves_transaction_and_result_status(self):
        import ferrocopg
        from ferrocopg import sql

        class Query(str):
            pass

        with ferrocopg.connect(os.environ["PHASE5_DSN"]) as conn:
            for query in (
                "COPY (select 42) TO STDOUT",
                " \t\nCoPy (select 42) TO STDOUT",
                b"COPY (select 42) TO STDOUT",
                sql.SQL("COPY (select {}) TO STDOUT").format(sql.Literal(42)),
                Query("COPY (select 42) TO STDOUT"),
            ):
                with self.subTest(query=query):
                    with self.assertRaisesRegex(
                        ferrocopg.ProgrammingError, "use copy\\(\\) instead"
                    ):
                        conn.execute(query)
                    self.assertEqual(
                        conn.info.transaction_status,
                        ferrocopg.pq.TransactionStatus.IDLE,
                    )

            cur = conn.execute("select %s::int as copycat", (42,))
            self.assertEqual(cur.fetchone(), (42,))
            self.assertEqual(cur.statusmessage, "SELECT 1")
            conn.execute("create temporary table copy_preflight (n int)")
            cur = conn.execute("insert into copy_preflight values (%s)", (42,))
            self.assertEqual(cur.statusmessage, "INSERT 0 1")
            self.assertEqual(cur.rowcount, 1)
            with self.assertRaises(ferrocopg.errors.SyntaxError):
                conn.execute("COPYCAT")
            conn.rollback()
            self.assertEqual(conn.execute("select 43").fetchone(), (43,))

    def test_transformer_dumper_cache_survives_registration(self):
        import ferrocopg
        from ferrocopg._ferrocopg import (
            _AdaptContext,
            _BackendTransformer,
            _pure_python_adapters,
        )
        from ferrocopg.adapt import Dumper, PyFormat

        calls = []

        class ReplacementDumper(Dumper):
            oid = 25

            def dump(self, value):
                calls.append(value)
                return b"replacement"

        with ferrocopg.connect(os.environ["PHASE5_DSN"]) as conn:
            for fmt in (PyFormat.AUTO, PyFormat.TEXT, PyFormat.BINARY):
                with self.subTest(format=fmt):
                    tx = _BackendTransformer(
                        _AdaptContext(conn, _pure_python_adapters(conn.adapters))
                    )
                    tx._encoding = conn.info.encoding
                    original = tx.dump_sequence((41, None), (fmt, fmt))
                    original_types, original_formats = tx.types, tx.formats
                    original_dumper = tx.get_dumper(41, fmt)
                    ReplacementDumper.format = (
                        ferrocopg.pq.Format.BINARY
                        if fmt == PyFormat.BINARY
                        else ferrocopg.pq.Format.TEXT
                    )
                    tx.adapters.register_dumper(int, ReplacementDumper)
                    tx.adapters.register_dumper(type(None), ReplacementDumper)
                    # NULL OIDs are resolved using the text registration.
                    if fmt == PyFormat.BINARY:

                        class TextNoneDumper(ReplacementDumper):
                            format = ferrocopg.pq.Format.TEXT

                        tx.adapters.register_dumper(type(None), TextNoneDumper)
                    self.assertEqual(tx.dump_sequence((41, None), (fmt, fmt)), original)
                    self.assertEqual(tx.types, original_types)
                    self.assertEqual(tx.formats, original_formats)
                    self.assertIs(tx.get_dumper(41, fmt), original_dumper)
                    self.assertEqual(calls, [])

                    fresh = _BackendTransformer(tx)
                    fresh._encoding = tx.encoding
                    self.assertEqual(
                        fresh.dump_sequence((41, None), (fmt, fmt)),
                        [b"replacement", None],
                    )
                    self.assertEqual(fresh.types, (25, 25))
                    self.assertEqual(calls, [41])
                    calls.clear()

    def test_transformer_dispatch_preserves_overrides_and_context_cycles(self):
        import ferrocopg
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg.adapt import Dumper, Loader, PyFormat

        calls = []

        def make_cycle():
            tx = adapter._BackendTransformer()
            tx._adapters = adapter._pure_python_adapters(tx.adapters)

            class Value:
                pass

            class ContextDumper(Dumper):
                oid = 23

                def __init__(self, cls, context):
                    super().__init__(cls, context)
                    self.context = context
                    calls.append("dumper init")

                def dump(self, value):
                    self_test.assertIs(self.context, tx)
                    calls.append("dump")
                    return b"42"

            class ContextLoader(Loader):
                def __init__(self, oid, context):
                    super().__init__(oid, context)
                    self.context = context
                    calls.append("loader init")

                def load(self, data):
                    self_test.assertIs(self.context, tx)
                    return int(data)

            self_test = self
            tx.adapters.register_dumper(Value, ContextDumper)
            tx.adapters.register_loader(23, ContextLoader)
            original = tx.get_dumper

            def tracked(value, format):
                calls.append("dispatch")
                return original(value, format)

            tx.get_dumper = tracked
            self.assertEqual(
                tx.dump_sequence((Value(), None, Value()), [PyFormat.AUTO] * 3),
                [b"42", None, b"42"],
            )
            loader = tx.get_loader(23, ferrocopg.pq.Format.TEXT)
            self.assertIs(tx.get_loader(23, ferrocopg.pq.Format.TEXT), loader)
            self.assertEqual(loader.load(b"42"), 42)
            return weakref.ref(tx), weakref.ref(loader)

        refs = make_cycle()
        self.assertEqual(
            calls,
            ["dispatch", "dumper init", "dump", "dispatch", "dump", "loader init"],
        )
        gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))

        tx = adapter._BackendTransformer()
        with self.assertRaises(ferrocopg.ProgrammingError) as raised:
            tx.get_dumper(object(), PyFormat.AUTO)
        self.assertTrue(raised.exception.__suppress_context__)
        self.assertIsNone(raised.exception.__cause__)

    def test_transformer_callbacks_can_replace_lookup_state(self):
        import ferrocopg
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg.adapt import Dumper, Loader, PyFormat

        tx = adapter._BackendTransformer()
        tx._adapters = adapter._pure_python_adapters(tx.adapters)
        calls = []

        class ResettingLoader(Loader):
            def __init__(self, oid, context):
                super().__init__(oid, context)
                calls.append("init")
                context._loaders = ({}, {})

            def load(self, data):
                return int(data)

        tx.adapters.register_loader(23, ResettingLoader)
        first = tx.get_loader(23, ferrocopg.pq.Format.TEXT)
        self.assertIs(tx.get_loader(23, ferrocopg.pq.Format.TEXT), first)
        self.assertEqual(calls, ["init"])

        class FallbackMap:
            def get_loader(self, oid, format):
                self_test.assertEqual(oid, 0)
                return ResettingLoader

        class ReplacingMap:
            def get_loader(self, oid, format):
                self_test.assertEqual(oid, 987654)
                tx._adapters = FallbackMap()
                return None

        self_test = self
        tx._adapters = ReplacingMap()
        self.assertEqual(
            tx.get_loader(987654, ferrocopg.pq.Format.TEXT).load(b"42"), 42
        )

        class ReplacementDumper(Dumper):
            def dump(self, obj):
                return b"replacement"

        class ReplacingDumper(Dumper):
            def dump(self, obj):
                tx._row_dumpers = [self, ReplacementDumper(int)]
                return b"first"

        tx._row_dumpers = [ReplacingDumper(int), ReplacementDumper(int)]
        # A discarded second dumper makes stale row-dumper snapshots visible.
        tx._row_dumpers[1].dump = lambda obj: b"discarded"
        self.assertEqual(
            tx.dump_sequence((1, 2), [PyFormat.AUTO] * 2),
            [b"first", b"replacement"],
        )

    def test_transformer_reentrant_callbacks_and_oid_cache_replacement(self):
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg import pq
        from ferrocopg.adapt import Dumper, PyFormat

        tx = adapter._BackendTransformer()
        tx._adapters = adapter._pure_python_adapters(tx.adapters)
        tx._encoding = "utf-8"
        events = []

        class ReentrantDumper(Dumper):
            oid = 23

            def __init__(self, cls, context):
                super().__init__(cls, context)
                self.context = context
                events.append(context.get_loader(23, pq.Format.TEXT).load(b"7"))

            def get_key(self, value, format):
                self_test.assertEqual(
                    self.context.dump_sequence((None,), [PyFormat.AUTO]), [None]
                )
                return self.cls

            def dump(self, value):
                return b"42"

        self_test = self
        tx.adapters.register_dumper(int, ReentrantDumper)
        self.assertEqual(tx.dump_sequence((42,), [PyFormat.AUTO]), [b"42"])
        self.assertEqual(tx.types, (23,))
        self.assertEqual(tx.formats, [pq.Format.TEXT])
        self.assertEqual(events, [7])

        class ResettingOidDumper(Dumper):
            oid = 23

            def __init__(self, cls, context):
                super().__init__(cls, context)
                context._oid_dumpers = ({}, {})

            def dump(self, value):
                return b"43"

        tx.adapters.register_dumper(None, ResettingOidDumper)
        first = tx.get_dumper_by_oid(23, pq.Format.TEXT)
        # Unlike loader construction, Python retains the captured OID cache.
        self.assertEqual(tx._oid_dumpers, ({}, {}))
        self.assertIsNot(tx.get_dumper_by_oid(23, pq.Format.TEXT), first)
        tx.set_dumper_types([23], pq.Format.TEXT)
        self.assertEqual(tx.dump_sequence((1,), [PyFormat.TEXT]), [b"43"])

    def test_transformer_defaults_and_independent_state(self):
        from unittest.mock import patch

        from ferrocopg import _ferrocopg as adapter
        from ferrocopg import postgres, pq
        from ferrocopg.adapt import AdaptersMap, PyFormat

        adapters = AdaptersMap(postgres.adapters)
        with patch.object(postgres, "adapters", adapters):
            first = adapter._BackendTransformer()
            second = adapter._BackendTransformer()
        self.assertIs(first.adapters, adapters)
        self.assertIsNone(first.connection)
        self.assertIsNone(first.pgresult)
        self.assertIsNone(first.types)
        self.assertIsNone(first.formats)
        self.assertEqual(first._none_oid, -1)
        self.assertIs(first.from_context(first), first)
        self.assertIsNot(first._dumpers, second._dumpers)
        self.assertIsNot(first._loaders[0], second._loaders[0])
        self.assertIsNot(first._loaders[1], second._loaders[1])
        self.assertIsNot(first._row_loaders, second._row_loaders)
        self.assertIsNot(first._oid_types, second._oid_types)
        first._encoding = second._encoding = "utf-8"
        first.set_loader_types([23, 25], pq.Format.TEXT)
        self.assertEqual(first.load_sequence((b"42", None)), (42, None))
        self.assertEqual(first.dump_sequence((), ()), [])
        self.assertEqual(first.types, ())
        self.assertEqual(first.formats, [])
        first.dump_sequence((None,), [PyFormat.AUTO])
        self.assertGreaterEqual(first._none_oid, 0)
        self.assertEqual(second._none_oid, -1)

    def test_parameter_errors_preserve_left_to_right_callbacks(self):
        import ferrocopg
        from ferrocopg.adapt import Dumper

        calls = []

        class Value:
            pass

        class ValueDumper(Dumper):
            oid = 25

            def __init__(self, cls, context):
                super().__init__(cls, context)
                calls.append("init")

            def dump(self, value):
                calls.append("dump")
                return b"custom"

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            conn.adapters.register_dumper(Value, ValueDumper)
            for later in (Value(), object()):
                with self.subTest(later=type(later)):
                    with self.assertRaises(ferrocopg.DataError):
                        conn.execute("select %s::text, %s::text", ("bad\x00", later))
                    self.assertEqual(calls, [])
            with self.assertRaises(ferrocopg.DataError):
                conn.execute("select %s::text, %s::text", (Value(), "bad\x00"))
            self.assertEqual(calls, ["init", "dump"])
            calls.clear()
            self.assertEqual(
                conn.execute(
                    "select %s::text, %s::text", (Value(), Value())
                ).fetchone(),
                ("custom", "custom"),
            )
            self.assertEqual(calls, ["init", "dump", "dump"])

    def test_session_setting_changes_keep_encoding_and_timeout_state_live(self):
        import ferrocopg

        with (
            ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as first,
            ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as second,
        ):
            for conn in (first, second, first):
                conn.execute("  SeT NaMeS 'LATIN1'")
                self.assertEqual(conn.info.encoding, "iso8859-1")
                self.assertEqual(
                    conn.execute("select %s::text", ("value\u00e9",)).fetchone(),
                    ("value\u00e9",),
                )
                conn.execute("reset client_encoding")
                self.assertEqual(conn.info.encoding, "utf-8")
                conn.execute("begin")
                conn.execute("set local client_encoding = 'LATIN1'")
                conn.execute("savepoint encoding_check")
                conn.execute("set local client_encoding = 'UTF8'")
                conn.execute("rollback to encoding_check")
                self.assertEqual(conn.info.encoding, "iso8859-1")
                conn.execute("rollback")
                self.assertEqual(conn.info.encoding, "utf-8")
                self.assertEqual(
                    conn.info.transaction_status, ferrocopg.pq.TransactionStatus.IDLE
                )
                conn.execute("start transaction")
                conn.execute("set local client_encoding = 'LATIN1'")
                conn.execute("commit")
                self.assertEqual(conn.info.encoding, "utf-8")
                conn.execute("set idle_in_transaction_session_timeout = '10s'")
                self.assertTrue(conn._idle_transaction_timeout_active)
                conn.execute("set idle_in_transaction_session_timeout = 0")
                self.assertFalse(conn._idle_transaction_timeout_active)

            first.execute("begin")
            second.execute("select pg_terminate_backend(%s)", (first.info.backend_pid,))
            with self.assertRaises(ferrocopg.OperationalError) as raised:
                first.execute("select 42")
            self.assertNotIsInstance(
                raised.exception, ferrocopg.errors.IdleInTransactionSessionTimeout
            )

    def test_unprepared_row_collection_keeps_metadata_counts_and_recovery(self):
        import ferrocopg

        for binary in (False, True):
            with (
                self.subTest(binary=binary),
                ferrocopg.connect(
                    os.environ["PHASE5_DSN"], autocommit=True, prepare_threshold=None
                ) as conn,
            ):
                conn.execute("create temporary table collected_rows (id int)")
                with conn.cursor(binary=binary) as cur:
                    cur.execute(
                        "select i as value from generate_series(1, %s) i", (2048,)
                    )
                    self.assertEqual(cur.rowcount, 2048)
                    self.assertEqual(cur.description[0].name, "value")
                    self.assertEqual(cur.fetchall(), [(i,) for i in range(1, 2049)])
                    cur.execute("select %s::int as empty_value where false", (42,))
                    self.assertEqual(cur.rowcount, 0)
                    self.assertEqual(cur.description[0].type_code, 23)
                    self.assertEqual(cur.fetchall(), [])
                    cur.execute(
                        "insert into collected_rows select i from generate_series(1, %s) i",
                        (7,),
                    )
                    self.assertEqual(cur.rowcount, 7)
                    self.assertIsNone(cur.description)
                    cur.execute(
                        "delete from collected_rows where id > %s returning id", (4,)
                    )
                    self.assertEqual(cur.rowcount, 3)
                    self.assertEqual(sorted(cur.fetchall()), [(5,), (6,), (7,)])
                    with self.assertRaises(ferrocopg.errors.DivisionByZero):
                        cur.execute(
                            "select 100 / (100 - i) from generate_series(1, %s) i",
                            (150,),
                        )
                    cur.execute("select %s::int", (42,))
                    self.assertEqual(cur.fetchone(), (42,))

    def test_unprepared_description_preserves_inference_and_error_recovery(self):
        import ferrocopg
        from ferrocopg._rust import _ferrocopg as native

        session = native.connect_session(os.environ["PHASE5_DSN"])
        try:
            session.run_params_format(
                "create temporary table portal_rows (id int)", [], False
            )
            session.run_params_format(
                "create type pg_temp.portal_mood as enum ('ready')", [], False
            )
            for binary in (False, True):
                with self.subTest(binary=binary):
                    result = session.run_params_format(
                        "select $1::int4 as number, $2::pg_temp.portal_mood as mood",
                        [(0, False, b"42"), (0, False, b"ready")],
                        binary,
                    )
                    self.assertEqual(result.column_name(0), "number")
                    self.assertEqual(result.column_oid(0), 23)
                    self.assertEqual(result.column_name(1), "mood")
                    self.assertGreater(result.column_oid(1), 16383)
                    self.assertEqual(
                        result.get_value(0, 0), b"\x00\x00\x00*" if binary else b"42"
                    )
                    self.assertEqual(result.get_value(0, 1), b"ready")
                    for query, values, error in (
                        ("select from where", [], ferrocopg.errors.SyntaxError),
                        (
                            "select $1::int4",
                            [(0, False, b"invalid")],
                            ferrocopg.errors.InvalidTextRepresentation,
                        ),
                        (
                            "select 1 / $1::int4",
                            [(0, False, b"0")],
                            ferrocopg.errors.DivisionByZero,
                        ),
                    ):
                        with self.assertRaises(error):
                            session.run_params_format(query, values, binary)
                        recovered = session.run_params_format(
                            "select $1::int4", [(0, False, b"42")], binary
                        )
                        self.assertEqual(recovered.column_oids, [23])
                        self.assertEqual(recovered.row_count, 1)
        finally:
            session.close()

    def test_statement_splitting_preserves_quotes_comments_and_empty_queries(self):
        import ferrocopg

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            for query in ("select 42", "  select 42  ", "/* comment */ select 42"):
                with conn.execute(query) as cur:
                    self.assertEqual(cur.fetchone(), (42,))
                    self.assertIsNone(cur.nextset())
            with conn.execute(" \t\n") as cur:
                self.assertEqual(
                    cur.pgresult.status, ferrocopg.pq.ExecStatus.EMPTY_QUERY
                )
            with conn.execute(
                "select ';'::text; /* outer ; /* inner ; */ */ "
                "select $tag$semi;colon$tag$::text; -- trailing ;\nselect 43"
            ) as cur:
                self.assertEqual(cur.fetchone(), (";",))
                self.assertTrue(cur.nextset())
                self.assertEqual(cur.fetchone(), ("semi;colon",))
                self.assertTrue(cur.nextset())
                self.assertEqual(cur.fetchone(), (43,))
                self.assertIsNone(cur.nextset())

    def test_native_rows_initialize_factories_without_fallback_wrappers(self):
        from unittest.mock import patch

        import ferrocopg
        from ferrocopg import _ferrocopg as adapter

        factories = []
        loaded = []

        def factory(cur):
            names = tuple(column.name for column in cur.description)
            factories.append(names)

            def row(values):
                loaded.append(tuple(values))
                return dict(zip(names, values))

            return row

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            with (
                conn.cursor(row_factory=factory) as cur,
                patch.object(
                    adapter.NoTlsCursorAdapter,
                    "_make_row_for_result",
                    side_effect=AssertionError(
                        "native result allocated a fallback wrapper"
                    ),
                ),
            ):
                cur.execute("select %s::int as first", (41,))
                self.assertEqual(factories, [("first",)])
                self.assertEqual(loaded, [])
                self.assertEqual(cur.fetchone(), {"first": 41})
                cur.execute("select 42 as second")
                self.assertEqual(factories, [("first",), ("second",)])
                self.assertEqual(cur.fetchall(), [{"second": 42}])
                with conn.pipeline():
                    cur.execute("select 43 as third")
                self.assertEqual(factories, [("first",), ("second",)])
                self.assertEqual(cur.fetchone(), {"third": 43})
                self.assertEqual(factories, [("first",), ("second",), ("third",)])
                self.assertEqual(loaded, [(41,), (42,), (43,)])

                error = ValueError("factory failed during execute")

                def fail_factory(cur):
                    raise error

                with conn.cursor(row_factory=fail_factory) as failing:
                    with self.assertRaises(ValueError) as caught:
                        failing.execute("select 44 as fourth")
                self.assertIs(caught.exception, error)

    def test_copy_parsers_preserve_mutable_snapshots_and_bytes_coercion(self):
        from types import SimpleNamespace

        from ferrocopg import _ferrocopg as adapter
        from ferrocopg import pq
        from ferrocopg._rust import _ferrocopg as native

        for binary in (False, True):
            payload = (
                struct.pack("!hi", 2, 5) + b"first" + struct.pack("!i", 6) + b"second"
                if binary
                else b"first\tsecond\n"
            )
            parse = native.parse_row_binary if binary else native.parse_row_text
            for use_view in (False, True):
                buffer = bytearray(payload)
                source = memoryview(buffer) if use_view else buffer

                def mutate(data):
                    if use_view:
                        buffer[:] = b"x" * len(buffer)
                    else:
                        buffer.clear()
                    return data

                tx = SimpleNamespace(
                    _nfields=2,
                    _copy_loaders=native.CopyCodecPlan([0, 0], [mutate, bytes]),
                )
                self.assertEqual(parse(source, tx), (b"first", b"second"))

            class CoercedBytes(bytes):
                def __bytes__(self):
                    return payload

            tx = SimpleNamespace(
                _nfields=2,
                _copy_loaders=native.CopyCodecPlan([0, 0], [bytes, bytes]),
            )
            self.assertEqual(parse(CoercedBytes(b"ignored"), tx), (b"first", b"second"))

        for payload, expected in (
            (b"\n", ()),
            (b"\n", ("",)),
            (b"\t\n", ("", "")),
            (b"value", ("value",)),
            (b"", ("",)),
            (b"a\t\\N\n", ("a", None)),
            (b"\\\\N\t\\q\\123\\x41\\\n", ("\\N", "\\q\\123\\x41\\")),
            (b"\\b\\t\\n\\v\\f\\r\\\\\n", ("\b\t\n\v\f\r\\",)),
        ):
            tx = adapter._BackendTransformer()
            tx.set_loader_types([25] * len(expected), pq.Format.TEXT)
            for source in (payload, bytearray(payload), memoryview(payload)):
                self.assertEqual(native.parse_row_text(source, tx), expected)

    def test_native_loader_classification_keeps_context_and_custom_classes(self):
        import ferrocopg
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg import pq
        from ferrocopg.types.numeric import (
            Int2BinaryLoader,
            Int4BinaryLoader,
            Int8BinaryLoader,
            IntLoader,
        )
        from ferrocopg.types.string import TextBinaryLoader, TextLoader

        for cls, code in (
            (IntLoader, 1),
            (Int2BinaryLoader, 4),
            (Int4BinaryLoader, 5),
            (Int8BinaryLoader, 6),
        ):
            self.assertEqual(adapter._native_loader_code(cls(23).load), code)
            self.assertIs(adapter._pure_loader_class(cls, {}), cls)
        for cls in (TextLoader, TextBinaryLoader):
            loader = cls(25)
            for encoding, code in (("utf-8", 2), ("", 3), ("latin-1", 0), ("utf-8", 2)):
                loader._encoding = encoding
                self.assertEqual(adapter._native_loader_code(loader.load), code)
        self.assertEqual(adapter._native_loader_code(lambda value: value), 0)

        class UnhashableMeta(type(IntLoader)):
            __hash__ = None

        class UnhashableLoader(IntLoader, metaclass=UnhashableMeta):
            pass

        self.assertEqual(adapter._native_loader_code(UnhashableLoader(23).load), 0)

        def check_custom_class():
            class CustomIntLoader(IntLoader):
                def load(self, data):
                    return super().load(data) + 100

            instance = CustomIntLoader(23)
            adapters = adapter._BackendAdaptersMap(ferrocopg.adapters, frozenset())
            adapters.register_loader("int4", CustomIntLoader)
            self.assertIs(adapters.get_loader(23, pq.Format.TEXT), CustomIntLoader)
            self.assertEqual(adapter._native_loader_code(instance.load), 0)
            self.assertEqual(instance.load(b"42"), 142)
            self.assertIs(
                adapter._pure_loader_class(IntLoader, {IntLoader: CustomIntLoader}),
                CustomIntLoader,
            )
            return weakref.ref(CustomIntLoader), weakref.ref(instance)

        refs = check_custom_class()
        gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))

    def test_native_result_metadata_is_detached_and_matches_public_rows(self):
        import ferrocopg
        from ferrocopg import rows
        from ferrocopg._rust import _ferrocopg as native

        names = [f"value_{i}" for i in range(64)]
        query = "select " + ", ".join(
            f"{i}::int4 as {name}" for i, name in enumerate(names)
        )
        session = native.connect_session(os.environ["PHASE5_DSN"])
        try:
            result = session.run_params_format(query, [], False)
            empty = session.run_params_format(
                "select from generate_series(1, 2)", [], False
            )
        finally:
            session.close()
        self.assertEqual(result.column_count, 64)
        self.assertEqual(result.column_oids, [23] * 64)
        self.assertEqual([result.column_name(i) for i in range(64)], names)
        self.assertEqual([result.column_oid(i) for i in range(64)], [23] * 64)
        result.column_oids.append(999)
        self.assertEqual(result.column_oids, [23] * 64)
        self.assertEqual(empty.column_count, 0)
        self.assertEqual(empty.column_oids, [])
        self.assertEqual(empty.row_count, 2)
        for item, index in ((result, 64), (empty, 0)):
            with self.assertRaises(IndexError):
                item.column_name(index)
            with self.assertRaises(IndexError):
                item.column_oid(index)

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            for binary in (False, True):
                for factory in (rows.tuple_row, rows.dict_row, rows.namedtuple_row):
                    with conn.cursor(binary=binary, row_factory=factory) as cur:
                        cur.execute(query)
                        self.assertEqual(
                            [column.name for column in cur.description], names
                        )
                        self.assertEqual(
                            [column.type_code for column in cur.description], [23] * 64
                        )
                        row = cur.fetchone()
                        expected = tuple(range(64))
                        self.assertEqual(
                            row,
                            dict(zip(names, expected))
                            if factory is rows.dict_row
                            else expected,
                        )

    def test_pgresult_projection_preserves_metadata_snapshots_and_lifetime(self):
        from ferrocopg import pq
        from ferrocopg._ferrocopg import BackendResultCursor
        from ferrocopg._rust import _ferrocopg as native

        for binary in (False, True):
            format = pq.Format.BINARY if binary else pq.Format.TEXT
            session = native.connect_session(os.environ["PHASE5_DSN"])
            try:
                result = session.run_params_format(
                    "select 42::int4 as \"value\u00e9\", NULL::text, ''::text",
                    [],
                    binary,
                )
                empty = session.run_params_format(
                    "select from generate_series(1, 2)", [], binary
                )
                command = session.run_params_format("set timezone = 'UTC'", [], binary)
            finally:
                session.close()

            cursor = BackendResultCursor(
                [result, empty, command], ["SELECT 1", "SELECT 2", "SET"]
            )
            projections = cursor.pgresults("utf-8", format)
            self.assertIs(cursor.pgresults("utf-8", format), projections)
            projected, zero_columns, status = projections
            self.assertIs(projected._result, result)
            self.assertEqual(projected.fname(0), b"value\xc3\xa9")
            self.assertEqual(projected.fname(-3), b"value\xc3\xa9")
            self.assertEqual(projected.status, pq.ExecStatus.TUPLES_OK)
            self.assertEqual((projected.nfields, projected.ntuples), (3, 1))
            self.assertEqual(projected.command_status, b"SELECT 1")
            self.assertEqual([projected.ftype(i) for i in range(3)], [23, 25, 25])
            self.assertEqual([projected.fformat(i) for i in range(3)], [format] * 3)
            for index in (-1, 3):
                self.assertEqual(projected.ftype(index), 0)
                with self.assertRaises(IndexError):
                    projected.fformat(index)
            for index in (-4, 3):
                with self.assertRaises(IndexError):
                    projected.fname(index)
            for row, column in ((1, 0), (0, 3)):
                with self.assertRaises(IndexError):
                    projected.get_value(row, column)

            latin = cursor.pgresults("iso8859-1", format)
            self.assertIsNot(latin, projections)
            self.assertEqual(latin[0].fname(0), b"value\xe9")
            self.assertEqual(projected.fname(0), b"value\xc3\xa9")
            self.assertEqual(zero_columns.status, pq.ExecStatus.TUPLES_OK)
            self.assertEqual((zero_columns.nfields, zero_columns.ntuples), (0, 2))
            self.assertEqual(zero_columns.command_status, b"SELECT 2")
            self.assertEqual(status.status, pq.ExecStatus.COMMAND_OK)
            self.assertEqual((status.nfields, status.ntuples), (0, 0))
            self.assertEqual(status.command_status, b"SET")
            for item in (zero_columns, status):
                self.assertEqual(item.ftype(0), 0)
                with self.assertRaises(IndexError):
                    item.fname(0)
                with self.assertRaises(IndexError):
                    item.fformat(0)

            del cursor, result, empty, command, projections, latin, session
            gc.collect()
            self.assertEqual(
                [projected.get_value(0, i) for i in range(3)],
                [struct.pack("!i", 42) if binary else b"42", None, b""],
            )

    def test_bound_parameters_preserve_buffers_formats_and_prepared_recovery(self):
        import ferrocopg
        from ferrocopg._rust import _ferrocopg as native

        session = native.connect_session(os.environ["PHASE5_DSN"])
        payload = bytes(range(256)) * 4096
        text = "value\u00e9".encode()
        number = (42).to_bytes(4, "big")
        values = [
            (17, True, payload),
            (25, False, text),
            (23, True, number),
            (25, False, None),
            (17, True, b""),
        ]
        query = "select $1::bytea, $2::text, $3::int4, $4::text, $5::bytea"
        try:
            prepared = session.prepare_params(query, [17, 25, 23, 25, 17])
            results = [session.run_params_format(query, values, True)]
            for supplied in (values[:-1], values + [values[0]]):
                with self.assertRaisesRegex(
                    ferrocopg.ProgrammingError, "expected 5 params"
                ):
                    session.run_prepared_params_format(
                        prepared.statement_id, supplied, True
                    )
            results.append(
                session.run_prepared_params_format(prepared.statement_id, values, True)
            )
        finally:
            session.close()
        del values
        for result in results:
            self.assertEqual(result.row_count, 1)
            self.assertEqual(
                [result.get_value(0, i) for i in range(5)],
                [payload, text, number, None, b""],
            )

    def test_cursor_metadata_stays_current_after_fetch_and_navigation(self):
        import ferrocopg

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            with conn.execute("select 1 as first; select 2 as second") as cur:
                first, second = cur._results
                self.assertEqual(cur.fetchone(), (1,))
                self.assertIs(cur.pgresult, first)
                self.assertTrue(cur.nextset())
                self.assertEqual(cur.fetchall(), [(2,)])
                self.assertIs(cur.pgresult, second)
                cur.set_result(0)
                self.assertEqual(cur.fetchmany(1), [(1,)])
                self.assertIs(cur.pgresult, first)
            self.assertIsNone(cur.pgresult)
            self.assertEqual(cur._results, [])

            with conn.pipeline():
                cur = conn.execute("select 3 as pipelined")
                self.assertEqual(cur.fetchone(), (3,))
                self.assertIs(cur.pgresult, cur._results[0])
                self.assertEqual(cur.description[0].name, "pipelined")

            with conn.cursor() as cur:
                for i, row in enumerate(
                    cur.stream("select generate_series(1, 3) as i"), 1
                ):
                    self.assertEqual(row, (i,))
                    self.assertIsNotNone(cur.pgresult)
                    self.assertEqual(cur.description[0].name, "i")

    def test_cursor_adapters_do_not_require_cyclic_collection(self):
        import ferrocopg
        from ferrocopg.types.numeric import IntLoader

        was_enabled = gc.isenabled()
        gc.collect()
        gc.disable()
        try:
            with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
                for close in (False, True):
                    cur = conn.execute("select %s::int4", (42,))
                    self.assertEqual(cur.fetchone(), (42,))
                    self.assertIs(cur.adapters, cur._ferrocopg_cursor.adapters)
                    adapter_ref = weakref.ref(cur._ferrocopg_cursor)
                    public_ref = weakref.ref(cur)
                    adapters = cur.adapters
                    if close:
                        cur.close()
                    del cur
                    self.assertIsNone(public_ref())
                    self.assertIsNone(adapter_ref())
                    # An independently retained map must have a harmless dead callback.
                    adapters.register_loader("int4", IntLoader)
        finally:
            if was_enabled:
                gc.enable()

    def test_native_row_construction_preserves_callbacks_and_error_cleanup(self):
        from ferrocopg._rust import _ferrocopg as native

        session = native.connect_session(os.environ["PHASE5_DSN"])
        try:
            result = session.run_params_format(
                "select i::text, ''::text, NULL::text from generate_series(1, 3) i",
                [],
                False,
            )
            empty = session.run_params_format(
                "select from generate_series(1, 2)", [], False
            )
        finally:
            session.close()

        calls = []

        def load(data):
            calls.append(data)
            return data.decode()

        class TupleSubclass(tuple):
            pass

        for factory in (tuple, TupleSubclass, list):
            calls.clear()
            rows = result.load_rows(0, 3, [0] * 3, [load] * 3, factory)
            self.assertEqual(rows, [factory((str(i), "", None)) for i in (1, 2, 3)])
            self.assertTrue(all(type(row) is factory for row in rows))
            self.assertEqual(calls, [b"1", b"", b"2", b"", b"3", b""])
            self.assertEqual(
                result.load_row(1, [0] * 3, [load] * 3, factory),
                factory(("2", "", None)),
            )
        self.assertEqual(empty.load_rows(0, 2, [], [], tuple), [(), ()])
        self.assertEqual(empty.load_row(0, [], [], tuple), ())
        with self.assertRaises(IndexError):
            result.load_row(3, [0] * 3, [load] * 3, tuple)
        with self.assertRaises(ValueError):
            result.load_row(0, [0], [load], tuple)
        self.assertEqual(result.load_rows(1, 1, [0] * 3, [load] * 3, tuple), [])
        for start, end in ((2, 1), (0, 4)):
            with self.assertRaises(ValueError):
                result.load_rows(start, end, [0] * 3, [load] * 3, tuple)
        with self.assertRaises(ValueError):
            result.load_rows(0, 3, [0], [load], tuple)

        class Value:
            pass

        refs = []
        seen = []
        error = ValueError("loader failure")

        def partial_load(data):
            seen.append(data)
            if len(seen) == 4:
                raise error
            value = Value()
            refs.append(weakref.ref(value))
            return value

        with self.assertRaises(ValueError) as caught:
            result.load_rows(0, 3, [0] * 3, [partial_load] * 3, tuple)
        self.assertIs(caught.exception, error)
        gc.collect()
        self.assertTrue(refs)
        self.assertTrue(all(ref() is None for ref in refs))
        seen.clear()
        seen.extend([b"1", b""])
        with self.assertRaises(ValueError) as caught:
            result.load_row(1, [0] * 3, [partial_load] * 3, tuple)
        self.assertIs(caught.exception, error)
        gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))

        retained = []

        def failing_factory(values):
            retained.append(values)
            if values[0] == "2":
                raise error
            return values

        with self.assertRaises(ValueError) as caught:
            result.load_rows(0, 3, [0] * 3, [load] * 3, failing_factory)
        self.assertIs(caught.exception, error)
        self.assertEqual(retained, [("1", "", None), ("2", "", None)])
        self.assertEqual(
            result.load_rows(1, 3, [0] * 3, [load] * 3, tuple),
            [("2", "", None), ("3", "", None)],
        )

    def test_single_row_loading_tracks_factories_loaders_and_navigation(self):
        import ferrocopg
        from ferrocopg import rows
        from ferrocopg.types.numeric import Int4BinaryLoader, IntLoader

        class CustomIntLoader(IntLoader):
            def load(self, data):
                return super().load(data) + 100

        class CustomBinaryLoader(Int4BinaryLoader):
            def load(self, data):
                return super().load(data) + 100

        class TupleSubclass(tuple):
            pass

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            for binary in (False, True):
                with conn.cursor(binary=binary) as cur:
                    cur.execute("select i::int4 as value from generate_series(1, 5) i")
                    self.assertEqual(cur.fetchone(), (1,))
                    cur.adapters.register_loader("int4", CustomIntLoader)
                    cur.adapters.register_loader("int4", CustomBinaryLoader)
                    self.assertEqual(cur.fetchmany(1), [(102,)])
                    self.assertEqual(cur.fetchone(), (103,))
                    cur.row_factory = lambda cur: TupleSubclass
                    self.assertEqual(cur.fetchone(), (104,))
                    cur.row_factory = rows.dict_row
                    self.assertEqual(cur.fetchall(), [{"value": 105}])
                    self.assertEqual(cur.rownumber, 5)
                    self.assertIsNone(cur.fetchone())
                    cur.scroll(0, mode="absolute")
                    self.assertEqual(cur.fetchone(), {"value": 101})
                    cur.execute("select from generate_series(1, 2)")
                    cur.row_factory = rows.tuple_row
                    self.assertEqual(cur.fetchone(), ())
                    self.assertEqual(cur.fetchall(), [()])

            def factory(cur):
                def make_row(values):
                    if values[0] == 1:
                        raise ValueError("factory failed")
                    return None if values[0] == 2 else values

                return make_row

            with conn.cursor(row_factory=factory) as cur:
                cur.execute("select i from generate_series(1, 3) i")
                with self.assertRaisesRegex(ValueError, "factory failed"):
                    cur.fetchone()
                self.assertEqual(cur.rownumber, 1)
                self.assertIsNone(next(cur))
                self.assertEqual(next(cur), (3,))
                with self.assertRaises(StopIteration):
                    next(cur)

    def test_native_session_lock_wait_releases_interpreter(self):
        code = """
import os
import threading
import time
import psycopg
from ferrocopg._rust import _ferrocopg as native

dsn = os.environ["PHASE5_DSN"]
with psycopg.connect(dsn, autocommit=True) as observer:
    for action in ("parameter", "drain_notices", "close"):
        session = native.connect_session(dsn)
        pid = session.backend_pid()
        errors = []
        def query():
            try:
                session.run_params_format("select pg_sleep(0.5)", [], False)
            except BaseException as exc:
                errors.append(exc)
        worker = threading.Thread(target=query, daemon=True)
        worker.start()
        try:
            deadline = time.monotonic() + 3
            while not observer.execute(
                "select state = 'active' and query like '%%pg_sleep%%' "
                "from pg_stat_activity where pid = %s", (pid,)
            ).fetchone()[0]:
                assert time.monotonic() < deadline, "query did not start"
                time.sleep(0.005)
            if action == "parameter":
                session.parameter("application_name")
            elif action == "drain_notices":
                assert session.drain_notices() == []
            else:
                session.close()
            worker.join(timeout=2)
            assert not worker.is_alive(), "session lock retained the interpreter"
            assert not errors, errors
        finally:
            session.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_native_notice_drain_preserves_order_errors_and_closed_state(self):
        import ferrocopg
        from ferrocopg._rust import _ferrocopg as native

        session = native.connect_session(os.environ["PHASE5_DSN"])
        primary = ferrocopg.pq.DiagnosticField.MESSAGE_PRIMARY
        try:
            self.assertEqual(session.drain_notices(), [])
            session.run_params_format(
                "do $$ begin raise notice 'first'; raise notice 'second'; end $$",
                [],
                False,
            )
            self.assertEqual(
                [notice[primary] for notice in session.drain_notices()],
                [b"first", b"second"],
            )
            self.assertEqual(session.drain_notices(), [])
            with self.assertRaises(ferrocopg.errors.RaiseException):
                session.run_params_format(
                    "do $$ begin raise notice 'before error'; "
                    "raise exception 'failure'; end $$",
                    [],
                    False,
                )
            self.assertEqual(
                [notice[primary] for notice in session.drain_notices()],
                [b"before error"],
            )
            self.assertEqual(session.drain_notices(), [])
            session.run_params_format("select 42", [], False)
        finally:
            session.close()
        with self.assertRaises(ferrocopg.OperationalError):
            session.drain_notices()

    @unittest.skipIf(os.name == "nt", "requires POSIX signal delivery")
    def test_native_signal_error_is_not_consumed_by_queued_operation(self):
        code = """
import os
import signal
import threading
import time
import psycopg
from ferrocopg._rust import _ferrocopg as native

class Interrupted(RuntimeError):
    pass

dsn = os.environ["PHASE5_DSN"]
session = native.connect_session(dsn)
pid = session.backend_pid()
previous = signal.getsignal(signal.SIGINT)
try:
    with psycopg.connect(dsn, autocommit=True) as observer:
        for iteration in range(3):
            expected = Interrupted(iteration)
            def interrupt(signum, frame):
                raise expected
            signal.signal(signal.SIGINT, interrupt)
            results, failures = [], []
            def queued_query():
                try:
                    deadline = time.monotonic() + 3
                    while not observer.execute(
                        "select state = 'active' and query like '%%pg_sleep%%' "
                        "from pg_stat_activity where pid = %s", (pid,)
                    ).fetchone()[0]:
                        assert time.monotonic() < deadline, "query did not start"
                        time.sleep(0.005)
                    os.kill(os.getpid(), signal.SIGINT)
                    result = session.run_params_format("select 42", [], False)
                    results.append(result.get_value(0, 0))
                except BaseException as exc:
                    failures.append(exc)
            worker = threading.Thread(target=queued_query, daemon=True)
            worker.start()
            try:
                session.run_params_format("select pg_sleep(10)", [], False)
            except Interrupted as caught:
                assert caught is expected, "exception belonged to another operation"
            else:
                raise AssertionError("query was not interrupted")
            worker.join(timeout=3)
            assert not worker.is_alive(), "queued operation did not finish"
            assert not failures, failures
            assert results == [b"42"], results
            assert session.run_params_format("select 43", [], False).get_value(0, 0) == b"43"
finally:
    signal.signal(signal.SIGINT, previous)
    session.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    @unittest.skipIf(os.name == "nt", "requires POSIX signal delivery")
    def test_signal_handler_can_query_another_connection_and_recover(self):
        code = """
import os
import signal
import threading
import ferrocopg

with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as active:
    with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as other:
        handled = []
        def interrupt(signum, frame):
            handled.append(other.execute("select 42").fetchone())
            raise KeyboardInterrupt

        previous = signal.signal(signal.SIGINT, interrupt)
        timer = threading.Timer(0.15, lambda: os.kill(os.getpid(), signal.SIGINT))
        timer.start()
        try:
            try:
                active.execute("select pg_sleep(10)")
            except KeyboardInterrupt:
                pass
            else:
                raise AssertionError("query did not receive KeyboardInterrupt")
            assert handled == [(42,)]
            assert active.execute("select 43").fetchone() == (43,)
        finally:
            timer.cancel()
            timer.join()
            signal.signal(signal.SIGINT, previous)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_wire_rows_survive_session_close_and_keep_raw_access(self):
        from ferrocopg._rust import _ferrocopg as native

        for binary in (False, True):
            session = native.connect_session(os.environ["PHASE5_DSN"])
            try:
                fields = [
                    "$1::int4",
                    "''::text",
                    "NULL::text",
                    "decode('00ff', 'hex')",
                    "repeat('x', 10000)",
                ]
                results = [
                    session.run_params_format(
                        "select " + ", ".join(fields[:count]),
                        [(23, False, b"42")],
                        binary,
                    )
                    for count in (1, 2, 3, 5)
                ]
                empty = session.run_params_format(
                    "select from generate_series(1, 3)", [], binary
                )
                session.run_params_format("select 'replacement'", [], binary)
            finally:
                session.close()

            expected = [
                struct.pack("!i", 42) if binary else b"42",
                b"",
                None,
                b"\x00\xff" if binary else b"\\x00ff",
                b"x" * 10000,
            ]
            for count, result in zip((1, 2, 3, 5), results):
                self.assertEqual(result.row_count, 1)
                self.assertEqual(
                    [result.get_value(0, i) for i in range(count)], expected[:count]
                )
                self.assertEqual(result.row(0), result.rows[0])
                self.assertEqual(
                    [None if v is None else bytes(v) for v in result.row(0)],
                    expected[:count],
                )
                with self.assertRaises(IndexError):
                    result.get_value(1, 0)
                with self.assertRaises(IndexError):
                    result.get_value(0, count)
                with self.assertRaises(IndexError):
                    result.row(1)
            self.assertTrue(empty.is_tuples)
            self.assertEqual(empty.rows, [[], [], []])
            self.assertEqual(empty.row_count, 3)

    def test_untyped_text_copy_keeps_dispatch_and_type_metadata(self):
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg._rust import _ferrocopg as native
        from ferrocopg.types.numeric import IntDumper

        class IntSubclass(int):
            def __str__(self):
                return "not an integer"

        for encoding in ("utf-8", "latin-1", "ascii"):
            for values in (
                (1, "text", None),
                (-(2**15), 2**15, 2**31, 2**63, -(2**100)),
                ("\u00e9\t\\\n",),
                (True, IntSubclass(42)),
                ([1, 2], "array"),
                (),
            ):
                original, optimized = (
                    adapter._BackendTransformer(),
                    adapter._BackendTransformer(),
                )
                original._encoding = optimized._encoding = encoding
                expected, actual = bytearray(), bytearray()
                adapter._format_row_text(values, original, expected)
                native.format_row_text(values, optimized, actual)
                self.assertEqual(actual, expected)
                self.assertEqual(optimized.types, original.types)
                self.assertEqual(optimized.formats, original.formats)

        class CustomIntDumper(IntDumper):
            def get_key(self, obj, format):
                return self.cls

            def dump(self, obj):
                return str(obj + 100).encode()

        tx = adapter._BackendTransformer()
        tx._adapters = adapter._pure_python_adapters(tx.adapters)
        tx.adapters.register_dumper(int, CustomIntDumper)
        output = bytearray()
        native.format_row_text((42, "custom"), tx, output)
        self.assertEqual(output, b"142\tcustom\n")

    def test_native_copy_codec_plan_releases_callback_cycles(self):
        from ferrocopg._rust import _ferrocopg as native

        class Owner:
            def convert(self, value):
                return value

        owner = Owner()
        owner.plan = native.CopyCodecPlan([0], [owner.convert])
        self.assertTrue(gc.is_tracked(owner.plan))
        ref = weakref.ref(owner)
        del owner
        gc.collect()
        self.assertIsNone(ref())
        with self.assertRaises(ValueError):
            native.CopyCodecPlan([1], [])

    def test_copy_pinned_dumpers_preserve_bytes_subclasses_and_errors(self):
        import ferrocopg
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg import pq
        from ferrocopg._rust import _ferrocopg as native
        from ferrocopg.types.numeric import Int4BinaryDumper

        class IntSubclass(int):
            def __str__(self):
                return "not the integer representation"

        class ReversedList(list):
            def __iter__(self):
                return reversed(self)

        for binary in (False, True):
            original = (
                adapter._format_row_binary if binary else adapter._format_row_text
            )
            optimized = native.format_row_binary if binary else native.format_row_text
            for encoding in ("utf-8", "latin-1", "ascii"):
                tx = adapter._BackendTransformer()
                tx._encoding = encoding
                tx.set_dumper_types([21, 23, 20, 25], pq.Format(binary))
                for values in (
                    (-2, 42, 2**40, "value\t\\\n"),
                    (None, None, None, None),
                    (IntSubclass(2), True, -(2**63), "\u00e9"),
                    ReversedList([1, 2, 3, "text"]),
                ):
                    expected, actual = bytearray(), bytearray()
                    original(values, tx, expected)
                    optimized(values, tx, actual)
                    self.assertEqual(actual, expected)
                invalid = [(1, 2)]
                invalid += [(2**15, 2, 3, "text")] if binary else [(1, 2, 3, "\x00")]
                for values in invalid:
                    with self.assertRaises(Exception) as expected:
                        original(values, tx, bytearray())
                    with self.assertRaises(type(expected.exception)) as actual:
                        optimized(values, tx, bytearray())
                    self.assertEqual(str(actual.exception), str(expected.exception))

        class CustomDumper(Int4BinaryDumper):
            def dump(self, value):
                return memoryview(struct.pack("!i", value + 100))

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            conn.execute("create temporary table phase5_custom_copy (value int)")
            with conn.cursor() as cur:
                cur.adapters.register_dumper(int, CustomDumper)
                with cur.copy(
                    "copy phase5_custom_copy from stdin (format binary)"
                ) as copy:
                    copy.set_types(["int4"])
                    copy.write_row((42,))
                    copy.write_row((None,))
            self.assertEqual(
                conn.execute("select * from phase5_custom_copy").fetchall(),
                [(142,), (None,)],
            )

    def test_query_context_reuse_invalidates_loaders_and_preserves_results(self):
        import ferrocopg
        from ferrocopg.types.numeric import Int4BinaryDumper, IntLoader

        adapted = []

        class CustomIntDumper(Int4BinaryDumper):
            def get_key(self, obj, format):
                adapted.append(obj)
                return super().get_key(obj, format)

        class CustomIntLoader(IntLoader):
            def load(self, data):
                return super().load(data) + 100

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select i from generate_series(%s::int4, %s::int4) i", (1, 3)
                )
                original = cur.pgresult
                self.assertEqual(cur.fetchone(), (1,))
                self.assertIs(cur.pgresult, original)
                cur.adapters.register_loader("int4", CustomIntLoader)
                self.assertEqual(cur.fetchall(), [(102,), (103,)])
                self.assertEqual(original.get_value(0, 0), b"1")
                cur.execute("select %s::int4", (4,))
                self.assertEqual(cur.fetchone(), (104,))
                self.assertIsNot(cur.pgresult, original)
                cur.execute("select 5::int4; select 6::int4")
                first = cur.pgresult
                self.assertEqual(cur.fetchone(), (105,))
                self.assertTrue(cur.nextset())
                self.assertIsNot(cur.pgresult, first)
                self.assertEqual(cur.fetchone(), (106,))
                self.assertEqual(first.get_value(0, 0), b"5")
                cur.adapters.register_dumper(int, CustomIntDumper)
                cur.execute("select %s::int4", (7,))
                self.assertEqual(cur.fetchone(), (107,))
                self.assertEqual(adapted, [7])

    def test_copy_native_loaders_preserve_custom_types_encoding_and_errors(self):
        import ferrocopg
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg import pq
        from ferrocopg._rust import _ferrocopg as native
        from ferrocopg.types.numeric import Int4BinaryLoader, IntLoader

        class CustomIntLoader(IntLoader):
            def load(self, data):
                return super().load(data) + 100

        class CustomBinaryLoader(Int4BinaryLoader):
            def load(self, data):
                return super().load(data) + 200

        def binary_row(*fields):
            return struct.pack("!h", len(fields)) + b"".join(
                struct.pack("!i", -1 if value is None else len(value)) + (value or b"")
                for value in fields
            )

        tx = adapter._BackendTransformer()
        tx._encoding = "utf-8"
        tx.set_loader_types([23, 25, 20], pq.Format.TEXT)
        self.assertEqual(
            native.parse_row_text(b"42\tvalue\\ttext\t\\N\n", tx),
            (42, "value\ttext", None),
        )
        with self.assertRaises(ferrocopg.ProgrammingError):
            native.parse_row_text(b"42\n", tx)
        with self.assertRaises(ValueError):
            native.parse_row_text(b"bad\tvalue\t1\n", tx)
        with self.assertRaises(ValueError) as original:
            tx.load_sequence([b"42", b"\xff", b"1"])
        with self.assertRaises(type(original.exception)) as optimized:
            native.parse_row_text(b"42\t\xff\t1\n", tx)
        self.assertEqual(str(optimized.exception), str(original.exception))
        tx.set_loader_types([21, 23, 20, 25], pq.Format.BINARY)
        data = binary_row(
            struct.pack("!h", -2),
            struct.pack("!i", 42),
            struct.pack("!q", 2**40),
            b"text",
        )
        self.assertEqual(native.parse_row_binary(data, tx), (-2, 42, 2**40, "text"))
        with self.assertRaises(struct.error):
            native.parse_row_binary(binary_row(b"x", b"", b"", b""), tx)
        for encoding, expected in (("latin-1", "\u00ff"), ("ascii", b"\xff")):
            tx = adapter._BackendTransformer()
            tx._encoding = encoding
            tx.set_loader_types([25], pq.Format.TEXT)
            self.assertEqual(native.parse_row_text(b"\xff\n", tx), (expected,))

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.adapters.register_loader("int4", CustomIntLoader)
                cur.adapters.register_loader("int4", CustomBinaryLoader)
                for binary in (False, True):
                    suffix = " (format binary)" if binary else ""
                    with cur.copy("copy (select 42::int4) to stdout" + suffix) as copy:
                        copy.set_types(["int4"])
                        self.assertEqual(list(copy.rows()), [(242 if binary else 142,)])

    def test_native_binary_copy_scanner_preserves_blocks_and_rejects_truncation(self):
        from ferrocopg import _ferrocopg as adapter
        from ferrocopg._rust import _ferrocopg as native

        header = b"PGCOPY\n\xff\r\n\0" + struct.pack("!II", 0, 3) + b"ext"
        row = struct.pack("!hii", 2, -1, 3) + b"abc"
        for data in (b"", header + b"\xff\xff", header + row * 3 + b"\xff\xff"):
            blocks, count = native.split_binary_copy(data)
            self.assertEqual(blocks, adapter._split_binary_copy_blocks(data))
            self.assertEqual(count, adapter._binary_copy_row_count(data))
        data = header + row + b"\xff\xff"
        for end in range(1, len(data)):
            self.assertIsNone(native.split_binary_copy(data[:end]))
        self.assertIsNone(native.split_binary_copy(header + b"\xff\xfe"))

    def test_checkout_reuses_connection_and_preserves_transaction_context(self):
        import ferrocopg

        from psycopg_pool import ConnectionPool

        class Connection(ferrocopg.Connection):
            @classmethod
            def connect(cls, conninfo="", **kwargs):
                return ferrocopg.connect(conninfo, **kwargs)

        with ConnectionPool(
            os.environ["PHASE5_DSN"],
            connection_class=Connection,
            min_size=1,
            max_size=1,
            check=ConnectionPool.check_connection,
        ) as pool:
            pool.wait(timeout=10)
            with pool.connection() as conn:
                pid = conn.info.backend_pid
                conn.execute("create temporary table phase5_pool (value int)")
                conn.execute("insert into phase5_pool values (1)")
            self.assertFalse(conn.closed)
            with self.assertRaises(ZeroDivisionError):
                with pool.connection() as conn:
                    self.assertEqual(conn.info.backend_pid, pid)
                    conn.execute("insert into phase5_pool values (2)")
                    raise ZeroDivisionError("exercise context rollback")
            for _ in range(10):
                with pool.connection() as conn:
                    self.assertEqual(conn.info.backend_pid, pid)
                    self.assertEqual(
                        conn.execute("select * from phase5_pool").fetchall(), [(1,)]
                    )
            self.assertEqual(pool.get_stats().get("returns_bad", 0), 0)
        self.assertTrue(conn.closed)

    def test_bulk_rows_preserve_custom_loaders_factories_and_navigation(self):
        import ferrocopg
        from ferrocopg import rows
        from ferrocopg.types.numeric import IntLoader

        class CustomIntLoader(IntLoader):
            def load(self, data):
                return super().load(data) + 100

        with ferrocopg.connect(os.environ["PHASE5_DSN"], autocommit=True) as conn:
            for binary in (False, True):
                for factory in (rows.tuple_row, rows.dict_row, rows.namedtuple_row):
                    with conn.cursor(binary=binary, row_factory=factory) as cur:
                        cur.execute(
                            "select i::int2 as i, 'value-' || i as value, "
                            "null::int8 as missing from generate_series(1, 1000) i"
                        )
                        cur.fetchone()
                        batch = cur.fetchall()
                        self.assertEqual(len(batch), 999)
                        expected = (1000, "value-1000", None)
                        if factory is rows.dict_row:
                            self.assertEqual(
                                batch[-1],
                                dict(zip(("i", "value", "missing"), expected)),
                            )
                        else:
                            self.assertEqual(batch[-1], expected)
                        self.assertEqual(cur.rownumber, 1000)
                        self.assertEqual(cur.fetchall(), [])
                        cur.scroll(0, mode="absolute")
                        self.assertEqual(len(cur.fetchall()), 1000)
            with conn.cursor() as cur:
                cur.adapters.register_loader("int4", CustomIntLoader)
                cur.execute("select i::int4 from generate_series(1, 10) i")
                self.assertEqual(cur.fetchall(), [(i + 100,) for i in range(1, 11)])


if __name__ == "__main__":
    unittest.main()
