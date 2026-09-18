"""Live regression tests for the staged wheel and official pool package."""

import gc
import os
import struct
import subprocess
import sys
import unittest
import weakref


@unittest.skipUnless(
    os.environ.get("PHASE5_DSN"), "requires an installed wheel and DSN"
)
class InstalledPoolTests(unittest.TestCase):
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
        self.assertEqual(empty.load_rows(0, 2, [], [], tuple), [(), ()])
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

    def test_native_session_lock_wait_releases_interpreter(self):
        code = """
import os
import threading
import time
import psycopg
from ferrocopg._rust import _ferrocopg as native

dsn = os.environ["PHASE5_DSN"]
with psycopg.connect(dsn, autocommit=True) as observer:
    for action in ("parameter", "close"):
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
                result = session.run_params_format(
                    "select $1::int4, ''::text, NULL::text, "
                    "decode('00ff', 'hex'), repeat('x', 10000)",
                    [(23, False, b"42")],
                    binary,
                )
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
            self.assertEqual(result.row_count, 1)
            self.assertEqual([result.get_value(0, i) for i in range(5)], expected)
            self.assertEqual(result.row(0), result.rows[0])
            self.assertEqual(
                [None if v is None else bytes(v) for v in result.row(0)],
                expected,
            )
            with self.assertRaises(IndexError):
                result.get_value(1, 0)
            with self.assertRaises(IndexError):
                result.get_value(0, 5)
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
