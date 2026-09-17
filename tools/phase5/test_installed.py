"""Live regression tests for the staged wheel and official pool package."""

import os
import struct
import unittest


@unittest.skipUnless(
    os.environ.get("PHASE5_DSN"), "requires an installed wheel and DSN"
)
class InstalledPoolTests(unittest.TestCase):
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
