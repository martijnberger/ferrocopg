"""Live regression tests for the staged wheel and official pool package."""

import os
import unittest


@unittest.skipUnless(
    os.environ.get("PHASE5_DSN"), "requires an installed wheel and DSN"
)
class InstalledPoolTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
