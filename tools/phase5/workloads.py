"""Identical public-API workloads for installed ferrocopg and official Psycopg."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator
from typing import Any

BENCHMARKS = (
    "connect_plain",
    "connect_tls",
    "parameterized",
    "prepared",
    "rows_tuple",
    "rows_dict",
    "rows_namedtuple",
    "transaction",
    "copy_text",
    "copy_binary",
    "pool",
)
SOAKS = (
    "churn",
    "transaction",
    "cancel",
    "copy_text",
    "copy_binary",
    "pipeline",
    "pool",
)


class Workload:
    def __init__(self, driver: Any, dsn: str, name: str, rows: int = 1000):
        self.driver, self.dsn, self.name, self.rows = driver, dsn, name, rows
        self.conn: Any = None
        self.pool: Any = None

    def connect(self, **kwargs: Any) -> Any:
        return self.driver.connect(
            self.dsn, autocommit=True, connect_timeout=10, **kwargs
        )

    def __enter__(self) -> Workload:
        if self.name in ("connect_plain", "connect_tls", "churn"):
            return self
        if self.name == "pool":
            from psycopg_pool import ConnectionPool

            # The installed Rust adapter is intentionally a different concrete class.
            driver = self.driver

            class Connection(driver.Connection):
                @classmethod
                def connect(cls, conninfo: str = "", **kwargs: Any) -> Any:
                    return driver.connect(conninfo, **kwargs)

            self.pool = ConnectionPool(
                self.dsn,
                connection_class=Connection,
                min_size=2,
                max_size=4,
                kwargs={"autocommit": True, "connect_timeout": 10},
                open=False,
            )
            self.pool.open(wait=True, timeout=15)
            return self
        self.conn = self.connect()
        if self.name == "prepared":
            self.conn.prepare_threshold = 0
        if self.name.startswith("rows_"):
            from importlib import import_module

            rows_module = import_module(self.driver.__name__ + ".rows")
            self.conn.row_factory = getattr(rows_module, self.name[5:] + "_row")
        if self.name.startswith("copy_"):
            self.conn.execute("create temporary table phase5_copy (id int, value text)")
        return self

    def __exit__(self, *exc: Any) -> None:
        if self.pool is not None:
            self.pool.close(timeout=15)
        if self.conn is not None:
            self.conn.close()

    def run(self) -> int:
        name, conn = self.name, self.conn
        if name in ("connect_plain", "connect_tls", "churn"):
            sslmode = "require" if name == "connect_tls" else "disable"
            with self.connect(sslmode=sslmode) as c:
                assert c.execute("select 42").fetchone() == (42,)
                if name == "connect_tls":
                    assert c.execute(
                        "select ssl from pg_stat_ssl where pid = pg_backend_pid()"
                    ).fetchone() == (True,)
            return 1
        if name in ("parameterized", "prepared"):
            assert conn.execute("select %s::int + 1", (41,)).fetchone() == (42,)
            return 1
        if name.startswith("rows_"):
            rows = conn.execute(
                "select i, 'value-' || i as value from generate_series(1, %s) i",
                (self.rows,),
            ).fetchall()
            assert len(rows) == self.rows
            if name == "rows_dict":
                assert rows[-1] == {"i": self.rows, "value": f"value-{self.rows}"}
            else:
                assert rows[-1] == (self.rows, f"value-{self.rows}")
            return self.rows
        if name == "transaction":
            with conn.transaction():
                assert conn.execute("select 42").fetchone() == (42,)
                try:
                    with conn.transaction():
                        conn.execute("select 1 / 0")
                except self.driver.errors.DivisionByZero:
                    pass
                else:
                    raise AssertionError("savepoint did not raise DivisionByZero")
                assert conn.execute("select 43").fetchone() == (43,)
            assert conn.info.transaction_status == self.driver.pq.TransactionStatus.IDLE
            return 1
        if name.startswith("copy_"):
            binary = name == "copy_binary"
            option = " (format binary)" if binary else ""
            conn.execute("truncate phase5_copy")
            with conn.cursor() as cur:
                with cur.copy("copy phase5_copy from stdin" + option) as cp:
                    if binary:
                        cp.set_types(["int4", "text"])
                    for i in range(self.rows):
                        cp.write_row((i, f"value-{i}"))
                with cur.copy("copy phase5_copy to stdout" + option) as cp:
                    cp.set_types(["int4", "text"])
                    for count, row in enumerate(cp.rows(), 1):
                        assert tuple(row) == (count - 1, f"value-{count - 1}")
                    assert count == self.rows
            return self.rows * 2
        if name == "pipeline":
            with conn.pipeline():
                cursors = [conn.execute("select %s::int", (i,)) for i in range(8)]
            assert [c.fetchone() for c in cursors] == [(i,) for i in range(8)]
            try:
                with conn.pipeline():
                    conn.execute("select 1 / 0")
            except self.driver.errors.DivisionByZero:
                pass
            else:
                raise AssertionError("pipeline did not propagate the server error")
            assert conn.execute("select 42").fetchone() == (42,)
            return 1
        if name == "cancel":
            failures: list[BaseException] = []

            def query() -> None:
                try:
                    conn.execute("select pg_sleep(30)")
                except self.driver.errors.QueryCanceled:
                    return
                except BaseException as ex:
                    failures.append(ex)
                    return
                failures.append(AssertionError("sleep query was not canceled"))

            thread = threading.Thread(target=query, daemon=True)
            thread.start()
            # Observe server execution instead of racing a fixed sleep against startup.
            try:
                with self.connect() as observer:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        row = observer.execute(
                            "select wait_event from pg_stat_activity where pid = %s",
                            (conn.info.backend_pid,),
                        ).fetchone()
                        if row == ("PgSleep",):
                            break
                        time.sleep(0.002)
                    else:
                        raise TimeoutError("sleep query never became cancellable")
                    conn.cancel_safe(timeout=5)
            finally:
                thread.join(timeout=10)
            assert not thread.is_alive(), "query thread survived cancellation"
            if failures:
                raise failures[0]
            assert conn.execute("select 42").fetchone() == (42,)
            return 1
        if name == "pool":
            with self.pool.connection(timeout=10) as c:
                assert c.execute("select %s::int", (42,)).fetchone() == (42,)
            assert not c.closed, "pool checkout context closed a reusable connection"
            assert self.pool.get_stats().get("returns_bad", 0) == 0
            return 1
        raise ValueError(name)


@contextlib.contextmanager
def workload(driver: Any, dsn: str, name: str, rows: int) -> Iterator[Workload]:
    with Workload(driver, dsn, name, rows) as item:
        yield item
