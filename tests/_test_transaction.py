import inspect
import sys

import pytest

from psycopg import pq

# TODOCRDB: is this the expected behaviour?
crdb_skip_external_observer = pytest.mark.crdb(
    "skip", reason="deadlock on observer connection"
)


@pytest.fixture(autouse=True)
def create_test_table(svcconn):
    """Creates a table called 'test_table' for use in tests."""
    cur = svcconn.cursor()
    cur.execute("drop table if exists test_table")
    cur.execute("create table test_table (id text primary key)")
    yield
    cur.execute("drop table test_table")


def insert_row(conn, value):
    sql = "INSERT INTO test_table VALUES (%s)"
    cur = conn.cursor()
    if inspect.iscoroutinefunction(cur.execute):

        async def f():
            await cur.execute(sql, (value,))

        return f()
    else:
        cur.execute(sql, (value,))


def inserted(conn):
    """Return the values inserted in the test table."""
    sql = "SELECT * FROM test_table"
    cur = conn.cursor()
    if inspect.iscoroutinefunction(cur.execute):

        async def f():
            await cur.execute(sql)
            rows = await cur.fetchall()
            return {v for (v,) in rows}

        return f()
    else:
        rows = cur.execute(sql).fetchall()
        return {v for (v,) in rows}


def in_transaction(conn):
    if conn.pgconn.transaction_status == pq.TransactionStatus.IDLE:
        return False
    elif conn.pgconn.transaction_status == pq.TransactionStatus.INTRANS:
        return True
    else:
        assert False, conn.pgconn.transaction_status


def get_exc_info(exc):
    """Return the exc info for an exception or a success if exc is None"""
    if not exc:
        return (None,) * 3
    try:
        raise exc
    except exc:
        return sys.exc_info()


class ExpectedException(Exception):
    pass
