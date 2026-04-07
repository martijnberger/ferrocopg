"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from time import monotonic
from typing import NamedTuple, Protocol, cast

from . import errors as e
from ._connection_base import Notify
from ._enums import IsolationLevel
from ._rmodule import __version__ as __version__
from ._rmodule import _ferrocopg
from .transaction import Rollback


class _ResultSetLike(Protocol):
    columns: list[str]
    rows: list[list[str | None]]
    rows_affected: int


class _SyntheticResult:
    def __init__(
        self,
        columns: list[str] | None = None,
        rows: list[list[str | None]] | None = None,
        rows_affected: int = 0,
        statusmessage: str | None = None,
    ):
        self.columns = columns or []
        self.rows = rows or []
        self.rows_affected = rows_affected
        self.statusmessage = statusmessage


class _PreparedStatementLike(Protocol):
    statement_id: int


class _BackendNotificationLike(Protocol):
    channel: str
    payload: str
    process_id: int


class BackendColumn(NamedTuple):
    name: str
    type_code: None = None
    display_size: None = None
    internal_size: None = None
    precision: None = None
    scale: None = None
    null_ok: None = None


RowFactory = Callable[[list[str], list[str | None]], object]


def list_row(columns: list[str], row: list[str | None]) -> list[str | None]:
    return list(row)


def tuple_row(columns: list[str], row: list[str | None]) -> tuple[str | None, ...]:
    return tuple(row)


def dict_row(columns: list[str], row: list[str | None]) -> dict[str, str | None]:
    return dict(zip(columns, row, strict=False))


def scalar_row(columns: list[str], row: list[str | None]) -> str | None:
    if len(row) != 1:
        raise RuntimeError(f"scalar_row requires exactly 1 column, got {len(row)}")
    return row[0]


class _NoTlsSessionLike(Protocol):
    closed: bool

    def close(self) -> None: ...

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def listen(self, channel: str) -> None: ...

    def unlisten(self, channel: str) -> None: ...

    def notify(self, channel: str, payload: str) -> None: ...

    def drain_notifications(self) -> list[_BackendNotificationLike]: ...

    def wait_for_notification(
        self, timeout_ms: int
    ) -> _BackendNotificationLike | None: ...

    def prepare_text(self, query: str) -> _PreparedStatementLike: ...

    def simple_query_results(self, query: str) -> list[_ResultSetLike]: ...

    def pipeline_simple_query_results(
        self, queries: list[str]
    ) -> list[list[_ResultSetLike]]: ...

    def run_text_params(
        self, query: str, params: list[str | None]
    ) -> _ResultSetLike: ...

    def run_prepared_text_params(
        self, statement_id: int, params: list[str | None]
    ) -> _ResultSetLike: ...


def is_available() -> bool:
    """Return `True` if the bootstrap ferrocopg Rust extension is importable."""
    return _ferrocopg is not None


def conninfo_summary(conninfo: str) -> object | None:
    """
    Return a Rust-backed conninfo summary if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.parse_conninfo_summary(conninfo))


def connect_plan(conninfo: str) -> object | None:
    """
    Return a Rust-backed connect plan if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.parse_connect_plan(conninfo))


def connect_target(conninfo: str) -> object | None:
    """
    Return a Rust-backed backend connect target if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.parse_connect_target(conninfo))


def connect_no_tls_probe(conninfo: str) -> object | None:
    """
    Return a live Rust-backed no-TLS connection probe if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.probe_connect_no_tls(conninfo))


def query_text_no_tls(conninfo: str, query: str) -> object | None:
    """
    Return a live Rust-backed no-TLS text query result if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.query_text_no_tls(conninfo, query))


def simple_query_no_tls(conninfo: str, query: str) -> object | None:
    """
    Return structured simple-query messages from the Rust backend if available.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.simple_query_no_tls(conninfo, query))


def simple_query_results_no_tls(conninfo: str, query: str) -> object | None:
    """
    Return statement-sized simple-query results from the Rust backend if available.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.simple_query_results_no_tls(conninfo, query))


def pipeline_simple_query_results_no_tls(
    conninfo: str, queries: list[str]
) -> object | None:
    """
    Return batches of statement-sized simple-query results from the Rust backend.
    """
    if not _ferrocopg:
        return None
    return cast(
        object, _ferrocopg.pipeline_simple_query_results_no_tls(conninfo, queries)
    )


def query_text_params_no_tls(
    conninfo: str, query: str, params: list[str | None]
) -> object | None:
    """
    Return a Rust-backed no-TLS text query result for bound text parameters.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.query_text_params_no_tls(conninfo, query, params))


def run_text_params_no_tls(
    conninfo: str, query: str, params: list[str | None]
) -> object | None:
    """
    Return a unified Rust-backed result set for a bound text statement.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.run_text_params_no_tls(conninfo, query, params))


def execute_text_params_no_tls(
    conninfo: str, query: str, params: list[str | None]
) -> object | None:
    """
    Return a Rust-backed no-TLS execute result for bound text parameters.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.execute_text_params_no_tls(conninfo, query, params))


def describe_text_no_tls(conninfo: str, query: str) -> object | None:
    """
    Return a Rust-backed no-TLS statement description if the extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.describe_text_no_tls(conninfo, query))


def no_tls_session(conninfo: str) -> object | None:
    """
    Return a live Rust-backed reusable no-TLS backend session if the extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.connect_no_tls_session(conninfo))


class BackendResultCursor:
    """Small cursor-like wrapper over ferrocopg backend result sets."""

    def __init__(
        self,
        results: Sequence[_ResultSetLike],
        statusmessages: Sequence[str | None] | None = None,
    ):
        self._results = list(results)
        if statusmessages is None:
            self._statusmessages = [
                getattr(result, "statusmessage", None) for result in self._results
            ]
        else:
            self._statusmessages = list(statusmessages)
        self._index = 0 if self._results else -1
        self._pos = 0

    @property
    def current_result(self) -> _ResultSetLike | None:
        if self._index < 0:
            return None
        return self._results[self._index]

    @property
    def columns(self) -> list[str]:
        result = self.current_result
        if result is None:
            return []
        return result.columns

    @property
    def rows_affected(self) -> int:
        result = self.current_result
        if result is None:
            return -1
        return result.rows_affected

    @property
    def statusmessage(self) -> str | None:
        if self._index < 0 or self._index >= len(self._statusmessages):
            return None
        return self._statusmessages[self._index]

    def fetchone(self) -> list[str | None] | None:
        result = self.current_result
        if result is None:
            return None

        rows = result.rows
        if self._pos >= len(rows):
            return None

        row = rows[self._pos]
        self._pos += 1
        return row

    def fetchall(self) -> list[list[str | None]]:
        result = self.current_result
        if result is None:
            return []

        rows = result.rows
        rv = rows[self._pos :]
        self._pos = len(rows)
        return rv

    def nextset(self) -> bool | None:
        if self._index < 0 or self._index + 1 >= len(self._results):
            return None

        self._index += 1
        self._pos = 0
        return True

    def set_result(self, index: int) -> BackendResultCursor:
        if not -len(self._results) <= index < len(self._results):
            raise IndexError(
                f"index {index} out of range: {len(self._results)} result(s) available"
            )
        if index < 0:
            index += len(self._results)

        self._index = index
        self._pos = 0
        return self

    def results(self) -> Iterator[BackendResultCursor]:
        if self.current_result is not None:
            while True:
                yield self
                if not self.nextset():
                    break


class NoTlsSessionAdapter:
    """Thin Python adapter over the Rust no-TLS backend session."""

    def __init__(self, session: _NoTlsSessionLike):
        self._session = session

    @property
    def closed(self) -> bool:
        return self._session.closed

    def close(self) -> None:
        self._session.close()

    def execute_simple(self, query: str) -> BackendResultCursor:
        results = self._session.simple_query_results(query)
        statuses = [_statusmessage_for_query(query, result) for result in results]
        return BackendResultCursor(results, statuses)

    def execute_pipeline_simple(self, queries: list[str]) -> list[BackendResultCursor]:
        batches = self._session.pipeline_simple_query_results(queries)
        return [
            BackendResultCursor(
                results,
                [_statusmessage_for_query(query, result) for result in results],
            )
            for query, results in zip(queries, batches, strict=True)
        ]

    def execute_params(
        self, query: str, params: list[str | None]
    ) -> BackendResultCursor:
        result = self._session.run_text_params(query, params)
        return BackendResultCursor([result], [_statusmessage_for_query(query, result)])

    def execute_prepared(
        self,
        statement_id: int,
        params: list[str | None],
        *,
        statusmessage: str | None = None,
    ) -> BackendResultCursor:
        result = self._session.run_prepared_text_params(statement_id, params)
        return BackendResultCursor([result], [statusmessage])

    def begin(self) -> None:
        self._session.begin()

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    def listen(self, channel: str) -> None:
        self._session.listen(channel)

    def unlisten(self, channel: str) -> None:
        self._session.unlisten(channel)

    def notify(self, channel: str, payload: str = "") -> None:
        self._session.notify(channel, payload)

    def drain_notifications(self) -> list[Notify]:
        return [
            Notify(n.channel, n.payload, n.process_id)
            for n in self._session.drain_notifications()
        ]

    def wait_for_notification(self, timeout: float = 0.0) -> Notify | None:
        timeout_ms = max(0, int(timeout * 1000))
        notification = self._session.wait_for_notification(timeout_ms)
        if notification is None:
            return None
        return Notify(
            notification.channel,
            notification.payload,
            notification.process_id,
        )

    def prepare_text(self, query: str) -> _PreparedStatementLike:
        return self._session.prepare_text(query)

    def __getattr__(self, name: str) -> object:
        return getattr(self._session, name)


class NoTlsCursorAdapter:
    """Experimental cursor-like bridge over the ferrocopg session adapter."""

    def __init__(
        self,
        conn: NoTlsConnectionAdapter,
        *,
        row_factory: RowFactory = list_row,
    ):
        self._conn = conn
        self._result: BackendResultCursor | None = None
        self._closed = False
        self._row_factory = row_factory
        self._rownumber = 0
        self.arraysize = 1

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def rowcount(self) -> int:
        if self._result is None:
            return -1
        return self._result.rows_affected

    @property
    def rownumber(self) -> int | None:
        result = self._result
        if result is None:
            return None
        current = result.current_result
        if current is None:
            return None
        if not current.columns and not current.rows:
            return None
        return self._rownumber

    @property
    def description(self) -> list[BackendColumn] | None:
        if self._result is None:
            return None
        return [BackendColumn(name) for name in self._result.columns]

    @property
    def statusmessage(self) -> str | None:
        if self._result is None:
            return None
        return self._result.statusmessage

    def close(self) -> None:
        self._closed = True
        self._result = None

    def execute(
        self,
        query: str,
        params: list[str | None] | None = None,
        *,
        prepare: bool = False,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        self._conn._check_closed()
        self._result = self._conn._execute(query, params, prepare=prepare)
        self._rownumber = 0
        return self

    def executemany(
        self,
        query: str,
        params_seq: Sequence[list[str | None]],
        *,
        returning: bool = False,
        prepare: bool = False,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        self._conn._check_closed()
        if returning:
            results = [
                self._conn._execute(query, params, prepare=prepare).current_result
                for params in params_seq
            ]
            self._result = BackendResultCursor(
                [result for result in results if result is not None]
            )
        else:
            total = 0
            for params in params_seq:
                result = self._conn._execute(
                    query, params, prepare=prepare
                ).current_result
                if result is not None:
                    total += result.rows_affected
            synthetic = _SyntheticResult(rows_affected=total)
            synthetic.statusmessage = _statusmessage_for_query(query, synthetic)
            self._result = BackendResultCursor([synthetic])
        self._rownumber = 0
        return self

    def fetchone(self) -> object | None:
        result = self._require_result()
        row = result.fetchone()
        if row is None:
            return None
        self._rownumber += 1
        return self._row_factory(result.columns, row)

    def fetchall(self) -> list[object]:
        result = self._require_result()
        rows = result.fetchall()
        self._rownumber += len(rows)
        return [self._row_factory(result.columns, row) for row in rows]

    def fetchmany(self, size: int | None = None) -> list[object]:
        result = self._require_result()
        if size is None:
            size = self.arraysize
        rows: list[object] = []
        while len(rows) < size:
            row = result.fetchone()
            if row is None:
                break
            self._rownumber += 1
            rows.append(self._row_factory(result.columns, row))
        return rows

    def nextset(self) -> bool | None:
        result = self._require_result()
        rv = result.nextset()
        if rv:
            self._rownumber = 0
        return rv

    def set_result(self, index: int) -> NoTlsCursorAdapter:
        result = self._require_result()
        result.set_result(index)
        self._rownumber = 0
        return self

    def results(self) -> Iterator[NoTlsCursorAdapter]:
        self._require_result()
        while True:
            yield self
            if not self.nextset():
                break

    def __iter__(self) -> Iterator[object]:
        return self

    def __next__(self) -> object:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> NoTlsCursorAdapter:
        self._check_closed()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _check_closed(self) -> None:
        if self._closed:
            raise e.InterfaceError("the cursor is closed")

    def _require_result(self) -> BackendResultCursor:
        self._check_closed()
        self._conn._check_closed()
        if self._result is None:
            raise e.ProgrammingError("no result available")
        return self._result


class NoTlsPipelineAdapter:
    """Experimental pipeline context over the ferrocopg connection adapter."""

    def __init__(self, conn: NoTlsConnectionAdapter):
        self._conn = conn
        self._queued: list[tuple[str, NoTlsCursorAdapter]] = []
        self._entered = False
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def execute(
        self,
        query: str,
        *,
        row_factory: RowFactory | None = None,
        params: list[str | None] | None = None,
    ) -> NoTlsCursorAdapter:
        self._check_open()
        if params is not None:
            raise e.NotSupportedError(
                "ferrocopg pipeline currently supports simple queries only"
            )
        cur = self._conn.cursor(row_factory=row_factory)
        self._queued.append((query, cur))
        return cur

    def sync(self) -> None:
        self._check_open()
        if not self._queued:
            return

        queries = [query for query, _cur in self._queued]
        results = self._conn.execute_pipeline_simple(queries)
        for (_query, queued_cur), result_cur in zip(
            self._queued, results, strict=True
        ):
            queued_cur._result = result_cur._result
            queued_cur._rownumber = 0
        self._queued.clear()

    def __enter__(self) -> NoTlsPipelineAdapter:
        self._conn._check_closed()
        if self._entered:
            raise TypeError("pipeline blocks can be used only once")
        self._entered = True
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if exc_type is None:
                self.sync()
        finally:
            self._closed = True
            self._queued.clear()

    def _check_open(self) -> None:
        self._conn._check_closed()
        if self._closed or not self._entered:
            raise e.OperationalError("pipeline is not active")


class NoTlsConnectionAdapter:
    """Experimental connection-like bridge over the ferrocopg session adapter."""

    def __init__(
        self,
        session: NoTlsSessionAdapter,
        *,
        row_factory: RowFactory = list_row,
        prepare_threshold: int | None = 5,
        autocommit: bool = True,
    ):
        self._session = session
        self.row_factory = row_factory
        self.prepare_threshold = prepare_threshold
        self._autocommit = autocommit
        self._prepared: dict[str, int] = {}
        self._prepared_statusmessages: dict[int, str | None] = {}
        self._prepare_counts: dict[str, int] = {}
        self._in_transaction = False
        self._tx_depth = 0
        self._savepoint_counter = 0
        self._isolation_level: IsolationLevel | None = None
        self._read_only: bool | None = None
        self._deferrable: bool | None = None

    @property
    def closed(self) -> bool:
        return self._session.closed

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check_closed()
        if self._in_transaction:
            raise e.ProgrammingError(
                "can't change autocommit now: connection in transaction status INTRANS"
            )
        self._autocommit = bool(value)

    def set_autocommit(self, value: bool) -> None:
        self.autocommit = bool(value)

    @property
    def isolation_level(self) -> IsolationLevel | None:
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel | None) -> None:
        self.set_isolation_level(value)

    @property
    def read_only(self) -> bool | None:
        return self._read_only

    @read_only.setter
    def read_only(self, value: bool | None) -> None:
        self.set_read_only(value)

    @property
    def deferrable(self) -> bool | None:
        return self._deferrable

    @deferrable.setter
    def deferrable(self, value: bool | None) -> None:
        self.set_deferrable(value)

    def close(self) -> None:
        self._session.close()

    def cursor(self, *, row_factory: RowFactory | None = None) -> NoTlsCursorAdapter:
        self._check_closed()
        if row_factory is None:
            row_factory = self.row_factory
        return NoTlsCursorAdapter(self, row_factory=row_factory)

    def execute(
        self,
        query: str,
        params: list[str | None] | None = None,
        *,
        prepare: bool = False,
        row_factory: RowFactory | None = None,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        cur = self.cursor(row_factory=row_factory)
        return cur.execute(query, params, prepare=prepare)

    def execute_pipeline_simple(
        self,
        queries: list[str],
        *,
        row_factory: RowFactory | None = None,
    ) -> list[NoTlsCursorAdapter]:
        self._check_closed()
        self._ensure_transaction()
        cursors: list[NoTlsCursorAdapter] = []
        if row_factory is None:
            row_factory = self.row_factory
        for result in self._session.execute_pipeline_simple(queries):
            cur = self.cursor(row_factory=row_factory)
            cur._result = result
            cursors.append(cur)
        return cursors

    def begin(self) -> None:
        self._check_closed()
        if not self._in_transaction:
            begin_sql = self._tx_start_query()
            if begin_sql == "BEGIN":
                self._session.begin()
            else:
                self._session.execute_simple(begin_sql)
            self._in_transaction = True

    def commit(self) -> None:
        self._check_closed()
        if not self._in_transaction:
            return
        self._session.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        self._check_closed()
        if not self._in_transaction:
            return
        self._session.rollback()
        self._in_transaction = False

    def set_isolation_level(self, value: IsolationLevel | int | None) -> None:
        self._check_set_transaction_param("isolation_level")
        self._isolation_level = (
            IsolationLevel(value) if value is not None else None
        )

    def set_read_only(self, value: bool | None) -> None:
        self._check_set_transaction_param("read_only")
        self._read_only = bool(value) if value is not None else None

    def set_deferrable(self, value: bool | None) -> None:
        self._check_set_transaction_param("deferrable")
        self._deferrable = bool(value) if value is not None else None

    def transaction(
        self, savepoint_name: str | None = None, force_rollback: bool = False
    ) -> NoTlsTransactionAdapter:
        return NoTlsTransactionAdapter(self, savepoint_name, force_rollback)

    def listen(self, channel: str) -> None:
        self._check_closed()
        self._session.listen(channel)

    def unlisten(self, channel: str) -> None:
        self._check_closed()
        self._session.unlisten(channel)

    def notify(self, channel: str, payload: str = "") -> None:
        self._check_closed()
        self._session.notify(channel, payload)

    def drain_notifications(self) -> list[Notify]:
        self._check_closed()
        return self._session.drain_notifications()

    def wait_for_notification(self, timeout: float = 0.0) -> Notify | None:
        self._check_closed()
        return self._session.wait_for_notification(timeout)

    def notifies(
        self, *, timeout: float | None = None, stop_after: int | None = None
    ) -> Iterator[Notify]:
        self._check_closed()
        if timeout is not None:
            deadline = monotonic() + timeout
            interval = min(timeout, 0.1)
        else:
            deadline = None
            interval = 0.1

        nreceived = 0
        while True:
            backlog = self.drain_notifications()
            if backlog:
                for notification in backlog:
                    yield notification
                    nreceived += 1
                if stop_after is not None and nreceived >= stop_after:
                    break
                continue

            wait_timeout = interval
            if deadline is not None:
                wait_timeout = deadline - monotonic()
                if wait_timeout <= 0:
                    break

            next_notification: Notify | None = self.wait_for_notification(wait_timeout)
            if next_notification is None:
                if timeout == 0.0 or deadline is not None:
                    break
                continue

            yield next_notification
            nreceived += 1
            if stop_after is not None and nreceived >= stop_after:
                break

    def pipeline(self) -> NoTlsPipelineAdapter:
        self._check_closed()
        return NoTlsPipelineAdapter(self)

    def __enter__(self) -> NoTlsConnectionAdapter:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self.closed:
            return
        if exc_type:
            if self._in_transaction:
                self.rollback()
        else:
            if self._in_transaction:
                self.commit()
        self.close()

    def _execute(
        self,
        query: str,
        params: list[str | None] | None,
        *,
        prepare: bool,
    ) -> BackendResultCursor:
        self._ensure_transaction()
        if params is None:
            return self._session.execute_simple(query)

        if not prepare and self.prepare_threshold is not None:
            count = self._prepare_counts.get(query, 0)
            prepare = count >= self.prepare_threshold
            self._prepare_counts[query] = count + 1

        if prepare:
            statement_id = self._prepared.get(query)
            if statement_id is None:
                prepared = self._session.prepare_text(query)
                statement_id = prepared.statement_id
                self._prepared[query] = statement_id
                self._prepared_statusmessages[statement_id] = _statusmessage_for_query(
                    query
                )
            return self._session.execute_prepared(
                statement_id,
                params,
                statusmessage=self._prepared_statusmessages.get(statement_id),
            )

        return self._session.execute_params(query, params)

    def _ensure_transaction(self) -> None:
        if not self._autocommit and not self._in_transaction:
            self.begin()

    def _check_closed(self) -> None:
        if self.closed:
            raise e.OperationalError("the connection is closed")

    def _check_set_transaction_param(self, attribute: str) -> None:
        self._check_closed()
        if self._tx_depth:
            raise e.ProgrammingError(
                f"can't change {attribute!r} now: "
                "connection.transaction() context in progress"
            )
        if self._in_transaction:
            raise e.ProgrammingError(
                f"can't change {attribute!r} now: "
                "connection in transaction status INTRANS"
            )

    def _tx_start_query(self) -> str:
        parts = ["BEGIN"]

        if self._isolation_level is not None:
            parts.extend(
                [
                    "ISOLATION LEVEL",
                    self._isolation_level.name.replace("_", " "),
                ]
            )

        if self._read_only is not None:
            parts.append("READ ONLY" if self._read_only else "READ WRITE")

        if self._deferrable is not None:
            parts.append("DEFERRABLE" if self._deferrable else "NOT DEFERRABLE")

        return " ".join(parts)


class NoTlsTransactionAdapter:
    """Experimental transaction context over the ferrocopg connection adapter."""

    def __init__(
        self,
        conn: NoTlsConnectionAdapter,
        savepoint_name: str | None = None,
        force_rollback: bool = False,
    ):
        self._conn = conn
        self._savepoint_name = savepoint_name
        self.force_rollback = force_rollback
        self._outer = False
        self._entered = False

    @property
    def savepoint_name(self) -> str | None:
        return self._savepoint_name

    def __enter__(self) -> NoTlsTransactionAdapter:
        if self._entered:
            raise TypeError("transaction blocks can be used only once")
        self._entered = True

        if self._conn._tx_depth == 0:
            if self._conn._in_transaction:
                if self._savepoint_name is None:
                    self._conn._savepoint_counter += 1
                    self._savepoint_name = f"_ferrocopg_{self._conn._savepoint_counter}"
                self._conn.execute(_savepoint_sql("SAVEPOINT", self._savepoint_name))
            else:
                self._outer = True
                self._conn.begin()
        else:
            if self._savepoint_name is None:
                self._conn._savepoint_counter += 1
                self._savepoint_name = f"_ferrocopg_{self._conn._savepoint_counter}"
            self._conn.execute(_savepoint_sql("SAVEPOINT", self._savepoint_name))

        self._conn._tx_depth += 1
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> bool:
        self._conn._tx_depth -= 1
        should_rollback = exc is not None or self.force_rollback

        if should_rollback:
            if self._outer:
                self._conn.rollback()
            else:
                assert self._savepoint_name is not None
                self._conn.execute(_savepoint_sql("ROLLBACK TO", self._savepoint_name))
                self._conn.execute(_savepoint_sql("RELEASE", self._savepoint_name))
        else:
            if self._outer:
                self._conn.commit()
            else:
                assert self._savepoint_name is not None
                self._conn.execute(_savepoint_sql("RELEASE", self._savepoint_name))

        if isinstance(exc, Rollback):
            target = cast(object | None, exc.transaction)
            if target is None or target is self:
                return True

        return False


def _savepoint_sql(command: str, name: str) -> str:
    quoted = name.replace('"', '""')
    return f'{command} "{quoted}"'


def _statusmessage_for_query(
    query: str, result: _ResultSetLike | None = None
) -> str | None:
    tokens = query.strip().split()
    if not tokens:
        return None

    first = tokens[0].upper()
    second = tokens[1].upper() if len(tokens) > 1 else ""
    rows_affected = result.rows_affected if result is not None else 0
    has_tuples = bool(result and result.columns)

    if first == "SELECT":
        return f"SELECT {rows_affected}"
    if first == "INSERT":
        return f"INSERT 0 {rows_affected}"
    if first in {"UPDATE", "DELETE", "MERGE", "MOVE", "FETCH", "COPY"}:
        return f"{first} {rows_affected}"
    if first == "CREATE" and second:
        return f"CREATE {second}"
    if first == "ALTER" and second:
        return f"ALTER {second}"
    if first == "DROP" and second:
        return f"DROP {second}"
    if first == "SAVEPOINT":
        return "SAVEPOINT"
    if first == "RELEASE":
        return "RELEASE"
    if first == "ROLLBACK":
        return "ROLLBACK"
    if first == "BEGIN":
        return "BEGIN"
    if first == "COMMIT":
        return "COMMIT"
    if has_tuples:
        return f"{first} {rows_affected}"
    return first


def no_tls_session_adapter(conninfo: str) -> NoTlsSessionAdapter | None:
    """
    Return a small Python-side adapter over the Rust backend session if loaded.
    """
    session = no_tls_session(conninfo)
    if session is None:
        return None
    return NoTlsSessionAdapter(cast(_NoTlsSessionLike, session))


def no_tls_connection_adapter(
    conninfo: str,
    *,
    row_factory: RowFactory = list_row,
    prepare_threshold: int | None = 5,
    autocommit: bool = True,
    isolation_level: IsolationLevel | int | None = None,
    read_only: bool | None = None,
    deferrable: bool | None = None,
) -> NoTlsConnectionAdapter | None:
    """
    Return an experimental connection-like adapter over the Rust backend session.
    """
    session = no_tls_session_adapter(conninfo)
    if session is None:
        return None
    conn = NoTlsConnectionAdapter(
        session,
        row_factory=row_factory,
        prepare_threshold=prepare_threshold,
        autocommit=autocommit,
    )
    if isolation_level is not None:
        conn.set_isolation_level(isolation_level)
    if read_only is not None:
        conn.set_read_only(read_only)
    if deferrable is not None:
        conn.set_deferrable(deferrable)
    return conn
