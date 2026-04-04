"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from typing import NamedTuple, Protocol, cast

from ._rmodule import __version__ as __version__
from ._rmodule import _ferrocopg


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
    ):
        self.columns = columns or []
        self.rows = rows or []
        self.rows_affected = rows_affected


class _PreparedStatementLike(Protocol):
    statement_id: int


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

    def prepare_text(self, query: str) -> _PreparedStatementLike: ...

    def simple_query_results(self, query: str) -> list[_ResultSetLike]: ...

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

    def __init__(self, results: Sequence[_ResultSetLike]):
        self._results = list(results)
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
        return BackendResultCursor(self._session.simple_query_results(query))

    def execute_params(
        self, query: str, params: list[str | None]
    ) -> BackendResultCursor:
        return BackendResultCursor([self._session.run_text_params(query, params)])

    def execute_prepared(
        self, statement_id: int, params: list[str | None]
    ) -> BackendResultCursor:
        return BackendResultCursor(
            [self._session.run_prepared_text_params(statement_id, params)]
        )

    def begin(self) -> None:
        self._session.begin()

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

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

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def rowcount(self) -> int:
        if self._result is None:
            return -1
        return self._result.rows_affected

    @property
    def rownumber(self) -> int:
        return self._rownumber

    @property
    def description(self) -> list[BackendColumn] | None:
        if self._result is None:
            return None
        return [BackendColumn(name) for name in self._result.columns]

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
                result = self._conn._execute(query, params, prepare=prepare).current_result
                if result is not None:
                    total += result.rows_affected
            self._result = BackendResultCursor([_SyntheticResult(rows_affected=total)])
        self._rownumber = 0
        return self

    def fetchone(self) -> object | None:
        if self._result is None:
            return None
        row = self._result.fetchone()
        if row is None:
            return None
        self._rownumber += 1
        return self._row_factory(self._result.columns, row)

    def fetchall(self) -> list[object]:
        if self._result is None:
            return []
        rows = self._result.fetchall()
        self._rownumber += len(rows)
        return [self._row_factory(self._result.columns, row) for row in rows]

    def nextset(self) -> bool | None:
        if self._result is None:
            return None
        return self._result.nextset()

    def __enter__(self) -> NoTlsCursorAdapter:
        self._check_closed()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("cursor is closed")


class NoTlsConnectionAdapter:
    """Experimental connection-like bridge over the ferrocopg session adapter."""

    def __init__(self, session: NoTlsSessionAdapter):
        self._session = session
        self._prepared: dict[str, int] = {}

    @property
    def closed(self) -> bool:
        return self._session.closed

    def close(self) -> None:
        self._session.close()

    def cursor(self, *, row_factory: RowFactory = list_row) -> NoTlsCursorAdapter:
        return NoTlsCursorAdapter(self, row_factory=row_factory)

    def execute(
        self,
        query: str,
        params: list[str | None] | None = None,
        *,
        prepare: bool = False,
        row_factory: RowFactory = list_row,
    ) -> NoTlsCursorAdapter:
        cur = self.cursor(row_factory=row_factory)
        return cur.execute(query, params, prepare=prepare)

    def begin(self) -> None:
        self._session.begin()

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    def __enter__(self) -> NoTlsConnectionAdapter:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _execute(
        self,
        query: str,
        params: list[str | None] | None,
        *,
        prepare: bool,
    ) -> BackendResultCursor:
        if params is None:
            return self._session.execute_simple(query)

        if prepare:
            statement_id = self._prepared.get(query)
            if statement_id is None:
                prepared = self._session.prepare_text(query)
                statement_id = prepared.statement_id
                self._prepared[query] = statement_id
            return self._session.execute_prepared(statement_id, params)

        return self._session.execute_params(query, params)


def no_tls_session_adapter(conninfo: str) -> NoTlsSessionAdapter | None:
    """
    Return a small Python-side adapter over the Rust backend session if loaded.
    """
    session = no_tls_session(conninfo)
    if session is None:
        return None
    return NoTlsSessionAdapter(cast(_NoTlsSessionLike, session))


def no_tls_connection_adapter(conninfo: str) -> NoTlsConnectionAdapter | None:
    """
    Return an experimental connection-like adapter over the Rust backend session.
    """
    session = no_tls_session_adapter(conninfo)
    if session is None:
        return None
    return NoTlsConnectionAdapter(session)
