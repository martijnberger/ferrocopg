"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from typing import NamedTuple, Protocol, cast

from . import errors as e
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
                result = self._conn._execute(query, params, prepare=prepare).current_result
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


class NoTlsConnectionAdapter:
    """Experimental connection-like bridge over the ferrocopg session adapter."""

    def __init__(self, session: NoTlsSessionAdapter):
        self._session = session
        self._prepared: dict[str, int] = {}
        self._prepared_statusmessages: dict[int, str | None] = {}
        self._tx_depth = 0
        self._savepoint_counter = 0

    @property
    def closed(self) -> bool:
        return self._session.closed

    def close(self) -> None:
        self._session.close()

    def cursor(self, *, row_factory: RowFactory = list_row) -> NoTlsCursorAdapter:
        self._check_closed()
        return NoTlsCursorAdapter(self, row_factory=row_factory)

    def execute(
        self,
        query: str,
        params: list[str | None] | None = None,
        *,
        prepare: bool = False,
        row_factory: RowFactory = list_row,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        cur = self.cursor(row_factory=row_factory)
        return cur.execute(query, params, prepare=prepare)

    def begin(self) -> None:
        self._check_closed()
        self._session.begin()

    def commit(self) -> None:
        self._check_closed()
        self._session.commit()

    def rollback(self) -> None:
        self._check_closed()
        self._session.rollback()

    def transaction(
        self, savepoint_name: str | None = None, force_rollback: bool = False
    ) -> NoTlsTransactionAdapter:
        return NoTlsTransactionAdapter(self, savepoint_name, force_rollback)

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
                self._prepared_statusmessages[statement_id] = _statusmessage_for_query(
                    query
                )
            return self._session.execute_prepared(
                statement_id,
                params,
                statusmessage=self._prepared_statusmessages.get(statement_id),
            )

        return self._session.execute_params(query, params)

    def _check_closed(self) -> None:
        if self.closed:
            raise e.OperationalError("the connection is closed")


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


def no_tls_connection_adapter(conninfo: str) -> NoTlsConnectionAdapter | None:
    """
    Return an experimental connection-like adapter over the Rust backend session.
    """
    session = no_tls_session_adapter(conninfo)
    if session is None:
        return None
    return NoTlsConnectionAdapter(session)
