"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import timedelta, timezone, tzinfo
from time import monotonic
from typing import Any, NamedTuple, Protocol, cast
from zoneinfo import ZoneInfo

from . import errors as e
from . import postgres, pq
from ._adapters_map import AdaptersMap
from ._connection_base import Notify
from ._copy_base import _format_row_text, _parse_row_text
from ._encodings import conninfo_encoding, pg2pyenc
from ._enums import IsolationLevel, PyFormat
from ._oids import BYTEA_OID
from ._py_transformer import Transformer as AdaptTransformer
from ._queries import PostgresQuery
from ._rmodule import __version__ as __version__
from ._rmodule import _ferrocopg
from ._tpc import Xid
from .abc import Buffer, Params, Query, Transformer
from .adapt import Dumper
from .conninfo import conninfo_to_dict, make_conninfo
from .pq import ExecStatus
from .transaction import Rollback
from .types.string import BytesDumper


class _StatementColumnLike(Protocol):
    name: str
    oid: int
    type_name: str


class _ResultSetLike(Protocol):
    columns: list[str]
    column_descriptions: list[_StatementColumnLike]
    rows: list[list[bytes | str | None]]
    rows_affected: int
    is_tuples: bool


class _SyntheticResult:
    def __init__(
        self,
        columns: list[str] | None = None,
        column_descriptions: list[_StatementColumnLike] | None = None,
        rows: list[list[bytes | str | None]] | None = None,
        rows_affected: int = 0,
        statusmessage: str | None = None,
        is_tuples: bool = False,
    ):
        self.columns = columns or []
        self.column_descriptions = column_descriptions or []
        self.rows = rows or []
        self.rows_affected = rows_affected
        self.statusmessage = statusmessage
        self.is_tuples = is_tuples


class _BoundParams(NamedTuple):
    values: list[tuple[int, bool, bytes | None]]
    types: tuple[int, ...]


class _PreparedStatementLike(Protocol):
    statement_id: int


class _BackendNotificationLike(Protocol):
    channel: str
    payload: str
    process_id: int


class _BackendCopyOutLike(Protocol):
    data: bytes


class _BackendProbeLike(Protocol):
    backend_pid: int
    current_user: str
    current_database: str
    server_version_num: int
    application_name: str
    server_address: str | None
    server_port: int | None


class _CancelHandleLike(Protocol):
    def cancel(self) -> None: ...


def _coerce_param_text(value: object) -> str:
    if isinstance(value, timedelta):
        return str(value).replace(",", "")
    return str(value)


class _TextCopyTransformer:
    def __init__(self, encoding: str):
        self._encoding = encoding
        self.types: tuple[int, ...] | None = None
        self.formats: None = None

    @property
    def encoding(self) -> str:
        return self._encoding

    def dump_sequence(
        self, params: Sequence[object], formats: list[object]
    ) -> list[bytes | None]:
        return [
            None
            if value is None
            else value
            if isinstance(value, bytes)
            else _coerce_param_text(value).encode(self._encoding)
            for value in params
        ]

    def load_sequence(
        self, record: list[bytes | memoryview | bytearray | None]
    ) -> tuple[str | None, ...]:
        return tuple(
            None if item is None else bytes(item).decode(self._encoding)
            for item in record
        )


class _NoopCancelHandle:
    def cancel(self) -> None:
        pass


class _PgconnEncodingShim:
    def __init__(self, encoding: str, conn: NoTlsConnectionAdapter | None = None):
        self._encoding = encoding
        self._conn = conn

    @property
    def status(self) -> pq.ConnStatus:
        if self._conn is None or self._conn.closed:
            return pq.ConnStatus.BAD
        return pq.ConnStatus.OK

    def parameter_status(self, param_name: bytes) -> bytes | None:
        if self._conn is None:
            return None
        value = self._conn.info.parameter_status(param_name.decode(self._encoding))
        return None if value is None else value.encode(self._encoding)


class _AdaptContext:
    def __init__(
        self,
        conn: NoTlsConnectionAdapter,
        adapters: AdaptersMap | None = None,
        *,
        expose_connection: bool = True,
    ):
        self._conn = conn
        self._adapters = adapters or conn.adapters
        self._connection = conn if expose_connection else None

    @property
    def adapters(self) -> AdaptersMap:
        return self._adapters

    @property
    def connection(self) -> Any:
        return self._connection


class _WireByteaDumper(Dumper):
    """Encode bytea text parameters without relying on a libpq PGconn."""

    oid = BYTEA_OID

    def dump(self, obj: Buffer) -> Buffer:
        return b"\\x" + bytes(obj).hex().encode()


def _install_wire_bytea_dumper(adapters: AdaptersMap) -> None:
    """Replace only Psycopg's default `%t` bytea dumper for this backend."""
    default_dumper = AdaptersMap._optimised.get(BytesDumper, BytesDumper)
    classes = [
        cls
        for cls in (bytes, bytearray, memoryview)
        if adapters.get_dumper(cls, PyFormat.TEXT) is default_dumper
    ]
    if not classes:
        return

    # `register_dumper()` also changes `%s`; only `%t` needs this wire form.
    if not adapters._own_dumpers[PyFormat.TEXT]:
        adapters._dumpers[PyFormat.TEXT] = adapters._dumpers[PyFormat.TEXT].copy()
        adapters._own_dumpers[PyFormat.TEXT] = True
    for cls in classes:
        adapters._dumpers[PyFormat.TEXT][cls] = _WireByteaDumper


def _pure_python_adapters(
    template: AdaptersMap, *, text_loader_oids: frozenset[int] = frozenset()
) -> AdaptersMap:
    """Copy an adapter map without the libpq/C-only loader replacements."""
    adapters = AdaptersMap(template)
    original_loaders = {
        optimized: original
        for original, optimized in AdaptersMap._optimised.items()
        if original is not optimized
    }
    for format in (pq.Format.TEXT, pq.Format.BINARY):
        adapters._loaders[format] = {
            oid: original_loaders.get(loader, loader)
            for oid, loader in adapters._loaders[format].items()
        }
        adapters._own_loaders[format] = True

    if text_loader_oids:
        adapters._loaders[pq.Format.BINARY] = adapters._loaders[pq.Format.BINARY].copy()
        for oid in text_loader_oids:
            if loader := adapters._loaders[pq.Format.TEXT].get(oid):
                adapters._loaders[pq.Format.BINARY][oid] = loader
        adapters._own_loaders[pq.Format.BINARY] = True

    return adapters


class BackendColumn(NamedTuple):
    name: str
    type_code: int | None = None
    display_size: None = None
    internal_size: None = None
    precision: None = None
    scale: None = None
    null_ok: None = None


LegacyRowFactory = Callable[[list[str], list[str | None]], object]
RowFactory = Callable[..., object]
RowMaker = Callable[[Sequence[object]], object]
NotifyHandler = Callable[[Notify], None]
_timezones: dict[str | None, tzinfo] = {None: timezone.utc, "UTC": timezone.utc}
_NO_ROW = object()
_TEXT_WIRE_OIDS = frozenset({18, 19, 25, 1042, 1043, 114, 142})


class FerrocopgConnection:
    """Fixture-facing facade matching `Connection.connect()` defaults."""

    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: int | None = 5,
        context: object | None = None,
        row_factory: object | None = None,
        cursor_factory: type[object] | None = None,
        server_cursor_factory: type[object] | None = None,
        isolation_level: IsolationLevel | int | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
        **kwargs: str | int | None,
    ) -> object | None:
        from . import connect_ferrocopg

        return connect_ferrocopg(
            conninfo,
            autocommit=autocommit,
            prepare_threshold=prepare_threshold,
            context=context,
            row_factory=row_factory,
            cursor_factory=cursor_factory,
            server_cursor_factory=server_cursor_factory,
            isolation_level=isolation_level,
            read_only=read_only,
            deferrable=deferrable,
            **kwargs,
        )


class BackendConnectionInfo:
    __module__ = "psycopg"

    def __init__(self, conn: NoTlsConnectionAdapter):
        self._conn = conn

    @property
    def vendor(self) -> str:
        return "PostgreSQL"

    @property
    def dbname(self) -> str:
        return self._conn._probe().current_database

    @property
    def user(self) -> str:
        return self._conn._probe().current_user

    @property
    def password(self) -> str:
        return str(conninfo_to_dict(self._conn._conninfo).get("password") or "")

    @property
    def options(self) -> str:
        return str(conninfo_to_dict(self._conn._conninfo).get("options") or "")

    @property
    def application_name(self) -> str:
        return self._conn._probe().application_name

    @property
    def server_version(self) -> int:
        return self._conn._probe().server_version_num

    @property
    def backend_pid(self) -> int:
        return self._conn._probe().backend_pid

    @property
    def host(self) -> str:
        return self.hostaddr

    @property
    def hostaddr(self) -> str:
        return self._conn._probe().server_address or ""

    @property
    def port(self) -> int:
        port = self._conn._probe().server_port
        if port is None:
            raise e.InternalError("couldn't find the connection port")
        return port

    def get_parameters(self) -> dict[str, str]:
        params = conninfo_to_dict(self._conn._conninfo)
        return {k: str(v) for k, v in params.items() if k != "password"}

    @property
    def dsn(self) -> str:
        return make_conninfo(**self.get_parameters())

    @property
    def status(self) -> pq.ConnStatus:
        return pq.ConnStatus.BAD if self._conn.closed else pq.ConnStatus.OK

    def parameter_status(self, param_name: str) -> str | None:
        row = self._conn._session.execute_params(
            "select current_setting($1::text, true)::text as value",
            [param_name],
        ).fetchone()
        value = row[0] if row else None
        if value is None or isinstance(value, str):
            return value
        return bytes(value).decode(self._conn.pgconn._encoding)

    @property
    def encoding(self) -> str:
        pgenc = self.parameter_status("client_encoding")
        return pg2pyenc((pgenc or "UTF8").encode())

    @property
    def full_protocol_version(self) -> int:
        return 30000

    @property
    def error_message(self) -> str:
        return ""

    @property
    def timezone(self) -> tzinfo:
        tzname = self.parameter_status("TimeZone")
        try:
            return _timezones[tzname]
        except KeyError:
            try:
                zi: tzinfo = ZoneInfo(tzname or "UTC")
            except Exception:
                zi = timezone.utc
            _timezones[tzname] = zi
            return zi

    @property
    def transaction_status(self) -> pq.TransactionStatus:
        return (
            pq.TransactionStatus.INTRANS
            if self._conn._in_transaction
            else pq.TransactionStatus.IDLE
        )

    @property
    def pipeline_status(self) -> pq.PipelineStatus:
        return (
            pq.PipelineStatus.ON
            if self._conn._pipeline_depth > 0
            else pq.PipelineStatus.OFF
        )


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


_LEGACY_ROW_FACTORIES = frozenset({list_row, tuple_row, dict_row, scalar_row})


class _BackendPgResultShim:
    def __init__(self, result: _ResultSetLike, encoding: str, format: pq.Format):
        self._result = result
        self._encoding = encoding
        self._format = format
        self.status = (
            ExecStatus.TUPLES_OK
            if getattr(result, "is_tuples", bool(result.columns or result.rows))
            else ExecStatus.COMMAND_OK
        )
        self.nfields = len(result.columns)

    def fname(self, index: int) -> bytes | None:
        return self._result.columns[index].encode(self._encoding)

    def fformat(self, index: int) -> int:
        if not 0 <= index < len(self._result.columns):
            raise IndexError(index)
        return int(self._format)


def _buffer_to_text(value: Buffer, encoding: str) -> str:
    return bytes(value).decode(encoding)


def _coerce_native_params(params: Params | None) -> list[str | None] | None:
    if params is None:
        return None
    if isinstance(params, Mapping):
        raise e.ProgrammingError(
            "ferrocopg native $n placeholders require a sequence of parameters"
        )
    if isinstance(params, (bytes, str)):
        raise TypeError(
            "query parameters should be a sequence or a mapping,"
            f" got {type(params).__qualname__}"
        )
    return [None if value is None else _coerce_param_text(value) for value in params]


def _split_extended_statements(query: str) -> list[str]:
    """Split top-level SQL statements without inspecting SQL values."""
    statements: list[str] = []
    start = position = 0
    length = len(query)

    while position < length:
        char = query[position]
        if char in "'\"":
            position = _skip_sql_quote(query, position, char)
        elif query.startswith("--", position):
            newline = query.find("\n", position + 2)
            position = length if newline < 0 else newline + 1
        elif query.startswith("/*", position):
            position = _skip_sql_comment(query, position)
        elif char == "$":
            position = _skip_dollar_quote(query, position) or position + 1
        elif char == ";":
            statement = query[start:position].strip()
            if statement:
                statements.append(statement)
            start = position + 1
            position += 1
        else:
            position += 1

    statement = query[start:].strip()
    if statement:
        statements.append(statement)
    return statements or [query]


def _skip_sql_quote(query: str, position: int, quote: str) -> int:
    position += 1
    while position < len(query):
        if query[position] == quote:
            if position + 1 < len(query) and query[position + 1] == quote:
                position += 2
            else:
                return position + 1
        elif quote == "'" and query[position] == "\\":
            position += 2
        else:
            position += 1
    return position


def _skip_sql_comment(query: str, position: int) -> int:
    depth = 1
    position += 2
    while position < len(query) and depth:
        if query.startswith("/*", position):
            depth += 1
            position += 2
        elif query.startswith("*/", position):
            depth -= 1
            position += 2
        else:
            position += 1
    return position


def _skip_dollar_quote(query: str, position: int) -> int | None:
    tag_end = query.find("$", position + 1)
    if tag_end < 0:
        return None

    tag = query[position + 1 : tag_end]
    if tag and (
        not (tag[0].isalpha() or tag[0] == "_")
        or not all(char.isalnum() or char == "_" for char in tag)
    ):
        return None

    delimiter = query[position : tag_end + 1]
    content_end = query.find(delimiter, tag_end + 1)
    return len(query) if content_end < 0 else content_end + len(delimiter)


class _NoTlsSessionLike(Protocol):
    closed: bool

    def close(self) -> None: ...

    def probe(self) -> _BackendProbeLike: ...

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def cancel_handle(self) -> _CancelHandleLike: ...

    def listen(self, channel: str) -> None: ...

    def unlisten(self, channel: str) -> None: ...

    def notify(self, channel: str, payload: str) -> None: ...

    def drain_notifications(self) -> list[_BackendNotificationLike]: ...

    def wait_for_notification(
        self, timeout_ms: int
    ) -> _BackendNotificationLike | None: ...

    def copy_from_stdin(self, query: str, data: bytes) -> int: ...

    def copy_to_stdout(self, query: str) -> _BackendCopyOutLike: ...

    def prepare_text(self, query: str) -> _PreparedStatementLike: ...

    def prepare_params(
        self, query: str, param_oids: list[int]
    ) -> _PreparedStatementLike: ...

    def simple_query_results(self, query: str) -> list[_ResultSetLike]: ...

    def pipeline_simple_query_results(
        self, queries: list[str]
    ) -> list[list[_ResultSetLike]]: ...

    def run_text_params(
        self, query: str, params: list[str | None]
    ) -> _ResultSetLike: ...

    def run_params(
        self, query: str, params: list[tuple[int, bool, bytes | None]]
    ) -> _ResultSetLike: ...

    def run_prepared_text_params(
        self, statement_id: int, params: list[str | None]
    ) -> _ResultSetLike: ...

    def run_prepared_params(
        self, statement_id: int, params: list[tuple[int, bool, bytes | None]]
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


def backend_session(conninfo: str) -> object | None:
    """
    Return a live Rust-backed reusable backend session if the extension is loaded.
    """
    if not _ferrocopg:
        return None
    return cast(object, _ferrocopg.connect_session(conninfo))


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

    def fetchone(self) -> list[bytes | str | None] | None:
        result = self.current_result
        if result is None:
            return None

        rows = result.rows
        if self._pos >= len(rows):
            return None

        row = rows[self._pos]
        self._pos += 1
        return row

    def fetchall(self) -> list[list[bytes | str | None]]:
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

    def probe(self) -> _BackendProbeLike:
        return self._session.probe()

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

    def execute_bound(self, query: str, params: _BoundParams) -> BackendResultCursor:
        result = self._session.run_params(query, params.values)
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

    def prepare_bound(self, query: str, params: _BoundParams) -> _PreparedStatementLike:
        return self._session.prepare_params(query, list(params.types))

    def execute_prepared_bound(
        self,
        statement_id: int,
        params: _BoundParams,
        *,
        statusmessage: str | None = None,
    ) -> BackendResultCursor:
        result = self._session.run_prepared_params(statement_id, params.values)
        return BackendResultCursor([result], [statusmessage])

    def begin(self) -> None:
        self._session.begin()

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    def cancel_handle(self) -> _CancelHandleLike:
        return self._session.cancel_handle()

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

    def copy_from_stdin(self, query: str, data: bytes) -> int:
        return self._session.copy_from_stdin(query, data)

    def copy_to_stdout(self, query: str) -> bytes:
        return self._session.copy_to_stdout(query).data

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
        self._adapters = AdaptersMap(conn.adapters)
        self._make_row: RowMaker | None = None
        self._result_transformer: AdaptTransformer | None = None
        self._rownumber: int | None = 0
        self.format = pq.Format.TEXT
        self.arraysize = 1

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def connection(self) -> NoTlsConnectionAdapter:
        return self._conn

    @property
    def row_factory(self) -> RowFactory:
        return self._row_factory

    @row_factory.setter
    def row_factory(self, row_factory: RowFactory) -> None:
        self._row_factory = row_factory
        self._make_row = None

    @property
    def adapters(self) -> AdaptersMap:
        return self._adapters

    @property
    def _encoding(self) -> str:
        return self._conn.pgconn._encoding

    @property
    def pgresult(self) -> _BackendPgResultShim | None:
        result = self._result
        if result is None:
            return None
        current = result.current_result
        if current is None:
            return None
        return _BackendPgResultShim(current, self._encoding, self.format)

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
        if not getattr(current, "is_tuples", bool(current.columns or current.rows)):
            return None
        return self._rownumber

    @property
    def description(self) -> list[BackendColumn] | None:
        if self._result is None:
            return None
        current = self._result.current_result
        if current is None:
            return []
        descriptions = getattr(current, "column_descriptions", None)
        if descriptions:
            return [
                BackendColumn(column.name, column.oid)
                for column in cast(list[_StatementColumnLike], descriptions)
            ]
        return [BackendColumn(name) for name in current.columns]

    @property
    def statusmessage(self) -> str | None:
        if self._result is None:
            return None
        return self._result.statusmessage

    def close(self) -> None:
        self._closed = True
        self._result = None
        self._result_transformer = None

    def copy(
        self,
        statement: str,
        params: list[str | None] | None = None,
        *,
        writer: object | None = None,
    ) -> NoTlsCopyAdapter:
        self._check_closed()
        self._conn._check_closed()
        if params is not None:
            raise e.NotSupportedError(
                "ferrocopg cursor.copy() doesn't support parameters yet"
            )
        if writer is not None:
            raise e.NotSupportedError(
                "ferrocopg cursor.copy() doesn't support custom writers yet"
            )
        return NoTlsCopyAdapter(self, statement)

    def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool = False,
        binary: bool | None = None,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        self._conn._check_closed()
        if binary is not None:
            self.format = pq.Format.BINARY if binary else pq.Format.TEXT
        self._result = self._conn._execute(
            query,
            params,
            prepare=prepare,
            prefer_extended=self._row_factory not in _LEGACY_ROW_FACTORIES,
            adapters=self.adapters,
        )
        self._make_row = None
        self._result_transformer = None
        self._rownumber = 0
        if self._row_factory not in _LEGACY_ROW_FACTORIES:
            self._make_row_for_result(self._result)
        return self

    def executemany(
        self,
        query: str,
        params_seq: Sequence[list[str | None]],
        *,
        returning: bool = False,
        prepare: bool = False,
    ) -> None:
        self._check_closed()
        self._conn._check_closed()
        if returning:
            results = [
                self._conn._execute(
                    query, params, prepare=prepare, adapters=self.adapters
                ).current_result
                for params in params_seq
            ]
            self._result = BackendResultCursor(
                [result for result in results if result is not None]
            )
        else:
            total = 0
            for params in params_seq:
                result = self._conn._execute(
                    query, params, prepare=prepare, adapters=self.adapters
                ).current_result
                if result is not None:
                    total += result.rows_affected
            synthetic = _SyntheticResult(rows_affected=total)
            synthetic.statusmessage = _statusmessage_for_query(query, synthetic)
            self._result = BackendResultCursor([synthetic])
        self._make_row = None
        self._result_transformer = None
        self._rownumber = 0

    def stream(
        self,
        query: Query,
        params: Params | None = None,
        *,
        binary: bool | None = None,
        size: int = 1,
    ) -> Iterator[object]:
        if self._conn._pipeline_depth:
            raise e.ProgrammingError("stream() cannot be used in pipeline mode")
        if size < 1:
            raise ValueError("size must be >= 1")
        self.execute(query, params, binary=binary)
        while True:
            row = self._fetchone_row()
            if row is _NO_ROW:
                break
            yield row

    def fetchone(self) -> object | None:
        row = self._fetchone_row()
        return None if row is _NO_ROW else row

    def _fetchone_row(self) -> object:
        result = self._require_result()
        self._check_result_for_fetch(result)
        row = result.fetchone()
        if row is None:
            return _NO_ROW
        self._rownumber = (self._rownumber or 0) + 1
        return self._make_row_for_result(result)(row)

    def fetchall(self) -> list[object]:
        result = self._require_result()
        self._check_result_for_fetch(result)
        rows = result.fetchall()
        self._rownumber = (self._rownumber or 0) + len(rows)
        make_row = self._make_row_for_result(result)
        return [make_row(row) for row in rows]

    def fetchmany(self, size: int = 0) -> list[object]:
        result = self._require_result()
        self._check_result_for_fetch(result)
        if not size:
            size = self.arraysize
        rows: list[object] = []
        while len(rows) < size:
            row = result.fetchone()
            if row is None:
                break
            self._rownumber = (self._rownumber or 0) + 1
            rows.append(self._make_row_for_result(result)(row))
        return rows

    def scroll(self, value: int, mode: str = "relative") -> None:
        result = self._require_result()
        current = result.current_result
        assert current is not None
        if mode == "relative":
            newpos = (self._rownumber or 0) + value
        elif mode == "absolute":
            newpos = value
        else:
            raise ValueError(f"bad mode: {mode}. It should be 'relative' or 'absolute'")
        if not 0 <= newpos < len(current.rows):
            raise IndexError("position out of bound")
        result._pos = newpos
        self._rownumber = newpos

    def nextset(self) -> bool | None:
        result = self._require_result()
        rv = result.nextset()
        if rv:
            self._make_row = None
            self._result_transformer = None
            self._rownumber = 0
        return rv

    def set_result(self, index: int) -> NoTlsCursorAdapter:
        result = self._require_result()
        result.set_result(index)
        self._make_row = None
        self._result_transformer = None
        self._rownumber = 0
        return self

    def setinputsizes(self, sizes: object) -> None:
        return None

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        return None

    def results(self) -> Iterator[NoTlsCursorAdapter]:
        if self._result is None:
            return
        while True:
            yield self
            if not self.nextset():
                break

    def __iter__(self) -> Iterator[object]:
        return self

    def __next__(self) -> object:
        row = self._fetchone_row()
        if row is _NO_ROW:
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

    def _check_result_for_fetch(self, result: BackendResultCursor) -> None:
        current = result.current_result
        if current is None or not getattr(
            current, "is_tuples", bool(current.columns or current.rows)
        ):
            raise e.ProgrammingError("the last operation didn't produce a result")

    def _make_row_for_result(self, result: BackendResultCursor) -> RowMaker:
        current = result.current_result
        if current is None:
            raise e.ProgrammingError("no result available")
        row_factory = self._row_factory
        if row_factory in _LEGACY_ROW_FACTORIES:
            legacy = cast(LegacyRowFactory, row_factory)

            def make_legacy_row(row: Sequence[object]) -> object:
                values = self._load_result_values(current, row)
                return legacy(current.columns, cast(list[str | None], list(values)))

            return make_legacy_row

        if self._make_row is None:
            self._make_row = cast(RowMaker, row_factory(self))
        make_row = self._make_row

        def make_typed_row(row: Sequence[object]) -> object:
            return make_row(self._load_result_values(current, row))

        return make_typed_row

    def _load_result_values(
        self, result: _ResultSetLike, row: Sequence[object]
    ) -> tuple[object, ...]:
        descriptions = getattr(result, "column_descriptions", None)
        if not descriptions:
            return tuple(row)

        # Test doubles and simple-query results may already carry text. Live
        # extended-query results always arrive here as binary wire values.
        if any(
            value is not None and not isinstance(value, (bytes, bytearray, memoryview))
            for value in row
        ):
            return tuple(row)

        if self._result_transformer is None:
            tx = AdaptTransformer(
                _AdaptContext(
                    self._conn,
                    _pure_python_adapters(
                        self.adapters,
                        text_loader_oids=(
                            _TEXT_WIRE_OIDS
                            if self.format == pq.Format.TEXT
                            else frozenset()
                        ),
                    ),
                    expose_connection=False,
                )
            )
            tx._encoding = self._encoding
            tx._row_loaders = [
                tx.get_loader(
                    column.oid,
                    pq.Format.TEXT
                    if self.format == pq.Format.TEXT and column.oid in _TEXT_WIRE_OIDS
                    else pq.Format.BINARY,
                ).load
                for column in descriptions
            ]
            self._result_transformer = tx

        return self._result_transformer.load_sequence(
            cast(Sequence[Buffer | None], row)
        )


class NoTlsCopyAdapter:
    """Small COPY bridge over the ferrocopg session adapter."""

    def __init__(self, cursor: NoTlsCursorAdapter, statement: str):
        self._cursor = cursor
        self._statement = statement
        normalized = " ".join(statement.lower().split())
        self._binary = _copy_statement_requests_binary(normalized)
        if " from stdin" in normalized:
            self._direction = "in"
        elif " to stdout" in normalized:
            self._direction = "out"
        else:
            raise e.ProgrammingError(
                "copy() requires a COPY FROM STDIN or COPY TO STDOUT statement"
            )
        self._buffer = bytearray()
        self._read_buffer = b""
        self._read_pos = 0
        self._tx = cast(Transformer, _TextCopyTransformer(cursor._conn.info.encoding))

    def __enter__(self) -> NoTlsCopyAdapter:
        if self._direction == "out":
            self._read_buffer = self._cursor._conn._session.copy_to_stdout(
                self._statement
            )
            self._read_pos = 0
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if exc is not None:
            return
        if self._direction == "in":
            rowcount = self._cursor._conn._session.copy_from_stdin(
                self._statement, bytes(self._buffer)
            )
            synthetic = _SyntheticResult(rows_affected=rowcount)
            synthetic.statusmessage = _statusmessage_for_query("COPY", synthetic)
            self._cursor._result = BackendResultCursor([synthetic])
            self._cursor._rownumber = None

    def write(self, buffer: bytes | str) -> None:
        if self._direction != "in":
            raise e.ProgrammingError("write() is only available during COPY FROM STDIN")
        if isinstance(buffer, str):
            buffer = buffer.encode(self._tx.encoding)
        self._buffer.extend(buffer)

    def write_row(self, row: Sequence[object]) -> None:
        if self._direction != "in":
            raise e.ProgrammingError(
                "write_row() is only available during COPY FROM STDIN"
            )
        if self._binary:
            raise e.NotSupportedError(
                "ferrocopg copy.write_row() doesn't support binary COPY yet"
            )
        _format_row_text(row, self._tx, self._buffer)

    def read(self, size: int = -1) -> bytes:
        if self._direction != "out":
            raise e.ProgrammingError("read() is only available during COPY TO STDOUT")
        if size < 0:
            size = len(self._read_buffer) - self._read_pos
        start = self._read_pos
        end = min(len(self._read_buffer), start + size)
        self._read_pos = end
        return self._read_buffer[start:end]

    def read_row(self) -> tuple[str | None, ...] | None:
        if self._direction != "out":
            raise e.ProgrammingError(
                "read_row() is only available during COPY TO STDOUT"
            )
        if self._binary:
            raise e.NotSupportedError(
                "ferrocopg copy.read_row() doesn't support binary COPY yet"
            )
        if self._read_pos >= len(self._read_buffer):
            return None
        end = self._read_buffer.find(b"\n", self._read_pos)
        if end < 0:
            return None
        row = self._read_buffer[self._read_pos : end + 1]
        self._read_pos = end + 1
        return cast(tuple[str | None, ...], _parse_row_text(row, self._tx))

    def rows(self) -> Iterator[tuple[str | None, ...]]:
        while row := self.read_row():
            yield row

    def __iter__(self) -> Iterator[bytes]:
        while data := self.read():
            yield data


def _copy_statement_requests_binary(normalized_statement: str) -> bool:
    return (
        "format binary" in normalized_statement
        or "with binary" in normalized_statement
        or normalized_statement.endswith(" binary")
    )


class NoTlsPipelineAdapter:
    """Experimental pipeline context over the ferrocopg connection adapter."""

    def __init__(self, conn: NoTlsConnectionAdapter):
        self._conn = conn
        self._queued: list[tuple[Query, NoTlsCursorAdapter, Params | None, bool]] = []
        self._entered = False
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def execute(
        self,
        query: Query,
        *,
        row_factory: RowFactory | None = None,
        params: Params | None = None,
        prepare: bool = False,
    ) -> NoTlsCursorAdapter:
        self._check_open()
        cur = self._conn.cursor(row_factory=row_factory)
        self._queued.append((query, cur, params, prepare))
        return cur

    def sync(self) -> None:
        self._check_open()
        if not self._queued:
            return

        if all(
            params is None and not prepare
            for _query, _cur, params, prepare in self._queued
        ):
            queries = [
                self._conn._convert_query_params(query, None)[0]
                for query, _cur, _params, _prepare in self._queued
            ]
            results = self._conn.execute_pipeline_simple(queries)
            for (_query, queued_cur, _params, _prepare), result_cur in zip(
                self._queued, results, strict=True
            ):
                queued_cur._result = result_cur._result
                queued_cur._rownumber = 0
        else:
            for query, queued_cur, params, prepare in self._queued:
                queued_cur._result = self._conn._execute(
                    query, params, prepare=prepare, adapters=queued_cur.adapters
                )
                queued_cur._rownumber = 0
        self._queued.clear()

    def __enter__(self) -> NoTlsPipelineAdapter:
        self._conn._check_closed()
        if self._entered:
            raise TypeError("pipeline blocks can be used only once")
        self._entered = True
        self._conn._pipeline_depth += 1
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if exc_type is None:
                self.sync()
        finally:
            self._conn._pipeline_depth -= 1
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
        conninfo: str = "",
        row_factory: RowFactory = list_row,
        cursor_factory: type[NoTlsCursorAdapter] = NoTlsCursorAdapter,
        prepare_threshold: int | None = 5,
        autocommit: bool = True,
    ):
        self._session = session
        self._conninfo = conninfo
        self.row_factory = row_factory
        self.cursor_factory = cursor_factory
        self.prepare_threshold = prepare_threshold
        self._autocommit = autocommit
        self._info = BackendConnectionInfo(self)
        self._probe_cache: _BackendProbeLike | None = None
        self._prepared: dict[str | tuple[str, tuple[int, ...]], int] = {}
        self._prepared_statusmessages: dict[int, str | None] = {}
        self._prepare_counts: dict[str | tuple[str, tuple[int, ...]], int] = {}
        self._in_transaction = False
        self._pipeline_depth = 0
        self._tx_depth = 0
        self._savepoint_counter = 0
        self._closed = False
        self._adapters: AdaptersMap | None = None
        self._pgconn = _PgconnEncodingShim("utf-8", self)
        self._isolation_level: IsolationLevel | None = None
        self._read_only: bool | None = None
        self._deferrable: bool | None = None
        self._notify_handlers: list[NotifyHandler] = []
        self._cancel_handle: _CancelHandleLike | None = None
        self._ensure_cancel_handle()

    @property
    def closed(self) -> bool:
        return self._closed or self._session.closed

    @property
    def broken(self) -> bool:
        return self.closed and not self._closed

    @property
    def info(self) -> BackendConnectionInfo:
        return self._info

    @property
    def adapters(self) -> AdaptersMap:
        if self._adapters is None:
            self._adapters = AdaptersMap(postgres.adapters)
            _install_wire_bytea_dumper(self._adapters)
        return self._adapters

    @property
    def connection(self) -> None:
        # C adaptation expects a real libpq PGconn here. Ferrocopg has no
        # such handle, so exposing its compatibility shim would be invalid.
        return None

    @property
    def pgconn(self) -> _PgconnEncodingShim:
        return self._pgconn

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

    def fileno(self) -> int:
        raise e.NotSupportedError("ferrocopg doesn't expose a libpq socket fileno")

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
        if self._closed:
            return
        self._session.close()
        self._prepared.clear()
        self._prepared_statusmessages.clear()
        self._prepare_counts.clear()
        self._cancel_handle = None
        self._closed = True

    def cursor(
        self,
        name: str = "",
        *,
        binary: bool = False,
        row_factory: RowFactory | None = None,
        scrollable: bool | None = None,
        withhold: bool = False,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        if name:
            raise e.NotSupportedError(
                "ferrocopg doesn't support server-side cursors yet"
            )
        if scrollable is not None:
            raise e.NotSupportedError(
                "ferrocopg doesn't support scrollable cursors yet"
            )
        if withhold:
            raise e.NotSupportedError("ferrocopg doesn't support withhold cursors yet")
        if row_factory is None:
            row_factory = self.row_factory
        cur = self.cursor_factory(self, row_factory=row_factory)
        if binary:
            cur.format = pq.Format.BINARY
        return cur

    def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool = False,
        binary: bool = False,
        row_factory: RowFactory | None = None,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        cur = self.cursor(row_factory=row_factory)
        return cur.execute(query, params, prepare=prepare, binary=binary)

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

    def cancel(self) -> None:
        if self._closed:
            return
        self._ensure_cancel_handle().cancel()

    def cancel_safe(self, *, timeout: float = 30.0) -> None:
        del timeout
        self.cancel()

    def set_isolation_level(self, value: IsolationLevel | int | None) -> None:
        self._check_set_transaction_param("isolation_level")
        self._isolation_level = IsolationLevel(value) if value is not None else None

    def set_read_only(self, value: bool | None) -> None:
        self._check_set_transaction_param("read_only")
        self._read_only = bool(value) if value is not None else None

    def set_deferrable(self, value: bool | None) -> None:
        self._check_set_transaction_param("deferrable")
        self._deferrable = bool(value) if value is not None else None

    def xid(self, format_id: int, gtrid: str, bqual: str) -> Xid:
        return Xid.from_parts(format_id, gtrid, bqual)

    def tpc_begin(self, xid: object) -> None:
        raise e.NotSupportedError(
            "ferrocopg doesn't support two-phase transactions yet"
        )

    def tpc_prepare(self) -> None:
        raise e.NotSupportedError(
            "ferrocopg doesn't support two-phase transactions yet"
        )

    def tpc_commit(self, xid: object | None = None) -> None:
        raise e.NotSupportedError(
            "ferrocopg doesn't support two-phase transactions yet"
        )

    def tpc_rollback(self, xid: object | None = None) -> None:
        raise e.NotSupportedError(
            "ferrocopg doesn't support two-phase transactions yet"
        )

    def tpc_recover(self) -> list[object]:
        raise e.NotSupportedError(
            "ferrocopg doesn't support two-phase transactions yet"
        )

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
        notifications = self._session.drain_notifications()
        self._dispatch_notifications(notifications)
        return notifications

    def wait_for_notification(self, timeout: float = 0.0) -> Notify | None:
        self._check_closed()
        notification = self._session.wait_for_notification(timeout)
        if notification is not None:
            self._dispatch_notifications([notification])
        return notification

    def add_notify_handler(self, callback: NotifyHandler) -> None:
        self._notify_handlers.append(callback)

    def remove_notify_handler(self, callback: NotifyHandler) -> None:
        self._notify_handlers.remove(callback)

    def add_notice_handler(self, callback: object) -> None:
        del callback
        raise e.NotSupportedError("ferrocopg doesn't support notice handlers yet")

    def remove_notice_handler(self, callback: object) -> None:
        del callback
        raise e.NotSupportedError("ferrocopg doesn't support notice handlers yet")

    def notifies(
        self, *, timeout: float | None = None, stop_after: int | None = None
    ) -> Iterator[Notify]:
        self._check_closed()
        if self._notify_handlers:
            warnings.warn(
                "using 'notifies()' together with notifies handlers on the same connection is not reliable. Please use only one of these methods",
                RuntimeWarning,
                stacklevel=2,
            )
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

    def _dispatch_notifications(self, notifications: Sequence[Notify]) -> None:
        for notification in notifications:
            for callback in self._notify_handlers:
                callback(notification)

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
        query: Query,
        params: Params | None,
        *,
        prepare: bool,
        prefer_extended: bool = False,
        adapters: AdaptersMap | None = None,
    ) -> BackendResultCursor:
        self._ensure_cancel_handle()
        self._ensure_transaction()
        query, params = self._convert_query_params(query, params, adapters=adapters)
        if params is None:
            if prefer_extended:
                return self._execute_extended_no_params(query)
            return self._session.execute_simple(query)

        if isinstance(params, _BoundParams):
            return self._execute_bound(query, params, prepare)

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

    def _execute_bound(
        self, query: str, params: _BoundParams, prepare: bool
    ) -> BackendResultCursor:
        key = (query, params.types)
        if not prepare and self.prepare_threshold is not None:
            count = self._prepare_counts.get(key, 0)
            prepare = count >= self.prepare_threshold
            self._prepare_counts[key] = count + 1

        if prepare:
            statement_id = self._prepared.get(key)
            if statement_id is None:
                prepared = self._session.prepare_bound(query, params)
                statement_id = prepared.statement_id
                self._prepared[key] = statement_id
                self._prepared_statusmessages[statement_id] = _statusmessage_for_query(
                    query
                )
            return self._session.execute_prepared_bound(
                statement_id,
                params,
                statusmessage=self._prepared_statusmessages.get(statement_id),
            )

        return self._session.execute_bound(query, params)

    def _execute_extended_no_params(self, query: str) -> BackendResultCursor:
        results: list[_ResultSetLike] = []
        statuses: list[str | None] = []
        for statement in _split_extended_statements(query):
            result_cursor = self._session.execute_params(statement, [])
            result = result_cursor.current_result
            if result is not None:
                results.append(result)
                statuses.append(_statusmessage_for_query(statement, result))
        return BackendResultCursor(results, statuses)

    def _convert_query_params(
        self,
        query: Query,
        params: Params | None,
        *,
        adapters: AdaptersMap | None = None,
    ) -> tuple[str, list[str | None] | _BoundParams | None]:
        if params is None:
            if isinstance(query, bytes):
                return query.decode(self._pgconn._encoding), None
            if isinstance(query, str):
                return query, None

        if isinstance(query, str) and "%" not in query:
            return query, _coerce_native_params(params)
        if isinstance(query, bytes) and b"%" not in query:
            return query.decode(self._pgconn._encoding), _coerce_native_params(params)

        tx = AdaptTransformer(
            _AdaptContext(self, adapters or self.adapters, expose_connection=False)
        )
        tx._encoding = self._pgconn._encoding
        pgq = PostgresQuery(tx)
        pgq.convert(query, params)
        if pgq.params is None:
            return pgq.query.decode(tx.encoding), None

        assert pgq.formats is not None
        assert len(pgq.params) == len(pgq.types) == len(pgq.formats)
        bound = _BoundParams(
            values=[
                (
                    oid,
                    format == pq.Format.BINARY,
                    None if value is None else bytes(value),
                )
                for oid, format, value in zip(
                    pgq.types, pgq.formats, pgq.params, strict=True
                )
            ],
            types=pgq.types,
        )
        return pgq.query.decode(tx.encoding), bound

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

    def _ensure_cancel_handle(self) -> _CancelHandleLike:
        if self._cancel_handle is None:
            try:
                self._cancel_handle = self._session.cancel_handle()
            except AttributeError:
                self._cancel_handle = _NoopCancelHandle()
        return self._cancel_handle

    def _probe(self) -> _BackendProbeLike:
        if self._probe_cache is None:
            self._probe_cache = self._session.probe()
        return self._probe_cache


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
    query: Query, result: _ResultSetLike | None = None
) -> str | None:
    if isinstance(query, bytes):
        query = query.decode()
    elif not isinstance(query, str):
        query = query.as_string(None)

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


def backend_session_adapter(conninfo: str) -> NoTlsSessionAdapter | None:
    """
    Return a Python-side adapter over the Rust backend session if loaded.
    """
    session = backend_session(conninfo)
    if session is None:
        return None
    return NoTlsSessionAdapter(cast(_NoTlsSessionLike, session))


def no_tls_connection_adapter(
    conninfo: str,
    *,
    row_factory: RowFactory = list_row,
    cursor_factory: type[NoTlsCursorAdapter] = NoTlsCursorAdapter,
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
        conninfo=conninfo,
        row_factory=row_factory,
        cursor_factory=cursor_factory,
        prepare_threshold=prepare_threshold,
        autocommit=autocommit,
    )
    conn.pgconn._encoding = conninfo_encoding(conninfo)
    if isolation_level is not None:
        conn.set_isolation_level(isolation_level)
    if read_only is not None:
        conn.set_read_only(read_only)
    if deferrable is not None:
        conn.set_deferrable(deferrable)
    return conn


def backend_connection_adapter(
    conninfo: str,
    *,
    row_factory: RowFactory = list_row,
    cursor_factory: type[NoTlsCursorAdapter] = NoTlsCursorAdapter,
    prepare_threshold: int | None = 5,
    autocommit: bool = True,
    isolation_level: IsolationLevel | int | None = None,
    read_only: bool | None = None,
    deferrable: bool | None = None,
) -> NoTlsConnectionAdapter | None:
    """
    Return an experimental connection-like adapter over the Rust backend.
    """
    session = backend_session_adapter(conninfo)
    if session is None:
        return None
    conn = NoTlsConnectionAdapter(
        session,
        conninfo=conninfo,
        row_factory=row_factory,
        cursor_factory=cursor_factory,
        prepare_threshold=prepare_threshold,
        autocommit=autocommit,
    )
    conn.pgconn._encoding = conninfo_encoding(conninfo)
    if isolation_level is not None:
        conn.set_isolation_level(isolation_level)
    if read_only is not None:
        conn.set_read_only(read_only)
    if deferrable is not None:
        conn.set_deferrable(deferrable)
    return conn
