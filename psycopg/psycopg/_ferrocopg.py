"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import timedelta, timezone, tzinfo
from time import monotonic
from types import SimpleNamespace
from typing import Any, NamedTuple, ParamSpec, Protocol, TypeVar, cast
from zoneinfo import ZoneInfo

from . import errors as e
from . import postgres, pq
from ._adapters_map import AdaptersMap
from ._compat import Template
from ._connection_base import NoticeHandler, Notify
from ._copy_base import BinaryFormatter, TextFormatter
from ._encodings import conninfo_encoding, pg2pyenc
from ._enums import IsolationLevel, PyFormat
from ._oids import BYTEA_OID
from ._py_transformer import Transformer as AdaptTransformer
from ._queries import PostgresClientQuery, PostgresQuery
from ._rmodule import __version__ as __version__
from ._rmodule import _ferrocopg
from ._tpc import Xid
from .abc import Buffer, Params, Query
from .adapt import Dumper
from .conninfo import _param_escape, conninfo_to_dict, make_conninfo
from .pq import ExecStatus
from .transaction import Rollback
from .types.string import BytesDumper

logger = logging.getLogger("psycopg")
P = ParamSpec("P")
T = TypeVar("T")


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
        wire_format: pq.Format | None = None,
    ):
        self.columns = columns or []
        self.column_descriptions = column_descriptions or []
        self.rows = rows or []
        self.rows_affected = rows_affected
        self.statusmessage = statusmessage
        self.is_tuples = is_tuples
        self.wire_format = wire_format


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

    def exec_(self, query: bytes) -> _BackendPgResultShim | None:
        if self._conn is None:
            raise e.OperationalError("connection is closed")
        return self._conn._exec_command(query)


class _BackendErrorResult:
    def __init__(self, info: dict[int, bytes | None], message: str):
        self._info = info
        self._message = message

    def error_field(self, field: int) -> bytes | None:
        return self._info.get(field)

    def get_error_message(self, encoding: str = "utf-8") -> str:
        del encoding
        return self._message


class _BackendConnectPgconn:
    def __init__(self, conninfo: str):
        summary = conninfo_summary(conninfo)
        dbname = getattr(summary, "dbname", "") if summary is not None else ""
        self.db = str(dbname or "").encode()


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


class _BackendTransformer(AdaptTransformer):
    """Keep connection-free dumpers/loaders on the backend wire encoding."""

    def get_dumper(self, obj: Any, format: PyFormat) -> Any:
        dumper = super().get_dumper(obj, format)
        if hasattr(dumper, "_encoding"):
            dumper._encoding = self.encoding
        return dumper

    def get_dumper_by_oid(self, oid: int, format: pq.Format) -> Any:
        dumper = super().get_dumper_by_oid(oid, format)
        if hasattr(dumper, "_encoding"):
            dumper._encoding = self.encoding
        return dumper

    def get_loader(self, oid: int, format: pq.Format) -> Any:
        loader = super().get_loader(oid, format)
        if hasattr(loader, "_encoding"):
            loader._encoding = self.encoding
        return loader


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
        if self._conn._copy_active:
            return pq.TransactionStatus.ACTIVE
        if self._conn._transaction_failed:
            return pq.TransactionStatus.INERROR
        if self._conn._in_transaction:
            return pq.TransactionStatus.INTRANS
        return pq.TransactionStatus.IDLE

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
    def __init__(
        self,
        result: _ResultSetLike,
        encoding: str,
        format: pq.Format,
        statusmessage: str | None = None,
    ):
        self._result = result
        self._encoding = encoding
        self._format = format
        self.status = (
            ExecStatus.TUPLES_OK
            if getattr(result, "is_tuples", bool(result.columns or result.rows))
            else ExecStatus.COMMAND_OK
        )
        self.nfields = len(result.columns)
        self.ntuples = len(result.rows)
        self.command_status = (statusmessage or "").encode(encoding)

    def fname(self, index: int) -> bytes | None:
        return self._result.columns[index].encode(self._encoding)

    def fformat(self, index: int) -> int:
        if not 0 <= index < len(self._result.columns):
            raise IndexError(index)
        return int(self._format)

    def ftype(self, index: int) -> int:
        descriptions = getattr(self._result, "column_descriptions", ())
        if not 0 <= index < len(descriptions):
            return 0
        return int(descriptions[index].oid)

    def get_value(self, row: int, column: int) -> bytes | None:
        value = self._result.rows[row][column]
        if value is None or isinstance(value, bytes):
            return value
        return (
            bytes(value) if not isinstance(value, str) else value.encode(self._encoding)
        )


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

    def drain_notices(self) -> list[dict[int, bytes | None]]: ...

    def copy_from_stdin(self, query: str, data: bytes) -> int: ...

    def copy_to_stdout(self, query: str) -> _BackendCopyOutLike: ...

    def prepare_text(self, query: str) -> _PreparedStatementLike: ...

    def describe_text(self, query: str) -> Any: ...

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


def merge_conninfo(conninfo: str, params: Mapping[str, str | int | None]) -> str:
    """Merge connection parameters without validating them through libpq."""
    overrides: dict[str, str | int] = {}
    for key, value in params.items():
        if value is not None:
            overrides[key] = value
    if not overrides:
        return str(conninfo)

    merged: dict[str, str | int] = {}
    if conninfo:
        for key, value in conninfo_to_dict(conninfo).items():
            if value is not None:
                merged[key] = value
    merged.update(overrides)
    return " ".join(
        f"{key}={_param_escape(str(value))}" for key, value in merged.items()
    )


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
    try:
        return cast(object, _ferrocopg.connect_session(conninfo))
    except e.Error as ex:
        raise e.OperationalError(
            str(ex), info=ex._info, pgconn=cast(Any, _BackendConnectPgconn(conninfo))
        ) from None


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
        self.encoding = "utf-8"
        self.error_handler: Callable[[BaseException], None] | None = None
        self.notice_handler: (
            Callable[[Sequence[dict[int, bytes | None]]], None] | None
        ) = None

    @property
    def closed(self) -> bool:
        return self._session.closed

    def close(self) -> None:
        self._session.close()

    def probe(self) -> _BackendProbeLike:
        return self._call(self._session.probe)

    def execute_simple(self, query: str) -> BackendResultCursor:
        results = self._call(self._session.simple_query_results, query)
        statuses = [_statusmessage_for_query(query, result) for result in results]
        return BackendResultCursor(results, statuses)

    def execute_pipeline_simple(self, queries: list[str]) -> list[BackendResultCursor]:
        batches = self._call(self._session.pipeline_simple_query_results, queries)
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
        result = self._call(self._session.run_text_params, query, params)
        return BackendResultCursor([result], [_statusmessage_for_query(query, result)])

    def execute_bound(self, query: str, params: _BoundParams) -> BackendResultCursor:
        result = self._call(self._session.run_params, query, params.values)
        return BackendResultCursor([result], [_statusmessage_for_query(query, result)])

    def execute_prepared(
        self,
        statement_id: int,
        params: list[str | None],
        *,
        statusmessage: str | None = None,
    ) -> BackendResultCursor:
        result = self._call(
            self._session.run_prepared_text_params, statement_id, params
        )
        return BackendResultCursor([result], [statusmessage])

    def prepare_bound(self, query: str, params: _BoundParams) -> _PreparedStatementLike:
        return self._call(self._session.prepare_params, query, list(params.types))

    def execute_prepared_bound(
        self,
        statement_id: int,
        params: _BoundParams,
        *,
        statusmessage: str | None = None,
    ) -> BackendResultCursor:
        result = self._call(
            self._session.run_prepared_params, statement_id, params.values
        )
        return BackendResultCursor([result], [statusmessage])

    def begin(self) -> None:
        self._call(self._session.begin)

    def commit(self) -> None:
        self._call(self._session.commit)

    def rollback(self) -> None:
        self._call(self._session.rollback)

    def cancel_handle(self) -> _CancelHandleLike:
        return self._call(self._session.cancel_handle)

    def listen(self, channel: str) -> None:
        self._call(self._session.listen, channel)

    def unlisten(self, channel: str) -> None:
        self._call(self._session.unlisten, channel)

    def notify(self, channel: str, payload: str = "") -> None:
        self._call(self._session.notify, channel, payload)

    def drain_notifications(self) -> list[Notify]:
        return [
            Notify(n.channel, n.payload, n.process_id)
            for n in self._call(self._session.drain_notifications)
        ]

    def wait_for_notification(self, timeout: float = 0.0) -> Notify | None:
        timeout_ms = max(0, int(timeout * 1000))
        notification = self._call(self._session.wait_for_notification, timeout_ms)
        if notification is None:
            return None
        return Notify(
            notification.channel,
            notification.payload,
            notification.process_id,
        )

    def copy_from_stdin(self, query: str, data: bytes) -> int:
        return self._call(self._session.copy_from_stdin, query, data)

    def copy_to_stdout(self, query: str) -> bytes:
        return self._call(self._session.copy_to_stdout, query).data

    def prepare_text(self, query: str) -> _PreparedStatementLike:
        return self._call(self._session.prepare_text, query)

    def describe_text(self, query: str) -> Any:
        return self._call(self._session.describe_text, query)

    def _call(self, method: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        try:
            result = method(*args, **kwargs)
        except BaseException as ex:
            if isinstance(ex, e.Error):
                self._normalize_error(ex, method, tuple(args))
                if self.error_handler is not None:
                    self.error_handler(ex)
            try:
                self._drain_notices()
            except Exception:
                logger.exception("error draining notices after backend failure")
            raise
        self._drain_notices()
        return result

    def _normalize_error(
        self, ex: e.Error, method: Callable[..., object], args: tuple[object, ...]
    ) -> None:
        if not isinstance(ex._info, dict):
            return

        raw_message = ex._info.get(pq.DiagnosticField.MESSAGE_PRIMARY)
        message = (
            raw_message.decode(self.encoding, "replace")
            if raw_message is not None
            else str(ex)
        )
        ex._encoding = self.encoding
        method_name = getattr(method, "__name__", "")
        if (
            method_name
            in {
                "simple_query_results",
                "pipeline_simple_query_results",
                "run_text_params",
                "run_params",
                "prepare_text",
                "prepare_params",
            }
            and args
            and isinstance(args[0], str)
        ):
            query = args[0].strip()
            if query and query not in message:
                message = f"{message}\nLINE 1: {query}"

        ex.args = (message,)

        ex._info = cast(Any, _BackendErrorResult(ex._info, message))

    def _drain_notices(self) -> None:
        drain = cast(
            Callable[[], list[dict[int, bytes | None]]] | None,
            getattr(self._session, "drain_notices", None),
        )
        if drain is None:
            return
        notices = drain()
        if notices and self.notice_handler is not None:
            self.notice_handler(notices)

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
        self._query: PostgresQuery | PostgresClientQuery | None = None
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
        statement: Query,
        params: Params | None = None,
        *,
        writer: object | None = None,
    ) -> NoTlsCopyAdapter:
        self._check_closed()
        self._conn._check_closed()
        return NoTlsCopyAdapter(self, statement, params=params, writer=writer)

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

        wire_format = getattr(result, "wire_format", None)

        # Test doubles and simple-query results may already carry text. Live
        # extended-query results always arrive here as binary wire values.
        if wire_format is None and any(
            value is not None and not isinstance(value, (bytes, bytearray, memoryview))
            for value in row
        ):
            return tuple(row)

        if self._result_transformer is None:
            tx = _BackendTransformer(
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
                    wire_format
                    if wire_format is not None
                    else (
                        pq.Format.TEXT
                        if self.format == pq.Format.TEXT
                        and column.oid in _TEXT_WIRE_OIDS
                        else pq.Format.BINARY
                    ),
                ).load
                for column in descriptions
            ]
            self._result_transformer = tx

        if wire_format == pq.Format.TEXT:
            row = tuple(
                value.encode(self._encoding) if isinstance(value, str) else value
                for value in row
            )
        return self._result_transformer.load_sequence(
            cast(Sequence[Buffer | None], row)
        )


class NoTlsServerCursorAdapter(NoTlsCursorAdapter):
    """Backend-native server cursor implemented with PostgreSQL cursor SQL."""

    def __init__(
        self,
        conn: NoTlsConnectionAdapter,
        name: str,
        *,
        row_factory: RowFactory = list_row,
        scrollable: bool | None = None,
        withhold: bool = False,
        factory_name: str = "ServerCursor",
    ):
        super().__init__(conn, row_factory=row_factory)
        self._name = name
        self._scrollable = scrollable
        self._withhold = withhold
        self._factory_name = factory_name
        self._declared = False
        self._descriptions: list[_StatementColumnLike] = []
        self._pos = 0
        self.itersize = 100
        self._iter_rows: Iterator[object] | None = None

    def __repr__(self) -> str:
        return f"<psycopg.{self._factory_name} {self._name!r} [{self._state_name()}]>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def scrollable(self) -> bool | None:
        return self._scrollable

    @property
    def withhold(self) -> bool:
        return self._withhold

    @property
    def rownumber(self) -> int | None:
        return self._pos if self._descriptions else None

    def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool = False,
        binary: bool | None = None,
    ) -> NoTlsServerCursorAdapter:
        del prepare
        self._check_closed()
        self._conn._check_closed()
        if self._declared:
            self._close_server_cursor()
        if binary is not None:
            self.format = pq.Format.BINARY if binary else pq.Format.TEXT

        query_text, converted = self._conn._convert_query_params(
            query, params, adapters=self.adapters
        )
        parts = ["DECLARE", _quoted_identifier(self._name)]
        if self._scrollable is not None:
            parts.append("SCROLL" if self._scrollable else "NO SCROLL")
        parts.append("CURSOR")
        if self._withhold:
            parts.extend(("WITH", "HOLD"))
        parts.extend(("FOR", query_text))
        declaration = " ".join(parts)

        self._conn._ensure_transaction()
        query_params: Sequence[bytes | str | None] | None
        if isinstance(converted, _BoundParams):
            result = self._conn._session.execute_bound(declaration, converted)
            query_params = [value for _oid, _binary, value in converted.values]
        elif converted is not None:
            result = self._conn._session.execute_params(declaration, converted)
            query_params = converted
        else:
            result = self._conn._session.execute_params(declaration, [])
            query_params = None
        self._query = cast(
            Any,
            SimpleNamespace(
                query=declaration.encode(self._encoding), params=query_params
            ),
        )
        self._result = result
        self._declared = True
        self._pos = 0
        self._iter_rows = None
        self._describe_server_cursor()
        return self

    def executemany(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        raise e.NotSupportedError("executemany() is not supported on server cursors")

    def fetchone(self) -> object | None:
        rows = self._fetch_server_rows(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int = 0) -> list[object]:
        return self._fetch_server_rows(size or self.arraysize)

    def fetchall(self) -> list[object]:
        return self._fetch_server_rows(None)

    def stream(
        self,
        query: Query,
        params: Params | None = None,
        *,
        binary: bool | None = None,
        size: int = 1,
    ) -> Iterator[object]:
        del size
        self.execute(query, params, binary=binary)
        yield from self

    def scroll(self, value: int, mode: str = "relative") -> None:
        self._ensure_described()
        if mode not in {"relative", "absolute"}:
            raise ValueError(f"bad mode: {mode}. It should be 'relative' or 'absolute'")
        direction = "ABSOLUTE " if mode == "absolute" else ""
        self._conn._session.execute_simple(
            f"MOVE {direction}{value} FROM {_quoted_identifier(self._name)}"
        )
        self._pos = value if mode == "absolute" else self._pos + value
        self._iter_rows = None

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._close_server_cursor()
        finally:
            super().close()

    def nextset(self) -> None:
        return None

    def results(self) -> Iterator[NoTlsServerCursorAdapter]:
        if self._result is not None:
            yield self

    def __next__(self) -> object:
        if self._iter_rows is None:
            self._iter_rows = iter(self._fetch_server_rows(self.itersize))
        try:
            return next(self._iter_rows)
        except StopIteration:
            rows = self._fetch_server_rows(self.itersize)
            if not rows:
                self._iter_rows = None
                raise
            self._iter_rows = iter(rows)
            return next(self._iter_rows)

    def _describe_server_cursor(self) -> None:
        fetch = f"FETCH FORWARD 0 FROM {_quoted_identifier(self._name)}"
        description = self._conn._session.describe_text(fetch)
        self._descriptions = list(description.columns)
        result = _SyntheticResult(
            columns=[column.name for column in self._descriptions],
            column_descriptions=self._descriptions,
            rows=[],
            is_tuples=True,
            wire_format=self.format,
        )
        self._result = BackendResultCursor([result], [None])
        self._make_row = None
        self._result_transformer = None

    def _ensure_described(self) -> None:
        self._check_closed()
        self._conn._check_closed()
        if not self._descriptions:
            self._describe_server_cursor()

    def _fetch_server_rows(self, count: int | None) -> list[object]:
        self._ensure_described()
        amount = "ALL" if count is None else str(count)
        fetch = f"FETCH FORWARD {amount} FROM {_quoted_identifier(self._name)}"
        if self.format == pq.Format.BINARY:
            result_cursor = self._conn._session.execute_params(fetch, [])
        else:
            simple = self._conn._session.execute_simple(fetch)
            current = simple.current_result
            rows = [] if current is None else current.rows
            result = _SyntheticResult(
                columns=[column.name for column in self._descriptions],
                column_descriptions=self._descriptions,
                rows=rows,
                rows_affected=len(rows),
                is_tuples=True,
                wire_format=pq.Format.TEXT,
            )
            result_cursor = BackendResultCursor([result], [f"FETCH {len(rows)}"])
        self._result = result_cursor
        self._make_row = None
        self._result_transformer = None
        self._rownumber = 0
        loaded_rows: list[object] = super().fetchall()
        self._pos += len(loaded_rows)
        return loaded_rows

    def _close_server_cursor(self) -> None:
        if self._conn.closed or self._conn._transaction_failed:
            return
        try:
            self._conn._session.execute_simple(
                f"CLOSE {_quoted_identifier(self._name)}"
            )
        except e.InvalidCursorName:
            pass
        self._declared = False

    def _state_name(self) -> str:
        if self._conn.closed:
            return "BAD"
        if self._conn._transaction_failed:
            return "INERROR"
        return "INTRANS" if self._conn._in_transaction else "IDLE"


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class NoTlsCopyAdapter:
    """COPY bridge using Psycopg's formatters over the Rust byte pipes."""

    def __init__(
        self,
        cursor: NoTlsCursorAdapter,
        statement: Query,
        *,
        params: Params | None = None,
        writer: object | None = None,
    ):
        self._cursor = cursor
        self.connection = cursor.connection
        tx = _BackendTransformer(
            _AdaptContext(
                self.connection,
                _pure_python_adapters(cursor.adapters),
                expose_connection=False,
            )
        )
        tx._encoding = cursor._conn.info.encoding
        query = PostgresClientQuery(tx)
        query.convert(statement, params)
        cursor._query = query
        self._statement = query.query.decode(tx.encoding)
        normalized = " ".join(self._statement.lower().split())
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
        self._read_blocks: list[bytes] = []
        self._read_pos = 0
        self._tx = tx
        self.formatter = (
            BinaryFormatter(tx)
            if self._binary
            else TextFormatter(tx, encoding=tx.encoding)
        )
        self.writer = writer
        writer_type = type(writer)
        self._database_writer = writer is None or (
            writer_type.__module__ == "psycopg._copy"
            and writer_type.__name__ in {"LibpqWriter", "QueuedLibpqWriter"}
        )
        self._queued_writer = bool(
            writer is not None and writer_type.__name__ == "QueuedLibpqWriter"
        )
        self._types_set = False
        self._entered = False
        self._finished = False
        self._rowcount = 0

    def __repr__(self) -> str:
        status = (
            "BAD"
            if self.connection.closed
            else (
                "INERROR"
                if self.connection._transaction_failed
                else ("INTRANS" if self.connection._in_transaction else "IDLE")
            )
        )
        if self._entered and not self._finished:
            status = "ACTIVE"
        return f"<{type(self).__module__}.{type(self).__qualname__} [{status}]>"

    def __enter__(self) -> NoTlsCopyAdapter:
        if self._entered:
            raise TypeError("copy blocks can be used only once")
        self._entered = True
        self.connection._ensure_transaction()
        self.connection._copy_active = True
        if self._direction == "out":
            try:
                data = self._cursor._conn._session.copy_to_stdout(self._statement)
            except BaseException:
                self.connection._copy_active = False
                raise
            self._read_blocks = (
                _split_binary_copy_blocks(data)
                if self._binary
                else data.splitlines(keepends=True)
            )
            self._rowcount = (
                _binary_copy_row_count(data) if self._binary else len(self._read_blocks)
            )
            descriptions: Sequence[_StatementColumnLike] = ()
            if inner_query := _copy_inner_query(self._statement):
                descriptions = self._cursor._conn._session.describe_text(
                    inner_query
                ).columns
            self._set_cursor_result(self._rowcount, descriptions)
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if self._finished:
            return
        self._finished = True
        self.connection._copy_active = False
        if exc is None and self._direction == "in":
            if data := self.formatter.end():
                self._write_data(data)
            if self._database_writer:
                self._rowcount = self._cursor._conn._session.copy_from_stdin(
                    self._statement, bytes(self._buffer)
                )
                self._set_cursor_result(self._rowcount)
        elif exc is not None and self._direction == "in":
            self.connection._transaction_failed = self.connection._in_transaction

        if self._queued_writer and self.writer is not None:
            setattr(self.writer, "_worker", None)
        elif not self._database_writer and self.writer is not None:
            finish = getattr(self.writer, "finish", None)
            if finish is not None:
                finish(exc)

    def set_types(self, types: Sequence[int | str]) -> None:
        registry = self._cursor.adapters.types
        oids = [typ if isinstance(typ, int) else registry.get_oid(typ) for typ in types]
        if self._direction == "in":
            self._tx.set_dumper_types(oids, self.formatter.format)
        else:
            self._tx.set_loader_types(oids, self.formatter.format)
        self._types_set = True

    def write(self, buffer: bytes | str) -> None:
        if self._direction != "in":
            raise e.ProgrammingError("write() is only available during COPY FROM STDIN")
        if data := self.formatter.write(buffer):
            self._write_data(data)

    def write_row(self, row: Sequence[object]) -> None:
        if self._direction != "in":
            raise e.ProgrammingError(
                "write_row() is only available during COPY FROM STDIN"
            )
        pending = self.formatter._write_buffer
        size = len(pending)
        try:
            data = self.formatter.write_row(row)
        except Exception:
            del pending[size:]
            raise
        if data:
            self._write_data(data)

    def read(self, size: int = -1) -> bytes:
        if self._direction != "out":
            raise e.ProgrammingError("read() is only available during COPY TO STDOUT")
        if self._read_pos >= len(self._read_blocks):
            return b""
        block = self._read_blocks[self._read_pos]
        self._read_pos += 1
        if size >= 0 and size < len(block):
            self._read_pos -= 1
            self._read_blocks[self._read_pos] = block[size:]
            return block[:size]
        return block

    def read_row(self) -> tuple[object, ...] | None:
        if self._direction != "out":
            raise e.ProgrammingError(
                "read_row() is only available during COPY TO STDOUT"
            )
        data = self.read()
        if not data:
            return None
        if not self._types_set:
            nfields = _copy_block_field_count(data, binary=self._binary)
            loader = (
                (lambda value: bytes(value))
                if self._binary
                else (lambda value: bytes(value).decode(self._tx.encoding))
            )
            self._tx._row_loaders = [loader] * nfields
        return self.formatter.parse_row(data)

    def rows(self) -> Iterator[tuple[object, ...]]:
        while (row := self.read_row()) is not None:
            yield row

    def __iter__(self) -> Iterator[bytes]:
        while data := self.read():
            yield data

    def _write_data(self, data: Buffer) -> None:
        if self._database_writer:
            self._buffer.extend(data)
            if self._queued_writer and self.writer is not None:
                setattr(self.writer, "_worker", object())
        else:
            assert self.writer is not None
            write = cast(Callable[[Buffer], None], getattr(self.writer, "write"))
            write(data)

    def _set_cursor_result(
        self,
        rowcount: int,
        descriptions: Sequence[_StatementColumnLike] = (),
    ) -> None:
        result = _SyntheticResult(
            columns=[column.name for column in descriptions],
            column_descriptions=list(descriptions),
            rows_affected=rowcount,
            is_tuples=False,
        )
        result.statusmessage = f"COPY {rowcount}"
        self._cursor._result = BackendResultCursor([result])
        self._cursor._rownumber = None


def _copy_statement_requests_binary(normalized_statement: str) -> bool:
    return (
        "format binary" in normalized_statement
        or "with binary" in normalized_statement
        or normalized_statement.endswith(" binary")
    )


def _split_binary_copy_blocks(data: bytes) -> list[bytes]:
    if not data:
        return []
    if not data.startswith(b"PGCOPY\n\xff\r\n\x00") or len(data) < 19:
        return [data]

    extension_length = int.from_bytes(data[15:19], "big")
    pos = 19 + extension_length
    start = 0
    blocks: list[bytes] = []
    while pos + 2 <= len(data):
        row_start = pos
        nfields = int.from_bytes(data[pos : pos + 2], "big", signed=True)
        pos += 2
        if nfields == -1:
            if not blocks:
                return [data[:pos]]
            if start < row_start:
                blocks.append(data[start:row_start])
            blocks.append(data[row_start:pos])
            return blocks
        for _ in range(nfields):
            if pos + 4 > len(data):
                return [data]
            length = int.from_bytes(data[pos : pos + 4], "big", signed=True)
            pos += 4
            if length >= 0:
                pos += length
                if pos > len(data):
                    return [data]
        if not blocks:
            blocks.append(data[:pos])
        else:
            blocks.append(data[row_start:pos])
        start = pos
    return [data]


def _binary_copy_row_count(data: bytes) -> int:
    if not data.startswith(b"PGCOPY\n\xff\r\n\x00") or len(data) < 19:
        return 0
    pos = 19 + int.from_bytes(data[15:19], "big")
    count = 0
    while pos + 2 <= len(data):
        nfields = int.from_bytes(data[pos : pos + 2], "big", signed=True)
        pos += 2
        if nfields == -1:
            return count
        for _ in range(nfields):
            if pos + 4 > len(data):
                return count
            length = int.from_bytes(data[pos : pos + 4], "big", signed=True)
            pos += 4
            if length >= 0:
                pos += length
                if pos > len(data):
                    return count
        count += 1
    return count


def _copy_inner_query(statement: str) -> str | None:
    normalized = statement.lstrip()
    if normalized[:4].lower() != "copy":
        return None
    start = normalized.find("(", 4)
    if start < 0:
        return None

    depth = 0
    quote: str | None = None
    pos = start
    while pos < len(normalized):
        char = normalized[pos]
        if quote is not None:
            if char == quote:
                if pos + 1 < len(normalized) and normalized[pos + 1] == quote:
                    pos += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                suffix = normalized[pos + 1 :].lstrip().lower()
                return normalized[start + 1 : pos] if suffix.startswith("to") else None
        pos += 1
    return None


def _copy_block_field_count(data: bytes, *, binary: bool) -> int:
    if binary:
        if data.startswith(b"PGCOPY\n\xff\r\n\x00"):
            extension_length = int.from_bytes(data[15:19], "big")
            pos = 19 + extension_length
        else:
            pos = 0
        if len(data) < pos + 2:
            return 0
        return max(0, int.from_bytes(data[pos : pos + 2], "big", signed=True))
    return max(1, data.count(b"\t") + 1)


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

    Warning = e.Warning
    Error = e.Error
    InterfaceError = e.InterfaceError
    DatabaseError = e.DatabaseError
    DataError = e.DataError
    OperationalError = e.OperationalError
    IntegrityError = e.IntegrityError
    InternalError = e.InternalError
    ProgrammingError = e.ProgrammingError
    NotSupportedError = e.NotSupportedError

    def __init__(
        self,
        session: NoTlsSessionAdapter,
        *,
        conninfo: str = "",
        row_factory: RowFactory = list_row,
        cursor_factory: type[NoTlsCursorAdapter] = NoTlsCursorAdapter,
        server_cursor_factory: type[object] | None = None,
        prepare_threshold: int | None = 5,
        autocommit: bool = True,
    ):
        self._session = session
        self._conninfo = conninfo
        self.row_factory = row_factory
        self.cursor_factory = cursor_factory
        self.server_cursor_factory = server_cursor_factory or NoTlsServerCursorAdapter
        self.prepare_threshold = prepare_threshold
        self._autocommit = autocommit
        self._info = BackendConnectionInfo(self)
        self._probe_cache: _BackendProbeLike | None = None
        self._prepared: dict[str | tuple[str, tuple[int, ...]], int] = {}
        self._prepared_statusmessages: dict[int, str | None] = {}
        self._prepare_counts: dict[str | tuple[str, tuple[int, ...]], int] = {}
        self._in_transaction = False
        self._transaction_failed = False
        self._pipeline_depth = 0
        self._tx_depth = 0
        self._tpc: tuple[Xid, bool] | None = None
        self._closed = False
        self._adapters: AdaptersMap | None = None
        self._pgconn = _PgconnEncodingShim("utf-8", self)
        self._session.encoding = self._pgconn._encoding
        self._isolation_level: IsolationLevel | None = None
        self._read_only: bool | None = None
        self._deferrable: bool | None = None
        self._notice_handlers: list[NoticeHandler] = []
        self._notify_handlers: list[NotifyHandler] = []
        self._encoding_changed_in_transaction = False
        self._copy_active = False
        self._session.notice_handler = self._dispatch_notices
        self._session.error_handler = self._on_backend_error
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

    def parameter_status(self, param_name: bytes) -> bytes | None:
        return self._pgconn.parameter_status(param_name)

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
            factory = self.server_cursor_factory
            adapter_factory = (
                factory
                if isinstance(factory, type)
                and issubclass(factory, NoTlsServerCursorAdapter)
                else NoTlsServerCursorAdapter
            )
            return cast(
                NoTlsCursorAdapter,
                adapter_factory(
                    self,
                    name,
                    row_factory=row_factory or self.row_factory,
                    scrollable=scrollable,
                    withhold=withhold,
                    factory_name=getattr(factory, "__name__", "ServerCursor"),
                ),
            )
        if scrollable is not None or withhold:
            raise e.ProgrammingError(
                "scrollable and withhold options require a named server cursor"
            )
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
            self._exec_command(self._tx_start_query())
            self._in_transaction = True
            self._transaction_failed = False

    def commit(self) -> None:
        self._check_closed()
        if self._tpc:
            raise e.ProgrammingError(
                "commit() cannot be used during a two-phase transaction"
            )
        if not self._in_transaction:
            return
        self._exec_command("COMMIT")
        self._in_transaction = False
        self._transaction_failed = False

    def rollback(self) -> None:
        self._check_closed()
        if self._tpc:
            raise e.ProgrammingError(
                "rollback() cannot be used during a two-phase transaction"
            )
        if not self._in_transaction:
            return
        self._exec_command("ROLLBACK")
        self._in_transaction = False
        self._transaction_failed = False

    def _exec_command(
        self, command: Query, result_format: pq.Format = pq.Format.TEXT
    ) -> _BackendPgResultShim | None:
        """Execute an internal command through the backend-native session API."""
        self._check_closed()
        query, params = self._convert_query_params(command, None)
        if params is not None:
            raise e.ProgrammingError("internal commands cannot have parameters")

        if result_format == pq.Format.TEXT:
            result_cursor = self._session.execute_simple(query)
        elif result_format == pq.Format.BINARY:
            result_cursor = self._execute_extended_no_params(query)
        else:
            raise ValueError(f"bad result format: {result_format!r}")

        while result_cursor.nextset():
            pass
        result = result_cursor.current_result
        if result is None:
            self._refresh_client_encoding(query)
            return None
        self._refresh_client_encoding(query)
        return _BackendPgResultShim(
            result,
            self._pgconn._encoding,
            result_format,
            result_cursor.statusmessage,
        )

    def cancel(self) -> None:
        if self._closed:
            return
        if self._tpc and self._tpc[1]:
            raise e.ProgrammingError(
                "cancel() cannot be used during a prepared two-phase transaction"
            )
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

    def tpc_begin(self, xid: Xid | str) -> None:
        self._check_closed()
        if self._tpc:
            raise e.ProgrammingError(
                "can't start two-phase transaction: transaction already active"
            )
        if self._in_transaction:
            raise e.ProgrammingError(
                "can't start two-phase transaction: connection in status INTRANS"
            )
        if self._autocommit:
            raise e.ProgrammingError(
                "can't use two-phase transactions in autocommit mode"
            )
        if not isinstance(xid, Xid):
            xid = Xid.from_string(xid)
        self._tpc = (xid, False)
        self.begin()

    def tpc_prepare(self) -> None:
        self._check_closed()
        if not self._tpc:
            raise e.ProgrammingError(
                "'tpc_prepare()' must be called inside a two-phase transaction"
            )
        if self._tpc[1]:
            raise e.ProgrammingError(
                "'tpc_prepare()' cannot be used during a prepared two-phase transaction"
            )
        xid = self._tpc[0]
        try:
            self._exec_tpc_command("PREPARE TRANSACTION", xid)
        except e.ObjectNotInPrerequisiteState as ex:
            raise e.NotSupportedError(str(ex)) from None
        self._tpc = (xid, True)
        self._in_transaction = False
        self._transaction_failed = False

    def tpc_commit(self, xid: Xid | str | None = None) -> None:
        self._tpc_finish("COMMIT", xid)

    def tpc_rollback(self, xid: Xid | str | None = None) -> None:
        self._tpc_finish("ROLLBACK", xid)

    def tpc_recover(self) -> list[Xid]:
        self._check_closed()
        result = self._session.execute_params(Xid._get_recover_query(), [])
        cursor = self.cursor(row_factory=tuple_row)
        cursor._result = result
        rows = cursor.fetchall()
        return [
            Xid._from_record(
                cast(str, row[0]),
                cast(Any, row[1]),
                cast(str, row[2]),
                cast(str, row[3]),
            )
            for row in cast(list[Sequence[object]], rows)
        ]

    def _tpc_finish(self, action: str, xid: Xid | str | None) -> None:
        self._check_closed()
        fname = f"tpc_{action.lower()}()"
        if xid is None:
            if not self._tpc:
                raise e.ProgrammingError(
                    f"{fname} without xid must be called inside a two-phase transaction"
                )
            xid = self._tpc[0]
        else:
            if self._tpc:
                raise e.ProgrammingError(
                    f"{fname} with xid must be called outside a two-phase transaction"
                )
            if not isinstance(xid, Xid):
                xid = Xid.from_string(xid)

        if self._tpc and not self._tpc[1]:
            self._tpc = None
            if action == "COMMIT":
                self.commit()
            else:
                self.rollback()
            return

        self._exec_tpc_command(f"{action} PREPARED", xid)
        self._tpc = None
        self._in_transaction = False
        self._transaction_failed = False

    def _exec_tpc_command(self, command: str, xid: Xid) -> None:
        tx = _BackendTransformer(
            _AdaptContext(self, self.adapters, expose_connection=False)
        )
        tx._encoding = self._pgconn._encoding
        query = PostgresClientQuery(tx)
        query.convert(f"{command} %s", (str(xid),))
        self._exec_command(query.query.decode(tx.encoding))

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

    def add_notice_handler(self, callback: NoticeHandler) -> None:
        self._notice_handlers.append(callback)

    def remove_notice_handler(self, callback: NoticeHandler) -> None:
        self._notice_handlers.remove(callback)

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

    def _on_backend_error(self, ex: BaseException) -> None:
        del ex
        if self._in_transaction:
            self._transaction_failed = True

    def _dispatch_notices(self, notices: Sequence[dict[int, bytes | None]]) -> None:
        for info in notices:
            diag = e.Diagnostic(info, encoding=self._pgconn._encoding)
            for callback in self._notice_handlers:
                try:
                    callback(diag)
                except Exception as ex:
                    logger.exception(
                        "error processing notice callback '%s': %s", callback, ex
                    )

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
                result = self._execute_extended_no_params(query)
            else:
                result = self._session.execute_simple(query)
        elif isinstance(params, _BoundParams):
            result = self._execute_bound(query, params, prepare)
        else:
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
                    self._prepared_statusmessages[statement_id] = (
                        _statusmessage_for_query(query)
                    )
                result = self._session.execute_prepared(
                    statement_id,
                    params,
                    statusmessage=self._prepared_statusmessages.get(statement_id),
                )
            else:
                result = self._session.execute_params(query, params)

        self._refresh_client_encoding(query)
        return result

    def _refresh_client_encoding(self, query: str) -> None:
        normalized = query.lstrip().lower()
        changes_encoding = (
            "client_encoding" in normalized
            or normalized.startswith("set names")
            or normalized.startswith("reset all")
            or normalized.startswith("discard all")
        )
        transaction_boundary = normalized.startswith(("commit", "rollback"))
        if not changes_encoding and not (
            transaction_boundary and self._encoding_changed_in_transaction
        ):
            return

        row = self._session.execute_params(
            "select current_setting($1::text, true)::text as value",
            ["client_encoding"],
        ).fetchone()
        pg_encoding = row[0] if row else None
        if isinstance(pg_encoding, bytes):
            pg_encoding = pg_encoding.decode("ascii")
        if isinstance(pg_encoding, str):
            self._pgconn._encoding = pg2pyenc(pg_encoding.encode("ascii"))
            self._session.encoding = self._pgconn._encoding
        if changes_encoding and self._in_transaction:
            self._encoding_changed_in_transaction = True
        elif transaction_boundary and not normalized.startswith("rollback to"):
            self._encoding_changed_in_transaction = False

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

        tx = _BackendTransformer(
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
                    self._savepoint_name = "_pg3_1"
                self._conn._exec_command(
                    _savepoint_sql("SAVEPOINT", self._savepoint_name)
                )
            else:
                self._outer = True
                self._conn.begin()
                if self._savepoint_name is not None:
                    self._conn._exec_command(
                        _savepoint_sql("SAVEPOINT", self._savepoint_name)
                    )
        else:
            if self._savepoint_name is None:
                self._savepoint_name = f"_pg3_{self._conn._tx_depth + 1}"
            self._conn._exec_command(_savepoint_sql("SAVEPOINT", self._savepoint_name))

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
                self._conn._exec_command(
                    _savepoint_sql("ROLLBACK TO", self._savepoint_name)
                )
                self._conn._exec_command(
                    _savepoint_sql("RELEASE", self._savepoint_name)
                )
        else:
            if self._outer:
                self._conn.commit()
            else:
                assert self._savepoint_name is not None
                self._conn._exec_command(
                    _savepoint_sql("RELEASE", self._savepoint_name)
                )

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
    elif isinstance(query, Template):
        query = query.strings[0]
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
    server_cursor_factory: type[object] | None = None,
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
        server_cursor_factory=server_cursor_factory,
        prepare_threshold=prepare_threshold,
        autocommit=autocommit,
    )
    conn.pgconn._encoding = conninfo_encoding(conninfo)
    session.encoding = conn.pgconn._encoding
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
    server_cursor_factory: type[object] | None = None,
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
        server_cursor_factory=server_cursor_factory,
        prepare_threshold=prepare_threshold,
        autocommit=autocommit,
    )
    conn.pgconn._encoding = conn.info.encoding
    session.encoding = conn.pgconn._encoding
    if isolation_level is not None:
        conn.set_isolation_level(isolation_level)
    if read_only is not None:
        conn.set_read_only(read_only)
    if deferrable is not None:
        conn.set_deferrable(deferrable)
    return conn
