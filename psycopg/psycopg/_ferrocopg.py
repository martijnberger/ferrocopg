"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import warnings
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import timedelta, tzinfo
from enum import Enum
from time import monotonic
from types import SimpleNamespace
from typing import Any, NamedTuple, ParamSpec, Protocol, TypeVar, cast
from warnings import warn

from . import _rmodule, postgres, pq
from . import errors as e
from ._adapters_map import AdaptersMap
from ._compat import Template
from ._connection_base import NoticeHandler, Notify
from ._copy_base import (
    BinaryFormatter,
    TextFormatter,
    _format_row_binary,
    _format_row_text,
    _parse_row_binary,
    _parse_row_text,
)
from ._encodings import conninfo_encoding, pg2pyenc, py2pgenc
from ._enums import IsolationLevel, PyFormat
from ._oids import BYTEA_OID, INVALID_OID, TEXT_OID
from ._preparing import Prepare, PrepareManager
from ._py_transformer import Transformer as AdaptTransformer
from ._queries import PostgresClientQuery, PostgresQuery, PostgresRawQuery
from ._rmodule import __version__ as __version__
from ._rmodule import _ferrocopg
from ._tpc import Xid
from ._tz import get_tzinfo
from .abc import AdaptContext, Buffer, Loader, Params, Query
from .adapt import Dumper, RecursiveDumper, RecursiveLoader
from .conninfo import _param_escape, conninfo_attempts, conninfo_to_dict, make_conninfo
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
    is_enum: bool
    type_modifier: int
    type_size: int


class _ResultSetLike(Protocol):
    columns: list[str]
    column_descriptions: list[_StatementColumnLike]
    rows: list[list[bytes | str | None]]
    rows_affected: int
    is_tuples: bool
    wire_format: pq.Format | int | None


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
        status: ExecStatus | None = None,
    ):
        self.columns = columns or []
        self.column_descriptions = column_descriptions or []
        self.rows = rows or []
        self.rows_affected = rows_affected
        self.statusmessage = statusmessage
        self.is_tuples = is_tuples
        self.wire_format: pq.Format | int | None = wire_format
        self.status = status


class _BoundParams(NamedTuple):
    values: list[tuple[int, bool, bytes | None]]
    types: tuple[int, ...]
    transcode: tuple[bool, ...]


class _BackendPreparedQuery(NamedTuple):
    query: bytes
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

    @property
    def backend_pid(self) -> int:
        if self._conn is None:
            return 0
        return self._conn.info.backend_pid

    @property
    def db(self) -> bytes:
        return self._info_bytes("dbname")

    @property
    def host(self) -> bytes:
        return self._info_bytes("host")

    @property
    def hostaddr(self) -> bytes:
        return self._info_bytes("hostaddr")

    @property
    def user(self) -> bytes:
        return self._info_bytes("user")

    @property
    def password(self) -> bytes:
        return self._info_bytes("password")

    @property
    def options(self) -> bytes:
        return self._info_bytes("options")

    @property
    def port(self) -> bytes:
        if self._conn is None:
            return b""
        return str(self._conn.info.port).encode("ascii")

    @property
    def server_version(self) -> int:
        if self._conn is None:
            return 0
        return self._conn.info.server_version

    @property
    def protocol_version(self) -> int:
        if self._conn is None or self._conn.closed:
            raise e.OperationalError("the connection is closed")
        return 3

    @property
    def full_protocol_version(self) -> int:
        if self._conn is None or self._conn.closed:
            raise e.OperationalError("the connection is closed")
        return 30000

    @property
    def error_message(self) -> bytes:
        if self._conn is None:
            return b"NULL"
        return self._conn._last_error_message.encode(self._encoding, "replace")

    @property
    def transaction_status(self) -> pq.TransactionStatus:
        if self._conn is None or self._conn.closed:
            return pq.TransactionStatus.UNKNOWN
        return self._conn.info.transaction_status

    @property
    def pipeline_status(self) -> pq.PipelineStatus:
        if self._conn is None:
            return pq.PipelineStatus.OFF
        return self._conn.info.pipeline_status

    def parameter_status(self, param_name: bytes) -> bytes | None:
        if self._conn is None:
            return None
        value = self._conn.info.parameter_status(param_name.decode(self._encoding))
        return None if value is None else value.encode(self._encoding)

    def exec_(self, query: bytes) -> _BackendPgResultShim | None:
        if self._conn is None:
            raise e.OperationalError("connection is closed")
        return self._conn._exec_command(query)

    def _info_bytes(self, name: str) -> bytes:
        if self._conn is None:
            raise e.OperationalError("the connection is closed")
        return str(getattr(self._conn.info, name)).encode(self._encoding)


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
        if isinstance(dumper, RecursiveDumper):
            dumper._tx = self
        if hasattr(dumper, "_encoding"):
            dumper._encoding = self._dumper_encoding
        return dumper

    def get_dumper_by_oid(self, oid: int, format: pq.Format) -> Any:
        dumper = super().get_dumper_by_oid(oid, format)
        if isinstance(dumper, RecursiveDumper):
            dumper._tx = self
        if hasattr(dumper, "_encoding"):
            dumper._encoding = self._dumper_encoding
        return dumper

    def get_loader(self, oid: int, format: pq.Format) -> Any:
        loader = super().get_loader(oid, format)
        if isinstance(loader, RecursiveLoader):
            loader._tx = self
        if hasattr(loader, "_encoding"):
            loader._encoding = self._loader_encoding
        return loader

    def as_literal(self, obj: Any) -> bytes:
        dumper = self.get_dumper(obj, PyFormat.TEXT)
        if type(dumper).quote is Dumper.quote:
            value = dumper.dump(obj)
            rv = b"NULL" if value is None else _quote_backend_literal(bytes(value))
        else:
            rv = bytes(dumper.quote(obj))

        oid = dumper.oid
        if oid and rv.endswith(b"'") and oid != TEXT_OID:
            try:
                type_sql = self._oid_types[oid]
            except KeyError:
                if ti := self.adapters.types.get(oid):
                    type_sql = (
                        ti.name.encode(self.encoding)
                        if oid < 8192
                        else ti.regtype.encode(self.encoding)
                    )
                    if oid == ti.array_oid:
                        type_sql += b"[]"
                else:
                    type_sql = b""
                self._oid_types[oid] = type_sql
            if type_sql:
                rv = b"%s::%s" % (rv, type_sql)

        return rv

    @property
    def _dumper_encoding(self) -> str:
        # SQL_ASCII accepts arbitrary UTF-8 input but returns undecoded bytes.
        return "utf-8" if self.encoding == "ascii" else self.encoding

    @property
    def _loader_encoding(self) -> str:
        return "" if self.encoding == "ascii" else self.encoding

    def _fork(self) -> _BackendTransformer:
        tx = type(self)(self)
        tx._encoding = self.encoding
        return tx


class _WireByteaDumper(Dumper):
    """Encode bytea text parameters without relying on a libpq PGconn."""

    oid = BYTEA_OID

    def dump(self, obj: Buffer) -> Buffer:
        obj = getattr(obj, "obj", obj)
        return b"\\x" + bytes(obj).hex().encode()


def _install_wire_bytea_dumper(adapters: AdaptersMap) -> None:
    """Replace only Psycopg's default `%t` bytea dumper for this backend."""
    from .dbapi20 import Binary

    default_dumper = AdaptersMap._optimised.get(BytesDumper, BytesDumper)
    classes = [
        cls
        for cls in (bytes, bytearray, memoryview, Binary)
        if (
            (dumper := adapters.get_dumper(cls, PyFormat.TEXT)) is default_dumper
            or dumper.__module__ == "psycopg.dbapi20"
        )
    ]
    if not classes:
        return

    # `register_dumper()` also changes `%s`; only `%t` needs this wire form.
    if not adapters._own_dumpers[PyFormat.TEXT]:
        adapters._dumpers[PyFormat.TEXT] = adapters._dumpers[PyFormat.TEXT].copy()
        adapters._own_dumpers[PyFormat.TEXT] = True
    for cls in classes:
        adapters._dumpers[PyFormat.TEXT][cls] = _WireByteaDumper

    oid_dumper = adapters.get_dumper_by_oid(BYTEA_OID, pq.Format.TEXT)
    if oid_dumper.__module__ in {"psycopg.dbapi20", "psycopg.types.string"}:
        adapters.register_dumper(None, _WireByteaDumper)


def _backend_copy_impl() -> tuple[Callable[..., Any], ...]:
    if _ferrocopg and hasattr(_ferrocopg, "format_row_text"):
        return (
            _ferrocopg.format_row_text,
            _ferrocopg.format_row_binary,
            _ferrocopg.parse_row_text,
            _ferrocopg.parse_row_binary,
        )
    return (
        _format_row_text,
        _format_row_binary,
        _parse_row_text,
        _parse_row_binary,
    )


def _pure_python_adapters(
    template: AdaptersMap, *, text_loader_oids: frozenset[int] = frozenset()
) -> AdaptersMap:
    """Copy an adapter map without the libpq/C-only replacements."""
    from .types.array import ArrayBinaryLoader

    adapters = AdaptersMap(template)
    originals = {
        optimized: original
        for original, optimized in AdaptersMap._optimised.items()
        if original is not optimized
    }

    for py_format in PyFormat:
        adapters._dumpers[py_format] = {
            key: originals.get(dumper, dumper)
            for key, dumper in adapters._dumpers[py_format].items()
        }
        adapters._own_dumpers[py_format] = True

    for pg_format in (pq.Format.TEXT, pq.Format.BINARY):
        adapters._dumpers_by_oid[pg_format] = {
            oid: originals.get(dumper, dumper)
            for oid, dumper in adapters._dumpers_by_oid[pg_format].items()
        }
        adapters._own_dumpers_by_oid[pg_format] = True

    for pg_format in (pq.Format.TEXT, pq.Format.BINARY):
        adapters._loaders[pg_format] = {
            oid: _pure_loader_class(loader, originals, ArrayBinaryLoader)
            for oid, loader in adapters._loaders[pg_format].items()
        }
        adapters._own_loaders[pg_format] = True

    if text_loader_oids:
        adapters._loaders[pq.Format.BINARY] = adapters._loaders[pq.Format.BINARY].copy()
        for oid in text_loader_oids:
            if loader := adapters._loaders[pq.Format.TEXT].get(oid):
                adapters._loaders[pq.Format.BINARY][oid] = loader
        adapters._own_loaders[pq.Format.BINARY] = True

    return adapters


_pure_array_loader_classes: dict[type[Any], type[Any]] = {}


def _pure_array_loader_class(loader: type[Any]) -> type[Any]:
    from .types.array import ArrayLoader

    if pure := _pure_array_loader_classes.get(loader):
        return pure
    attrs = {
        "__module__": loader.__module__,
        "base_oid": loader.base_oid,
        "delimiter": loader.delimiter,
    }
    pure = type(loader.__name__, (ArrayLoader,), attrs)
    _pure_array_loader_classes[loader] = pure
    return pure


def _pure_loader_class(
    loader: type[Any],
    originals: dict[type[Any], type[Any]],
    array_binary_loader: type[Any],
) -> type[Any]:
    if loader.__module__ == "psycopg_c._psycopg":
        if loader.__name__ == "ArrayBinaryLoader":
            return array_binary_loader
        return originals.get(loader, loader)
    if any(
        base.__module__ == "psycopg_c._psycopg" and base.__name__ == "ArrayLoader"
        for base in loader.__mro__
    ):
        return _pure_array_loader_class(loader)
    return originals.get(loader, loader)


class BackendColumn(Sequence[Any]):
    _attrs = (
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    )

    def __init__(
        self,
        name: str,
        type_code: int | None = None,
        type_modifier: int = -1,
        type_size: int = -1,
        adapters: AdaptersMap | None = None,
    ):
        self.name = name
        self.type_code = type_code
        self._type_modifier = type_modifier
        self._type_size = type_size
        self._type = (adapters or postgres.adapters).types.get(type_code or 0)

    def __len__(self) -> int:
        return len(self._attrs)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return tuple(getattr(self, name) for name in self._attrs[index])
        return getattr(self, self._attrs[index])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BackendColumn):
            return NotImplemented
        return tuple(self) == tuple(other)

    def __repr__(self) -> str:
        return (
            f"<Column {self.name!r}, type: {self.type_display} (oid: {self.type_code})>"
        )

    @property
    def type_display(self) -> str:
        if self._type is None:
            return str(self.type_code)
        return self._type.get_type_display(oid=self.type_code, fmod=self._type_modifier)

    @property
    def display_size(self) -> int | None:
        if self._type is None:
            return None
        return self._type.get_display_size(self._type_modifier)

    @property
    def internal_size(self) -> int | None:
        return self._type_size if self._type_size >= 0 else None

    @property
    def precision(self) -> int | None:
        if self._type is None:
            return None
        return self._type.get_precision(self._type_modifier)

    @property
    def scale(self) -> int | None:
        if self._type is None:
            return None
        return self._type.get_scale(self._type_modifier)

    @property
    def null_ok(self) -> None:
        return None


LegacyRowFactory = Callable[[list[str], list[str | None]], object]
RowFactory = Callable[..., object]
RowMaker = Callable[[Sequence[object]], object]
NotifyHandler = Callable[[Notify], None]
_NO_ROW = object()
_TEXT_WIRE_OIDS = frozenset({INVALID_OID, 18, 19, 25, 1042, 1043, 114, 142})


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
        params = cls._get_connection_params(conninfo, **kwargs)
        attempts = conninfo_attempts(params)
        has_target = "target_session_attrs" in params
        target = str(params.get("target_session_attrs") or "any")
        passes = ("standby", "any") if target == "prefer-standby" else (target,)
        errors: list[tuple[e.Error, str]] = []
        connect_options: dict[str, Any] = {
            "autocommit": autocommit,
            "prepare_threshold": prepare_threshold,
        }
        optional_options = {
            "context": context,
            "row_factory": row_factory,
            "cursor_factory": cursor_factory,
            "server_cursor_factory": server_cursor_factory,
            "isolation_level": isolation_level,
            "read_only": read_only,
            "deferrable": deferrable,
        }
        connect_options.update(
            (name, value)
            for name, value in optional_options.items()
            if value is not None
        )

        for pass_target in passes:
            for attempt in attempts:
                attempt = dict(attempt)
                if has_target or pass_target != "any":
                    attempt["target_session_attrs"] = pass_target
                else:
                    attempt.pop("target_session_attrs", None)
                description = "host: %r, port: %r, hostaddr: %r" % (
                    attempt.get("host"),
                    attempt.get("port"),
                    attempt.get("hostaddr"),
                )
                try:
                    return cls._connect_gen(
                        make_conninfo("", **attempt), **connect_options
                    )
                except e.Error as ex:
                    errors.append((ex, description))

        if not errors:
            raise e.OperationalError("no connection attempts available")

        last_error = errors[-1][0]
        lines = [str(last_error)]
        if len(errors) > 1:
            lines.append("Multiple connection attempts failed. All failures were:")
            lines.extend(f"- {description}: {error}" for error, description in errors)
        elif errors[0][1] not in lines[0]:
            lines.append(errors[0][1])
        raise type(last_error)("\n".join(lines), pgconn=last_error.pgconn) from None

    @classmethod
    def _connect_gen(cls, conninfo: str, **kwargs: Any) -> object:
        from . import connect_ferrocopg

        return connect_ferrocopg(conninfo, **kwargs)

    @classmethod
    def _get_connection_params(
        cls, conninfo: str, **kwargs: str | int | None
    ) -> dict[str, str | int | None]:
        """Manipulate connection parameters before connecting."""
        return conninfo_to_dict(conninfo, **kwargs)


class BackendConnectionInfo:
    __module__ = "psycopg"

    def __init__(self, conn: NoTlsConnectionAdapter):
        self._conn = conn

    @property
    def vendor(self) -> str:
        return "PostgreSQL"

    @property
    def dbname(self) -> str:
        self._ensure_open()
        return self._conn._probe().current_database

    @property
    def user(self) -> str:
        self._ensure_open()
        return self._conn._probe().current_user

    @property
    def password(self) -> str:
        self._ensure_open()
        return str(conninfo_to_dict(self._conn._conninfo).get("password") or "")

    @property
    def options(self) -> str:
        self._ensure_open()
        return str(conninfo_to_dict(self._conn._conninfo).get("options") or "")

    @property
    def application_name(self) -> str:
        self._ensure_open()
        return self._conn._probe().application_name

    @property
    def server_version(self) -> int:
        self._ensure_open()
        return self._conn._probe().server_version_num

    @property
    def backend_pid(self) -> int:
        self._ensure_open()
        return self._conn._probe().backend_pid

    @property
    def host(self) -> str:
        self._ensure_open()
        params = conninfo_to_dict(self._conn._conninfo)
        return str(params.get("host") or self.hostaddr)

    @property
    def hostaddr(self) -> str:
        self._ensure_open()
        return self._conn._probe().server_address or ""

    @property
    def port(self) -> int:
        self._ensure_open()
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
        self._ensure_open()
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
        self._ensure_open()
        return 30000

    @property
    def error_message(self) -> str:
        return self._conn._last_error_message

    @property
    def timezone(self) -> tzinfo:
        self._ensure_open()
        return get_tzinfo(cast(Any, self._conn.pgconn))

    @property
    def transaction_status(self) -> pq.TransactionStatus:
        if self._conn.closed:
            return pq.TransactionStatus.UNKNOWN
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

    def _ensure_open(self) -> None:
        if self._conn.closed:
            raise e.OperationalError("the connection is closed")


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
        status = getattr(result, "status", None)
        self.status = (
            status
            if status is not None
            else (
                ExecStatus.TUPLES_OK
                if getattr(result, "is_tuples", bool(result.columns or result.rows))
                else ExecStatus.COMMAND_OK
            )
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
    return statements


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

    def run_text_params_format(
        self, query: str, params: list[str | None], binary: bool
    ) -> _ResultSetLike: ...

    def run_params(
        self, query: str, params: list[tuple[int, bool, bytes | None]]
    ) -> _ResultSetLike: ...

    def run_params_format(
        self,
        query: str,
        params: list[tuple[int, bool, bytes | None]],
        binary: bool,
    ) -> _ResultSetLike: ...

    def run_prepared_text_params(
        self, statement_id: int, params: list[str | None]
    ) -> _ResultSetLike: ...

    def run_prepared_text_params_format(
        self, statement_id: int, params: list[str | None], binary: bool
    ) -> _ResultSetLike: ...

    def run_prepared_params(
        self, statement_id: int, params: list[tuple[int, bool, bytes | None]]
    ) -> _ResultSetLike: ...

    def run_prepared_params_format(
        self,
        statement_id: int,
        params: list[tuple[int, bool, bytes | None]],
        binary: bool,
    ) -> _ResultSetLike: ...

    def close_prepared(self, statement_id: int) -> None: ...


def is_available() -> bool:
    """Return `True` if the bootstrap ferrocopg Rust extension is importable."""
    return _ferrocopg is not None


def require_available() -> None:
    """Raise an actionable error if the Rust extension cannot be imported."""
    if is_available():
        return

    message = (
        "the ferrocopg Rust backend is unavailable; install a ferrocopg wheel "
        "for this platform, or build the source extension with "
        "`uv run maturin develop --manifest-path "
        "crates/ferrocopg-python/Cargo.toml`"
    )
    raise ImportError(message) from _rmodule._import_error


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


def merge_conninfo(
    conninfo: str,
    params: Mapping[str, str | int | None],
    *,
    use_environment: bool = False,
) -> str:
    """Merge connection parameters without validating them through libpq."""
    overrides: dict[str, str | int] = {}
    for key, value in params.items():
        if value is not None:
            overrides[key] = value
    application_name = os.environ.get("PGAPPNAME") if use_environment else None
    if not overrides and not application_name:
        return str(conninfo)

    merged: dict[str, str | int] = {}
    if conninfo:
        for key, value in conninfo_to_dict(conninfo).items():
            if value is not None:
                merged[key] = value
    merged.update(overrides)
    if application_name and "application_name" not in merged:
        merged["application_name"] = application_name
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
        encodings: Sequence[str | None] | None = None,
    ):
        self._results = list(results)
        if statusmessages is None:
            self._statusmessages = [
                getattr(result, "statusmessage", None) for result in self._results
            ]
        else:
            self._statusmessages = list(statusmessages)
        self._encodings: list[str | None] = (
            list(encodings) if encodings is not None else [None] * len(self._results)
        )
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

    @property
    def encoding(self) -> str | None:
        if self._index < 0 or self._index >= len(self._encodings):
            return None
        return self._encodings[self._index]

    def set_encoding(self, encoding: str | None) -> None:
        self._encodings = [encoding for _ in self._results]

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
        statements = _split_extended_statements(query)
        if not statements:
            return BackendResultCursor(
                [_SyntheticResult(status=ExecStatus.EMPTY_QUERY)], [None]
            )
        statuses = [
            _statusmessage_for_query(
                statements[index] if index < len(statements) else query, result
            )
            for index, result in enumerate(results)
        ]
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
        self,
        query: str,
        params: list[str | None],
        result_format: pq.Format = pq.Format.BINARY,
    ) -> BackendResultCursor:
        method = getattr(self._session, "run_text_params_format", None)
        result = self._call(
            method or self._session.run_text_params,
            query,
            params,
            *(() if method is None else (result_format == pq.Format.BINARY,)),
        )
        return BackendResultCursor([result], [_statusmessage_for_query(query, result)])

    def execute_bound(
        self,
        query: str,
        params: _BoundParams,
        result_format: pq.Format = pq.Format.BINARY,
    ) -> BackendResultCursor:
        method = getattr(self._session, "run_params_format", None)
        fallback = getattr(self._session, "run_params", None)
        if method is None and fallback is None:
            values = [
                None if value is None else value.decode(self.encoding)
                for _oid, _binary, value in params.values
            ]
            return self.execute_params(query, values, result_format)
        bound_method = cast(Callable[..., Any], method or fallback)
        result = self._call(
            bound_method,
            query,
            params.values,
            *(() if method is None else (result_format == pq.Format.BINARY,)),
        )
        return BackendResultCursor([result], [_statusmessage_for_query(query, result)])

    def execute_prepared(
        self,
        statement_id: int,
        params: list[str | None],
        *,
        statusmessage: str | None = None,
        result_format: pq.Format = pq.Format.BINARY,
    ) -> BackendResultCursor:
        method = getattr(self._session, "run_prepared_text_params_format", None)
        result = self._call(
            method or self._session.run_prepared_text_params,
            statement_id,
            params,
            *(() if method is None else (result_format == pq.Format.BINARY,)),
        )
        return BackendResultCursor([result], [statusmessage])

    def prepare_bound(self, query: str, params: _BoundParams) -> _PreparedStatementLike:
        if method := getattr(self._session, "prepare_params", None):
            return cast(
                _PreparedStatementLike,
                self._call(method, query, list(params.types)),
            )
        return self._call(self._session.prepare_text, query)

    def execute_prepared_bound(
        self,
        statement_id: int,
        params: _BoundParams,
        *,
        statusmessage: str | None = None,
        result_format: pq.Format = pq.Format.BINARY,
    ) -> BackendResultCursor:
        method = getattr(self._session, "run_prepared_params_format", None)
        fallback = getattr(self._session, "run_prepared_params", None)
        if method is None and fallback is None:
            values = [
                None if value is None else value.decode(self.encoding)
                for _oid, _binary, value in params.values
            ]
            return self.execute_prepared(
                statement_id,
                values,
                statusmessage=statusmessage,
                result_format=result_format,
            )
        prepared_method = cast(Callable[..., Any], method or fallback)
        result = self._call(
            prepared_method,
            statement_id,
            params.values,
            *(() if method is None else (result_format == pq.Format.BINARY,)),
        )
        return BackendResultCursor([result], [statusmessage])

    def close_prepared(self, statement_id: int) -> None:
        if method := getattr(self._session, "close_prepared", None):
            self._call(method, statement_id)

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
                if ex.sqlstate and (
                    ex.sqlstate.startswith("08")
                    or ex.sqlstate in {"57P01", "57P02", "57P03"}
                ):
                    self._session.close()
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
                "run_text_params_format",
                "run_params",
                "run_params_format",
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
        if self.closed:
            return
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
        query_cls: type[PostgresQuery] = PostgresQuery,
    ):
        self._conn = conn
        self._result: BackendResultCursor | None = None
        self._closed = False
        self._row_factory = row_factory
        self._adapters = AdaptersMap(conn.adapters)
        self._adapters._register_loader_callback = self._loaders_changed
        self._make_row: RowMaker | None = None
        self._result_transformer: AdaptTransformer | None = None
        self._stream_result: _ResultSetLike | None = None
        self._rowcount_override: int | None = None
        self._statusmessage_override: str | None = None
        self._rownumber: int | None = 0
        self._query: PostgresQuery | PostgresClientQuery | None = None
        self._query_cls = query_cls
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
        if self._stream_result is not None:
            return _BackendPgResultShim(
                self._stream_result, self._encoding, self.format
            )
        result = self._result
        if result is None:
            return None
        current = result.current_result
        if current is None:
            return None
        return _BackendPgResultShim(
            current,
            result.encoding or self._encoding,
            self.format,
            result.statusmessage,
        )

    @property
    def pgresults(self) -> list[_BackendPgResultShim]:
        result = self._result
        if result is None:
            return []
        return [
            _BackendPgResultShim(
                item,
                result._encodings[index] or self._encoding,
                self.format,
                result._statusmessages[index],
            )
            for index, item in enumerate(result._results)
        ]

    @property
    def rowcount(self) -> int:
        if self._rowcount_override is not None:
            return self._rowcount_override
        if self._result is None:
            return -1
        current = self._result.current_result
        if current is None:
            return -1
        return _result_rowcount(current, self._result.statusmessage)

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
                BackendColumn(
                    column.name,
                    column.oid,
                    getattr(column, "type_modifier", -1),
                    getattr(column, "type_size", -1),
                    self.adapters,
                )
                for column in cast(list[_StatementColumnLike], descriptions)
            ]
        return [BackendColumn(name) for name in current.columns]

    @property
    def statusmessage(self) -> str | None:
        if self._result is None:
            return self._statusmessage_override
        return self._result.statusmessage

    def close(self) -> None:
        self._closed = True
        self._result = None
        self._result_transformer = None
        self._stream_result = None

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

    def mogrify(self, query: Query, params: Params | None = None) -> str:
        converted, _ = self._conn._convert_query_params(
            query,
            params,
            adapters=self.adapters,
            query_cls=self._query_cls,
        )
        return converted

    def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> NoTlsCursorAdapter:
        self._check_closed()
        self._conn._check_closed()
        if binary is not None:
            self.format = pq.Format.BINARY if binary else pq.Format.TEXT
        if self._query_cls is PostgresClientQuery and self.format == pq.Format.BINARY:
            raise e.NotSupportedError(
                "client-side cursors don't support binary results"
            )
        self._reset_result()
        with self._conn.lock:
            try:
                self._result = self._conn._execute(
                    query,
                    params,
                    prepare=prepare,
                    prefer_extended=self._row_factory not in _LEGACY_ROW_FACTORIES,
                    adapters=self.adapters,
                    result_format=self.format,
                    cursor_state=self,
                )
            except e.Error as ex:
                translated = self._conn._translate_session_error(ex)
                if translated is not ex:
                    raise translated from None
                raise
        if self._row_factory not in _LEGACY_ROW_FACTORIES:
            self._make_row_for_result(self._result)
        return self

    def executemany(
        self,
        query: Query,
        params_seq: Sequence[Params],
        *,
        returning: bool = False,
        prepare: bool | None = None,
    ) -> None:
        self._check_closed()
        self._conn._check_closed()
        self._reset_result()
        results: list[_ResultSetLike] = []
        statuses: list[str | None] = []
        encodings: list[str | None] = []
        total = 0
        try:
            with self._conn.lock:
                for params in params_seq:
                    cursor = self._conn._execute(
                        query,
                        params,
                        prepare=prepare,
                        prefer_extended=self._row_factory not in _LEGACY_ROW_FACTORIES,
                        adapters=self.adapters,
                        result_format=self.format,
                        cursor_state=self,
                    )
                    result = cursor.current_result
                    if result is None:
                        continue
                    if returning:
                        results.append(result)
                        statuses.append(cursor.statusmessage)
                        encodings.append(cursor.encoding)
                    else:
                        total += max(_result_rowcount(result, cursor.statusmessage), 0)
                        statuses.append(cursor.statusmessage)
        except e.Error as ex:
            translated = self._conn._translate_session_error(ex)
            if translated is not ex:
                raise translated from None
            raise

        if returning:
            self._result = BackendResultCursor(results, statuses, encodings)
            if not results:
                self._rowcount_override = 0
        else:
            self._rowcount_override = total
            if statuses:
                aggregate = _SyntheticResult(rows_affected=total)
                self._statusmessage_override = _statusmessage_for_query(
                    query, aggregate
                )

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
        if size > 1:
            from ._capabilities import capabilities

            capabilities.has_stream_chunked(check=True)
        self.execute(query, params, binary=binary)
        result = self._require_result()
        self._check_result_for_fetch(result)
        current = result.current_result
        assert current is not None
        try:
            while True:
                self._conn._check_closed()
                if (row := result.fetchone()) is None:
                    break
                self._rownumber = (self._rownumber or 0) + 1
                self._stream_result = _SyntheticResult(
                    columns=current.columns,
                    column_descriptions=getattr(current, "column_descriptions", None),
                    rows=[list(row)],
                    rows_affected=1,
                    is_tuples=True,
                    wire_format=getattr(current, "wire_format", None),
                )
                yield self._make_row_for_result(result)(row)
        finally:
            self._stream_result = None

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
        if self._result is None:
            return None
        result = self._result
        rv = result.nextset()
        if rv:
            self._make_row = None
            self._result_transformer = None
            self._rownumber = 0
        return rv

    def set_result(self, index: int) -> NoTlsCursorAdapter:
        if self._result is None:
            self._check_closed()
            self._conn._check_closed()
            raise IndexError(f"index {index} out of range: 0 result(s) available")
        result = self._result
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
        if self._result is None or self._result.current_result is None:
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
            if (
                self._rowcount_override is not None
                and self._statusmessage_override is not None
            ):
                raise e.ProgrammingError("the last operation didn't produce a result")
            raise e.ProgrammingError("no result available")
        return self._result

    def _check_result_for_fetch(self, result: BackendResultCursor) -> None:
        current = result.current_result
        if current is None or not getattr(
            current, "is_tuples", bool(current.columns or current.rows)
        ):
            pgresult = self.pgresult
            if pgresult is not None and pgresult.command_status:
                detail = f" (command status: {pgresult.command_status.decode()})"
            elif pgresult is not None:
                detail = f" (result status: {ExecStatus(pgresult.status).name})"
            else:
                detail = ""
            raise e.ProgrammingError(
                f"the last operation didn't produce records{detail}"
            )

    def _reset_result(self) -> None:
        self._result = None
        self._make_row = None
        self._result_transformer = None
        self._stream_result = None
        self._rowcount_override = None
        self._statusmessage_override = None
        self._rownumber = 0

    def _loaders_changed(self, oid: int, loader: type[Loader]) -> None:
        del oid, loader
        self._result_transformer = None

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
        cursor_result = self._result
        assert cursor_result is not None
        descriptions = getattr(result, "column_descriptions", None)
        if not descriptions:
            return tuple(row)

        wire_format_value = getattr(result, "wire_format", None)
        wire_format = (
            None if wire_format_value is None else pq.Format(wire_format_value)
        )

        # Test doubles and simple-query results may already carry Python text;
        # live extended-query results report their wire format explicitly.
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
                    expose_connection=True,
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

        wire_encoding = cursor_result.encoding or self._encoding
        row = tuple(
            _transcode_result_value(
                value,
                column,
                wire_format,
                source_encoding=wire_encoding,
                target_encoding=self._encoding,
            )
            for value, column in zip(row, descriptions, strict=True)
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
        query_cls: type[PostgresQuery] = PostgresQuery,
    ):
        super().__init__(conn, row_factory=row_factory, query_cls=query_cls)
        self._name = name
        self._scrollable = scrollable
        self._withhold = withhold
        self._factory_name = factory_name
        self._declared = False
        self._descriptions: list[_StatementColumnLike] = []
        self._pos = 0
        self.itersize = 100
        self._iter_rows: Iterator[object] | None = None
        self._iter_exhausted = False

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
        prepare: bool | None = None,
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
            query, params, adapters=self.adapters, cursor_state=self
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
        self._iter_exhausted = False
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
        self._iter_exhausted = False

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
            rows = self._fetch_server_rows(self.itersize, advance=False)
            self._iter_rows = iter(rows)
            self._iter_exhausted = len(rows) < self.itersize
        try:
            row = next(self._iter_rows)
        except StopIteration:
            if self._iter_exhausted:
                self._iter_rows = None
                raise
            rows = self._fetch_server_rows(self.itersize, advance=False)
            if not rows:
                self._iter_rows = None
                raise
            self._iter_rows = iter(rows)
            self._iter_exhausted = len(rows) < self.itersize
            row = next(self._iter_rows)
        self._pos += 1
        return row

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

    def _fetch_server_rows(
        self, count: int | None, *, advance: bool = True
    ) -> list[object]:
        self._ensure_described()
        amount = "ALL" if count is None else str(count)
        fetch = f"FETCH FORWARD {amount} FROM {_quoted_identifier(self._name)}"
        pgresult = self._conn._exec_command(fetch, result_format=self.format)
        if pgresult is None:
            raise e.InternalError("server cursor FETCH returned no result")
        result = pgresult._result
        if self.format == pq.Format.TEXT:
            result = _SyntheticResult(
                columns=[column.name for column in self._descriptions],
                column_descriptions=self._descriptions,
                rows=result.rows,
                rows_affected=result.rows_affected,
                is_tuples=True,
                wire_format=pq.Format.TEXT,
            )
        result_cursor = BackendResultCursor(
            [result],
            [pgresult.command_status.decode(self._encoding) or None],
        )
        self._result = result_cursor
        self._rownumber = 0
        loaded_rows: list[object] = super().fetchall()
        if advance:
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
        copy_adapters = _pure_python_adapters(cursor.adapters)
        _install_wire_bytea_dumper(copy_adapters)
        query_tx = _BackendTransformer(
            _AdaptContext(
                self.connection,
                copy_adapters,
                expose_connection=False,
            )
        )
        query_tx._encoding = cursor._conn.info.encoding
        query = PostgresClientQuery(query_tx)
        query.convert(statement, params)
        cursor._query = query
        self._statement = query.query.decode(query_tx.encoding)
        tx = _BackendTransformer(
            _AdaptContext(
                self.connection,
                copy_adapters,
                expose_connection=True,
            )
        )
        tx._encoding = query_tx.encoding
        normalized = " ".join(self._statement.lower().split())
        self._binary = _copy_statement_requests_binary(normalized)
        if " from stdin" in normalized:
            self._direction = "in"
        elif " to stdout" in normalized:
            self._direction = "out"
        else:
            if normalized.startswith(
                ("select ", "insert ", "update ", "delete ", "create ", "drop ")
            ):
                raise e.ProgrammingError(
                    "copy() requires a COPY FROM STDIN or COPY TO STDOUT statement"
                )
            self._direction = "invalid"
        self._buffer = bytearray()
        self._read_blocks: list[bytes] = []
        self._read_pos = 0
        self._tx = tx
        format_text, format_binary, parse_text, parse_binary = _backend_copy_impl()
        self.formatter = (
            BinaryFormatter(
                tx,
                format_row=format_binary,
                parse_row=parse_binary,
            )
            if self._binary
            else TextFormatter(
                tx,
                encoding=tx.encoding,
                format_row=format_text,
                parse_row=parse_text,
            )
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
        self._out_fully_buffered = False
        self._read_error: e.DataError | None = None
        self._lock_acquired = False

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
        self.connection.lock.acquire()
        self._lock_acquired = True
        try:
            self.connection._ensure_transaction()
            self.connection._copy_active = True
            if self._direction == "invalid":
                self._cursor._conn._session.execute_simple(self._statement)
                raise e.ProgrammingError(
                    "copy() requires a COPY FROM STDIN or COPY TO STDOUT statement"
                )
            if self._direction == "out":
                try:
                    data = self._cursor._conn._session.copy_to_stdout(self._statement)
                except e.DataError as ex:
                    # libpq exposes server-side COPY conversion errors while
                    # consuming output rather than while entering the block.
                    self._read_error = ex
                    return self
                self._out_fully_buffered = len(data) <= 8192
                self._read_blocks = (
                    _split_binary_copy_blocks(data)
                    if self._binary
                    else data.splitlines(keepends=True)
                )
                self._rowcount = (
                    _binary_copy_row_count(data)
                    if self._binary
                    else len(self._read_blocks)
                )
                descriptions: Sequence[_StatementColumnLike] = ()
                if inner_query := _copy_inner_query(self._statement):
                    descriptions = [
                        SimpleNamespace(
                            name=(
                                f"column_{index}"
                                if column.name == "?column?"
                                else column.name
                            ),
                            oid=column.oid,
                            type_name=column.type_name,
                        )
                        for index, column in enumerate(
                            self._cursor._conn._session.describe_text(
                                inner_query
                            ).columns,
                            start=1,
                        )
                    ]
                self._set_cursor_result(self._rowcount, descriptions)
            return self
        except BaseException:
            self.connection._copy_active = False
            self._release_lock()
            raise

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if self._finished:
            return
        self._finished = True
        self.connection._copy_active = False
        try:
            if exc is None and self._direction == "in":
                if data := self.formatter.end():
                    self._write_data(data)
                if self._database_writer:
                    self._rowcount = self._cursor._conn._session.copy_from_stdin(
                        self._statement, bytes(self._buffer)
                    )
                    self._set_cursor_result(self._rowcount)
            elif exc is not None:
                interrupted = self._direction == "in" or not self._out_fully_buffered
                if interrupted:
                    self.connection._transaction_failed = (
                        self.connection._in_transaction
                    )
                    self._cursor._result = None

            if self._queued_writer and self.writer is not None:
                setattr(self.writer, "_worker", None)
            elif not self._database_writer and self.writer is not None:
                finish = getattr(self.writer, "finish", None)
                if finish is not None:
                    finish(exc)
        except BaseException:
            self.connection._transaction_failed = self.connection._in_transaction
            self._cursor._result = None
            raise
        finally:
            self._release_lock()

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
        if self._read_error is not None:
            self.connection._transaction_failed = self.connection._in_transaction
            self._cursor._result = None
            raise self._read_error
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

    def _release_lock(self) -> None:
        if self._lock_acquired:
            self.connection.lock.release()
            self._lock_acquired = False


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


def _backend_cursor_adapter(cursor: object) -> NoTlsCursorAdapter:
    hosted = getattr(cursor, "_ferrocopg_cursor", None)
    return cast(NoTlsCursorAdapter, hosted or cursor)


class NoTlsPipelineAdapter:
    """Experimental pipeline context over the ferrocopg connection adapter."""

    def __init__(self, conn: NoTlsConnectionAdapter):
        self._conn = conn
        self._queued: list[tuple[Query, object, Params | None, bool]] = []
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
    ) -> object:
        self._check_open()
        cur = self._conn.cursor(row_factory=row_factory)
        self._queued.append((query, _backend_cursor_adapter(cur), params, prepare))
        return cur

    def sync(self) -> None:
        self._check_open()
        self._conn._pipeline_sync_count += 1
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
                queued = _backend_cursor_adapter(queued_cur)
                result = _backend_cursor_adapter(result_cur)
                queued._result = result._result
                queued._rownumber = 0
        else:
            for query, queued_cur, params, prepare in self._queued:
                queued = _backend_cursor_adapter(queued_cur)
                queued._result = self._conn._execute(
                    query,
                    params,
                    prepare=prepare,
                    adapters=queued.adapters,
                    result_format=queued.format,
                    cursor_state=queued,
                )
                queued._rownumber = 0
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
        adapters: AdaptersMap | None = None,
        prepare_threshold: int | None = 5,
        autocommit: bool = True,
    ):
        self._session = session
        self.lock = threading.RLock()
        self._is_ferrocopg = True
        self._conninfo = conninfo
        self.row_factory = row_factory
        self.cursor_factory = cursor_factory
        self.server_cursor_factory = server_cursor_factory or NoTlsServerCursorAdapter
        self._prepared = PrepareManager()
        self._prepared.prepare_threshold = prepare_threshold
        self._autocommit = autocommit
        self._info = BackendConnectionInfo(self)
        self._probe_cache: _BackendProbeLike | None = None
        self._prepared_ids: dict[bytes, int] = {}
        self._prepared_statusmessages: dict[int, str | None] = {}
        self._in_transaction = False
        self._transaction_failed = False
        self._pipeline_depth = 0
        self._pipeline_sync_count = 0
        self._tx_depth = 0
        self._tpc: tuple[Xid, bool] | None = None
        self._closed = False
        self._broken = False
        self._warn_on_del = False
        if adapters is None:
            self._adapters = None
        else:
            self._adapters = AdaptersMap(adapters)
            _install_wire_bytea_dumper(self._adapters)
        self._pgconn = _PgconnEncodingShim("utf-8", self)
        self._session.encoding = self._pgconn._encoding
        self._isolation_level: IsolationLevel | None = None
        self._read_only: bool | None = None
        self._deferrable: bool | None = None
        self._notice_handlers: list[NoticeHandler] = []
        self._notify_handlers: list[NotifyHandler] = []
        self._encoding_changed_in_transaction = False
        self._copy_active = False
        self._last_error_message = ""
        self._unsupported_client_encoding: e.NotSupportedError | None = None
        self._idle_transaction_timeout_active = False
        self._session.notice_handler = self._dispatch_notices
        self._session.error_handler = self._on_backend_error
        self._cancel_handle: _CancelHandleLike | None = None
        self._ensure_cancel_handle()

    @property
    def closed(self) -> bool:
        return self._closed or self._session.closed

    def __del__(self, __warn: Any = warn) -> None:
        if (
            not getattr(self, "_warn_on_del", False)
            or self.closed
            or hasattr(self, "_pool")
        ):
            return
        __warn(
            f"FerrocopgConnection {object.__repr__(self)} was deleted while still open."
            " Please use 'with' or '.close()' to close the connection properly",
            ResourceWarning,
        )

    def __repr__(self) -> str:
        status = "BAD" if self.closed else self.info.transaction_status.name
        cls = f"{type(self).__module__}.{type(self).__qualname__}"
        return f"<{cls} [{status}] at 0x{id(self):x}>"

    __str__ = __repr__

    @property
    def broken(self) -> bool:
        return self._broken or (self.closed and not self._closed)

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
    def connection(self) -> NoTlsConnectionAdapter:
        return self

    @property
    def pgconn(self) -> _PgconnEncodingShim:
        return self._pgconn

    @pgconn.setter
    def pgconn(self, value: Any) -> None:
        self._pgconn = value

    def parameter_status(self, param_name: bytes) -> bytes | None:
        return self._pgconn.parameter_status(param_name)

    @property
    def prepare_threshold(self) -> int | None:
        return self._prepared.prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: int | None) -> None:
        self._prepared.prepare_threshold = value

    @property
    def prepared_max(self) -> int | None:
        value = self._prepared.prepared_max
        return value if value != sys.maxsize else None

    @prepared_max.setter
    def prepared_max(self, value: int | None) -> None:
        self._prepared.prepared_max = sys.maxsize if value is None else value

    def _quote_sql_identifier(self, value: str) -> bytes:
        return _quoted_identifier(value).encode(self._pgconn._encoding)

    def _quote_sql_literal(self, value: object, context: AdaptContext) -> bytes:
        tx = _BackendTransformer(
            _AdaptContext(
                self,
                _pure_python_adapters(context.adapters),
                expose_connection=True,
            )
        )
        tx._encoding = self._pgconn._encoding
        return tx.as_literal(value)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check_closed()
        self._check_transaction_context("set autocommit")
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
        if self._session.closed:
            self._broken = True
        self._session.close()
        self._prepared = PrepareManager()
        self._prepared_ids.clear()
        self._prepared_statusmessages.clear()
        self._cancel_handle = None
        self._closed = True
        self._last_error_message = "NULL"

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
            factory = cast(Any, self.server_cursor_factory)
            cur = cast(
                NoTlsCursorAdapter,
                factory(
                    self,
                    name,
                    row_factory=row_factory or self.row_factory,
                    scrollable=scrollable,
                    withhold=withhold,
                ),
            )
            if binary:
                cur.format = pq.Format.BINARY
                _backend_cursor_adapter(cur).format = pq.Format.BINARY
            return cur
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
        prepare: bool | None = None,
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
    ) -> list[object]:
        self._check_closed()
        self._ensure_transaction()
        cursors: list[object] = []
        if row_factory is None:
            row_factory = self.row_factory
        for result in self._session.execute_pipeline_simple(queries):
            cur = self.cursor(row_factory=row_factory)
            _backend_cursor_adapter(cur)._result = result
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
        self._check_transaction_context("commit")
        if self._tpc:
            raise e.ProgrammingError(
                "commit() cannot be used during a two-phase transaction"
            )
        if not self._in_transaction:
            return
        try:
            self._exec_command("COMMIT")
        except BaseException:
            # PostgreSQL ends a failed COMMIT by rolling the transaction back.
            self._in_transaction = False
            self._transaction_failed = False
            raise
        self._in_transaction = False
        self._transaction_failed = False

    def rollback(self) -> None:
        self._check_closed()
        self._check_transaction_context("rollback")
        if self._tpc:
            raise e.ProgrammingError(
                "rollback() cannot be used during a two-phase transaction"
            )
        if not self._in_transaction:
            return
        self._exec_command("ROLLBACK")
        self._clear_prepared()
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

        try:
            if result_format == pq.Format.TEXT:
                result_cursor = self._session.execute_simple(query)
            elif result_format == pq.Format.BINARY:
                result_cursor = self._execute_extended_no_params(query, result_format)
            else:
                raise ValueError(f"bad result format: {result_format!r}")
        except BaseException:
            self._update_transaction_state(query, failed=True)
            raise

        while result_cursor.nextset():
            pass
        if query.lstrip().lower().startswith("rollback"):
            self._clear_prepared()
        result = result_cursor.current_result
        if result is None:
            self._refresh_client_encoding(query)
            self._update_transaction_state(query)
            return None
        self._refresh_client_encoding(query)
        self._update_transaction_state(query)
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

    def _try_cancel(self, *, timeout: float = 5.0) -> None:
        try:
            self.cancel_safe(timeout=timeout)
        except Exception as ex:
            logger.warning("%s", ex)

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
        diag = getattr(ex, "diag", None)
        severity = getattr(diag, "severity", None)
        self._last_error_message = f"{severity}: {ex}" if severity else str(ex)
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
        try:
            if exc_type:
                if self._in_transaction:
                    try:
                        self.rollback()
                    except Exception as rollback_error:
                        logger.warning(
                            "error ignored in rollback of %s: %s",
                            self,
                            rollback_error,
                        )
            elif self._in_transaction:
                self.commit()
        finally:
            self.close()

    def _execute(
        self,
        query: Query,
        params: Params | None,
        *,
        prepare: bool | None,
        prefer_extended: bool = False,
        adapters: AdaptersMap | None = None,
        result_format: pq.Format = pq.Format.TEXT,
        cursor_state: NoTlsCursorAdapter | None = None,
    ) -> BackendResultCursor:
        self._ensure_cancel_handle()
        query, params = self._convert_query_params(
            query, params, adapters=adapters, cursor_state=cursor_state
        )
        statusmessage = _statusmessage_for_query(query)
        if statusmessage and statusmessage.split(maxsplit=1)[0] == "COPY":
            raise e.ProgrammingError(
                "COPY cannot be used with this method; use copy() instead"
            )
        if self._unsupported_client_encoding and not _changes_client_encoding(query):
            raise e.NotSupportedError(str(self._unsupported_client_encoding))
        self._ensure_transaction()
        client_encoding = self._pgconn._encoding
        bridge_encoding = client_encoding not in {
            "utf-8",
            "ascii",
        } and not _changes_client_encoding(query)
        bridge_encoding = bridge_encoding and (
            not query.isascii()
            or isinstance(params, _BoundParams)
            and any(params.transcode)
        )
        bridge_encoding = bridge_encoding and hasattr(
            self._session._session,
            "run_text_params_format",
        )
        if bridge_encoding:
            self._session.execute_params("SET client_encoding TO 'UTF8'", [])
            if isinstance(params, _BoundParams):
                params = _BoundParams(
                    values=[
                        (
                            oid,
                            binary,
                            value
                            if not transcode or value is None
                            else value.decode(client_encoding).encode("utf-8"),
                        )
                        for (oid, binary, value), transcode in zip(
                            params.values, params.transcode, strict=True
                        )
                    ],
                    types=params.types,
                    transcode=params.transcode,
                )

        execution_error: BaseException | None = None
        try:
            if params is None:
                statements = _split_extended_statements(query)
                supports_bound = hasattr(self._session._session, "run_params")
                if len(statements) != 1:
                    result = self._session.execute_simple(query)
                    if any(
                        statement.lstrip()
                        .lower()
                        .startswith(("drop ", "alter ", "rollback", "discard "))
                        for statement in statements
                    ):
                        self._clear_prepared()
                elif not supports_bound:
                    result = (
                        self._execute_extended_no_params(query, result_format)
                        if prefer_extended
                        else self._session.execute_simple(query)
                    )
                else:
                    result = self._execute_bound(
                        query,
                        _BoundParams([], (), ()),
                        prepare,
                        result_format,
                    )
            elif isinstance(params, _BoundParams):
                result = self._execute_bound(query, params, prepare, result_format)
            else:
                result = self._session.execute_params(query, params, result_format)
        except BaseException as ex:
            execution_error = ex
            raise
        finally:
            if bridge_encoding:
                pg_encoding = py2pgenc(client_encoding).decode("ascii")
                try:
                    self._session.execute_params(
                        f"SET client_encoding TO '{pg_encoding}'", []
                    )
                except e.Error:
                    if execution_error is None:
                        raise

        if bridge_encoding:
            result.set_encoding("utf-8")

        self._refresh_client_encoding(query)
        self._refresh_session_timeout(query)
        self._update_transaction_state(query)
        return result

    def _set_client_encoding(self, encoding: str) -> None:
        escaped = encoding.replace("'", "''")
        self._exec_command(f"SET client_encoding TO '{escaped}'")

    def _refresh_client_encoding(self, query: str) -> None:
        normalized = query.lstrip().lower()
        changes_encoding = _changes_client_encoding(query)
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
            try:
                encoding = pg2pyenc(pg_encoding.encode("ascii"))
            except e.NotSupportedError as ex:
                self._unsupported_client_encoding = ex
            else:
                self._unsupported_client_encoding = None
                self._pgconn._encoding = encoding
                self._session.encoding = encoding
        if changes_encoding and self._in_transaction:
            self._encoding_changed_in_transaction = True
        elif transaction_boundary and not normalized.startswith("rollback to"):
            self._encoding_changed_in_transaction = False

    def _refresh_session_timeout(self, query: str) -> None:
        if "idle_in_transaction_session_timeout" not in query.lower():
            return
        row = self._session.execute_params(
            "select current_setting('idle_in_transaction_session_timeout')::text",
            [],
        ).fetchone()
        value = str(row[0]).lower() if row else "0"
        self._idle_transaction_timeout_active = value not in {"0", "0ms"}

    def _translate_session_error(self, ex: e.Error) -> e.Error:
        if (
            self._idle_transaction_timeout_active
            and self._in_transaction
            and self._session.closed
        ):
            return e.IdleInTransactionSessionTimeout(str(ex))
        return ex

    def _execute_bound(
        self,
        query: str,
        params: _BoundParams,
        prepare: bool | None,
        result_format: pq.Format,
    ) -> BackendResultCursor:
        pgq = cast(
            PostgresQuery,
            _BackendPreparedQuery(query.encode(self._pgconn._encoding), params.types),
        )
        prep, name = self._prepared.get(pgq, prepare)
        statement_id: int | None = None
        if prep is Prepare.SHOULD:
            prepared = self._session.prepare_bound(query, params)
            statement_id = prepared.statement_id
            self._prepared_ids[name] = statement_id
            self._prepared_statusmessages[statement_id] = _statusmessage_for_query(
                query
            )
        elif prep is Prepare.YES:
            statement_id = self._prepared_ids[name]

        try:
            if statement_id is None:
                result = self._session.execute_bound(query, params, result_format)
            else:
                result = self._session.execute_prepared_bound(
                    statement_id,
                    params,
                    statusmessage=self._prepared_statusmessages.get(statement_id),
                    result_format=result_format,
                )
        except BaseException:
            if statement_id is not None and prep is Prepare.SHOULD:
                self._prepared_ids.pop(name, None)
                self._prepared_statusmessages.pop(statement_id, None)
                try:
                    self._session.close_prepared(statement_id)
                except e.Error:
                    pass
            raise

        key = self._prepared.maybe_add_to_cache(pgq, prep, name)
        if key is not None:
            pgresults = [
                _BackendPgResultShim(
                    item,
                    result._encodings[index] or self._pgconn._encoding,
                    result_format,
                    result._statusmessages[index],
                )
                for index, item in enumerate(result._results)
            ]
            self._prepared.validate(key, prep, name, cast(Any, pgresults))
        self._maintain_prepared()
        return result

    def _clear_prepared(self) -> None:
        self._prepared.clear()
        self._maintain_prepared()

    def _maintain_prepared(self) -> None:
        while self._prepared._to_flush:
            name = self._prepared._to_flush.popleft()
            names = list(self._prepared_ids) if name is None else [name]
            for current_name in names:
                statement_id = self._prepared_ids.pop(current_name, None)
                if statement_id is None:
                    continue
                self._prepared_statusmessages.pop(statement_id, None)
                try:
                    self._session.close_prepared(statement_id)
                except e.Error:
                    pass

    def _execute_extended_no_params(
        self, query: str, result_format: pq.Format
    ) -> BackendResultCursor:
        results: list[_ResultSetLike] = []
        statuses: list[str | None] = []
        statements = _split_extended_statements(query)
        if not statements:
            return BackendResultCursor(
                [_SyntheticResult(status=ExecStatus.EMPTY_QUERY)], [None]
            )
        for statement in statements:
            result_cursor = self._session.execute_params(statement, [], result_format)
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
        cursor_state: NoTlsCursorAdapter | None = None,
        query_cls: type[PostgresQuery] | None = None,
    ) -> tuple[str, list[str | None] | _BoundParams | None]:
        query_cls = query_cls or (
            cursor_state._query_cls if cursor_state is not None else PostgresQuery
        )
        if params is None:
            if isinstance(query, bytes):
                if cursor_state is not None:
                    cursor_state._query = cast(
                        Any, SimpleNamespace(query=query, params=None)
                    )
                return query.decode(self._pgconn._encoding), None
            if isinstance(query, str):
                if cursor_state is not None:
                    cursor_state._query = cast(
                        Any,
                        SimpleNamespace(
                            query=query.encode(self._pgconn._encoding), params=None
                        ),
                    )
                return query, None

        if query_cls is PostgresQuery and isinstance(query, str) and "%" not in query:
            native = _coerce_native_params(params)
            return query, self._bound_native_params(native)
        if (
            query_cls is PostgresQuery
            and isinstance(query, bytes)
            and b"%" not in query
        ):
            native = _coerce_native_params(params)
            return query.decode(self._pgconn._encoding), self._bound_native_params(
                native
            )

        tx = _BackendTransformer(
            _AdaptContext(
                self,
                _pure_python_adapters(adapters or self.adapters),
                expose_connection=True,
            )
        )
        tx._encoding = self._pgconn._encoding
        pgq = query_cls(tx)
        if cursor_state is not None:
            cursor_state._query = pgq
        pgq.convert(query, params)
        if query_cls is PostgresClientQuery:
            return pgq.query.decode(tx.encoding), None
        if pgq.params is None:
            return pgq.query.decode(tx.encoding), None

        assert pgq.formats is not None
        assert len(pgq.params) == len(pgq.types) == len(pgq.formats)
        transcode = _query_param_transcode_flags(pgq, query, params, tx)
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
            transcode=transcode,
        )
        return pgq.query.decode(tx.encoding), bound

    def _bound_native_params(
        self, params: list[str | None] | None
    ) -> _BoundParams | None:
        if params is None:
            return None
        return _BoundParams(
            [
                (
                    0,
                    False,
                    None if value is None else value.encode(self._pgconn._encoding),
                )
                for value in params
            ],
            (0,) * len(params),
            (True,) * len(params),
        )

    def _ensure_transaction(self) -> None:
        if not self._autocommit and not self._in_transaction:
            self.begin()

    def _update_transaction_state(self, query: str, *, failed: bool = False) -> None:
        normalized = query.lstrip().lower()
        if normalized.startswith(("commit", "rollback")) and not normalized.startswith(
            "rollback to"
        ):
            self._in_transaction = False
            self._transaction_failed = False
        elif not failed and normalized.startswith(("begin", "start transaction")):
            self._in_transaction = True
            self._transaction_failed = False

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

    def _check_transaction_context(self, operation: str) -> None:
        if self._tx_depth:
            raise e.ProgrammingError(
                f"{operation} is forbidden: "
                "connection.transaction() context in progress"
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
    """Synchronous transaction context over the ferrocopg connection adapter."""

    class Status(str, Enum):
        NOT_STARTED = "not_started"
        ACTIVE = "active"
        COMMITTED = "committed"
        FAILED = "failed"
        ROLLED_BACK_EXPLICITLY = "rolled_back_explicitly"
        ROLLED_BACK_WITH_ERROR = "rolled_back_with_error"

    def __init__(
        self,
        conn: NoTlsConnectionAdapter,
        savepoint_name: str | None = None,
        force_rollback: bool = False,
    ):
        self._conn = conn
        self._savepoint_name = savepoint_name or ""
        self.force_rollback = force_rollback
        self._outer = False
        self._stack_index = -1
        self._pipeline_sync_count = conn._pipeline_sync_count
        self.status = self.Status.NOT_STARTED

    @property
    def connection(self) -> NoTlsConnectionAdapter:
        return self._conn

    @property
    def savepoint_name(self) -> str | None:
        return self._savepoint_name or None

    def __repr__(self) -> str:
        savepoint = f"{self.savepoint_name!r} " if self.savepoint_name else ""
        transaction_status = self._conn.info.transaction_status.name
        pipeline = ""
        if self._conn._pipeline_depth:
            pipeline = ", pipeline=ON"
            if (
                self._outer
                and self.status == self.Status.ACTIVE
                and self._conn._pipeline_sync_count == self._pipeline_sync_count
            ):
                transaction_status = "ACTIVE"
        return (
            f"<psycopg.Transaction {savepoint}({self.status.value}) "
            f"[{transaction_status}{pipeline}] at 0x{id(self):x}>"
        )

    def __enter__(self) -> NoTlsTransactionAdapter:
        if self.status != self.Status.NOT_STARTED:
            raise TypeError("transaction blocks can be used only once")
        self.status = self.Status.ACTIVE

        self._outer = not self._conn._in_transaction
        if not self._outer and not self._savepoint_name:
            self._savepoint_name = f"_pg3_{self._conn._tx_depth + 1}"

        self._stack_index = self._conn._tx_depth
        self._conn._tx_depth += 1

        if self._outer:
            self._conn.begin()
        if self._savepoint_name:
            self._conn._exec_command(_savepoint_sql("SAVEPOINT", self._savepoint_name))
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> bool:
        if self._conn.closed:
            self.status = self.Status.FAILED
            if exc is not None:
                logger.warning(
                    "error ignored in rollback of %s: connection closed", self
                )
            return False

        should_rollback = exc is not None or self.force_rollback
        action = "rollback" if should_rollback else "commit"
        self._pop_savepoint(action)

        if should_rollback:
            self.status = (
                self.Status.ROLLED_BACK_EXPLICITLY
                if isinstance(exc, Rollback) or self.force_rollback
                else self.Status.ROLLED_BACK_WITH_ERROR
            )
            try:
                if self._outer:
                    self._conn.rollback()
                else:
                    self._conn._exec_command(
                        _savepoint_sql("ROLLBACK TO", self._savepoint_name)
                    )
                    self._conn._exec_command(
                        _savepoint_sql("RELEASE", self._savepoint_name)
                    )
            except Exception as rollback_error:
                logger.warning(
                    "error ignored in rollback of %s: %s", self, rollback_error
                )
        else:
            self.status = self.Status.COMMITTED
            if self._outer:
                self._conn.commit()
            else:
                self._conn._exec_command(
                    _savepoint_sql("RELEASE", self._savepoint_name)
                )

        if isinstance(exc, Rollback):
            target = cast(object | None, exc.transaction)
            if target is None or target is self:
                return True

        return False

    def _pop_savepoint(self, action: str) -> None:
        self._conn._tx_depth -= 1
        if self._conn._tx_depth != self._stack_index:
            raise e.ProgrammingError(
                f"transaction {action} at the wrong nesting level: {self}"
            )


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


def _result_rowcount(result: _ResultSetLike, statusmessage: str | None) -> int:
    if getattr(result, "is_tuples", bool(result.columns or result.rows)):
        return len(result.rows)
    if statusmessage:
        command = statusmessage.split(maxsplit=1)[0]
        if command in {"INSERT", "UPDATE", "DELETE", "MERGE", "MOVE", "FETCH", "COPY"}:
            return result.rows_affected
    return -1


def _query_param_transcode_flags(
    pgq: PostgresQuery,
    query: Query,
    params: Params | None,
    tx: _BackendTransformer,
) -> tuple[bool, ...]:
    assert pgq.formats is not None
    flags = [format == pq.Format.TEXT for format in pgq.formats]
    if params is None or isinstance(query, Template):
        return tuple(flags)

    if isinstance(pgq, PostgresRawQuery):
        objects = cast(Sequence[Any], params)
    else:
        objects = pgq.validate_and_reorder_params(pgq._parts, params, pgq._order)
    want_formats = pgq._want_formats or [PyFormat.AUTO] * len(objects)
    for index, (obj, want_format) in enumerate(zip(objects, want_formats, strict=True)):
        if obj is not None and not flags[index]:
            dumper = tx.get_dumper(obj, want_format)
            flags[index] = any(
                base.__module__ in {"psycopg.types.enum", "psycopg.types.string"}
                for base in type(dumper).__mro__
            )
    return tuple(flags)


def _transcode_result_value(
    value: object,
    column: _StatementColumnLike,
    wire_format: pq.Format | None,
    *,
    source_encoding: str,
    target_encoding: str,
) -> object:
    if isinstance(value, str):
        value = value.encode(source_encoding)
    if (
        value is None
        or source_encoding == target_encoding
        or not isinstance(value, (bytes, bytearray, memoryview))
    ):
        return value
    if (
        wire_format == pq.Format.TEXT
        or column.oid in _TEXT_WIRE_OIDS
        or getattr(column, "is_enum", False)
    ):
        return bytes(value).decode(source_encoding).encode(target_encoding)
    return value


def _quote_backend_literal(value: bytes) -> bytes:
    if b"\x00" in value:
        raise e.DataError("PostgreSQL text values cannot contain NUL (0x00) bytes")
    value = value.replace(b"'", b"''")
    if b"\\" in value:
        return b"E'" + value.replace(b"\\", b"\\\\") + b"'"
    return b"'" + value + b"'"


def _changes_client_encoding(query: str) -> bool:
    normalized = query.lstrip().lower()
    return (
        "client_encoding" in normalized
        or normalized.startswith("set names")
        or normalized.startswith("reset all")
        or normalized.startswith("discard all")
    )


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
    adapters: AdaptersMap | None = None,
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
        adapters=adapters,
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
    adapters: AdaptersMap | None = None,
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
        adapters=adapters,
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
