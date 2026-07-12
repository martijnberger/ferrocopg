"""
psycopg -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team

import logging
import os
from collections.abc import Callable
from typing import Any, Literal, cast, overload

from . import (
    dbapi20,
    postgres,
    pq,  # noqa: F401 import early to stabilize side effects
    types,
)
from ._capabilities import Capabilities, capabilities
from ._column import Column
from ._connection_base import BaseConnection, Notify
from ._connection_info import ConnectionInfo
from ._enums import IsolationLevel
from ._ferrocopg_async import FerrocopgAsyncConnection
from ._pipeline import Pipeline
from ._pipeline_async import AsyncPipeline
from ._server_cursor import ServerCursor
from ._server_cursor_async import AsyncServerCursor
from ._tpc import Xid
from .abc import AdaptContext, ConnParam
from .client_cursor import AsyncClientCursor, ClientCursor
from .connection import Connection
from .connection_async import AsyncConnection
from .conninfo import conninfo_to_dict
from .copy import AsyncCopy, Copy
from .cursor import Cursor
from .cursor_async import AsyncCursor
from .dbapi20 import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from .errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from .raw_cursor import AsyncRawCursor, AsyncRawServerCursor, RawCursor, RawServerCursor
from .rows import Row, RowFactory, tuple_row
from .transaction import AsyncTransaction, Rollback, Transaction
from .version import __version__ as __version__  # noqa: F401


def connect_ferrocopg(
    conninfo: str = "",
    *,
    context: object | None = None,
    row_factory: object | None = None,
    cursor_factory: type[object] | None = None,
    server_cursor_factory: type[object] | None = None,
    prepare_threshold: int | None = 5,
    autocommit: bool = True,
    isolation_level: IsolationLevel | int | None = None,
    read_only: bool | None = None,
    deferrable: bool | None = None,
    **kwargs: str | int | None,
) -> object:
    """
    Return the Rust-backed ferrocopg connection adapter.

    This direct helper remains available during the source-tree transition.
    Normal synchronous code should use `connect()`.
    """
    from . import _ferrocopg as _ferrocopg_module

    if row_factory is None:
        row_factory = tuple_row

    effective_conninfo = _ferrocopg_module.merge_conninfo(
        conninfo, kwargs, use_environment=True
    )
    target_session_attrs = str(
        conninfo_to_dict(effective_conninfo).get("target_session_attrs") or "any"
    )
    valid_targets = {
        "any",
        "read-write",
        "read-only",
        "primary",
        "standby",
        "prefer-standby",
    }
    if target_session_attrs not in valid_targets:
        raise OperationalError(
            f"invalid target_session_attrs value: {target_session_attrs}"
        )
    rust_conninfo = effective_conninfo
    if target_session_attrs in {"primary", "standby", "prefer-standby"}:
        rust_conninfo = _ferrocopg_module.merge_conninfo(
            effective_conninfo, {"target_session_attrs": "any"}
        )
    adapter_options: dict[str, Any] = {
        "row_factory": cast(
            Callable[[list[str], list[str | None]], object], row_factory
        ),
        "cursor_factory": cursor_factory or Cursor,
        "server_cursor_factory": server_cursor_factory or ServerCursor,
        "prepare_threshold": prepare_threshold,
        "autocommit": autocommit,
        "isolation_level": isolation_level,
        "read_only": read_only,
        "deferrable": deferrable,
    }
    if context is not None:
        adapter_options["adapters"] = cast(Any, context).adapters

    try:
        conn = _ferrocopg_module.backend_connection_adapter(
            rust_conninfo,
            **adapter_options,
        )
    except OperationalError as ex:
        if target_session_attrs == "any":
            raise
        raise OperationalError(
            f"target_session_attrs={target_session_attrs}: {ex}"
        ) from None
    if conn is None:
        _ferrocopg_module.require_available()
        raise OperationalError("the ferrocopg Rust backend failed to initialize")
    if isinstance(conn, _ferrocopg_module.NoTlsConnectionAdapter):
        conn._conninfo = effective_conninfo
        if target_session_attrs in {"primary", "standby"}:
            row = conn._session.execute_params(
                "select pg_is_in_recovery()::text", []
            ).fetchone()
            in_recovery = bool(row and str(row[0]).lower() in {"t", "true", "on"})
            matches = (
                not in_recovery if target_session_attrs == "primary" else in_recovery
            )
            if not matches:
                conn.close()
                raise OperationalError(
                    f"target_session_attrs={target_session_attrs}: "
                    "server does not satisfy requested session attributes"
                )
        client_encoding = os.environ.get("PGCLIENTENCODING")
        if client_encoding and "client_encoding" not in (
            conninfo_to_dict(effective_conninfo)
        ):
            try:
                conn._set_client_encoding(client_encoding)
            except BaseException:
                conn.close()
                raise
            conn._conninfo = _ferrocopg_module.merge_conninfo(
                effective_conninfo, {"client_encoding": client_encoding}
            )
        conn._warn_on_del = True
    return conn


@overload
def connect(
    conninfo: str = "",
    /,
    *,
    autocommit: bool = False,
    prepare_threshold: int | None = 5,
    context: AdaptContext | None = None,
    row_factory: RowFactory[Row] | None = None,
    cursor_factory: type[Cursor[Row]] | None = None,
    impl: Literal["libpq"] | None = None,
    **kwargs: ConnParam,
) -> Connection[Row]: ...


@overload
def connect(
    conninfo: str = "",
    /,
    *,
    impl: Literal["ferrocopg"],
    **kwargs: Any,
) -> object: ...


def connect(
    conninfo: str = "",
    /,
    *,
    impl: Literal["libpq", "ferrocopg"] | None = None,
    **kwargs: Any,
) -> Any:
    """
    Connect to a database using the Rust backend by default.

    `impl="libpq"` selects the temporary source-tree comparison path. The
    `PSYCOPG_SOURCE_IMPL` environment variable exists only for upstream
    comparison automation and will not be part of the staged package contract.
    """
    selected_impl = (
        impl if impl is not None else os.environ.get("PSYCOPG_SOURCE_IMPL", "ferrocopg")
    )
    if selected_impl == "libpq":
        return Connection.connect(conninfo, **kwargs)
    if selected_impl == "ferrocopg":
        kwargs.setdefault("autocommit", False)
        from ._ferrocopg import FerrocopgConnection

        return FerrocopgConnection.connect(conninfo, **kwargs)

    raise ValueError(
        f"unsupported connect() implementation {selected_impl!r}: "
        "expected 'libpq' or 'ferrocopg'"
    )


# Set the logger to a quiet default, can be enabled if needed
if (logger := logging.getLogger("psycopg")).level == logging.NOTSET:
    logger.setLevel(logging.WARNING)

# DBAPI compliance
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

# register default adapters for PostgreSQL
adapters = postgres.adapters  # exposed by the package
postgres.register_default_types(adapters.types)
postgres.register_default_adapters(adapters)

# After the default ones, because these can deal with the bytea oid better
dbapi20.register_dbapi20_adapters(adapters)

# Must come after all the types have been registered
types.array.register_all_arrays(adapters)

# Note: defining the exported methods helps both Sphynx in documenting that
# this is the canonical place to obtain them and should be used by MyPy too,
# so that function signatures are consistent with the documentation.
__all__ = [
    "AsyncClientCursor",
    "AsyncConnection",
    "FerrocopgAsyncConnection",
    "AsyncCopy",
    "AsyncCursor",
    "AsyncPipeline",
    "AsyncRawCursor",
    "AsyncRawServerCursor",
    "AsyncServerCursor",
    "AsyncTransaction",
    "BaseConnection",
    "Capabilities",
    "capabilities",
    "ClientCursor",
    "Column",
    "Connection",
    "ConnectionInfo",
    "Copy",
    "Cursor",
    "connect_ferrocopg",
    "IsolationLevel",
    "Notify",
    "Pipeline",
    "RawCursor",
    "RawServerCursor",
    "Rollback",
    "ServerCursor",
    "Transaction",
    "Xid",
    # DBAPI exports
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # DBAPI type constructors and singletons
    "Binary",
    "Date",
    "DateFromTicks",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
]
