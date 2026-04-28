"""
psycopg -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team

import logging
from collections.abc import Callable
from typing import Any, cast

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
from ._pipeline import Pipeline
from ._pipeline_async import AsyncPipeline
from ._server_cursor import ServerCursor
from ._server_cursor_async import AsyncServerCursor
from ._tpc import Xid
from .client_cursor import AsyncClientCursor, ClientCursor
from .connection import Connection
from .connection_async import AsyncConnection
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
) -> object | None:
    """
    Return the experimental Rust-backed ferrocopg connection adapter.

    This is an explicit opt-in path and does not affect the normal `connect()`
    selector or the `PSYCOPG_IMPL` libpq-wrapper selection.
    """
    from . import _ferrocopg as _ferrocopg_module
    from .conninfo import make_conninfo

    if context is not None:
        raise NotSupportedError(
            "ferrocopg doesn't support custom adaptation contexts yet"
        )
    if server_cursor_factory is not None:
        raise NotSupportedError(
            "ferrocopg doesn't support server-side cursor factories yet"
        )

    if row_factory is None:
        return _ferrocopg_module.no_tls_connection_adapter(
            make_conninfo(conninfo, **kwargs),
            cursor_factory=cast(
                type[_ferrocopg_module.NoTlsCursorAdapter],
                cursor_factory or _ferrocopg_module.NoTlsCursorAdapter,
            ),
            prepare_threshold=prepare_threshold,
            autocommit=autocommit,
            isolation_level=isolation_level,
            read_only=read_only,
            deferrable=deferrable,
        )

    return _ferrocopg_module.no_tls_connection_adapter(
        make_conninfo(conninfo, **kwargs),
        row_factory=cast(Callable[[list[str], list[str | None]], object], row_factory),
        cursor_factory=cast(
            type[_ferrocopg_module.NoTlsCursorAdapter],
            cursor_factory or _ferrocopg_module.NoTlsCursorAdapter,
        ),
        prepare_threshold=prepare_threshold,
        autocommit=autocommit,
        isolation_level=isolation_level,
        read_only=read_only,
        deferrable=deferrable,
    )


def connect(conninfo: str = "", /, **kwargs: Any) -> Any:
    """
    Connect to a database and return a Psycopg or ferrocopg connection.

    The normal path remains `Connection.connect()`. Passing `impl="ferrocopg"`
    provides an explicit opt-in bridge into the experimental Rust-backed
    adapter without changing the default implementation selector.
    """
    impl = kwargs.pop("impl", None)
    if impl is None or impl == "libpq":
        return Connection.connect(conninfo, **kwargs)
    if impl == "ferrocopg":
        kwargs.setdefault("autocommit", False)
        return connect_ferrocopg(conninfo, **kwargs)

    raise ValueError(
        f"unsupported connect() implementation {impl!r}: expected 'libpq' or 'ferrocopg'"
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
