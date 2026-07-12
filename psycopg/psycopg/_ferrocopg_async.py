"""Asyncio facade for the synchronous ferrocopg backend adapter."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, TypeVar, cast

from ._ferrocopg import (
    FerrocopgConnection,
    NoTlsConnectionAdapter,
    NoTlsCursorAdapter,
)
from .abc import Params, Query

T = TypeVar("T")
_DONE = object()


def _next_or_done(iterator: Any) -> Any:
    return next(iterator, _DONE)


def _backend_cursor(cursor: Any) -> NoTlsCursorAdapter:
    hosted = getattr(cursor, "_ferrocopg_cursor", None)
    if hosted is None:
        return cast(NoTlsCursorAdapter, cursor)
    cursor._ferrocopg_cursor = None
    cursor._closed = True
    return cast(NoTlsCursorAdapter, hosted)


class FerrocopgAsyncConnection:
    """Async connection facade using serialized thread offload."""

    __module__ = "psycopg"

    def __init__(self, connection: NoTlsConnectionAdapter):
        self._connection = connection
        self._is_ferrocopg_async = True
        self._lock = asyncio.Lock()
        self._executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ferrocopg"
        )
        self._pipeline: FerrocopgAsyncPipeline | None = None

    @classmethod
    async def connect(
        cls, conninfo: str = "", **kwargs: Any
    ) -> FerrocopgAsyncConnection:
        connection = cast(
            NoTlsConnectionAdapter,
            await asyncio.to_thread(
                partial(FerrocopgConnection.connect, conninfo, **kwargs)
            ),
        )
        return cls(connection)

    async def _run(self, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        async with self._lock:
            executor = self._executor
            if executor is None:
                # Closed-connection methods only perform local state checks.
                return func(*args, **kwargs)
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(executor, partial(func, *args, **kwargs))

    def _shutdown_executor(self) -> None:
        if executor := self._executor:
            self._executor = None
            executor.shutdown(wait=True)

    @property
    def closed(self) -> bool:
        return self._connection.closed

    @property
    def broken(self) -> bool:
        return self._connection.broken

    @property
    def info(self) -> Any:
        return self._connection.info

    @property
    def pgconn(self) -> Any:
        return self._connection.pgconn

    @property
    def adapters(self) -> Any:
        return self._connection.adapters

    @property
    def autocommit(self) -> bool:
        return self._connection.autocommit

    @property
    def isolation_level(self) -> Any:
        return self._connection.isolation_level

    @property
    def read_only(self) -> bool | None:
        return self._connection.read_only

    @property
    def deferrable(self) -> bool | None:
        return self._connection.deferrable

    @property
    def row_factory(self) -> Any:
        return self._connection.row_factory

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        self._connection.row_factory = value

    @property
    def prepare_threshold(self) -> int | None:
        return self._connection.prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: int | None) -> None:
        self._connection.prepare_threshold = value

    async def close(self) -> None:
        try:
            await self._run(self._connection.close)
        finally:
            self._shutdown_executor()

    async def commit(self) -> None:
        await self._run(self._connection.commit)

    async def rollback(self) -> None:
        await self._run(self._connection.rollback)

    async def set_autocommit(self, value: bool) -> None:
        await self._run(self._connection.set_autocommit, value)

    async def set_isolation_level(self, value: Any) -> None:
        await self._run(self._connection.set_isolation_level, value)

    async def set_read_only(self, value: bool | None) -> None:
        await self._run(self._connection.set_read_only, value)

    async def set_deferrable(self, value: bool | None) -> None:
        await self._run(self._connection.set_deferrable, value)

    def cursor(
        self,
        name: str = "",
        *,
        binary: bool = False,
        row_factory: Any = None,
        scrollable: bool | None = None,
        withhold: bool = False,
    ) -> FerrocopgAsyncCursor:
        cursor = self._connection.cursor(
            name,
            binary=binary,
            row_factory=row_factory,
            scrollable=scrollable,
            withhold=withhold,
        )
        return FerrocopgAsyncCursor(self, _backend_cursor(cursor))

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool = False,
        binary: bool = False,
        row_factory: Any = None,
    ) -> FerrocopgAsyncCursor:
        if self._pipeline is not None:
            cursor = await self._pipeline._execute(
                query,
                params=params,
                prepare=prepare,
                row_factory=row_factory,
            )
            if binary:
                cursor.format = 1
            return cursor
        sync_cursor = await self._run(
            self._connection.execute,
            query,
            params,
            prepare=prepare,
            binary=binary,
            row_factory=row_factory,
        )
        return FerrocopgAsyncCursor(self, _backend_cursor(sync_cursor))

    def transaction(
        self, savepoint_name: str | None = None, force_rollback: bool = False
    ) -> FerrocopgAsyncTransaction:
        return FerrocopgAsyncTransaction(
            self, self._connection.transaction(savepoint_name, force_rollback)
        )

    def pipeline(self) -> FerrocopgAsyncPipeline:
        return FerrocopgAsyncPipeline(self, self._connection.pipeline())

    def xid(self, format_id: int, gtrid: str, bqual: str) -> Any:
        return self._connection.xid(format_id, gtrid, bqual)

    async def tpc_begin(self, xid: Any) -> None:
        await self._run(self._connection.tpc_begin, xid)

    async def tpc_prepare(self) -> None:
        await self._run(self._connection.tpc_prepare)

    async def tpc_commit(self, xid: Any = None) -> None:
        await self._run(self._connection.tpc_commit, xid)

    async def tpc_rollback(self, xid: Any = None) -> None:
        await self._run(self._connection.tpc_rollback, xid)

    async def tpc_recover(self) -> list[Any]:
        return await self._run(self._connection.tpc_recover)

    def cancel(self) -> None:
        self._connection.cancel()

    async def cancel_safe(self, *, timeout: float = 30.0) -> None:
        await self._run(self._connection.cancel_safe, timeout=timeout)

    def add_notice_handler(self, callback: Any) -> None:
        self._connection.add_notice_handler(callback)

    def remove_notice_handler(self, callback: Any) -> None:
        self._connection.remove_notice_handler(callback)

    def add_notify_handler(self, callback: Any) -> None:
        self._connection.add_notify_handler(callback)

    def remove_notify_handler(self, callback: Any) -> None:
        self._connection.remove_notify_handler(callback)

    async def notifies(
        self, *, timeout: float | None = None, stop_after: int | None = None
    ) -> AsyncIterator[Any]:
        iterator = self._connection.notifies(timeout=timeout, stop_after=stop_after)
        while True:
            item = await self._run(_next_or_done, iterator)
            if item is _DONE:
                return
            yield item

    async def __aenter__(self) -> FerrocopgAsyncConnection:
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, tb: object
    ) -> None:
        try:
            await self._run(self._connection.__exit__, exc_type, exc, tb)
        finally:
            self._shutdown_executor()


class FerrocopgAsyncCursor:
    """Async wrapper around a ferrocopg cursor adapter."""

    __module__ = "psycopg"

    def __init__(self, connection: FerrocopgAsyncConnection, cursor: Any):
        self.connection = connection
        self._cursor = cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    @property
    def row_factory(self) -> Any:
        return self._cursor.row_factory

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        self._cursor.row_factory = value

    @property
    def adapters(self) -> Any:
        return self._cursor.adapters

    @adapters.setter
    def adapters(self, value: Any) -> None:
        self._cursor.adapters = value

    @property
    def format(self) -> Any:
        return self._cursor.format

    @format.setter
    def format(self, value: Any) -> None:
        self._cursor.format = value

    @property
    def arraysize(self) -> int:
        return cast(int, self._cursor.arraysize)

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool = False,
        binary: bool | None = None,
    ) -> FerrocopgAsyncCursor:
        await self.connection._run(
            self._cursor.execute,
            query,
            params,
            prepare=prepare,
            binary=binary,
        )
        return self

    async def executemany(
        self,
        query: Query,
        params_seq: Sequence[Params],
        *,
        returning: bool = False,
        prepare: bool | None = None,
    ) -> None:
        await self.connection._run(
            self._cursor.executemany,
            query,
            params_seq,
            returning=returning,
            prepare=prepare,
        )

    async def fetchone(self) -> Any:
        return await self.connection._run(self._cursor.fetchone)

    async def fetchmany(self, size: int = 0) -> list[Any]:
        return await self.connection._run(self._cursor.fetchmany, size)

    async def fetchall(self) -> list[Any]:
        return await self.connection._run(self._cursor.fetchall)

    async def nextset(self) -> bool | None:
        return await self.connection._run(self._cursor.nextset)

    async def scroll(self, value: int, mode: str = "relative") -> None:
        await self.connection._run(self._cursor.scroll, value, mode)

    async def close(self) -> None:
        await self.connection._run(self._cursor.close)

    def copy(
        self,
        statement: Query,
        params: Params | None = None,
        *,
        writer: object | None = None,
    ) -> FerrocopgAsyncCopy:
        return FerrocopgAsyncCopy(
            self.connection, self._cursor.copy(statement, params, writer=writer)
        )

    async def stream(
        self,
        query: Query,
        params: Params | None = None,
        *,
        binary: bool | None = None,
        size: int = 1,
    ) -> AsyncIterator[Any]:
        iterator = self._cursor.stream(query, params, binary=binary, size=size)
        while True:
            row = await self.connection._run(_next_or_done, iterator)
            if row is _DONE:
                return
            yield row

    def __aiter__(self) -> FerrocopgAsyncCursor:
        return self

    async def __anext__(self) -> Any:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> FerrocopgAsyncCursor:
        await self.connection._run(self._cursor.__enter__)
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.connection._run(self._cursor.__exit__, exc_type, exc, tb)


class FerrocopgAsyncCopy:
    """Async COPY wrapper."""

    __module__ = "psycopg"

    def __init__(self, connection: FerrocopgAsyncConnection, copy: Any):
        self.connection = connection
        self._copy = copy

    def __getattr__(self, name: str) -> Any:
        return getattr(self._copy, name)

    def set_types(self, types: Sequence[int | str]) -> None:
        self._copy.set_types(types)

    async def write(self, buffer: bytes | str) -> None:
        await self.connection._run(self._copy.write, buffer)

    async def write_row(self, row: Sequence[object]) -> None:
        await self.connection._run(self._copy.write_row, row)

    async def read(self) -> bytes:
        return await self.connection._run(self._copy.read)

    async def read_row(self) -> tuple[object, ...] | None:
        return await self.connection._run(self._copy.read_row)

    async def rows(self) -> AsyncIterator[tuple[object, ...]]:
        while (row := await self.read_row()) is not None:
            yield row

    def __aiter__(self) -> FerrocopgAsyncCopy:
        return self

    async def __anext__(self) -> bytes:
        data = await self.read()
        if not data:
            raise StopAsyncIteration
        return data

    async def __aenter__(self) -> FerrocopgAsyncCopy:
        await self.connection._run(self._copy.__enter__)
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, tb: object
    ) -> None:
        await self.connection._run(self._copy.__exit__, exc_type, exc, tb)


class FerrocopgAsyncTransaction:
    """Async transaction context wrapper."""

    def __init__(self, connection: FerrocopgAsyncConnection, transaction: Any):
        self.connection = connection
        self._transaction = transaction

    def __getattr__(self, name: str) -> Any:
        return getattr(self._transaction, name)

    async def __aenter__(self) -> FerrocopgAsyncTransaction:
        await self.connection._run(self._transaction.__enter__)
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, tb: object
    ) -> bool:
        return await self.connection._run(self._transaction.__exit__, exc_type, exc, tb)


class FerrocopgAsyncPipeline:
    """Async pipeline context wrapper."""

    def __init__(self, connection: FerrocopgAsyncConnection, pipeline: Any):
        self.connection = connection
        self._pipeline = pipeline

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pipeline, name)

    async def _execute(
        self,
        query: Query,
        *,
        params: Params | None,
        prepare: bool,
        row_factory: Any,
    ) -> FerrocopgAsyncCursor:
        cursor = await self.connection._run(
            self._pipeline.execute,
            query,
            params=params,
            prepare=prepare,
            row_factory=row_factory,
        )
        return FerrocopgAsyncCursor(self.connection, _backend_cursor(cursor))

    async def sync(self) -> None:
        await self.connection._run(self._pipeline.sync)

    async def __aenter__(self) -> FerrocopgAsyncPipeline:
        await self.connection._run(self._pipeline.__enter__)
        self.connection._pipeline = self
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            await self.connection._run(self._pipeline.__exit__, exc_type, exc, tb)
        finally:
            self.connection._pipeline = None
