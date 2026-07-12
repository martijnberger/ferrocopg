"""
psycopg server-side cursor (async).
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, overload

from . import errors as e
from ._compat import Self
from ._server_cursor_base import ServerCursorMixin
from .abc import Params, Query
from .cursor_async import AsyncCursor
from .rows import AsyncRowFactory, Row

if False:  # ASYNC
    from typing import cast

if TYPE_CHECKING:
    from .connection_async import AsyncConnection


class AsyncServerCursor(
    ServerCursorMixin["AsyncConnection[Any]", Row], AsyncCursor[Row]
):
    __module__ = "psycopg"
    __slots__ = ()

    @overload
    def __init__(
        self,
        connection: AsyncConnection[Row],
        name: str,
        *,
        scrollable: bool | None = None,
        withhold: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        connection: AsyncConnection[Any],
        name: str,
        *,
        row_factory: AsyncRowFactory[Row],
        scrollable: bool | None = None,
        withhold: bool = False,
    ): ...

    def __init__(
        self,
        connection: AsyncConnection[Any],
        name: str,
        *,
        row_factory: AsyncRowFactory[Row] | None = None,
        scrollable: bool | None = None,
        withhold: bool = False,
    ):
        AsyncCursor.__init__(
            self, connection, row_factory=row_factory or connection.row_factory
        )
        ServerCursorMixin.__init__(self, name, scrollable, withhold)
        if False:  # ASYNC
            if getattr(connection, "_is_ferrocopg", False):
                from ._ferrocopg import NoTlsServerCursorAdapter

                self._ferrocopg_cursor = NoTlsServerCursorAdapter(
                    cast(Any, connection),
                    name,
                    row_factory=cast(Any, row_factory or connection.row_factory),
                    scrollable=scrollable,
                    withhold=withhold,
                    factory_name=type(self).__name__,
                    query_cls=self._query_cls,
                )

    async def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.close()
                self._sync_ferrocopg_cursor()
                return

        async with self._conn.lock:
            if self.closed:
                return
            if not self._conn.closed:
                await self._conn.wait(self._close_gen())
            await super().close()

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        binary: bool | None = None,
        **kwargs: Any,
    ) -> Self:
        """
        Open a cursor to execute a query to the database.
        """
        if kwargs:
            raise TypeError(f"keyword not supported: {list(kwargs)[0]}")
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.format = self.format
                self._ferrocopg_cursor.execute(query, params, binary=binary)
                self._sync_ferrocopg_cursor()
                return self

        if self._pgconn.pipeline_status:
            raise e.NotSupportedError(
                "server-side cursors not supported in pipeline mode"
            )

        try:
            async with self._conn.lock:
                await self._conn.wait(self._declare_gen(query, params, binary))
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

        return self

    async def executemany(
        self, query: Query, params_seq: Iterable[Params], *, returning: bool = True
    ) -> None:
        """Method not implemented for server-side cursors."""
        raise e.NotSupportedError("executemany not supported on server-side cursors")

    async def fetchone(self) -> Row | None:
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                row = self._ferrocopg_cursor.fetchone()
                self._sync_ferrocopg_cursor()
                return cast(Row | None, row)

        async with self._conn.lock:
            recs = await self._conn.wait(self._fetch_gen(1))
        if recs:
            self._pos += 1
            return recs[0]
        else:
            return None

    async def fetchmany(self, size: int = 0) -> list[Row]:
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                rows = self._ferrocopg_cursor.fetchmany(size)
                self._sync_ferrocopg_cursor()
                return cast(list[Row], rows)

        if not size:
            size = self.arraysize
        async with self._conn.lock:
            recs = await self._conn.wait(self._fetch_gen(size))
        self._pos += len(recs)
        return recs

    async def fetchall(self) -> list[Row]:
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                rows = self._ferrocopg_cursor.fetchall()
                self._sync_ferrocopg_cursor()
                return cast(list[Row], rows)

        async with self._conn.lock:
            recs = await self._conn.wait(self._fetch_gen(None))
        self._pos += len(recs)
        return recs

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Row:
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.itersize = self.itersize
                row = next(self._ferrocopg_cursor)
                self._sync_ferrocopg_cursor()
                return cast(Row, row)

        # Fetch a new page if we never fetched any, or we are at the end of
        # a page of size itersize, meaning there is likely a following one.
        if self._iter_rows is None or (
            self._page_pos >= len(self._iter_rows) >= self.itersize
        ):
            async with self._conn.lock:
                self._iter_rows = await self._conn.wait(self._fetch_gen(self.itersize))
                self._page_pos = 0

        if self._page_pos >= len(self._iter_rows):
            raise StopAsyncIteration("no more records to return")

        rec = self._iter_rows[self._page_pos]
        self._page_pos += 1
        self._pos += 1
        return rec

    async def scroll(self, value: int, mode: str = "relative") -> None:
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.scroll(value, mode)
                self._sync_ferrocopg_cursor()
                return

        async with self._conn.lock:
            await self._conn.wait(self._scroll_gen(value, mode))
        # Postgres doesn't have a reliable way to report a cursor out of bound
        if mode == "relative":
            self._pos += value
        else:
            self._pos = value
