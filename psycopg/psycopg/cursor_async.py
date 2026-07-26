"""
Psycopg AsyncCursor object.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, overload

from . import errors as e
from . import pq
from ._compat import Self, Template
from ._cursor_base import BaseCursor
from ._pipeline_async import AsyncPipeline
from .abc import Params, Query, QueryNoTemplate
from .copy import AsyncCopy, AsyncWriter
from .rows import AsyncRowFactory, Row, RowMaker

if False:  # ASYNC
    from collections.abc import Iterator
    from typing import cast

if TYPE_CHECKING:
    from .connection_async import AsyncConnection

ACTIVE = pq.TransactionStatus.ACTIVE


class AsyncCursor(BaseCursor["AsyncConnection[Any]", Row]):
    __module__ = "psycopg"
    __slots__ = ()

    @overload
    def __init__(self, connection: AsyncConnection[Row]): ...

    @overload
    def __init__(
        self, connection: AsyncConnection[Any], *, row_factory: AsyncRowFactory[Row]
    ): ...

    def __init__(
        self,
        connection: AsyncConnection[Any],
        *,
        row_factory: AsyncRowFactory[Row] | None = None,
    ):
        super().__init__(connection)
        self._row_factory = row_factory or connection.row_factory
        if False:  # ASYNC
            if getattr(connection, "_is_ferrocopg", False):
                from ._ferrocopg import NoTlsCursorAdapter

                self._ferrocopg_cursor = NoTlsCursorAdapter(
                    cast(Any, connection),
                    row_factory=cast(Any, self._row_factory),
                    query_cls=self._query_cls,
                )

    if False:  # ASYNC

        def _sync_ferrocopg_cursor(self) -> None:
            if self._ferrocopg_cursor is not None:
                if not hasattr(self, "_format"):
                    self.format = self._ferrocopg_cursor.format
                self._results = cast(Any, self._ferrocopg_cursor.pgresults)
                result = self._ferrocopg_cursor._result
                self._iresult = result._index if result is not None else 0
                self.pgresult = self._ferrocopg_cursor.pgresult
                self._closed = self._ferrocopg_cursor.closed
                self._query = self._ferrocopg_cursor._query
                if self._ferrocopg_cursor._make_row is not None:
                    self._make_row = self._ferrocopg_cursor._make_row

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.close()
                self._sync_ferrocopg_cursor()
            else:
                self._close()
        else:
            self._close()

    @property
    def row_factory(self) -> AsyncRowFactory[Row]:
        """Writable attribute to control how result rows are formed."""
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                return cast(AsyncRowFactory[Row], self._ferrocopg_cursor.row_factory)
        return self._row_factory

    @row_factory.setter
    def row_factory(self, row_factory: AsyncRowFactory[Row]) -> None:
        self._row_factory = row_factory
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.row_factory = row_factory
        if self.pgresult:
            self._make_row = row_factory(self)

    def _make_row_maker(self) -> RowMaker[Row]:
        return self._row_factory(self)

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> Self: ...

    @overload
    async def execute(
        self,
        query: Template,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> Self: ...

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> Self:
        """
        Execute a query or command to the database.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.format = self.format
                try:
                    self._ferrocopg_cursor.execute(
                        query, params, prepare=prepare, binary=binary
                    )
                finally:
                    self._sync_ferrocopg_cursor()
                return self

        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._execute_gen(query, params, prepare=prepare, binary=binary)
                )
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)
        return self

    async def executemany(
        self, query: Query, params_seq: Iterable[Params], *, returning: bool = False
    ) -> None:
        """
        Execute the same command with a sequence of input data.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.executemany(
                    query, list(params_seq), returning=returning
                )
                self._sync_ferrocopg_cursor()
                return

        try:
            async with self._conn.lock:
                if AsyncPipeline.is_supported():
                    # If there is already a pipeline, ride it, in order to avoid
                    # sending unnecessary Sync.
                    if self._conn._pipeline:
                        await self._conn.wait(
                            self._executemany_gen_pipeline(query, params_seq, returning)
                        )
                    # Otherwise, make a new one
                    else:
                        async with self._conn._pipeline_nolock():
                            await self._conn.wait(
                                self._executemany_gen_pipeline(
                                    query, params_seq, returning
                                )
                            )
                else:
                    await self._conn.wait(
                        self._executemany_gen_no_pipeline(query, params_seq, returning)
                    )
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    async def stream(
        self,
        query: Query,
        params: Params | None = None,
        *,
        binary: bool | None = None,
        size: int = 1,
    ) -> AsyncIterator[Row]:
        """
        Iterate row-by-row on a result from the database.

        :param size: if greater than 1, results will be retrieved by chunks of
            this size from the server (but still yielded row-by-row); this is only
            available from version 17 of the libpq.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.format = self.format
                stream = cast(
                    Iterator[Row],
                    self._ferrocopg_cursor.stream(
                        query, params, binary=binary, size=size
                    ),
                )
                try:
                    for row in stream:
                        self._sync_ferrocopg_cursor()
                        yield row
                finally:
                    self._sync_ferrocopg_cursor()
                return

        if self._pgconn.pipeline_status:
            raise e.ProgrammingError("stream() cannot be used in pipeline mode")

        async with self._conn.lock:
            try:
                await self._conn.wait(
                    self._stream_send_gen(query, params, binary=binary, size=size)
                )
                first = True
                while res := await self._conn.wait(self._stream_fetchone_gen(first)):
                    for pos in range(res.ntuples):
                        yield self._tx.load_row(pos, self._make_row)
                    first = False
            except e._NO_TRACEBACK as ex:
                raise ex.with_traceback(None)
            finally:
                if self._pgconn.transaction_status == ACTIVE:
                    # Try to cancel the query, then consume the results
                    # already received.
                    await self._conn._try_cancel()
                    try:
                        while await self._conn.wait(
                            self._stream_fetchone_gen(first=False)
                        ):
                            pass
                    except Exception:
                        pass

                    # Try to get out of ACTIVE state. Just do a single attempt, which
                    # should work to recover from an error or query cancelled.
                    try:
                        await self._conn.wait(self._stream_fetchone_gen(first=False))
                    except Exception:
                        pass

    async def results(self) -> AsyncIterator[Self]:
        """
        Iterate across multiple record sets received by the cursor.

        Multiple record sets are received after using `executemany()` with
        `!returning=True` or using `execute()` with more than one query in the
        command.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                for _ in self._ferrocopg_cursor.results():
                    self._sync_ferrocopg_cursor()
                    yield self
                return

        if self.pgresult:
            while True:
                yield self

                if not self.nextset():
                    break

    async def set_result(self, index: int) -> Self:
        """
        Move to a specific result set.

        :arg index: index of the result to go to
        :type index: `!int`

        More than one result will be available after executing calling
        `executemany()` or `execute()` with more than one query.

        `!index` is 0-based and supports negative values, counting from the end,
        the same way you can index items in a list.

        The function returns self, so that the result may be followed by a
        fetch operation. See `results()` for details.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.set_result(index)
                self._sync_ferrocopg_cursor()
                return self

        if not -len(self._results) <= index < len(self._results):
            raise IndexError(
                f"index {index} out of range: {len(self._results)} result(s) available"
            )
        if index < 0:
            index = len(self._results) + index

        self._select_current_result(index)
        return self

    async def fetchone(self) -> Row | None:
        """
        Return the next record from the current result set.

        Return `!None` the result set is finished.

        :rtype: Row | None, with Row defined by `row_factory`
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                row = self._ferrocopg_cursor.fetchone()
                self._sync_ferrocopg_cursor()
                return cast(Row | None, row)

        await self._fetch_pipeline()
        res = self._check_result_for_fetch()
        if self._pos < res.ntuples:
            record = self._tx.load_row(self._pos, self._make_row)
            self._pos += 1
            return record
        return None

    async def fetchmany(self, size: int = 0) -> list[Row]:
        """
        Return the next `!size` records from the current result set.

        `!size` default to `!self.arraysize` if not specified.

        :rtype: Sequence[Row], with Row defined by `row_factory`
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                rows = self._ferrocopg_cursor.fetchmany(size or self.arraysize)
                self._sync_ferrocopg_cursor()
                return cast(list[Row], rows)

        await self._fetch_pipeline()
        res = self._check_result_for_fetch()

        if not size:
            size = self.arraysize
        records = self._tx.load_rows(
            self._pos, min(self._pos + size, res.ntuples), self._make_row
        )
        self._pos += len(records)
        return records

    async def fetchall(self) -> list[Row]:
        """
        Return all the remaining records from the current result set.

        :rtype: Sequence[Row], with Row defined by `row_factory`
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                rows = self._ferrocopg_cursor.fetchall()
                self._sync_ferrocopg_cursor()
                return cast(list[Row], rows)

        await self._fetch_pipeline()
        res = self._check_result_for_fetch()
        records = self._tx.load_rows(self._pos, res.ntuples, self._make_row)
        self._pos = res.ntuples
        return records

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Row:
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                row = next(self._ferrocopg_cursor)
                self._sync_ferrocopg_cursor()
                return cast(Row, row)

        await self._fetch_pipeline()
        res = self._check_result_for_fetch()
        if self._pos < res.ntuples:
            record = self._tx.load_row(self._pos, self._make_row)
            self._pos += 1
            return record
        raise StopAsyncIteration("no more records to return")

    async def scroll(self, value: int, mode: str = "relative") -> None:
        """
        Move the cursor in the result set to a new position according to mode.

        If `!mode` is ``'relative'`` (default), `!value` is taken as offset to
        the current position in the result set; if set to ``'absolute'``,
        `!value` states an absolute target position.

        Raise `!IndexError` in case a scroll operation would leave the result
        set. In this case the position will not change.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                self._ferrocopg_cursor.scroll(value, mode)
                self._sync_ferrocopg_cursor()
                return

        await self._fetch_pipeline()
        self._scroll(value, mode)

    @asynccontextmanager
    async def copy(
        self,
        statement: Query,
        params: Params | None = None,
        *,
        writer: AsyncWriter | None = None,
    ) -> AsyncIterator[AsyncCopy]:
        """
        Initiate a :sql:`COPY` operation and return an object to manage it.
        """
        if False:  # ASYNC
            if self._ferrocopg_cursor is not None:
                with self._ferrocopg_cursor.copy(
                    statement, params, writer=writer
                ) as copy:
                    self._sync_ferrocopg_cursor()
                    yield cast(AsyncCopy, copy)
                self._sync_ferrocopg_cursor()
                return

        try:
            async with self._conn.lock:
                await self._conn.wait(self._start_copy_gen(statement, params))

                async with AsyncCopy(self, writer=writer) as copy:
                    yield copy
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

        # If a fresher result has been set on the cursor by the Copy object,
        # read its properties (especially rowcount).
        self._select_current_result(0)

    async def _fetch_pipeline(self) -> None:
        if (
            self._execmany_returning is not False
            and not self.pgresult
            and self._conn._pipeline
        ):
            async with self._conn.lock:
                await self._conn.wait(self._conn._pipeline._fetch_gen(flush=True))
