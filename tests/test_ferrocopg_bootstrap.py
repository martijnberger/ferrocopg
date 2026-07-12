import asyncio
import importlib
import os
import socket
import threading
import uuid
from collections import deque
from collections.abc import Callable, Generator
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest


class StubCopyTransformer:
    def __init__(self, adapted: list[bytes | None]):
        self._adapted = adapted
        self._nfields = len(adapted)

    def dump_sequence(
        self, params: tuple[object, ...], formats: list[object]
    ) -> list[bytes | None]:
        assert len(params) == len(formats) == len(self._adapted)
        return self._adapted

    def load_sequence(
        self, record: list[bytes | memoryview | bytearray | None]
    ) -> tuple[bytes | None, ...]:
        return tuple(None if item is None else bytes(item) for item in record)


class CopyImpl(Protocol):
    def format_row_text(
        self, row: tuple[object, ...] | tuple[int, ...], tx: object, out: bytearray
    ) -> None: ...

    def format_row_binary(
        self, row: tuple[object, ...] | tuple[int, ...], tx: object, out: bytearray
    ) -> None: ...

    def parse_row_text(
        self, data: bytearray, tx: object
    ) -> tuple[bytes | None, ...] | tuple[object, ...]: ...

    def parse_row_binary(
        self, data: bytearray, tx: object
    ) -> tuple[bytes | None, ...] | tuple[object, ...]: ...


class GeneratorImpl(Protocol):
    def send(self, pgconn: object) -> object: ...


class FetchImpl(Protocol):
    def fetch(self, pgconn: object) -> object: ...


class FetchManyImpl(Protocol):
    def fetch_many(self, pgconn: object) -> object: ...


class ExecuteImpl(Protocol):
    def execute(self, pgconn: object) -> object: ...


class PipelineImpl(Protocol):
    def pipeline_communicate(self, pgconn: object, commands: object) -> object: ...


class CancelImpl(Protocol):
    def cancel(self, cancel_conn: object, *, timeout: float = 0.0) -> object: ...


class ConnectImpl(Protocol):
    def connect(self, conninfo: str, *, timeout: float = 0.0) -> object: ...


class ArrayBinaryImpl(Protocol):
    def array_load_binary(self, data: object, tx: object) -> object: ...


class ArrayTextImpl(Protocol):
    def array_load_text(
        self, data: object, loader: object, delimiter: bytes = b","
    ) -> object: ...


class UUIDTextImpl(Protocol):
    def uuid_load_text(self, data: object) -> object: ...


class UUIDBinaryImpl(Protocol):
    def uuid_load_binary(self, data: object) -> object: ...


class BoolImpl(Protocol):
    def bool_dump_text(self, obj: bool) -> object: ...

    def bool_dump_binary(self, obj: bool) -> object: ...

    def bool_load_text(self, data: object) -> object: ...

    def bool_load_binary(self, data: object) -> object: ...


class StringImpl(Protocol):
    def str_dump_text(self, obj: str, encoding: str) -> object: ...

    def str_dump_binary(self, obj: str, encoding: str) -> object: ...

    def text_load(self, data: object, encoding: str) -> object: ...


class ByteaBinaryImpl(Protocol):
    def bytes_dump_binary(self, data: object) -> object: ...

    def bytea_load_binary(self, data: object) -> object: ...


class CompositeImpl(Protocol):
    def composite_dump_text_sequence(self, seq: object, tx: object) -> object: ...

    def composite_dump_binary_sequence(
        self, seq: object, types: object, formats: object, tx: object
    ) -> object: ...

    def composite_parse_text_record(self, data: object) -> object: ...


class NumericImpl(Protocol):
    def dump_decimal_to_text(self, obj: object) -> object: ...

    def dump_decimal_to_numeric_binary(self, obj: object) -> object: ...

    def dump_int_to_numeric_binary(self, obj: object) -> object: ...

    def numeric_load_text(self, data: object) -> object: ...

    def numeric_load_binary(self, data: object) -> object: ...


class DateTimeImpl(Protocol):
    def date_dump_text(self, obj: object) -> object: ...

    def date_dump_binary(self, obj: object) -> object: ...

    def date_load_binary(self, data: object) -> object: ...

    def time_dump_text(self, obj: object) -> object: ...

    def time_dump_binary(self, obj: object) -> object: ...

    def time_load_binary(self, data: object) -> object: ...

    def timetz_dump_binary(self, obj: object) -> object: ...

    def timetz_load_binary(self, data: object) -> object: ...

    def datetime_dump_text(self, obj: object) -> object: ...

    def datetime_dump_binary(self, obj: object) -> object: ...

    def datetime_notz_dump_binary(self, obj: object) -> object: ...

    def timestamp_load_binary(self, data: object) -> object: ...

    def timestamptz_load_binary(self, data: object, timezone_obj: object) -> object: ...

    def timedelta_dump_binary(self, obj: object) -> object: ...

    def interval_load_binary(self, data: object) -> object: ...


def _copy_impls() -> list[tuple[str, CopyImpl]]:
    ferrocopg = cast(CopyImpl, pytest.importorskip("ferrocopg_rust"))
    copy_base = importlib.import_module("psycopg._copy_base")
    python_impl = cast(
        CopyImpl,
        SimpleNamespace(
            format_row_text=copy_base._format_row_text,
            format_row_binary=copy_base._format_row_binary,
            parse_row_text=_expected_text_row,
            parse_row_binary=copy_base._parse_row_binary,
        ),
    )

    return [
        ("python", python_impl),
        ("rust", ferrocopg),
    ]


def _expected_text_row(
    data: bytearray, tx: StubCopyTransformer
) -> tuple[bytes | None, ...]:
    if not tx._nfields and bytes(data) == b"\n":
        return ()

    return cast(
        tuple[bytes | None, ...],
        importlib.import_module("psycopg._copy_base")._parse_row_text(data, tx),
    )


def _wait_ready_gen(
    wait_state: int, expected_ready: int, result: str = "ok"
) -> Generator[int, int, str]:
    ready = yield wait_state
    assert ready == expected_ready
    return result


class StubSendPgconn:
    def __init__(self, flush_results: list[int]):
        self._flush_results = list(flush_results)
        self.flush_calls = 0
        self.consume_input_calls = 0

    def flush(self) -> int:
        self.flush_calls += 1
        if self._flush_results:
            return self._flush_results.pop(0)
        return 0

    def consume_input(self) -> None:
        self.consume_input_calls += 1


class StubFetchPgconn:
    def __init__(
        self,
        busy_results: list[bool],
        result: object,
        notifies: list[object] | None = None,
    ):
        self._busy_results = list(busy_results)
        self._result = result
        self._notifies = list(notifies or [])
        self.consume_input_calls = 0
        self.notify_handler_calls: list[object] = []
        self.notify_handler = self.notify_handler_calls.append

    def is_busy(self) -> bool:
        if self._busy_results:
            return self._busy_results.pop(0)
        return False

    def consume_input(self) -> None:
        self.consume_input_calls += 1

    def notifies(self) -> object | None:
        if self._notifies:
            return self._notifies.pop(0)
        return None

    def get_result(self) -> object:
        return self._result


class StubResult:
    def __init__(self, status: int, label: str):
        self.status = status
        self.label = label

    def __repr__(self) -> str:
        return f"StubResult(status={self.status}, label={self.label!r})"


class StubFetchManyPgconn:
    def __init__(
        self,
        busy_sequences: list[list[bool]],
        results: list[StubResult | None],
        notifies_per_fetch: list[list[object]] | None = None,
    ):
        self._busy_sequences = [list(seq) for seq in busy_sequences]
        self._results = list(results)
        self._notifies_per_fetch = [list(seq) for seq in notifies_per_fetch or []]
        self._fetch_index = 0
        self.consume_input_calls = 0
        self.notify_handler_calls: list[object] = []
        self.notify_handler = self.notify_handler_calls.append
        self._current_busy = self._busy_sequences.pop(0) if self._busy_sequences else []
        self._current_notifies = (
            self._notifies_per_fetch.pop(0) if self._notifies_per_fetch else []
        )
        self._flush_results: list[int] = []
        self.flush_calls = 0

    def is_busy(self) -> bool:
        if self._current_busy:
            return self._current_busy.pop(0)
        return False

    def consume_input(self) -> None:
        self.consume_input_calls += 1

    def flush(self) -> int:
        self.flush_calls += 1
        if self._flush_results:
            return self._flush_results.pop(0)
        return 0

    def notifies(self) -> object | None:
        if self._current_notifies:
            return self._current_notifies.pop(0)
        return None

    def get_result(self) -> StubResult | None:
        result = self._results[self._fetch_index]
        self._fetch_index += 1
        self._current_busy = self._busy_sequences.pop(0) if self._busy_sequences else []
        self._current_notifies = (
            self._notifies_per_fetch.pop(0) if self._notifies_per_fetch else []
        )
        return result


class StubPipelinePgconn:
    def __init__(
        self,
        read_cycles: list[tuple[list[bool], list[StubResult | None], list[object]]],
    ):
        self._pending_cycles = [
            (list(busy), list(results), list(notifies))
            for busy, results, notifies in read_cycles
        ]
        self._current_busy: list[bool] = []
        self._current_results: list[StubResult | None] = []
        self._current_notifies: list[object] = []
        self.consume_input_calls = 0
        self.flush_calls = 0
        self.notify_handler_calls: list[object] = []
        self.notify_handler = self.notify_handler_calls.append

    def consume_input(self) -> None:
        self.consume_input_calls += 1
        if self._pending_cycles:
            self._current_busy, self._current_results, self._current_notifies = (
                self._pending_cycles.pop(0)
            )

    def is_busy(self) -> bool:
        if self._current_busy:
            return self._current_busy.pop(0)
        return False

    def get_result(self) -> StubResult | None:
        if self._current_results:
            return self._current_results.pop(0)
        return None

    def notifies(self) -> object | None:
        if self._current_notifies:
            return self._current_notifies.pop(0)
        return None

    def flush(self) -> int:
        self.flush_calls += 1
        return 0


class StubCancelConn:
    def __init__(
        self, statuses: list[int], socket: int = 42, error_message: str = "boom"
    ):
        self._statuses = list(statuses)
        self.socket = socket
        self._error_message = error_message

    def poll(self) -> int:
        if self._statuses:
            return self._statuses.pop(0)
        return 0

    def get_error_message(self) -> str:
        return self._error_message


class StubConnectConn:
    def __init__(
        self,
        status: int,
        poll_statuses: list[int],
        *,
        socket: int = 42,
        error_message: str = "connect boom",
    ):
        self.status = status
        self._poll_statuses = list(poll_statuses)
        self.socket = socket
        self.error_message = error_message
        self.nonblocking = 0

    def connect_poll(self) -> int:
        if self._poll_statuses:
            return self._poll_statuses.pop(0)
        return 0

    def get_error_message(self, _encoding: object) -> str:
        return self.error_message


class StubArrayLoader:
    def __init__(self, loadfunc: Callable[[bytes], object]):
        self.load = loadfunc


class StubArrayTransformer:
    def __init__(self, loadfunc: Callable[[bytes], object]):
        self._loader = StubArrayLoader(loadfunc)

    def get_loader(self, oid: int, _format: object) -> StubArrayLoader:
        assert oid > 0
        return self._loader


class StubDumper:
    def __init__(self, values: dict[object, bytes | None]):
        self._values = values

    def dump(self, obj: object) -> bytes | None:
        return self._values[obj]


class StubCompositeTransformer:
    def __init__(
        self,
        text_values: dict[object, bytes | None],
        binary_values: list[bytes | None],
    ):
        self._text_values = text_values
        self._binary_values = binary_values

    def get_dumper(self, obj: object, _format: object) -> StubDumper:
        return StubDumper(self._text_values)

    def dump_sequence(self, _seq: object, _formats: object) -> list[bytes | None]:
        return self._binary_values


def _drive_send_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[int], object]:
    waits: list[int] = []
    try:
        waits.append(next(cast(Generator[int, int | None, object], gen)))
        for ready in ready_values:
            waits.append(
                cast(
                    int,
                    cast(Any, gen).send(ready),
                )
            )
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _drive_fetch_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[int], object]:
    waits: list[int] = []
    try:
        waits.append(next(cast(Generator[int, int | None, object], gen)))
        for ready in ready_values:
            waits.append(cast(int, cast(Any, gen).send(ready)))
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _drive_fetch_many_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[int], object]:
    waits: list[int] = []
    try:
        waits.append(next(cast(Generator[int, int | None, object], gen)))
        for ready in ready_values:
            waits.append(cast(int, cast(Any, gen).send(ready)))
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _drive_execute_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[int], object]:
    waits: list[int] = []
    try:
        waits.append(next(cast(Generator[int, int | None, object], gen)))
        for ready in ready_values:
            waits.append(cast(int, cast(Any, gen).send(ready)))
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _drive_pipeline_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[int], object]:
    waits: list[int] = []
    try:
        waits.append(next(cast(Generator[int, int | None, object], gen)))
        for ready in ready_values:
            waits.append(cast(int, cast(Any, gen).send(ready)))
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _drive_cancel_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[tuple[int, int]], object]:
    waits: list[tuple[int, int]] = []
    try:
        waits.append(next(cast(Generator[tuple[int, int], int | None, object], gen)))
        for ready in ready_values:
            waits.append(cast(tuple[int, int], cast(Any, gen).send(ready)))
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _drive_connect_generator(
    gen: object, ready_values: list[int | None]
) -> tuple[list[tuple[int, int]], object]:
    waits: list[tuple[int, int]] = []
    try:
        waits.append(next(cast(Generator[tuple[int, int], int | None, object], gen)))
        for ready in ready_values:
            waits.append(cast(tuple[int, int], cast(Any, gen).send(ready)))
    except StopIteration as ex:
        return waits, ex.value

    raise AssertionError("generator did not finish")


def _send_impls() -> list[tuple[str, GeneratorImpl]]:
    ferrocopg = cast(GeneratorImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    python_impl = cast(GeneratorImpl, SimpleNamespace(send=generators._send))
    return [("python", python_impl), ("rust", ferrocopg)]


def _fetch_impls() -> list[tuple[str, FetchImpl]]:
    ferrocopg = cast(FetchImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    python_impl = cast(FetchImpl, SimpleNamespace(fetch=generators._fetch))
    return [("python", python_impl), ("rust", ferrocopg)]


def _fetch_many_impls() -> list[tuple[str, FetchManyImpl]]:
    ferrocopg = cast(FetchManyImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    generators_any = cast(Any, generators)

    def python_fetch_many(pgconn: object) -> Generator[int, int | None, object]:
        original_fetch = generators_any.fetch
        generators_any.fetch = generators._fetch
        try:
            return (yield from generators._fetch_many(pgconn))
        finally:
            generators_any.fetch = original_fetch

    python_impl = cast(FetchManyImpl, SimpleNamespace(fetch_many=python_fetch_many))
    return [("python", python_impl), ("rust", ferrocopg)]


def _execute_impls() -> list[tuple[str, ExecuteImpl]]:
    ferrocopg = cast(ExecuteImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    generators_any = cast(Any, generators)

    def python_execute(pgconn: object) -> Generator[int, int | None, object]:
        original_send = generators_any.send
        original_fetch = generators_any.fetch
        original_fetch_many = generators_any.fetch_many
        generators_any.send = generators._send
        generators_any.fetch = generators._fetch
        generators_any.fetch_many = generators._fetch_many
        try:
            return (yield from generators._execute(pgconn))
        finally:
            generators_any.send = original_send
            generators_any.fetch = original_fetch
            generators_any.fetch_many = original_fetch_many

    python_impl = cast(ExecuteImpl, SimpleNamespace(execute=python_execute))
    return [("python", python_impl), ("rust", ferrocopg)]


def _pipeline_impls() -> list[tuple[str, PipelineImpl]]:
    ferrocopg = cast(PipelineImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    python_impl = cast(
        PipelineImpl,
        SimpleNamespace(pipeline_communicate=generators._pipeline_communicate),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _cancel_impls() -> list[tuple[str, CancelImpl]]:
    ferrocopg = cast(CancelImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    python_impl = cast(CancelImpl, SimpleNamespace(cancel=generators._cancel))
    return [("python", python_impl), ("rust", ferrocopg)]


def _connect_impls(
    monkeypatch: pytest.MonkeyPatch, conn_factory: Callable[[], StubConnectConn]
) -> list[tuple[str, ConnectImpl]]:
    ferrocopg = cast(ConnectImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    pq_module = importlib.import_module("psycopg.pq")

    fake_pgconn = SimpleNamespace(
        connect_start=staticmethod(lambda _conninfo: conn_factory())
    )
    monkeypatch.setattr(
        generators,
        "pq",
        SimpleNamespace(**{**generators.pq.__dict__, "PGconn": fake_pgconn}),
    )
    monkeypatch.setattr(pq_module, "PGconn", fake_pgconn)
    monkeypatch.setattr(generators.e, "finish_pgconn", lambda pgconn: pgconn)

    python_impl = cast(ConnectImpl, SimpleNamespace(connect=generators._connect))
    return [("python", python_impl), ("rust", ferrocopg)]


def _array_binary_impls() -> list[tuple[str, ArrayBinaryImpl]]:
    ferrocopg = cast(ArrayBinaryImpl, pytest.importorskip("ferrocopg_rust"))
    array_mod = importlib.import_module("psycopg.types.array")
    python_impl = cast(
        ArrayBinaryImpl,
        SimpleNamespace(array_load_binary=array_mod._load_binary),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _array_text_impls() -> list[tuple[str, ArrayTextImpl]]:
    ferrocopg = cast(ArrayTextImpl, pytest.importorskip("ferrocopg_rust"))
    array_mod = importlib.import_module("psycopg.types.array")
    python_impl = cast(
        ArrayTextImpl,
        SimpleNamespace(array_load_text=array_mod._load_text),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _uuid_text_impls() -> list[tuple[str, UUIDTextImpl]]:
    ferrocopg = cast(UUIDTextImpl, pytest.importorskip("ferrocopg_rust"))
    python_impl = cast(
        UUIDTextImpl,
        SimpleNamespace(
            uuid_load_text=lambda data: uuid.UUID(
                (bytes(data) if isinstance(data, memoryview) else data).decode()
            )
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _uuid_binary_impls() -> list[tuple[str, UUIDBinaryImpl]]:
    ferrocopg = cast(UUIDBinaryImpl, pytest.importorskip("ferrocopg_rust"))
    python_impl = cast(
        UUIDBinaryImpl,
        SimpleNamespace(
            uuid_load_binary=lambda data: uuid.UUID(
                bytes=(bytes(data) if isinstance(data, memoryview) else data)
            )
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _bool_impls() -> list[tuple[str, BoolImpl]]:
    ferrocopg = cast(BoolImpl, pytest.importorskip("ferrocopg_rust"))
    python_impl = cast(
        BoolImpl,
        SimpleNamespace(
            bool_dump_text=lambda obj: b"t" if obj else b"f",
            bool_dump_binary=lambda obj: b"\x01" if obj else b"\x00",
            bool_load_text=lambda data: data == b"t",
            bool_load_binary=lambda data: data != b"\x00",
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _string_impls() -> list[tuple[str, StringImpl]]:
    ferrocopg = cast(StringImpl, pytest.importorskip("ferrocopg_rust"))
    python_impl = cast(
        StringImpl,
        SimpleNamespace(
            str_dump_text=_python_str_dump_text,
            str_dump_binary=lambda obj, encoding: obj.encode(encoding),
            text_load=_python_text_load,
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _bytea_binary_impls() -> list[tuple[str, ByteaBinaryImpl]]:
    ferrocopg = cast(ByteaBinaryImpl, pytest.importorskip("ferrocopg_rust"))
    python_impl = cast(
        ByteaBinaryImpl,
        SimpleNamespace(
            bytes_dump_binary=lambda data: bytes(data),
            bytea_load_binary=lambda data: bytes(data),
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _composite_impls() -> list[tuple[str, CompositeImpl]]:
    ferrocopg = cast(CompositeImpl, pytest.importorskip("ferrocopg_rust"))
    composite_mod = cast(Any, importlib.import_module("psycopg.types.composite"))

    def python_dump_text_sequence(seq: object, tx: object) -> object:
        original = composite_mod._rpsycopg
        composite_mod._rpsycopg = None
        try:
            return composite_mod._dump_text_sequence(seq, tx)
        finally:
            composite_mod._rpsycopg = original

    def python_dump_binary_sequence(
        seq: object, types: object, formats: object, tx: object
    ) -> object:
        original = composite_mod._rpsycopg
        composite_mod._rpsycopg = None
        try:
            return composite_mod._dump_binary_sequence(seq, types, formats, tx)
        finally:
            composite_mod._rpsycopg = original

    def python_parse_text_record(data: object) -> object:
        original = composite_mod._rpsycopg
        composite_mod._rpsycopg = None
        try:
            return composite_mod._parse_text_record(data)
        finally:
            composite_mod._rpsycopg = original

    python_impl = cast(
        CompositeImpl,
        SimpleNamespace(
            composite_dump_text_sequence=python_dump_text_sequence,
            composite_dump_binary_sequence=python_dump_binary_sequence,
            composite_parse_text_record=python_parse_text_record,
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _numeric_impls() -> list[tuple[str, NumericImpl]]:
    ferrocopg = cast(NumericImpl, pytest.importorskip("ferrocopg_rust"))
    numeric_mod = cast(Any, importlib.import_module("psycopg.types.numeric"))

    def python_numeric_load_binary(data: object) -> object:
        original = numeric_mod._rpsycopg
        numeric_mod._rpsycopg = None
        try:
            return numeric_mod.NumericBinaryLoader(0).load(data)
        finally:
            numeric_mod._rpsycopg = original

    python_impl = cast(
        NumericImpl,
        SimpleNamespace(
            dump_decimal_to_text=numeric_mod.dump_decimal_to_text,
            dump_decimal_to_numeric_binary=numeric_mod.dump_decimal_to_numeric_binary,
            dump_int_to_numeric_binary=numeric_mod.dump_int_to_numeric_binary,
            numeric_load_text=lambda data: Decimal(bytes(data).decode()),
            numeric_load_binary=python_numeric_load_binary,
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _datetime_impls() -> list[tuple[str, DateTimeImpl]]:
    ferrocopg = cast(DateTimeImpl, pytest.importorskip("ferrocopg_rust"))
    dt_mod = cast(Any, importlib.import_module("psycopg.types.datetime"))

    def without_rpsycopg(func: Callable[[], object]) -> object:
        original = dt_mod._rpsycopg
        dt_mod._rpsycopg = None
        try:
            return func()
        finally:
            dt_mod._rpsycopg = original

    def timestamptz_load_binary(data: object, timezone_obj: object) -> object:
        def load() -> object:
            loader = dt_mod.TimestamptzBinaryLoader(0)
            loader._timezone = timezone_obj
            return loader.load(data)

        return without_rpsycopg(load)

    python_impl = cast(
        DateTimeImpl,
        SimpleNamespace(
            date_dump_text=lambda obj: str(obj).encode(),
            date_dump_binary=lambda obj: without_rpsycopg(
                lambda: dt_mod.DateBinaryDumper(date).dump(obj)
            ),
            date_load_binary=lambda data: without_rpsycopg(
                lambda: dt_mod.DateBinaryLoader(0).load(data)
            ),
            time_dump_text=lambda obj: str(obj).encode(),
            time_dump_binary=lambda obj: without_rpsycopg(
                lambda: dt_mod.TimeBinaryDumper(time).dump(obj)
            ),
            time_load_binary=lambda data: without_rpsycopg(
                lambda: dt_mod.TimeBinaryLoader(0).load(data)
            ),
            timetz_dump_binary=lambda obj: without_rpsycopg(
                lambda: dt_mod.TimeTzBinaryDumper(time).dump(obj)
            ),
            timetz_load_binary=lambda data: without_rpsycopg(
                lambda: dt_mod.TimetzBinaryLoader(0).load(data)
            ),
            datetime_dump_text=lambda obj: str(obj).encode(),
            datetime_dump_binary=lambda obj: without_rpsycopg(
                lambda: dt_mod.DatetimeBinaryDumper(datetime).dump(obj)
            ),
            datetime_notz_dump_binary=lambda obj: without_rpsycopg(
                lambda: dt_mod.DatetimeNoTzBinaryDumper(datetime).dump(obj)
            ),
            timestamp_load_binary=lambda data: without_rpsycopg(
                lambda: dt_mod.TimestampBinaryLoader(0).load(data)
            ),
            timestamptz_load_binary=timestamptz_load_binary,
            timedelta_dump_binary=lambda obj: without_rpsycopg(
                lambda: dt_mod.TimedeltaBinaryDumper(timedelta).dump(obj)
            ),
            interval_load_binary=lambda data: without_rpsycopg(
                lambda: dt_mod.IntervalBinaryLoader(0).load(data)
            ),
        ),
    )
    return [("python", python_impl), ("rust", ferrocopg)]


def _python_str_dump_text(obj: str, encoding: str) -> bytes:
    if "\x00" in obj:
        errors = importlib.import_module("psycopg.errors")
        raise errors.DataError("PostgreSQL text fields cannot contain NUL (0x00) bytes")
    return obj.encode(encoding)


def _python_text_load(data: object, encoding: str) -> bytes | str:
    raw = bytes(data) if isinstance(data, memoryview) else cast(bytes, data)
    return raw if not encoding else raw.decode(encoding)


@pytest.mark.parametrize(
    "adapted",
    [
        [],
        [b"plain", None, b"text"],
        [b"alpha\tbeta", b"line1\nline2", b"slash\\path"],
        [b"", b"\b\t\n\v\f\r\\", b"trailing space "],
    ],
)
def test_copy_text_helpers_equivalent(adapted):
    impls = _copy_impls()
    baseline = importlib.import_module("psycopg._copy_base")

    baseline_tx = StubCopyTransformer(adapted)
    expected_out = bytearray()
    baseline._format_row_text(tuple(range(len(adapted))), baseline_tx, expected_out)
    expected_row = _expected_text_row(expected_out, baseline_tx)

    for name, impl in impls:
        tx = StubCopyTransformer(adapted)
        out = bytearray()
        impl.format_row_text(tuple(range(len(adapted))), tx, out)
        assert bytes(out) == bytes(expected_out), name
        assert impl.parse_row_text(out, tx) == expected_row, name


@pytest.mark.parametrize(
    "adapted",
    [
        [],
        [b"plain", None, b"binary"],
        [b"\x00\x01\x02", b"alpha\tbeta", b"line1\nline2"],
        [b"", b"slash\\path", b"\xff\x10\x80"],
    ],
)
def test_copy_binary_helpers_equivalent(adapted):
    impls = _copy_impls()
    baseline = importlib.import_module("psycopg._copy_base")

    baseline_tx = StubCopyTransformer(adapted)
    expected_out = bytearray()
    baseline._format_row_binary(tuple(range(len(adapted))), baseline_tx, expected_out)
    expected_row = baseline._parse_row_binary(expected_out, baseline_tx)

    for name, impl in impls:
        tx = StubCopyTransformer(adapted)
        out = bytearray()
        impl.format_row_binary(tuple(range(len(adapted))), tx, out)
        assert bytes(out) == bytes(expected_out), name
        assert impl.parse_row_binary(out, tx) == expected_row, name


def _make_text_transformer(impl_name: str, nfields: int) -> Any:
    pq = importlib.import_module("psycopg.pq")
    text_oid = 25

    if impl_name == "c":
        tx = importlib.import_module("psycopg_c._psycopg").Transformer()
    else:
        tx = importlib.import_module("psycopg._py_transformer").Transformer()

    tx.set_dumper_types([text_oid] * nfields, pq.Format.TEXT)
    tx.set_loader_types([text_oid] * nfields, pq.Format.TEXT)
    return tx


def _make_int4_binary_transformer(impl_name: str, nfields: int) -> Any:
    pq = importlib.import_module("psycopg.pq")
    int4_oid = 23

    if impl_name == "c":
        tx = importlib.import_module("psycopg_c._psycopg").Transformer()
    else:
        tx = importlib.import_module("psycopg._py_transformer").Transformer()

    tx.set_dumper_types([int4_oid] * nfields, pq.Format.BINARY)
    tx.set_loader_types([int4_oid] * nfields, pq.Format.BINARY)
    return tx


@pytest.mark.parametrize(
    "row",
    [
        (),
        ("plain", None, "text"),
        ("alpha\tbeta", "line1\nline2", "slash\\path"),
        ("", "\b\t\n\v\f\r\\", "trailing space "),
    ],
)
def test_copy_text_helpers_equivalent_with_cython(row):
    pytest.importorskip("ferrocopg_rust")
    importlib.import_module("psycopg")
    cmodule = cast(CopyImpl, pytest.importorskip("psycopg_c._psycopg"))
    baseline = importlib.import_module("psycopg._copy_base")

    py_tx = _make_text_transformer("python", len(row))
    expected_out = bytearray()
    baseline._format_row_text(row, py_tx, expected_out)
    expected_row = () if not row else baseline._parse_row_text(expected_out, py_tx)

    rust = cast(CopyImpl, importlib.import_module("ferrocopg_rust"))
    for name, impl in [("rust", rust), ("c", cmodule)]:
        tx = _make_text_transformer(name, len(row))
        out = bytearray()
        impl.format_row_text(row, tx, out)
        assert bytes(out) == bytes(expected_out), name
        if name == "rust":
            assert impl.parse_row_text(out, tx) == expected_row, name


@pytest.mark.parametrize(
    "row",
    [
        (),
        (1, None, 2),
        (0, 42, -7),
        (2**15 - 1, -(2**15), 123456),
    ],
)
def test_copy_binary_helpers_equivalent_with_cython(row):
    pytest.importorskip("ferrocopg_rust")
    importlib.import_module("psycopg")
    cmodule = cast(CopyImpl, pytest.importorskip("psycopg_c._psycopg"))
    baseline = importlib.import_module("psycopg._copy_base")

    py_tx = _make_int4_binary_transformer("python", len(row))
    expected_out = bytearray()
    baseline._format_row_binary(row, py_tx, expected_out)
    expected_row = baseline._parse_row_binary(expected_out, py_tx)

    rust = cast(CopyImpl, importlib.import_module("ferrocopg_rust"))
    for name, impl in [("rust", rust), ("c", cmodule)]:
        tx = _make_int4_binary_transformer(name, len(row))
        out = bytearray()
        impl.format_row_binary(row, tx, out)
        assert bytes(out) == bytes(expected_out), name
        assert impl.parse_row_binary(out, tx) == expected_row, name


def test_wait_c_read_ready_equivalent():
    ferrocopg = pytest.importorskip("ferrocopg_rust")
    waiting = importlib.import_module("psycopg.waiting")

    reader, writer = socket.socketpair()
    try:
        writer.sendall(b"x")

        expected = waiting.wait_select(
            _wait_ready_gen(waiting.WAIT_R, waiting.READY_R, "python"),
            reader.fileno(),
            interval=0.01,
        )
        got = ferrocopg.wait_c(
            _wait_ready_gen(waiting.WAIT_R, waiting.READY_R, "rust"),
            reader.fileno(),
            interval=0.01,
        )

        assert expected == "python"
        assert got == "rust"
    finally:
        reader.close()
        writer.close()


def test_wait_c_timeout_equivalent():
    ferrocopg = pytest.importorskip("ferrocopg_rust")
    waiting = importlib.import_module("psycopg.waiting")

    reader, writer = socket.socketpair()
    try:
        expected = waiting.wait_select(
            _wait_ready_gen(waiting.WAIT_R, waiting.READY_NONE, "python-timeout"),
            reader.fileno(),
            interval=0.0,
        )
        got = ferrocopg.wait_c(
            _wait_ready_gen(waiting.WAIT_R, waiting.READY_NONE, "rust-timeout"),
            reader.fileno(),
            interval=0.0,
        )

        assert expected == "python-timeout"
        assert got == "rust-timeout"
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    ("flush_results", "ready_values", "expected_waits", "expected_consume_calls"),
    [
        ([0], [], [], 0),
        ([1, 0], [2], [3], 0),
        ([1, 1, 0], [0, 1, 2], [3, 3, 3], 1),
    ],
)
def test_send_generator_equivalent(
    flush_results: list[int],
    ready_values: list[int | None],
    expected_waits: list[int],
    expected_consume_calls: int,
) -> None:
    wait_rw = cast(int, importlib.import_module("psycopg.waiting").WAIT_RW)
    ready_r = cast(int, importlib.import_module("psycopg.waiting").READY_R)

    assert expected_waits == [wait_rw] * len(expected_waits)
    if expected_consume_calls:
        assert ready_r in [rv for rv in ready_values if rv]

    for name, impl in _send_impls():
        pgconn = StubSendPgconn(flush_results)
        waits, result = _drive_send_generator(impl.send(pgconn), ready_values)
        assert waits == expected_waits, name
        assert result is None, name
        assert pgconn.consume_input_calls == expected_consume_calls, name


def test_generators_prefers_ferrocopg_send_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.send is ferrocopg.send


@pytest.mark.parametrize(
    (
        "busy_results",
        "ready_values",
        "expected_waits",
        "expected_consume_calls",
        "notifies",
        "expected_notifies",
    ),
    [
        ([False], [], [], 0, [], []),
        ([True, False], [1], [1], 1, [], []),
        ([True, True, False], [0, 1, 1], [1, 1, 1], 2, ["n1", "n2"], ["n1", "n2"]),
    ],
)
def test_fetch_generator_equivalent(
    busy_results: list[bool],
    ready_values: list[int | None],
    expected_waits: list[int],
    expected_consume_calls: int,
    notifies: list[object],
    expected_notifies: list[object],
) -> None:
    wait_r = cast(int, importlib.import_module("psycopg.waiting").WAIT_R)

    assert expected_waits == [wait_r] * len(expected_waits)

    for name, impl in _fetch_impls():
        pgconn = StubFetchPgconn(busy_results, result="result", notifies=notifies)
        waits, result = _drive_fetch_generator(impl.fetch(pgconn), ready_values)
        assert waits == expected_waits, name
        assert result == "result", name
        assert pgconn.consume_input_calls == expected_consume_calls, name
        assert pgconn.notify_handler_calls == expected_notifies, name


def test_generators_prefers_ferrocopg_fetch_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.fetch is ferrocopg.fetch


@pytest.mark.parametrize(
    (
        "busy_sequences",
        "ready_values",
        "result_specs",
        "expected_waits",
        "expected_labels",
        "expected_consume_calls",
    ),
    [
        ([[]], [], [None], [], [], 0),
        ([[], []], [], [("COMMAND_OK", "ok"), None], [], ["ok"], 0),
        (
            [[True, False], []],
            [1],
            [("COMMAND_OK", "waited"), None],
            [1],
            ["waited"],
            1,
        ),
        ([[]], [], [("COPY_OUT", "copy")], [], ["copy"], 0),
        ([[]], [], [("PIPELINE_SYNC", "pipeline")], [], ["pipeline"], 0),
    ],
)
def test_fetch_many_generator_equivalent(
    busy_sequences: list[list[bool]],
    ready_values: list[int | None],
    result_specs: list[tuple[str, str] | None],
    expected_waits: list[int],
    expected_labels: list[str],
    expected_consume_calls: int,
) -> None:
    wait_r = cast(int, importlib.import_module("psycopg.waiting").WAIT_R)
    exec_status = importlib.import_module("psycopg.pq").ExecStatus
    results = [
        None if spec is None else StubResult(getattr(exec_status, spec[0]), spec[1])
        for spec in result_specs
    ]

    assert expected_waits == [wait_r] * len(expected_waits)

    for name, impl in _fetch_many_impls():
        pgconn = StubFetchManyPgconn(busy_sequences, results)
        waits, got = _drive_fetch_many_generator(impl.fetch_many(pgconn), ready_values)
        assert waits == expected_waits, name
        assert [res.label for res in cast(list[StubResult], got)] == expected_labels, (
            name
        )
        assert pgconn.consume_input_calls == expected_consume_calls, name


def test_generators_prefers_ferrocopg_fetch_many_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.fetch_many is ferrocopg.fetch_many


@pytest.mark.parametrize(
    (
        "flush_results",
        "busy_sequences",
        "ready_values",
        "result_specs",
        "expected_waits",
        "expected_labels",
        "expected_consume_calls",
    ),
    [
        ([0], [[]], [], [None], [], [], 0),
        ([1, 0], [[], []], [2], [("COMMAND_OK", "sent"), None], [3], ["sent"], 0),
        (
            [0],
            [[True, False], []],
            [1],
            [("COMMAND_OK", "fetched"), None],
            [1],
            ["fetched"],
            1,
        ),
        (
            [1, 0],
            [[True, False], []],
            [2, 1],
            [("COMMAND_OK", "both"), None],
            [3, 1],
            ["both"],
            1,
        ),
    ],
)
def test_execute_generator_equivalent(
    flush_results: list[int],
    busy_sequences: list[list[bool]],
    ready_values: list[int | None],
    result_specs: list[tuple[str, str] | None],
    expected_waits: list[int],
    expected_labels: list[str],
    expected_consume_calls: int,
) -> None:
    wait_r = cast(int, importlib.import_module("psycopg.waiting").WAIT_R)
    wait_rw = cast(int, importlib.import_module("psycopg.waiting").WAIT_RW)
    exec_status = importlib.import_module("psycopg.pq").ExecStatus
    results = [
        None if spec is None else StubResult(getattr(exec_status, spec[0]), spec[1])
        for spec in result_specs
    ]

    translated_waits = [wait_rw if wait == 3 else wait_r for wait in expected_waits]

    for name, impl in _execute_impls():
        pgconn = StubFetchManyPgconn(busy_sequences, results)
        pgconn._flush_results = list(flush_results)
        waits, got = _drive_execute_generator(impl.execute(pgconn), ready_values)
        assert waits == translated_waits, name
        assert [res.label for res in cast(list[StubResult], got)] == expected_labels, (
            name
        )
        assert pgconn.consume_input_calls == expected_consume_calls, name


def test_generators_prefers_ferrocopg_execute_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.execute is ferrocopg.execute


@pytest.mark.parametrize(
    (
        "ready_values",
        "read_cycles",
        "expected_waits",
        "expected_labels",
        "expected_command_calls",
        "expected_consume_calls",
        "expected_flush_calls",
        "expected_notifies",
    ),
    [
        ([2, 2], [], [3, 3], [], ["cmd1"], 0, 2, []),
        (
            [3, 2, 2],
            [([False], [("COMMAND_OK", "row"), None], [])],
            [3, 3, 3],
            [["row"]],
            ["cmd1", "cmd2"],
            1,
            3,
            [],
        ),
        (
            [3, 2],
            [([False], [("PIPELINE_SYNC", "sync")], ["n1"])],
            [3, 3],
            [["sync"]],
            ["cmd1"],
            1,
            2,
            ["n1"],
        ),
    ],
)
def test_pipeline_communicate_equivalent(
    ready_values: list[int | None],
    read_cycles: list[tuple[list[bool], list[tuple[str, str] | None], list[object]]],
    expected_waits: list[int],
    expected_labels: list[list[str]],
    expected_command_calls: list[str],
    expected_consume_calls: int,
    expected_flush_calls: int,
    expected_notifies: list[object],
) -> None:
    wait_rw = cast(int, importlib.import_module("psycopg.waiting").WAIT_RW)
    exec_status = importlib.import_module("psycopg.pq").ExecStatus

    assert expected_waits == [wait_rw] * len(expected_waits)

    for name, impl in _pipeline_impls():
        command_calls: list[str] = []
        commands = deque(
            [
                (lambda label=label: command_calls.append(label))
                for label in expected_command_calls
            ]
        )
        pgconn = StubPipelinePgconn(
            [
                (
                    busy,
                    [
                        None
                        if result is None
                        else StubResult(getattr(exec_status, result[0]), result[1])
                        for result in results
                    ],
                    notifies,
                )
                for busy, results, notifies in read_cycles
            ]
        )
        waits, got = _drive_pipeline_generator(
            impl.pipeline_communicate(pgconn, commands), ready_values
        )
        assert waits == expected_waits, name
        assert [
            [res.label for res in batch] for batch in cast(list[list[StubResult]], got)
        ] == expected_labels, name
        assert command_calls == expected_command_calls, name
        assert pgconn.consume_input_calls == expected_consume_calls, name
        assert pgconn.flush_calls == expected_flush_calls, name
        assert pgconn.notify_handler_calls == expected_notifies, name


def test_generators_prefers_ferrocopg_pipeline_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.pipeline_communicate is ferrocopg.pipeline_communicate


@pytest.mark.parametrize(
    ("statuses", "expected_waits"),
    [
        (["READING", "OK"], [(42, 1)]),
        (["WRITING", "OK"], [(42, 2)]),
        (["READING", "WRITING", "OK"], [(42, 1), (42, 2)]),
    ],
)
def test_cancel_generator_equivalent(
    statuses: list[str],
    expected_waits: list[tuple[int, int]],
) -> None:
    waiting = importlib.import_module("psycopg.waiting")
    polling_status = importlib.import_module("psycopg.pq").PollingStatus
    cancel_statuses = [getattr(polling_status, status) for status in statuses]
    translated_waits = [
        (fileno, waiting.WAIT_R if wait == 1 else waiting.WAIT_W)
        for fileno, wait in expected_waits
    ]

    for name, impl in _cancel_impls():
        cancel_conn = StubCancelConn(cancel_statuses)
        waits, result = _drive_cancel_generator(
            impl.cancel(cancel_conn), [1] * len(expected_waits)
        )
        assert waits == translated_waits, name
        assert result is None, name


def test_generators_prefers_ferrocopg_cancel_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.cancel is ferrocopg.cancel


@pytest.mark.parametrize(
    ("poll_status_names", "ready_values", "expected_waits"),
    [
        (["OK"], [], []),
        (["READING", "OK"], [1], [(42, 1)]),
        (["WRITING", "OK"], [1], [(42, 2)]),
        (["READING", "READING", "OK"], [0, 1, 1], [(42, 1), (42, 1), (42, 1)]),
    ],
)
def test_connect_generator_equivalent(
    monkeypatch: pytest.MonkeyPatch,
    poll_status_names: list[str],
    ready_values: list[int | None],
    expected_waits: list[tuple[int, int]],
) -> None:
    waiting = importlib.import_module("psycopg.waiting")
    pq = importlib.import_module("psycopg.pq")
    poll_statuses = [getattr(pq.PollingStatus, name) for name in poll_status_names]
    translated_waits = [
        (fileno, waiting.WAIT_R if wait == 1 else waiting.WAIT_W)
        for fileno, wait in expected_waits
    ]

    for name, impl in _connect_impls(
        monkeypatch,
        lambda: StubConnectConn(pq.ConnStatus.OK, poll_statuses),
    ):
        waits, result = _drive_connect_generator(
            impl.connect("host=example dbname=test"),
            ready_values,
        )
        assert waits == translated_waits, name
        assert cast(StubConnectConn, result).nonblocking == 1, name


def test_generators_prefers_ferrocopg_connect_when_available():
    generators = importlib.import_module("psycopg.generators")
    if generators._psycopg is not None:
        pytest.skip("C accelerator installed")

    ferrocopg = pytest.importorskip("ferrocopg_rust")
    assert generators.connect is ferrocopg.connect


@pytest.mark.parametrize(
    ("dims", "values", "expected"),
    [
        ([], [], []),
        ([3], [1, None, 7], [1, None, 7]),
        ([2, 2], [1, 2, 3, 4], [[1, 2], [3, 4]]),
    ],
)
def test_array_load_binary_equivalent(
    dims: list[int],
    values: list[int | None],
    expected: list[object],
) -> None:
    def pack_array_payload() -> bytes:
        oid = 23
        data = bytearray()
        data.extend(len(dims).to_bytes(4, "big"))
        data.extend(int(any(v is None for v in values)).to_bytes(4, "big"))
        data.extend(oid.to_bytes(4, "big"))
        for dim in dims:
            data.extend(dim.to_bytes(4, "big"))
            data.extend((1).to_bytes(4, "big"))
        for value in values:
            if value is None:
                data.extend((-1).to_bytes(4, "big", signed=True))
            else:
                payload = int(value).to_bytes(4, "big", signed=True)
                data.extend(len(payload).to_bytes(4, "big", signed=True))
                data.extend(payload)
        return bytes(data)

    tx = StubArrayTransformer(lambda data: int.from_bytes(data, "big", signed=True))
    payload = pack_array_payload()

    for name, impl in _array_binary_impls():
        assert impl.array_load_binary(payload, tx) == expected, name


def test_array_binary_loader_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.array")

    class StubRustModule:
        @staticmethod
        def array_load_binary(data: object, tx: object) -> tuple[str, object, object]:
            return ("rust", data, tx)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    loader = module.ArrayBinaryLoader(None)
    loader._tx = "tx"
    assert loader.load(b"abc") == ("rust", b"abc", "tx")


@pytest.mark.parametrize(
    ("payload", "delimiter", "expected"),
    [
        (b"{}", b",", []),
        (b"{1,NULL,7}", b",", [1, None, 7]),
        (b"{{1,2},{3,4}}", b",", [[1, 2], [3, 4]]),
        (b"[1:2]={1;2}", b";", [1, 2]),
        (b'{"a,b","c\\\\d"}', b",", ["a,b", "c\\d"]),
    ],
)
def test_array_load_text_equivalent(
    payload: bytes, delimiter: bytes, expected: list[object]
) -> None:
    loader = StubArrayLoader(
        lambda data: int(data) if data.isdigit() else data.decode()
    )
    for name, impl in _array_text_impls():
        assert impl.array_load_text(payload, loader, delimiter) == expected, name


def test_array_text_loader_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.array")

    class StubRustModule:
        @staticmethod
        def array_load_text(
            data: object, loader: object, delimiter: bytes = b","
        ) -> tuple[str, object, object, bytes]:
            return ("rust", data, loader, delimiter)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    loader = module.ArrayLoader(None)
    loader._tx = SimpleNamespace(get_loader=lambda oid, fmt: ("loader", oid, fmt))
    loader.base_oid = 23
    loader.delimiter = b";"
    assert loader.load(b"abc") == ("rust", b"abc", ("loader", 23, loader.format), b";")


@pytest.mark.parametrize(
    "payload",
    [
        b"12345678-1234-5678-1234-567812345678",
        memoryview(b"{12345678-1234-5678-1234-567812345678}"),
        b"12345678123456781234567812345678",
    ],
)
def test_uuid_load_text_equivalent(payload: bytes | memoryview) -> None:
    for name, impl in _uuid_text_impls():
        assert impl.uuid_load_text(payload) == uuid.UUID(
            "12345678-1234-5678-1234-567812345678"
        ), name


@pytest.mark.parametrize(
    "payload",
    [
        b"\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78",
        memoryview(b"\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78"),
    ],
)
def test_uuid_load_binary_equivalent(payload: bytes | memoryview) -> None:
    expected = uuid.UUID("12345678-1234-5678-1234-567812345678")
    for name, impl in _uuid_binary_impls():
        assert impl.uuid_load_binary(payload) == expected, name


def test_uuid_loader_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.uuid")

    class StubRustModule:
        @staticmethod
        def uuid_load_text(data: object) -> tuple[str, object]:
            return ("rust", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.UUIDLoader(2950).load(b"abc") == ("rust", b"abc")


def test_uuid_binary_loader_prefers_ferrocopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg.types.uuid")

    class StubRustModule:
        @staticmethod
        def uuid_load_binary(data: object) -> tuple[str, object]:
            return ("rust", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.UUIDBinaryLoader(2950).load(b"abc") == ("rust", b"abc")


@pytest.mark.parametrize("value", [True, False])
def test_bool_helpers_equivalent(value: bool) -> None:
    for name, impl in _bool_impls():
        expected_text = b"t" if value else b"f"
        expected_binary = b"\x01" if value else b"\x00"
        assert impl.bool_dump_text(value) == expected_text, name
        assert impl.bool_dump_binary(value) == expected_binary, name
        assert impl.bool_load_text(expected_text) is value, name
        assert impl.bool_load_binary(expected_binary) is value, name


def test_bool_dumpers_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.bool")

    class StubRustModule:
        @staticmethod
        def bool_dump_text(obj: bool) -> tuple[str, bool]:
            return ("text", obj)

        @staticmethod
        def bool_dump_binary(obj: bool) -> tuple[str, bool]:
            return ("binary", obj)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.BoolDumper(bool).dump(True) == ("text", True)
    assert module.BoolBinaryDumper(bool).dump(False) == ("binary", False)


def test_bool_loaders_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.bool")

    class StubRustModule:
        @staticmethod
        def bool_load_text(data: object) -> tuple[str, object]:
            return ("text", data)

        @staticmethod
        def bool_load_binary(data: object) -> tuple[str, object]:
            return ("binary", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.BoolLoader(16).load(b"t") == ("text", b"t")
    assert module.BoolBinaryLoader(16).load(b"\x01") == ("binary", b"\x01")


@pytest.mark.parametrize(
    ("value", "encoding", "expected"),
    [
        ("plain", "utf-8", b"plain"),
        ("cafe", "latin-1", b"cafe"),
        ("cafe", "utf-8", b"cafe"),
        ("café", "utf-8", "café".encode("utf-8")),
        ("café", "latin-1", "café".encode("latin-1")),
    ],
)
def test_string_dump_helpers_equivalent(
    value: str, encoding: str, expected: bytes
) -> None:
    for name, impl in _string_impls():
        assert impl.str_dump_binary(value, encoding) == expected, name
        assert impl.str_dump_text(value, encoding) == expected, name


def test_string_dump_text_rejects_nul() -> None:
    errors = importlib.import_module("psycopg.errors")
    for _name, impl in _string_impls():
        with pytest.raises(errors.DataError, match="cannot contain NUL"):
            impl.str_dump_text("bad\x00text", "utf-8")


@pytest.mark.parametrize(
    ("payload", "encoding", "expected"),
    [
        (b"plain", "utf-8", "plain"),
        ("café".encode("utf-8"), "utf-8", "café"),
        ("café".encode("latin-1"), "latin-1", "café"),
        (b"plain", "", b"plain"),
    ],
)
def test_text_load_equivalent(
    payload: bytes, encoding: str, expected: bytes | str
) -> None:
    for name, impl in _string_impls():
        assert impl.text_load(payload, encoding) == expected, name


def test_string_dumpers_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.string")

    class StubRustModule:
        @staticmethod
        def str_dump_text(obj: str, encoding: str) -> tuple[str, str, str]:
            return ("text", obj, encoding)

        @staticmethod
        def str_dump_binary(obj: str, encoding: str) -> tuple[str, str, str]:
            return ("binary", obj, encoding)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    text_dumper = module.StrDumper(str)
    text_dumper._encoding = "latin-1"
    binary_dumper = module.StrBinaryDumper(str)
    binary_dumper._encoding = "utf-8"
    assert text_dumper.dump("abc") == ("text", "abc", "latin-1")
    assert binary_dumper.dump("abc") == ("binary", "abc", "utf-8")


def test_text_loaders_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.string")

    class StubRustModule:
        @staticmethod
        def text_load(data: object, encoding: str) -> tuple[str, object, str]:
            return ("load", data, encoding)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    text_loader = module.TextLoader(25)
    text_loader._encoding = "latin-1"
    binary_loader = module.TextBinaryLoader(25)
    binary_loader._encoding = ""
    assert text_loader.load(b"abc") == ("load", b"abc", "latin-1")
    assert binary_loader.load(b"abc") == ("load", b"abc", "")


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\x00\x01binary",
        bytearray(b"bytearray-data"),
        memoryview(b"memoryview-data"),
    ],
)
def test_bytea_binary_helpers_equivalent(
    payload: bytes | bytearray | memoryview,
) -> None:
    expected = bytes(payload)
    for name, impl in _bytea_binary_impls():
        assert impl.bytes_dump_binary(payload) == expected, name
        assert impl.bytea_load_binary(payload) == expected, name


def test_bytea_binary_dumper_prefers_ferrocopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg.types.string")

    class StubRustModule:
        @staticmethod
        def bytes_dump_binary(data: object) -> tuple[str, object]:
            return ("dump", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.BytesBinaryDumper(bytes).dump(b"abc") == ("dump", b"abc")


def test_bytea_binary_loader_prefers_ferrocopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg.types.string")

    class StubRustModule:
        @staticmethod
        def bytea_load_binary(data: object) -> tuple[str, object]:
            return ("load", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.ByteaBinaryLoader(17).load(b"abc") == ("load", b"abc")


def test_composite_dump_text_sequence_equivalent() -> None:
    seq = ("plain", "needs,quotes", 'say"hi', None, "")
    tx = StubCompositeTransformer(
        {
            "plain": b"plain",
            "needs,quotes": b"needs,quotes",
            'say"hi': b'say"hi',
            "": b"",
        },
        [],
    )

    for name, impl in _composite_impls():
        assert (
            impl.composite_dump_text_sequence(seq, tx)
            == b'(plain,"needs,quotes","say""hi",,"")'
        ), name


def test_composite_dump_binary_sequence_equivalent() -> None:
    seq = ("alpha", None, "omega")
    tx = StubCompositeTransformer({}, [b"a", None, b"xyz"])
    types = [23, 25, 23]
    formats = [object(), object(), object()]

    expected = (
        b"\x00\x00\x00\x03"
        b"\x00\x00\x00\x17\x00\x00\x00\x01a"
        b"\x00\x00\x00\x19\xff\xff\xff\xff"
        b"\x00\x00\x00\x17\x00\x00\x00\x03xyz"
    )
    for name, impl in _composite_impls():
        assert (
            impl.composite_dump_binary_sequence(seq, types, formats, tx) == expected
        ), name


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"foo,bar", [b"foo", b"bar"]),
        (b'"a","b""c",', [b"a", b'b"c', None]),
        (b'"\\\\",plain', [b"\\", b"plain"]),
        (b",", [None, None]),
        (b'"",plain', [b"", b"plain"]),
    ],
)
def test_composite_parse_text_record_equivalent(
    payload: bytes, expected: list[bytes | None]
) -> None:
    for name, impl in _composite_impls():
        assert impl.composite_parse_text_record(payload) == expected, name


def test_composite_helpers_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.composite")
    calls: list[tuple[str, object]] = []

    class StubRustModule:
        @staticmethod
        def composite_dump_text_sequence(
            seq: object, tx: object
        ) -> tuple[str, object, object]:
            return ("text", seq, tx)

        @staticmethod
        def composite_dump_binary_sequence(
            seq: object, types: object, formats: object, tx: object
        ) -> bytes:
            calls.append(("binary", (seq, types, formats, tx)))
            return b"rust-binary"

        @staticmethod
        def composite_parse_text_record(data: object) -> tuple[str, object]:
            return ("parse", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    tx = StubCompositeTransformer({}, [])
    formats = [object()]
    assert module._dump_text_sequence(("x",), tx) == ("text", ("x",), tx)
    assert module._dump_binary_sequence(("x",), [1], formats, tx) == bytearray(
        b"rust-binary"
    )
    assert calls == [("binary", (("x",), [1], formats, tx))]
    assert module._parse_text_record(b"foo") == ("parse", b"foo")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("12.3400"), b"12.3400"),
        (Decimal("-0.0012"), b"-0.0012"),
        (Decimal("NaN"), b"NaN"),
    ],
)
def test_numeric_decimal_text_equivalent(value: Decimal, expected: bytes) -> None:
    for name, impl in _numeric_impls():
        assert impl.dump_decimal_to_text(value) == expected, name


@pytest.mark.parametrize(
    "value",
    [
        Decimal("0"),
        Decimal("12.3400"),
        Decimal("-0.0012"),
        Decimal("10000"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("NaN"),
    ],
)
def test_numeric_decimal_binary_equivalent(value: Decimal) -> None:
    for name, impl in _numeric_impls():
        assert impl.dump_decimal_to_numeric_binary(value) == importlib.import_module(
            "psycopg.types.numeric"
        ).dump_decimal_to_numeric_binary(value), name


@pytest.mark.parametrize("value", [0, 42, -10000, 10**30 + 12345])
def test_numeric_int_binary_equivalent(value: int) -> None:
    for name, impl in _numeric_impls():
        assert impl.dump_int_to_numeric_binary(value) == importlib.import_module(
            "psycopg.types.numeric"
        ).dump_int_to_numeric_binary(value), name


@pytest.mark.parametrize("payload", [b"123.45", memoryview(b"-0.0012")])
def test_numeric_text_load_equivalent(payload: bytes | memoryview) -> None:
    for name, impl in _numeric_impls():
        assert impl.numeric_load_text(payload) == Decimal(bytes(payload).decode()), name


@pytest.mark.parametrize(
    "value", [Decimal("12.34"), Decimal("-0.0012"), Decimal("NaN")]
)
def test_numeric_binary_load_equivalent(value: Decimal) -> None:
    payload = importlib.import_module(
        "psycopg.types.numeric"
    ).dump_decimal_to_numeric_binary(value)
    for name, impl in _numeric_impls():
        result = cast(Decimal, impl.numeric_load_binary(payload))
        if value.is_nan():
            assert result.is_nan(), name
        else:
            assert result == value, name


def test_numeric_helpers_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.numeric")

    class StubRustModule:
        @staticmethod
        def dump_decimal_to_text(obj: object) -> tuple[str, object]:
            return ("text", obj)

        @staticmethod
        def dump_decimal_to_numeric_binary(obj: object) -> bytes:
            return b"decimal-binary"

        @staticmethod
        def dump_int_to_numeric_binary(obj: object) -> bytes:
            return b"int-binary"

        @staticmethod
        def numeric_load_text(data: object) -> tuple[str, object]:
            return ("load-text", data)

        @staticmethod
        def numeric_load_binary(data: object) -> tuple[str, object]:
            return ("load-binary", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.DecimalDumper(Decimal).dump(Decimal("1.2")) == (
        "text",
        Decimal("1.2"),
    )
    assert module.DecimalBinaryDumper(Decimal).dump(Decimal("1.2")) == b"decimal-binary"
    assert module.IntNumericBinaryDumper(int).dump(42) == b"int-binary"
    assert module.NumericLoader(0).load(b"12.3") == ("load-text", b"12.3")
    assert module.NumericBinaryLoader(0).load(b"payload") == ("load-binary", b"payload")


def test_datetime_date_helpers_equivalent() -> None:
    value = date(2024, 1, 2)
    for name, impl in _datetime_impls():
        assert impl.date_dump_text(value) == b"2024-01-02", name
        payload = impl.date_dump_binary(value)
        assert impl.date_load_binary(payload) == value, name


def test_datetime_time_helpers_equivalent() -> None:
    value = time(3, 4, 5, 678901)
    for name, impl in _datetime_impls():
        assert impl.time_dump_text(value) == b"03:04:05.678901", name
        payload = impl.time_dump_binary(value)
        assert impl.time_load_binary(payload) == value, name


def test_datetime_timetz_helpers_equivalent() -> None:
    value = time(3, 4, 5, 678901, timezone(timedelta(hours=-10, minutes=-20)))
    for name, impl in _datetime_impls():
        payload = impl.timetz_dump_binary(value)
        assert impl.timetz_load_binary(payload) == value, name


def test_datetime_timestamp_helpers_equivalent() -> None:
    naive = datetime(2024, 1, 2, 3, 4, 5, 678901)
    aware = datetime(2024, 1, 2, 3, 4, 5, 678901, timezone(timedelta(hours=2)))
    target_tz = timezone.utc

    for name, impl in _datetime_impls():
        assert impl.datetime_dump_text(naive) == b"2024-01-02 03:04:05.678901", name
        naive_payload = impl.datetime_notz_dump_binary(naive)
        aware_payload = impl.datetime_dump_binary(aware)
        assert impl.timestamp_load_binary(naive_payload) == naive, name
        assert impl.timestamptz_load_binary(
            aware_payload, target_tz
        ) == aware.astimezone(target_tz), name


def test_datetime_interval_helpers_equivalent() -> None:
    value = timedelta(days=3, seconds=3661, microseconds=42)
    for name, impl in _datetime_impls():
        payload = impl.timedelta_dump_binary(value)
        assert impl.interval_load_binary(payload) == value, name


def test_datetime_helpers_prefers_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg.types.datetime")

    class StubRustModule:
        @staticmethod
        def date_dump_text(obj: object) -> tuple[str, object]:
            return ("date-text", obj)

        @staticmethod
        def date_dump_binary(obj: object) -> bytes:
            return b"date-binary"

        @staticmethod
        def date_load_binary(data: object) -> tuple[str, object]:
            return ("date-load", data)

        @staticmethod
        def time_dump_text(obj: object) -> tuple[str, object]:
            return ("time-text", obj)

        @staticmethod
        def time_dump_binary(obj: object) -> bytes:
            return b"time-binary"

        @staticmethod
        def time_load_binary(data: object) -> tuple[str, object]:
            return ("time-load", data)

        @staticmethod
        def timetz_dump_binary(obj: object) -> bytes:
            return b"timetz-binary"

        @staticmethod
        def timetz_load_binary(data: object) -> tuple[str, object]:
            return ("timetz-load", data)

        @staticmethod
        def datetime_dump_text(obj: object) -> tuple[str, object]:
            return ("datetime-text", obj)

        @staticmethod
        def datetime_dump_binary(obj: object) -> bytes:
            return b"datetime-binary"

        @staticmethod
        def datetime_notz_dump_binary(obj: object) -> bytes:
            return b"datetime-notz-binary"

        @staticmethod
        def timestamp_load_binary(data: object) -> tuple[str, object]:
            return ("timestamp-load", data)

        @staticmethod
        def timestamptz_load_binary(
            data: object, timezone_obj: object
        ) -> tuple[str, object, object]:
            return ("timestamptz-load", data, timezone_obj)

        @staticmethod
        def timedelta_dump_binary(obj: object) -> bytes:
            return b"interval-binary"

        @staticmethod
        def interval_load_binary(data: object) -> tuple[str, object]:
            return ("interval-load", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.DateDumper(date).dump(date(2024, 1, 2)) == (
        "date-text",
        date(2024, 1, 2),
    )
    assert module.DateBinaryDumper(date).dump(date(2024, 1, 2)) == b"date-binary"
    assert module.DateBinaryLoader(0).load(b"x") == ("date-load", b"x")
    assert module.TimeDumper(time).dump(time(1, 2, 3)) == ("time-text", time(1, 2, 3))
    assert module.TimeBinaryDumper(time).dump(time(1, 2, 3)) == b"time-binary"
    assert module.TimeBinaryLoader(0).load(b"x") == ("time-load", b"x")
    assert (
        module.TimeTzBinaryDumper(time).dump(time(1, 2, 3, tzinfo=timezone.utc))
        == b"timetz-binary"
    )
    assert module.TimetzBinaryLoader(0).load(b"x") == ("timetz-load", b"x")
    assert module.DatetimeDumper(datetime).dump(datetime(2024, 1, 2, 3, 4, 5)) == (
        "datetime-text",
        datetime(2024, 1, 2, 3, 4, 5),
    )
    assert (
        module.DatetimeBinaryDumper(datetime).dump(
            datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        )
        == b"datetime-binary"
    )
    assert (
        module.DatetimeNoTzBinaryDumper(datetime).dump(datetime(2024, 1, 2, 3, 4, 5))
        == b"datetime-notz-binary"
    )
    assert module.TimestampBinaryLoader(0).load(b"x") == ("timestamp-load", b"x")
    ts_loader = module.TimestamptzBinaryLoader(0)
    ts_loader._timezone = timezone.utc
    assert ts_loader.load(b"x") == ("timestamptz-load", b"x", timezone.utc)
    assert (
        module.TimedeltaBinaryDumper(timedelta).dump(timedelta(seconds=1))
        == b"interval-binary"
    )
    assert module.IntervalBinaryLoader(0).load(b"x") == ("interval-load", b"x")


def test_transformer_prefers_ferrocopg_when_c_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = cast(Any, importlib.import_module("psycopg._transformer"))
    py_transformer = importlib.import_module("psycopg._py_transformer")

    class StubRustModule:
        Transformer = object()

    monkeypatch.setattr(module, "_psycopg", None)
    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    monkeypatch.setattr(module, "Transformer", py_transformer.Transformer)

    if module._rpsycopg and hasattr(module._rpsycopg, "Transformer"):
        module.Transformer = module._rpsycopg.Transformer

    assert module.Transformer is StubRustModule.Transformer


def test_ferrocopg_unavailable(monkeypatch):
    module = importlib.import_module("psycopg._ferrocopg")

    monkeypatch.setattr(module, "_ferrocopg", None)

    assert module.is_available() is False
    assert module.conninfo_summary("host=localhost") is None
    assert module.connect_plan("host=localhost") is None
    assert module.connect_target("host=localhost") is None
    assert module.connect_no_tls_probe("host=localhost") is None
    assert module.query_text_no_tls("host=localhost", "select 1") is None
    assert module.simple_query_no_tls("host=localhost", "select 1") is None
    assert module.simple_query_results_no_tls("host=localhost", "select 1") is None
    assert (
        module.pipeline_simple_query_results_no_tls(
            "host=localhost", ["select 1", "select 2"]
        )
        is None
    )
    assert (
        module.query_text_params_no_tls("host=localhost", "select $1::text", ["x"])
        is None
    )
    assert (
        module.run_text_params_no_tls("host=localhost", "select $1::text", ["x"])
        is None
    )
    assert module.execute_text_params_no_tls("host=localhost", "select 1", []) is None
    assert module.describe_text_no_tls("host=localhost", "select 1") is None
    assert module.no_tls_session("host=localhost") is None
    assert module.backend_session("host=localhost") is None
    assert module.no_tls_session_adapter("host=localhost") is None
    assert module.backend_session_adapter("host=localhost") is None


def test_ferrocopg_param_text_coerces_timedelta() -> None:
    module = importlib.import_module("psycopg._ferrocopg")
    value = timedelta(days=3, seconds=3661, microseconds=42)

    assert module._coerce_native_params([value]) == ["3 days 1:01:01.000042"]

    tx = module._TextCopyTransformer("utf-8")
    assert tx.dump_sequence([value], [object()]) == [b"3 days 1:01:01.000042"]


def test_split_extended_statements() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    assert module._split_extended_statements(
        "select ';' as quoted; select \"semi;colon\"; "
        "select $tag$semi;colon$tag$; "
        "/* outer ; /* inner ; */ */ select 4; -- trailing ;\nselect 5"
    ) == [
        "select ';' as quoted",
        'select "semi;colon"',
        "select $tag$semi;colon$tag$",
        "/* outer ; /* inner ; */ */ select 4",
        "-- trailing ;\nselect 5",
    ]


def test_ferrocopg_wrapper(monkeypatch):
    module = importlib.import_module("psycopg._ferrocopg")

    calls: list[tuple[str, str]] = []

    class StubRustModule:
        @staticmethod
        def parse_conninfo_summary(conninfo: str) -> tuple[str, str]:
            calls.append(("summary", conninfo))
            return ("summary", conninfo)

        @staticmethod
        def parse_connect_plan(conninfo: str) -> tuple[str, str]:
            calls.append(("plan", conninfo))
            return ("plan", conninfo)

        @staticmethod
        def parse_connect_target(conninfo: str) -> tuple[str, str]:
            calls.append(("target", conninfo))
            return ("target", conninfo)

        @staticmethod
        def probe_connect_no_tls(conninfo: str) -> tuple[str, str]:
            calls.append(("probe", conninfo))
            return ("probe", conninfo)

        @staticmethod
        def query_text_no_tls(conninfo: str, query: str) -> tuple[str, str, str]:
            calls.append(("query", conninfo))
            return ("query", conninfo, query)

        @staticmethod
        def simple_query_no_tls(conninfo: str, query: str) -> tuple[str, str, str]:
            calls.append(("simple-query", conninfo))
            return ("simple-query", conninfo, query)

        @staticmethod
        def simple_query_results_no_tls(
            conninfo: str, query: str
        ) -> tuple[str, str, str]:
            calls.append(("simple-query-results", conninfo))
            return ("simple-query-results", conninfo, query)

        @staticmethod
        def pipeline_simple_query_results_no_tls(
            conninfo: str, queries: list[str]
        ) -> tuple[str, str, list[str]]:
            calls.append(("pipeline-simple-query-results", conninfo))
            return ("pipeline-simple-query-results", conninfo, queries)

        @staticmethod
        def query_text_params_no_tls(
            conninfo: str, query: str, params: list[str | None]
        ) -> tuple[str, str, str, list[str | None]]:
            calls.append(("query-params", conninfo))
            return ("query-params", conninfo, query, params)

        @staticmethod
        def run_text_params_no_tls(
            conninfo: str, query: str, params: list[str | None]
        ) -> tuple[str, str, str, list[str | None]]:
            calls.append(("run-params", conninfo))
            return ("run-params", conninfo, query, params)

        @staticmethod
        def execute_text_params_no_tls(
            conninfo: str, query: str, params: list[str | None]
        ) -> tuple[str, str, str, list[str | None]]:
            calls.append(("execute-params", conninfo))
            return ("execute-params", conninfo, query, params)

        @staticmethod
        def describe_text_no_tls(conninfo: str, query: str) -> tuple[str, str, str]:
            calls.append(("describe", conninfo))
            return ("describe", conninfo, query)

        @staticmethod
        def connect_no_tls_session(conninfo: str) -> tuple[str, str]:
            calls.append(("session", conninfo))
            return ("session", conninfo)

        @staticmethod
        def connect_session(conninfo: str) -> tuple[str, str]:
            calls.append(("backend-session", conninfo))
            return ("backend-session", conninfo)

    monkeypatch.setattr(module, "_ferrocopg", StubRustModule)

    assert module.is_available() is True
    assert module.conninfo_summary("host=localhost") == ("summary", "host=localhost")
    assert module.connect_plan("host=localhost") == ("plan", "host=localhost")
    assert module.connect_target("host=localhost") == ("target", "host=localhost")
    assert module.connect_no_tls_probe("host=localhost") == ("probe", "host=localhost")
    assert module.query_text_no_tls("host=localhost", "select 1") == (
        "query",
        "host=localhost",
        "select 1",
    )
    assert module.simple_query_no_tls("host=localhost", "select 1") == (
        "simple-query",
        "host=localhost",
        "select 1",
    )
    assert module.simple_query_results_no_tls("host=localhost", "select 1") == (
        "simple-query-results",
        "host=localhost",
        "select 1",
    )
    assert module.pipeline_simple_query_results_no_tls(
        "host=localhost",
        ["select 1", "select 2"],
    ) == (
        "pipeline-simple-query-results",
        "host=localhost",
        ["select 1", "select 2"],
    )
    assert module.query_text_params_no_tls(
        "host=localhost", "select $1::text", ["x", None]
    ) == (
        "query-params",
        "host=localhost",
        "select $1::text",
        ["x", None],
    )
    assert module.run_text_params_no_tls(
        "host=localhost", "select $1::text", ["x", None]
    ) == (
        "run-params",
        "host=localhost",
        "select $1::text",
        ["x", None],
    )
    assert module.execute_text_params_no_tls(
        "host=localhost",
        "update demo set value = $1",
        ["x"],
    ) == (
        "execute-params",
        "host=localhost",
        "update demo set value = $1",
        ["x"],
    )
    assert module.describe_text_no_tls("host=localhost", "select 1") == (
        "describe",
        "host=localhost",
        "select 1",
    )
    assert module.no_tls_session("host=localhost") == ("session", "host=localhost")
    assert module.backend_session("host=localhost") == (
        "backend-session",
        "host=localhost",
    )
    adapter = module.no_tls_session_adapter("host=localhost")
    assert adapter is not None
    backend_adapter = module.backend_session_adapter("host=localhost")
    assert backend_adapter is not None
    assert calls == [
        ("summary", "host=localhost"),
        ("plan", "host=localhost"),
        ("target", "host=localhost"),
        ("probe", "host=localhost"),
        ("query", "host=localhost"),
        ("simple-query", "host=localhost"),
        ("simple-query-results", "host=localhost"),
        ("pipeline-simple-query-results", "host=localhost"),
        ("query-params", "host=localhost"),
        ("run-params", "host=localhost"),
        ("execute-params", "host=localhost"),
        ("describe", "host=localhost"),
        ("session", "host=localhost"),
        ("backend-session", "host=localhost"),
        ("session", "host=localhost"),
        ("backend-session", "host=localhost"),
    ]


def test_package_connect_ferrocopg(monkeypatch: pytest.MonkeyPatch) -> None:
    psycopg_module = importlib.import_module("psycopg")
    ferrocopg_module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    calls: list[str] = []

    TrackingCursor = cast(
        Any, type("TrackingCursor", (ferrocopg_module.NoTlsCursorAdapter,), {})
    )

    def stub_backend_connection_adapter(
        conninfo: str,
        *,
        row_factory: object = ferrocopg_module.list_row,
        cursor_factory: type[object] = ferrocopg_module.NoTlsCursorAdapter,
        server_cursor_factory: type[object] | None = None,
        prepare_threshold: int | None = 5,
        autocommit: bool = True,
        isolation_level: object | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
    ) -> tuple[
        str,
        str,
        object,
        type[object],
        type[object] | None,
        int | None,
        bool,
        object | None,
        bool | None,
        bool | None,
    ]:
        calls.append(conninfo)
        return (
            "adapter",
            conninfo,
            row_factory,
            cursor_factory,
            server_cursor_factory,
            prepare_threshold,
            autocommit,
            isolation_level,
            read_only,
            deferrable,
        )

    monkeypatch.setattr(
        ferrocopg_module,
        "backend_connection_adapter",
        stub_backend_connection_adapter,
    )

    got = psycopg_module.connect_ferrocopg(
        "dbname=postgres",
        host="localhost",
        port=5432,
        application_name="ferrocopg-tests",
    )
    assert got == (
        "adapter",
        "dbname=postgres host=localhost port=5432 application_name=ferrocopg-tests",
        psycopg_module.tuple_row,
        ferrocopg_module.NoTlsCursorAdapter,
        None,
        5,
        True,
        None,
        None,
        None,
    )
    got_scalar = psycopg_module.connect_ferrocopg(
        "dbname=postgres",
        row_factory=ferrocopg_module.scalar_row,
        prepare_threshold=0,
        autocommit=False,
        isolation_level=psycopg_module.IsolationLevel.SERIALIZABLE,
        read_only=True,
        deferrable=False,
    )
    assert got_scalar == (
        "adapter",
        "dbname=postgres",
        ferrocopg_module.scalar_row,
        ferrocopg_module.NoTlsCursorAdapter,
        None,
        0,
        False,
        psycopg_module.IsolationLevel.SERIALIZABLE,
        True,
        False,
    )
    got_cursor = psycopg_module.connect_ferrocopg(
        "dbname=postgres",
        cursor_factory=TrackingCursor,
    )
    assert got_cursor == (
        "adapter",
        "dbname=postgres",
        psycopg_module.tuple_row,
        TrackingCursor,
        None,
        5,
        True,
        None,
        None,
        None,
    )
    assert calls == [
        "dbname=postgres host=localhost port=5432 application_name=ferrocopg-tests",
        "dbname=postgres",
        "dbname=postgres",
    ]


def test_package_connect_impl_selector(monkeypatch: pytest.MonkeyPatch) -> None:
    psycopg_module = importlib.import_module("psycopg")
    monkeypatch.delenv("PSYCOPG_SOURCE_IMPL", raising=False)

    calls: list[tuple[str, str, object]] = []

    def stub_connect(conninfo: str = "", **kwargs: object) -> tuple[str, str, object]:
        calls.append(("libpq", conninfo, dict(kwargs)))
        return ("libpq", conninfo, dict(kwargs))

    def stub_connect_ferrocopg(
        conninfo: str = "", **kwargs: object
    ) -> tuple[str, str, object]:
        calls.append(("ferrocopg", conninfo, dict(kwargs)))
        return ("ferrocopg", conninfo, dict(kwargs))

    monkeypatch.setattr(psycopg_module.Connection, "connect", stub_connect)
    monkeypatch.setattr(psycopg_module, "connect_ferrocopg", stub_connect_ferrocopg)

    got_default = psycopg_module.connect("dbname=postgres", prepare_threshold=7)
    assert got_default == (
        "ferrocopg",
        "dbname=postgres",
        {"prepare_threshold": 7, "autocommit": False},
    )

    got_libpq = psycopg_module.connect("dbname=postgres", impl="libpq", autocommit=True)
    assert got_libpq == (
        "libpq",
        "dbname=postgres",
        {"autocommit": True},
    )

    got_ferrocopg = psycopg_module.connect("dbname=postgres", impl="ferrocopg")
    assert got_ferrocopg == (
        "ferrocopg",
        "dbname=postgres",
        {"autocommit": False},
    )

    got_ferrocopg_auto = psycopg_module.connect(
        "dbname=postgres",
        impl="ferrocopg",
        autocommit=True,
        prepare_threshold=0,
    )
    assert got_ferrocopg_auto == (
        "ferrocopg",
        "dbname=postgres",
        {"autocommit": True, "prepare_threshold": 0},
    )

    for invalid_impl in ("wat", ""):
        with pytest.raises(
            ValueError, match="unsupported connect\\(\\) implementation"
        ):
            psycopg_module.connect("dbname=postgres", impl=invalid_impl)

    assert calls == [
        (
            "ferrocopg",
            "dbname=postgres",
            {"prepare_threshold": 7, "autocommit": False},
        ),
        ("libpq", "dbname=postgres", {"autocommit": True}),
        ("ferrocopg", "dbname=postgres", {"autocommit": False}),
        (
            "ferrocopg",
            "dbname=postgres",
            {"autocommit": True, "prepare_threshold": 0},
        ),
    ]

    monkeypatch.setenv("PSYCOPG_SOURCE_IMPL", "libpq")
    assert psycopg_module.connect("dbname=source-comparison") == (
        "libpq",
        "dbname=source-comparison",
        {},
    )


def test_package_connect_missing_rust_is_hard_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    psycopg_module = importlib.import_module("psycopg")
    ferrocopg_module = cast(Any, importlib.import_module("psycopg._ferrocopg"))
    rmodule = cast(Any, importlib.import_module("psycopg._rmodule"))
    import_error = ModuleNotFoundError("No module named 'ferrocopg_rust'")

    monkeypatch.delenv("PSYCOPG_SOURCE_IMPL", raising=False)
    monkeypatch.setattr(ferrocopg_module, "_ferrocopg", None)
    monkeypatch.setattr(rmodule, "_import_error", import_error)

    with pytest.raises(ImportError, match="maturin develop") as excinfo:
        psycopg_module.connect("dbname=postgres")

    assert excinfo.value.__cause__ is import_error

    sentinel = object()
    monkeypatch.setattr(
        psycopg_module.Connection,
        "connect",
        lambda *_args, **_kwargs: sentinel,
    )
    assert psycopg_module.connect("dbname=postgres", impl="libpq") is sentinel


def test_package_ferrocopg_import_does_not_replace_pq_impl() -> None:
    import psycopg

    rust_module = pytest.importorskip("ferrocopg_rust")
    ferrocopg_module = importlib.import_module("psycopg._ferrocopg")

    assert ferrocopg_module.is_available() is True
    assert psycopg.pq.__impl__ in {"python", "c", "binary"}
    assert psycopg.pq.__impl__ != "ferrocopg"
    assert rust_module is not psycopg.pq
    assert psycopg.connect is not psycopg.connect_ferrocopg


def test_package_connect_ferrocopg_unsupported_connect_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    ferrocopg_module = cast(Any, importlib.import_module("psycopg._ferrocopg"))
    calls: list[str] = []

    def stub_backend_connection_adapter(conninfo: str, **kwargs: object) -> object:
        calls.append(conninfo)
        return object()

    monkeypatch.setattr(
        ferrocopg_module,
        "backend_connection_adapter",
        stub_backend_connection_adapter,
    )

    class StubContext:
        adapters = object()

    class StubServerCursor:
        pass

    with pytest.raises(
        psycopg.NotSupportedError,
        match="custom adaptation contexts",
    ):
        psycopg.connect_ferrocopg("dbname=postgres", context=StubContext())

    psycopg.connect(
        "dbname=postgres", impl="ferrocopg", server_cursor_factory=StubServerCursor
    )

    with pytest.raises(
        psycopg.NotSupportedError,
        match="concrete cursor factories require libpq",
    ):
        psycopg.connect(
            "dbname=postgres", impl="ferrocopg", cursor_factory=psycopg.Cursor
        )

    assert calls == ["dbname=postgres"]


def test_backend_result_cursor_navigation() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    results = [
        SimpleNamespace(columns=["a"], rows=[["one"], ["two"]], rows_affected=2),
        SimpleNamespace(columns=["b"], rows=[["three"]], rows_affected=1),
    ]

    cur = module.BackendResultCursor(results)
    assert cur.columns == ["a"]
    assert cur.rows_affected == 2
    assert cur.statusmessage is None
    assert cur.fetchone() == ["one"]
    assert cur.fetchall() == [["two"]]
    assert cur.nextset() is True
    assert cur.columns == ["b"]
    assert cur.fetchall() == [["three"]]
    assert cur.nextset() is None
    assert cur.set_result(0) is cur
    assert cur.fetchall() == [["one"], ["two"]]
    assert cur.set_result(-1) is cur
    assert cur.fetchall() == [["three"]]


def test_backend_result_cursor_results_iterator() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    results = [
        SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1),
        SimpleNamespace(columns=["b"], rows=[["two"]], rows_affected=1),
    ]

    cur = module.BackendResultCursor(results)
    observed = [res.fetchall() for res in cur.results()]
    assert observed == [[["one"]], [["two"]]]


def test_no_tls_session_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [
                SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1),
                SimpleNamespace(columns=["b"], rows=[["two"]], rows_affected=1),
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["c"], rows=[["three"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["d"], rows=[["four"]], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    adapter = module.no_tls_session_adapter("host=localhost")
    assert adapter is not None
    assert adapter.closed is False
    assert adapter.execute_simple("select 1").fetchall() == [["one"]]
    assert adapter.execute_params("select $1::text", ["x"]).fetchall() == [["three"]]
    assert adapter.execute_prepared(7, ["y"]).fetchall() == [["four"]]
    adapter.close()
    assert stub.calls == [
        ("simple", "select 1"),
        ("params", "select $1::text", ["x"]),
        ("prepared", 7, ["y"]),
        ("close",),
    ]


def test_no_tls_connection_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubPrepared:
        def __init__(self, statement_id: int) -> None:
            self.statement_id = statement_id

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))
            self.closed = True

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def prepare_text(self, query: str) -> StubPrepared:
            self.calls.append(("prepare", query))
            return StubPrepared(11)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["b"], rows=[["two"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["c"], rows=[["three"]], rows_affected=1)

    TrackingCursor = cast(Any, type("TrackingCursor", (module.NoTlsCursorAdapter,), {}))

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter(
        "host=localhost", cursor_factory=TrackingCursor
    )
    assert conn is not None
    assert conn.closed is False
    assert conn.cursor_factory is TrackingCursor

    exec_cur = conn.execute("select 1")
    assert isinstance(exec_cur, TrackingCursor)
    assert exec_cur.fetchall() == [["one"]]
    assert conn.execute("select $1::text", ["x"]).fetchall() == [["two"]]
    assert conn.execute("select $1::text", ["x"], prepare=True).fetchall() == [
        ["three"]
    ]
    assert conn.execute("select $1::text", ["y"], prepare=True).fetchall() == [
        ["three"]
    ]

    with conn.cursor() as cur:
        assert isinstance(cur, TrackingCursor)
        assert cur.execute("select 1").fetchone() == ["one"]
        assert cur.rowcount == 1

    command_result = conn._exec_command("select 1")
    assert command_result is not None
    assert command_result.status == module.ExecStatus.TUPLES_OK
    assert command_result.ntuples == 1
    assert command_result.nfields == 1
    assert command_result.get_value(0, 0) == b"one"

    conn.begin()
    conn.commit()
    conn.rollback()
    conn.close()
    assert conn.closed is True

    assert stub.calls == [
        ("simple", "select 1"),
        ("params", "select $1::text", ["x"]),
        ("prepare", "select $1::text"),
        ("prepared", 11, ["x"]),
        ("prepared", 11, ["y"]),
        ("simple", "select 1"),
        ("simple", "select 1"),
        ("simple", "BEGIN"),
        ("simple", "COMMIT"),
        ("close",),
    ]


def test_no_tls_connection_adapter_prepare_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubPrepared:
        def __init__(self, statement_id: int) -> None:
            self.statement_id = statement_id

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.closed = True

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> StubPrepared:
            self.calls.append(("prepare", query))
            return StubPrepared(21)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["b"], rows=[["two"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["c"], rows=[["three"]], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost", prepare_threshold=1)
    assert conn is not None

    assert conn.execute("select $1::text", ["x"]).fetchall() == [["two"]]
    assert conn.execute("select $1::text", ["y"]).fetchall() == [["three"]]
    assert conn.execute("select $1::text", ["z"]).fetchall() == [["three"]]

    assert stub.calls == [
        ("params", "select $1::text", ["x"]),
        ("prepare", "select $1::text"),
        ("prepared", 21, ["y"]),
        ("prepared", 21, ["z"]),
    ]
    assert conn._prepared == {"select $1::text": 21}
    assert conn._prepare_counts == {"select $1::text": 3}
    conn.close()
    assert conn._prepared == {}
    assert conn._prepared_statusmessages == {}
    assert conn._prepare_counts == {}


def test_no_tls_connection_adapter_psycopg_placeholders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["query"], rows=[[query]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["query"], rows=[[query]], rows_affected=1)

        def run_params(
            self, query: str, params: list[tuple[int, bool, bytes | None]]
        ) -> object:
            self.calls.append(("bound", query, params))
            return SimpleNamespace(columns=["query"], rows=[[query]], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    conn.execute("select %s::int4, %s::date, %s::text", [7, date(2020, 1, 1), None])
    conn.execute("select %(label)s::text", {"label": "named"})
    conn.execute("select $1::text", ["native"])

    first = stub.calls[0]
    assert first[:2] == ("bound", "select $1::int4, $2::date, $3::text")
    assert first[2] == [
        (21, True, b"\x00\x07"),
        (1082, True, b"\x00\x00\x1c\x89"),
        (0, False, None),
    ]
    assert stub.calls[1:] == [
        ("bound", "select $1::text", [(0, False, b"named")]),
        ("params", "select $1::text", ["native"]),
    ]


def test_no_tls_connection_adapter_autocommit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.closed = True

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["b"], rows=[["two"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["c"], rows=[["three"]], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost", autocommit=False)
    assert conn is not None
    assert conn.autocommit is False

    assert conn.execute("select 1").fetchall() == [["one"]]
    assert conn.execute("select $1::text", ["x"]).fetchall() == [["two"]]
    conn.rollback()
    assert conn.autocommit is False
    conn.set_autocommit(True)
    assert conn.autocommit is True
    assert conn.execute("select 1").fetchall() == [["one"]]

    conn.autocommit = False
    conn.execute("select 1")
    with pytest.raises(psycopg.ProgrammingError, match="can't change autocommit now"):
        conn.autocommit = True

    assert stub.calls == [
        ("simple", "BEGIN"),
        ("simple", "select 1"),
        ("params", "select $1::text", ["x"]),
        ("simple", "ROLLBACK"),
        ("simple", "select 1"),
        ("simple", "BEGIN"),
        ("simple", "select 1"),
    ]


def test_no_tls_connection_adapter_commit_rollback_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            self.calls.append(("pipeline", queries))
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    conn.commit()
    conn.rollback()
    assert stub.calls == []

    conn.autocommit = False
    conn.execute("select 1")
    conn.commit()
    conn.rollback()
    assert stub.calls == [
        ("simple", "BEGIN"),
        ("simple", "select 1"),
        ("simple", "COMMIT"),
    ]


def test_no_tls_connection_adapter_transaction_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def prepare_text(self, query: str) -> object:
            self.calls.append(("prepare", query))
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            self.calls.append(("pipeline", queries))
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost", autocommit=False)
    assert conn is not None

    conn.set_isolation_level(psycopg.IsolationLevel.SERIALIZABLE.value)
    conn.set_read_only(1)
    conn.set_deferrable(0)
    assert conn.isolation_level is psycopg.IsolationLevel.SERIALIZABLE
    assert conn.read_only is True
    assert conn.deferrable is False

    conn.execute("select 1")
    assert stub.calls[0] == (
        "simple",
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY NOT DEFERRABLE",
    )

    with pytest.raises(psycopg.ProgrammingError, match="can't change 'read_only' now"):
        conn.set_read_only(False)
    conn.rollback()

    with conn.transaction():
        with pytest.raises(
            psycopg.ProgrammingError,
            match="connection.transaction\\(\\) context in progress",
        ):
            conn.set_deferrable(True)


def test_no_tls_connection_adapter_notifications(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []
            self.notifications: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def listen(self, channel: str) -> None:
            self.calls.append(("listen", channel))

        def unlisten(self, channel: str) -> None:
            self.calls.append(("unlisten", channel))

        def notify(self, channel: str, payload: str) -> None:
            self.calls.append(("notify", channel, payload))

        def drain_notifications(self) -> list[object]:
            self.calls.append(("drain",))
            rv = list(self.notifications)
            self.notifications.clear()
            return rv

        def wait_for_notification(self, timeout_ms: int) -> object | None:
            self.calls.append(("wait", timeout_ms))
            if self.notifications:
                return self.notifications.pop(0)
            return None

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

    stub = StubSession()
    stub.notifications.extend(
        [
            SimpleNamespace(channel="ferro", payload="one", process_id=10),
            SimpleNamespace(channel="ferro", payload="two", process_id=11),
            SimpleNamespace(channel="ferro", payload="three", process_id=12),
        ]
    )
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    conn.listen("ferro")
    conn.notify("ferro", "payload")
    first = conn.wait_for_notification(0.25)
    assert first == psycopg.Notify("ferro", "one", 10)
    assert conn.drain_notifications() == [
        psycopg.Notify("ferro", "two", 11),
        psycopg.Notify("ferro", "three", 12),
    ]
    conn.unlisten("ferro")

    assert stub.calls == [
        ("listen", "ferro"),
        ("notify", "ferro", "payload"),
        ("wait", 250),
        ("drain",),
        ("unlisten", "ferro"),
    ]

    stub.calls.clear()
    stub.notifications.extend(
        [
            SimpleNamespace(channel="ferro", payload="four", process_id=13),
            SimpleNamespace(channel="ferro", payload="five", process_id=14),
        ]
    )
    assert list(conn.notifies(timeout=0.0, stop_after=1)) == [
        psycopg.Notify("ferro", "four", 13),
        psycopg.Notify("ferro", "five", 14),
    ]
    assert stub.calls == [("drain",)]


def test_no_tls_connection_adapter_notify_handlers(
    monkeypatch: pytest.MonkeyPatch,
    recwarn: pytest.WarningsRecorder,
) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.notifications: list[object] = []

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def listen(self, channel: str) -> None:
            pass

        def unlisten(self, channel: str) -> None:
            pass

        def notify(self, channel: str, payload: str) -> None:
            pass

        def drain_notifications(self) -> list[object]:
            rv = list(self.notifications)
            self.notifications.clear()
            return rv

        def wait_for_notification(self, timeout_ms: int) -> object | None:
            if self.notifications:
                return self.notifications.pop(0)
            return None

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

    stub = StubSession()
    stub.notifications.extend(
        [
            SimpleNamespace(channel="ferro", payload="one", process_id=10),
            SimpleNamespace(channel="ferro", payload="two", process_id=11),
        ]
    )
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    seen: list[psycopg.Notify] = []

    def cb(n: psycopg.Notify) -> None:
        seen.append(n)

    conn.add_notify_handler(cb)

    drained = conn.drain_notifications()
    assert drained == [
        psycopg.Notify("ferro", "one", 10),
        psycopg.Notify("ferro", "two", 11),
    ]
    assert seen == drained

    stub.notifications.append(
        SimpleNamespace(channel="ferro", payload="three", process_id=12)
    )
    got = conn.wait_for_notification(0.1)
    assert got == psycopg.Notify("ferro", "three", 12)
    assert seen[-1] == got

    stub.notifications.append(
        SimpleNamespace(channel="ferro", payload="four", process_id=13)
    )
    assert list(conn.notifies(timeout=0.0, stop_after=1)) == [
        psycopg.Notify("ferro", "four", 13)
    ]
    msg = str(recwarn.pop(RuntimeWarning).message)
    assert "notifies()" in msg

    conn.remove_notify_handler(cb)
    with pytest.raises(ValueError):
        conn.remove_notify_handler(cb)


def test_no_tls_connection_adapter_cancel_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            self.closed = True

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["q"], rows=[["ok"]], rows_affected=1)

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None
    conn.close()
    conn.cancel()
    conn.cancel_safe()


def test_no_tls_connection_adapter_context_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))
            self.closed = True

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=["b"], rows=[["two"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=["c"], rows=[["three"]], rows_affected=1)

    committed = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: committed)
    with module.no_tls_connection_adapter("host=localhost", autocommit=False) as conn:
        assert conn is not None
        conn.execute("select 1")

    assert committed.calls == [
        ("simple", "BEGIN"),
        ("simple", "select 1"),
        ("simple", "COMMIT"),
        ("close",),
    ]

    rolled_back = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: rolled_back)
    with pytest.raises(RuntimeError, match="boom"):
        with module.no_tls_connection_adapter(
            "host=localhost", autocommit=False
        ) as conn:
            assert conn is not None
            conn.execute("select 1")
            raise RuntimeError("boom")

    assert rolled_back.calls == [
        ("simple", "BEGIN"),
        ("simple", "select 1"),
        ("simple", "ROLLBACK"),
        ("close",),
    ]


def test_no_tls_connection_adapter_row_factories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg
    from psycopg import rows

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=99)

        def simple_query_results(self, query: str) -> list[object]:
            return [
                SimpleNamespace(
                    columns=["id", "label"],
                    rows=[["1", "one"], ["2", "two"]],
                    rows_affected=2,
                )
            ]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [
                    SimpleNamespace(
                        columns=["value"],
                        rows=[[query]],
                        rows_affected=1,
                    )
                ]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            if query == "select typed":
                return SimpleNamespace(
                    columns=["n", "ratio", "enabled", "disabled", "label", "nullable"],
                    column_descriptions=[
                        SimpleNamespace(name="n", oid=23),
                        SimpleNamespace(name="ratio", oid=701),
                        SimpleNamespace(name="enabled", oid=16),
                        SimpleNamespace(name="disabled", oid=16),
                        SimpleNamespace(name="label", oid=25),
                        SimpleNamespace(name="nullable", oid=25),
                    ],
                    rows=[
                        [
                            b"\x00\x00\x00\x07",
                            b"@\x04\x00\x00\x00\x00\x00\x00",
                            b"\x01",
                            b"\x00",
                            b"plain",
                            None,
                        ]
                    ],
                    rows_affected=1,
                )
            if not params:
                return SimpleNamespace(
                    columns=["id", "label"],
                    column_descriptions=[
                        SimpleNamespace(name="id", oid=23),
                        SimpleNamespace(name="label", oid=25),
                    ],
                    rows=[
                        [b"\x00\x00\x00\x01", b"one"],
                        [b"\x00\x00\x00\x02", b"two"],
                    ],
                    rows_affected=2,
                )
            return SimpleNamespace(columns=["value"], rows=[["3"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["value"], rows=[["4"]], rows_affected=1)

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    default_cur = conn.execute("select 1")
    assert default_cur.description == [
        module.BackendColumn("id"),
        module.BackendColumn("label"),
    ]
    assert default_cur.statusmessage == "SELECT 2"
    assert default_cur.fetchall() == [["1", "one"], ["2", "two"]]
    assert default_cur.rownumber == 2

    tuple_cur = conn.execute("select 1", row_factory=module.tuple_row)
    assert tuple_cur.fetchall() == [("1", "one"), ("2", "two")]

    dict_cur = conn.execute("select 1", row_factory=module.dict_row)
    assert dict_cur.fetchall() == [
        {"id": "1", "label": "one"},
        {"id": "2", "label": "two"},
    ]

    psycopg_tuple_cur = conn.execute("select 1", row_factory=rows.tuple_row)
    assert psycopg_tuple_cur.fetchall() == [(1, "one"), (2, "two")]

    psycopg_dict_cur = conn.execute("select 1", row_factory=rows.dict_row)
    assert psycopg_dict_cur.fetchall() == [
        {"id": 1, "label": "one"},
        {"id": 2, "label": "two"},
    ]

    psycopg_namedtuple_cur = conn.execute("select 1", row_factory=rows.namedtuple_row)
    named_rows = psycopg_namedtuple_cur.fetchall()
    assert [(row.id, row.label) for row in named_rows] == [(1, "one"), (2, "two")]

    class Item:
        def __init__(self, id: int, label: str) -> None:
            self.id = id
            self.label = label

    psycopg_class_cur = conn.execute("select 1", row_factory=rows.class_row(Item))
    assert [(row.id, row.label) for row in psycopg_class_cur.fetchall()] == [
        (1, "one"),
        (2, "two"),
    ]

    typed_cur = conn.execute("select typed", row_factory=rows.tuple_row)
    assert typed_cur.fetchone() == (7, 2.5, True, False, "plain", None)

    scalar_cur = conn.execute(
        "select $1::text",
        ["3"],
        row_factory=module.scalar_row,
        prepare=True,
    )
    assert scalar_cur.fetchone() == "4"

    psycopg_scalar_cur = conn.execute(
        "select $1::text",
        ["3"],
        row_factory=rows.scalar_row,
        prepare=True,
    )
    assert psycopg_scalar_cur.fetchone() == "4"

    iter_cur = conn.execute("select 1", row_factory=module.tuple_row)
    assert list(iter_cur) == [("1", "one"), ("2", "two")]

    many_cur = conn.execute("select 1")
    many_cur.arraysize = 2
    assert many_cur.fetchmany() == [["1", "one"], ["2", "two"]]

    many_cur_zero = conn.execute("select 1")
    many_cur_zero.arraysize = 1
    assert many_cur_zero.fetchmany(0) == [["1", "one"]]

    pipeline_cursors = conn.execute_pipeline_simple(
        ["select first", "select second"],
        row_factory=module.scalar_row,
    )
    assert [cur.fetchall() for cur in pipeline_cursors] == [
        ["select first"],
        ["select second"],
    ]

    scalar_conn = module.no_tls_connection_adapter(
        "host=localhost",
        row_factory=module.scalar_row,
    )
    assert scalar_conn is not None
    assert scalar_conn.execute("select $1::text", ["3"], prepare=True).fetchall() == [
        "4"
    ]

    row_factory_cur = conn.cursor()
    assert row_factory_cur.connection is conn
    assert row_factory_cur.row_factory is module.list_row
    row_factory_cur.row_factory = module.tuple_row
    assert row_factory_cur.row_factory is module.tuple_row
    assert row_factory_cur.execute("select 1").fetchall() == [
        ("1", "one"),
        ("2", "two"),
    ]

    row_factory_cur.row_factory = module.dict_row
    assert row_factory_cur.execute("select 1").fetchall() == [
        {"id": "1", "label": "one"},
        {"id": "2", "label": "two"},
    ]

    assert row_factory_cur.execute("select 1", binary=True).fetchall() == [
        {"id": "1", "label": "one"},
        {"id": "2", "label": "two"},
    ]
    assert row_factory_cur.format == psycopg.pq.Format.BINARY


def test_no_tls_cursor_adapter_executemany(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=17)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["v"], rows=[["simple"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            return SimpleNamespace(
                columns=["value"],
                rows=[[params[0]]],
                rows_affected=1,
            )

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(
                columns=["value"],
                rows=[[params[0]]],
                rows_affected=1,
            )

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    with conn.cursor() as cur:
        rv = cur.executemany(
            "insert into demo values ($1::text)",
            [["one"], ["two"]],
        )
        assert rv is None
        assert cur.rowcount == 2
        assert cur.statusmessage == "INSERT 0 2"
        assert cur.rownumber is None
        with pytest.raises(
            psycopg.ProgrammingError,
            match="the last operation didn't produce a result",
        ):
            cur.fetchall()

    with conn.cursor(row_factory=module.scalar_row) as cur:
        rv = cur.executemany(
            "select $1::text as value",
            [["one"], ["two"]],
            returning=True,
            prepare=True,
        )
        assert rv is None
        assert cur.fetchone() == "one"
        assert cur.nextset() is True
        assert cur.fetchone() == "two"
        assert cur.nextset() is None


def test_no_tls_cursor_adapter_result_navigation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [
                SimpleNamespace(
                    columns=["a"], rows=[["one"], ["two"]], rows_affected=2
                ),
                SimpleNamespace(columns=["b"], rows=[["three"]], rows_affected=1),
            ]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            return SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    fresh = conn.cursor()
    assert list(fresh.results()) == []

    cur = conn.execute("select 1")
    assert cur.fetchmany(1) == [["one"]]
    assert cur.rownumber == 1
    assert cur.set_result(0) is cur
    assert [res.fetchall() for res in cur.results()] == [
        [["one"], ["two"]],
        [["three"]],
    ]

    pipeline = conn.execute_pipeline_simple(["select left", "select right"])
    assert [cur.fetchall() for cur in pipeline] == [
        [["select left"]],
        [["select right"]],
    ]


def test_no_tls_cursor_adapter_scroll(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def simple_query_results(self, query: str) -> list[object]:
            return [
                SimpleNamespace(
                    columns=["n"],
                    rows=[["0"], ["1"], ["2"], ["3"], ["4"]],
                    rows_affected=5,
                )
            ]

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter(
        "host=localhost", row_factory=module.scalar_row
    )
    assert conn is not None

    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        cur.scroll(0)

    cur.execute("select generate_series(0,4)")
    cur.scroll(2)
    assert cur.fetchone() == "2"
    cur.scroll(1)
    assert cur.fetchone() == "4"
    cur.scroll(1, mode="absolute")
    assert cur.fetchone() == "1"

    cur.scroll(0, mode="absolute")
    assert cur.fetchone() == "0"

    with pytest.raises(IndexError, match="out of bound"):
        cur.scroll(-1, mode="absolute")
    with pytest.raises(IndexError, match="out of bound"):
        cur.scroll(5, mode="absolute")
    with pytest.raises(IndexError, match="out of bound"):
        cur.scroll(10)
    with pytest.raises(ValueError, match="bad mode"):
        cur.scroll(1, "wat")


def test_no_tls_cursor_adapter_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def simple_query_results(self, query: str) -> list[object]:
            return [
                SimpleNamespace(
                    columns=["n"],
                    rows=[["1"], ["2"], ["3"]],
                    rows_affected=3,
                )
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            key = params[0]
            values = {
                "client_encoding": "LATIN1",
                "TimeZone": "UTC",
            }
            return SimpleNamespace(
                columns=["n"],
                rows=[[values[key] if key in values else key]],
                rows_affected=1,
            )

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter(
        "host=localhost", row_factory=module.scalar_row
    )
    assert conn is not None

    with conn.cursor() as cur:
        assert list(cur.stream("select generate_series(1,3)")) == ["1", "2", "3"]

    with conn.cursor() as cur:
        assert list(cur.stream("select $1::text", ["7"])) == ["7"]

    with conn.cursor() as cur:
        assert list(cur.stream("select $1::text", [None])) == [None]

    with conn.cursor() as cur:
        with pytest.raises(ValueError, match="size must be >= 1"):
            next(cur.stream("select 1", size=0))

    with conn.cursor() as cur:
        assert next(cur.stream("select 1", binary=True)) == "1"
        assert cur.format == psycopg.pq.Format.BINARY

    with conn.pipeline():
        with conn.cursor() as cur:
            with pytest.raises(psycopg.ProgrammingError, match="pipeline mode"):
                next(cur.stream("select 1"))


def test_no_tls_cursor_adapter_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    from ._test_copy import sample_binary

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.copy_in_calls: list[tuple[str, bytes]] = []
            self.copy_out_calls: list[str] = []

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            key = params[0]
            values = {
                "client_encoding": "LATIN1",
                "TimeZone": "UTC",
            }
            return SimpleNamespace(
                columns=["value"],
                rows=[[values[key] if key in values else None]],
                rows_affected=1,
            )

        def copy_from_stdin(self, query: str, data: bytes) -> int:
            self.copy_in_calls.append((query, data))
            return 2

        def describe_text(self, query: str) -> object:
            del query
            return SimpleNamespace(
                columns=[SimpleNamespace(name="value", oid=25, type_name="text")]
            )

        def copy_to_stdout(self, query: str) -> object:
            self.copy_out_calls.append(query)
            if "bad encoding" in query.lower():
                raise psycopg.DataError("cannot transcode COPY output")
            if "binary" in query.lower():
                return SimpleNamespace(data=sample_binary)
            return SimpleNamespace(data=b"10\talpha\n11\tbeta\n13\tcaf\xe9\n")

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    with conn.cursor() as cur:
        with cur.copy("copy demo from stdin") as copy:
            copy.write("10\talpha\n")
            copy.write(b"11\tbeta\n")
            copy.write_row(["12", None])
            copy.write_row(["13", "café"])
        assert cur.rowcount == 2
        assert cur.statusmessage == "COPY 2"
        assert stub.copy_in_calls == [
            ("copy demo from stdin", b"10\talpha\n11\tbeta\n12\t\\N\n13\tcaf\xe9\n")
        ]

    with conn.cursor() as cur:
        with cur.copy("copy demo to stdout") as copy:
            assert copy.read(3) == b"10\t"
            assert copy.read() == b"alpha\n"
            assert copy.read() == b"11\tbeta\n"
            assert copy.read() == b"13\tcaf\xe9\n"
            assert copy.read() == b""
        assert stub.copy_out_calls == ["copy demo to stdout"]

    with conn.cursor() as cur:
        with cur.copy("copy demo to stdout") as copy:
            assert list(copy) == [b"10\talpha\n", b"11\tbeta\n", b"13\tcaf\xe9\n"]

    with conn.cursor() as cur:
        with cur.copy("copy demo to stdout") as copy:
            assert copy.read_row() == ("10", "alpha")
            assert copy.read_row() == ("11", "beta")
            assert copy.read_row() == ("13", "café")
            assert copy.read_row() is None

    with conn.cursor() as cur:
        with cur.copy("copy bad encoding to stdout") as copy:
            with pytest.raises(psycopg.DataError, match="cannot transcode"):
                copy.read()

    with conn.cursor() as cur:
        with cur.copy("copy (select %s::text) to stdout", params=["x"]) as copy:
            assert copy.read_row() == ("10", "alpha")
        assert stub.copy_out_calls[-1] == "copy (select 'x'::text) to stdout"

        class Writer:
            def __init__(self) -> None:
                self.data = bytearray()
                self.finished = False

            def write(self, data: bytes | bytearray | memoryview) -> None:
                self.data.extend(data)

            def finish(self, exc: BaseException | None = None) -> None:
                assert exc is None
                self.finished = True

        writer = Writer()
        with cur.copy("copy demo from stdin", writer=writer) as copy:
            copy.write_row(["written"])
        assert writer.data == b"written\n"
        assert writer.finished is True
        with pytest.raises(
            psycopg.ProgrammingError, match="COPY FROM STDIN or COPY TO STDOUT"
        ):
            cur.copy("select 1")
        with pytest.raises(psycopg.ProgrammingError, match="COPY TO STDOUT"):
            with cur.copy("copy demo from stdin") as copy:
                copy.read_row()
        with pytest.raises(psycopg.ProgrammingError, match="COPY FROM STDIN"):
            with cur.copy("copy demo to stdout") as copy:
                copy.write_row(["bad"])
        with cur.copy("copy demo from stdin with binary") as copy:
            copy.write(b"binary payload")
        assert stub.copy_in_calls[-1] == (
            "copy demo from stdin with binary",
            b"binary payload",
        )
        with cur.copy("copy demo to stdout with \\(format binary\\)") as copy:
            copy.set_types(["int4", "int4", "text"])
            assert copy.read_row() == (40010, 40020, "hello")


def test_no_tls_connection_adapter_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            return SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    with conn.pipeline() as p:
        left = p.execute("select left", row_factory=module.scalar_row)
        right = p.execute("select right")
        with pytest.raises(psycopg.ProgrammingError, match="no result available"):
            left.fetchall()
        p.sync()
        assert left.fetchall() == ["select left"]
        assert right.fetchall() == [
            [
                "select right",
            ]
        ]
        queued = p.execute("select queued", row_factory=module.scalar_row)
        queued_params = p.execute(
            "select $1::text as value",
            params=["1"],
            row_factory=module.scalar_row,
        )

    assert queued.fetchall() == ["select queued"]
    assert queued_params.fetchall() == ["one"]

    with pytest.raises(psycopg.OperationalError, match="pipeline is not active"):
        p.execute("select after")


def test_no_tls_cursor_adapter_dbapi_size_noops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["label"], rows=[["ok"]], rows_affected=1)]

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    with conn.cursor() as cur:
        assert cur.setinputsizes([23, 25]) is None
        assert cur.setoutputsize(128) is None
        assert cur.setoutputsize(256, 0) is None
        cur.execute("select 'ok'::text as label")
        assert cur.fetchall() == [["ok"]]


def test_no_tls_connection_adapter_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        def close(self) -> None:
            self.calls.append(("close",))

        def begin(self) -> None:
            self.calls.append(("begin",))

        def commit(self) -> None:
            self.calls.append(("commit",))

        def rollback(self) -> None:
            self.calls.append(("rollback",))

        def prepare_text(self, query: str) -> object:
            self.calls.append(("prepare", query))
            return SimpleNamespace(statement_id=23)

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(("simple", query))
            return [SimpleNamespace(columns=[], rows=[], rows_affected=0)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            self.calls.append(("params", query, params))
            return SimpleNamespace(columns=[], rows=[], rows_affected=1)

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            self.calls.append(("prepared", statement_id, params))
            return SimpleNamespace(columns=[], rows=[], rows_affected=1)

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    with conn.transaction():
        conn.execute("select 1")

    with conn.transaction(force_rollback=True):
        conn.execute("select 2")

    with conn.transaction():
        conn.execute("select outer")
        with conn.transaction("s1"):
            conn.execute("select inner")

    with conn.transaction():
        with conn.transaction() as inner:
            raise module.Rollback(inner)

    assert stub.calls == [
        ("simple", "BEGIN"),
        ("simple", "select 1"),
        ("simple", "COMMIT"),
        ("simple", "BEGIN"),
        ("simple", "select 2"),
        ("simple", "ROLLBACK"),
        ("simple", "BEGIN"),
        ("simple", "select outer"),
        ("simple", 'SAVEPOINT "s1"'),
        ("simple", "select inner"),
        ("simple", 'RELEASE "s1"'),
        ("simple", "COMMIT"),
        ("simple", "BEGIN"),
        ("simple", 'SAVEPOINT "_pg3_2"'),
        ("simple", 'ROLLBACK TO "_pg3_2"'),
        ("simple", 'RELEASE "_pg3_2"'),
        ("simple", "COMMIT"),
    ]


def test_no_tls_connection_adapter_tpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import datetime, timezone

    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[str] = []

        def close(self) -> None:
            pass

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(query)
            return [
                SimpleNamespace(columns=[], rows=[], rows_affected=0, is_tuples=False)
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            assert params == []
            self.calls.append(query)
            return SimpleNamespace(
                columns=["gid", "prepared", "owner", "database"],
                column_descriptions=[],
                rows=[
                    [
                        "42_Z3RyaWQ=_YnF1YWw=",
                        datetime(2026, 1, 1, tzinfo=timezone.utc),
                        "postgres",
                        "postgres",
                    ]
                ],
                rows_affected=1,
                is_tuples=True,
            )

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost", autocommit=False)
    assert conn is not None

    xid = conn.xid(42, "gtrid", "bqual")
    assert xid.format_id == 42
    assert xid.gtrid == "gtrid"
    assert xid.bqual == "bqual"

    conn.tpc_begin(xid)
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
    conn.tpc_prepare()
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    with pytest.raises(psycopg.ProgrammingError, match="prepared two-phase"):
        conn.cancel()
    conn.tpc_commit()

    conn.tpc_begin("plain")
    conn.tpc_rollback()
    conn.tpc_commit(xid)

    recovered = conn.tpc_recover()
    assert len(recovered) == 1
    assert recovered[0].format_id == 42
    assert recovered[0].gtrid == "gtrid"
    assert recovered[0].bqual == "bqual"
    assert recovered[0].owner == "postgres"
    assert recovered[0].database == "postgres"
    assert stub.calls == [
        "BEGIN",
        "PREPARE TRANSACTION '42_Z3RyaWQ=_YnF1YWw='",
        "COMMIT PREPARED '42_Z3RyaWQ=_YnF1YWw='",
        "BEGIN",
        "ROLLBACK",
        "COMMIT PREPARED '42_Z3RyaWQ=_YnF1YWw='",
        "SELECT gid, prepared, owner, database FROM pg_prepared_xacts",
    ]


def test_ferrocopg_async_connection_facade(monkeypatch: pytest.MonkeyPatch) -> None:
    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))
    async_module = cast(Any, importlib.import_module("psycopg._ferrocopg_async"))

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.calls: list[str | list[str]] = []

        def close(self) -> None:
            self.closed = True

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(query)
            if query in {"BEGIN", "COMMIT", "ROLLBACK"}:
                return [self._result([], is_tuples=False)]
            return [self._result([[query.removeprefix("select ")]])]

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            self.calls.append(queries)
            return [
                [self._result([[query.removeprefix("select ")]])] for query in queries
            ]

        @staticmethod
        def _result(rows: list[list[str]], *, is_tuples: bool = True) -> object:
            columns = ["value"] if is_tuples else []
            descriptions = (
                [SimpleNamespace(name="value", oid=25, type_name="text")]
                if is_tuples
                else []
            )
            return SimpleNamespace(
                columns=columns,
                column_descriptions=descriptions,
                rows=rows,
                rows_affected=len(rows),
                is_tuples=is_tuples,
            )

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)
    connection = module.no_tls_connection_adapter("host=localhost", autocommit=True)
    assert connection is not None
    aconn = async_module.FerrocopgAsyncConnection(connection)

    async def exercise() -> None:
        worker_threads = {await aconn._run(threading.get_ident) for _ in range(4)}
        assert len(worker_threads) == 1
        assert worker_threads != {threading.get_ident()}

        owned_lock = threading.RLock()
        await aconn._run(owned_lock.acquire)
        await aconn._run(owned_lock.release)

        cursor = await aconn.execute("select direct")
        assert await cursor.fetchone() == ["direct"]

        async with aconn.transaction():
            async with aconn.cursor() as tx_cursor:
                await tx_cursor.execute("select transaction")
                assert await tx_cursor.fetchall() == [["transaction"]]

        async with aconn.pipeline():
            left = await aconn.execute("select left")
            right = await aconn.execute("select right")
        assert await left.fetchone() == ["left"]
        assert await right.fetchone() == ["right"]

        await aconn.close()
        assert aconn.closed
        await aconn.close()
        with pytest.raises(module.e.OperationalError):
            await cursor.execute("select after_close")

    asyncio.run(exercise())
    assert stub.calls == [
        "select direct",
        "BEGIN",
        "select transaction",
        "COMMIT",
        ["select left", "select right"],
    ]


def test_no_tls_connection_info_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter(
        "host=example.com port=5432 dbname=testdb user=tester password=secret application_name=ferrocopg"
    )
    assert conn is not None

    params = conn.info.get_parameters()
    assert params["host"] == "example.com"
    assert params["port"] == "5432"
    assert params["dbname"] == "testdb"
    assert params["user"] == "tester"
    assert params["application_name"] == "ferrocopg"
    assert conn.info.password == "secret"
    assert conn.info.options == ""
    assert conn.info.status == module.pq.ConnStatus.OK
    assert conn.info.full_protocol_version == 30000
    assert conn.info.error_message == ""
    assert "password" not in params
    assert "password" not in conn.info.dsn

    conn.close()
    assert conn.info.status == module.pq.ConnStatus.BAD


def test_no_tls_connection_adapter_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            self.closed = True

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            key = params[0]
            values = {
                "client_encoding": "UTF8",
                "TimeZone": "UTC",
                "application_name": "ferrocopg-tests",
            }
            return SimpleNamespace(
                columns=["value"],
                rows=[[values[key] if key in values else None]],
                rows_affected=1,
            )

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError, match="no result available"):
        cur.fetchone()
    assert cur.statusmessage is None
    assert cur.rownumber is None
    cur.close()
    with pytest.raises(psycopg.InterfaceError, match="cursor is closed"):
        cur.execute("select 1")

    conn.close()
    assert conn.broken is False
    with pytest.raises(psycopg.OperationalError, match="connection is closed"):
        conn.cursor()
    with pytest.raises(psycopg.OperationalError, match="connection is closed"):
        conn.execute("select 1")


def test_no_tls_connection_adapter_notice_and_fileno_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None
    assert conn.broken is False

    with pytest.raises(psycopg.NotSupportedError, match="socket fileno"):
        conn.fileno()

    def handler(notice: object) -> None:
        del notice

    conn.add_notice_handler(handler)
    conn.remove_notice_handler(handler)
    with pytest.raises(ValueError):
        conn.remove_notice_handler(handler)

    conn._session._session.closed = True
    assert conn.closed is True
    assert conn.broken is True

    conn.close()
    assert conn.closed is True
    assert conn.broken is True


def test_no_tls_connection_adapter_cursor_modes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def close(self) -> None:
            self.closed = True

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    server_cursor = conn.cursor("named", scrollable=True, withhold=True)
    assert isinstance(server_cursor, module.NoTlsServerCursorAdapter)
    assert server_cursor.name == "named"
    assert server_cursor.scrollable is True
    assert server_cursor.withhold is True
    server_cursor.close()
    binary_cursor = conn.cursor(binary=True)
    assert binary_cursor.format == psycopg.pq.Format.BINARY
    with pytest.raises(psycopg.ProgrammingError, match="named server cursor"):
        conn.cursor(scrollable=True)
    with pytest.raises(psycopg.ProgrammingError, match="named server cursor"):
        conn.cursor(withhold=True)
    binary_cur = conn.execute("select 1", binary=True)
    assert binary_cur.fetchall() == [["one"]]
    assert binary_cur.format == psycopg.pq.Format.BINARY


def test_no_tls_server_cursor_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.rows = [[str(value)] for value in range(6)]
            self.pos = 0
            self.calls: list[str] = []

        def close(self) -> None:
            self.closed = True

        def simple_query_results(self, query: str) -> list[object]:
            self.calls.append(query)
            normalized = query.upper()
            if normalized.startswith("FETCH FORWARD"):
                amount = query.split()[2]
                count = len(self.rows) if amount == "ALL" else int(amount)
                rows = self.rows[self.pos : self.pos + count]
                self.pos += len(rows)
                return [
                    SimpleNamespace(
                        columns=["value"],
                        rows=rows,
                        rows_affected=len(rows),
                        is_tuples=True,
                    )
                ]
            if normalized.startswith("MOVE ABSOLUTE"):
                self.pos = int(query.split()[2])
            elif normalized.startswith("MOVE"):
                self.pos += int(query.split()[1])
            return [
                SimpleNamespace(columns=[], rows=[], rows_affected=0, is_tuples=False)
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            del params
            self.calls.append(query)
            return SimpleNamespace(
                columns=[],
                column_descriptions=[],
                rows=[],
                rows_affected=0,
                is_tuples=False,
            )

        def describe_text(self, query: str) -> object:
            self.calls.append(query)
            return SimpleNamespace(
                columns=[SimpleNamespace(name="value", oid=23, type_name="int4")]
            )

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)
    conn = module.no_tls_connection_adapter(
        "host=localhost", autocommit=False, row_factory=module.tuple_row
    )
    assert conn is not None

    with conn.cursor("odd-name", scrollable=True) as cur:
        cur.execute("select generate_series(0, 5)::int4 as value")
        assert cur.name == "odd-name"
        assert cur.description[0].name == "value"
        assert cur.fetchone() == (0,)
        assert cur.fetchmany(2) == [(1,), (2,)]
        cur.scroll(1)
        assert cur.fetchone() == (4,)
        cur.scroll(2, mode="absolute")
        assert cur.fetchall() == [(2,), (3,), (4,), (5,)]

    assert stub.calls[0] == "BEGIN"
    assert (
        'DECLARE "odd-name" SCROLL CURSOR FOR select generate_series' in stub.calls[1]
    )
    assert stub.calls[-1] == 'CLOSE "odd-name"'


def test_no_tls_connection_adapter_info(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubSession:
        closed = False

        def close(self) -> None:
            self.closed = True

        def probe(self) -> object:
            return SimpleNamespace(
                backend_pid=4321,
                current_user="ferro",
                current_database="ferrocopg",
                server_version_num=170004,
                application_name="ferrocopg-tests",
                server_address="127.0.0.1",
                server_port=5432,
            )

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def prepare_text(self, query: str) -> object:
            return SimpleNamespace(statement_id=1)

        def simple_query_results(self, query: str) -> list[object]:
            return [SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            key = params[0]
            values = {
                "client_encoding": "UTF8",
                "TimeZone": "UTC",
                "application_name": "ferrocopg-tests",
            }
            return SimpleNamespace(
                columns=["value"],
                rows=[[values[key] if key in values else None]],
                rows_affected=1,
            )

        def run_prepared_text_params(
            self, statement_id: int, params: list[str | None]
        ) -> object:
            return SimpleNamespace(columns=["a"], rows=[["one"]], rows_affected=1)

        def pipeline_simple_query_results(
            self, queries: list[str]
        ) -> list[list[object]]:
            return [
                [SimpleNamespace(columns=["q"], rows=[[query]], rows_affected=1)]
                for query in queries
            ]

    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: StubSession())

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None

    assert conn.info.vendor == "PostgreSQL"
    assert conn.info.dbname == "ferrocopg"
    assert conn.info.user == "ferro"
    assert conn.info.application_name == "ferrocopg-tests"
    assert conn.info.server_version == 170004
    assert conn.info.backend_pid == 4321
    assert conn.info.host == "127.0.0.1"
    assert conn.info.hostaddr == "127.0.0.1"
    assert conn.info.port == 5432
    assert conn.info.parameter_status("application_name") == "ferrocopg-tests"
    assert conn.info.parameter_status("client_encoding") == "UTF8"
    assert conn.info.parameter_status("nosuchparam") is None
    assert conn.info.encoding == "utf-8"
    assert conn.info.timezone is timezone.utc
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert conn.info.pipeline_status == psycopg.pq.PipelineStatus.OFF

    with conn.transaction():
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS

    with conn.pipeline():
        assert conn.info.pipeline_status == psycopg.pq.PipelineStatus.ON


def test_backend_connect_target_parses_endpoints() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    target = module.connect_target(
        "host=db1,db2 hostaddr=10.0.0.10,10.0.0.11 port=5433 dbname=postgres"
    )
    assert target is not None
    assert target.backend_stack == "rust-postgres"
    assert target.summary.dbname == "postgres"
    assert len(target.endpoints) == 2
    assert [endpoint.transport for endpoint in target.endpoints] == ["tcp", "tcp"]
    assert [endpoint.target for endpoint in target.endpoints] == ["db1", "db2"]
    assert [endpoint.hostaddr for endpoint in target.endpoints] == [
        "10.0.0.10",
        "10.0.0.11",
    ]
    assert [endpoint.port for endpoint in target.endpoints] == [5433, 5433]
    assert target.endpoints[0].inferred is False


def test_backend_connect_target_defaults_localhost() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    target = module.connect_target("dbname=postgres")
    assert target is not None
    assert len(target.endpoints) == 1
    assert target.endpoints[0].transport == "tcp"
    assert target.endpoints[0].target == "localhost"
    assert target.endpoints[0].port == 5432
    assert target.endpoints[0].inferred is True


def test_backend_connect_no_tls_probe_rejects_tls_required() -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    with pytest.raises(psycopg.NotSupportedError, match="requires TLS"):
        module.connect_no_tls_probe("host=localhost sslmode=require dbname=postgres")


def test_backend_connect_plan_accepts_libpq_tls_options() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    plan = module.connect_plan(
        "host=localhost sslmode=verify-ca sslrootcert=system "
        "sslcert='client cert.pem' sslkey=client.key"
    )
    assert plan is not None
    assert plan.tls_mode == "verify-ca"
    assert plan.can_bootstrap_with_no_tls is False
    assert plan.requires_external_tls_connector is True
    assert "CA verification and no hostname verification" in plan.tls_connector_hint


def test_backend_connect_session_reports_tls_config_error() -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    with pytest.raises(psycopg.OperationalError, match="sslrootcert"):
        module.backend_session(
            "host=localhost sslmode=verify-ca "
            "sslrootcert=/definitely/not/a/root.pem dbname=postgres"
        )


def test_backend_connect_ferrocopg_routes_tls_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))
    calls: list[str] = []

    class StubSession:
        closed = False

        def close(self) -> None:
            pass

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            assert params == ["client_encoding"]
            return SimpleNamespace(columns=["value"], rows=[["UTF8"]], rows_affected=1)

    def stub_backend_session(conninfo: str) -> StubSession:
        calls.append(conninfo)
        return StubSession()

    monkeypatch.setattr(module, "backend_session", stub_backend_session)

    conn = psycopg.connect_ferrocopg("host=localhost sslmode=require dbname=postgres")
    assert conn is not None
    selected = psycopg.connect(
        "host=localhost sslmode=require dbname=postgres", impl="ferrocopg"
    )
    assert selected is not None
    channel_bound = psycopg.connect(
        "host=localhost dbname=postgres",
        impl="ferrocopg",
        sslmode="verify-full",
        channel_binding="require",
    )
    assert channel_bound is not None
    assert calls[:2] == [
        "host=localhost sslmode=require dbname=postgres",
        "host=localhost sslmode=require dbname=postgres",
    ]
    assert dict(item.split("=", 1) for item in calls[2].split()) == {
        "host": "localhost",
        "dbname": "postgres",
        "sslmode": "verify-full",
        "channel_binding": "require",
    }


def test_backend_merge_conninfo_only_parses_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("psycopg._ferrocopg")
    calls: list[str] = []

    def parse_base(conninfo: str) -> dict[str, str]:
        calls.append(conninfo)
        return {"host": "localhost", "user": "postgres"}

    monkeypatch.setattr(module, "conninfo_to_dict", parse_base)

    assert module.merge_conninfo(
        "host=localhost user=postgres",
        {
            "channel_binding": "require",
            "application_name": "ferro copg",
            "port": None,
        },
    ) == (
        "host=localhost user=postgres channel_binding=require "
        "application_name='ferro copg'"
    )
    assert calls == ["host=localhost user=postgres"]
    assert module.merge_conninfo("channel_binding=require", {}) == (
        "channel_binding=require"
    )


def test_backend_connect_error_preserves_operational_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pickle

    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubRustModule:
        def connect_session(self, conninfo: str) -> None:
            raise psycopg.errors.InvalidCatalogName(
                'database "missing" does not exist',
                info={
                    psycopg.pq.DiagnosticField.SQLSTATE: b"3D000",
                    psycopg.pq.DiagnosticField.MESSAGE_PRIMARY: (
                        b'database "missing" does not exist'
                    ),
                },
            )

        def parse_conninfo_summary(self, conninfo: str) -> object:
            return SimpleNamespace(dbname="missing")

    monkeypatch.setattr(module, "_ferrocopg", StubRustModule())

    with pytest.raises(psycopg.OperationalError) as excinfo:
        module.backend_session("host=localhost dbname=missing")

    assert type(excinfo.value) is psycopg.OperationalError
    assert excinfo.value.diag.sqlstate == "3D000"
    assert excinfo.value.pgconn is not None
    assert excinfo.value.pgconn.db == b"missing"

    pickled = pickle.loads(pickle.dumps(excinfo.value))
    assert pickled.pgconn is None
    assert pickled.diag.sqlstate == "3D000"


@pytest.mark.parametrize(
    ("sslmode", "expected_ssl"),
    [
        ("disable", False),
        ("allow", False),
        ("prefer", True),
        ("require", True),
        ("verify-ca", True),
        ("verify-full", True),
    ],
)
def test_backend_tls_modes_live(dsn: str, sslmode: str, expected_ssl: bool) -> None:
    import psycopg

    rootcert = os.environ.get("FERROCOPG_TEST_TLS_ROOTCERT")
    if not rootcert:
        pytest.skip("FERROCOPG_TEST_TLS_ROOTCERT is not configured")

    kwargs = {"sslmode": sslmode}
    if sslmode in {"verify-ca", "verify-full"}:
        kwargs["sslrootcert"] = rootcert

    conn_cm = cast(Any, psycopg.connect(dsn, impl="ferrocopg", **kwargs))
    with conn_cm as conn:
        row = conn.execute(
            "select ssl from pg_catalog.pg_stat_ssl where pid = pg_backend_pid()"
        ).fetchone()
        assert row == (expected_ssl,)


def test_backend_tls_channel_binding_required_live(dsn: str) -> None:
    import psycopg

    rootcert = os.environ.get("FERROCOPG_TEST_TLS_ROOTCERT")
    if not rootcert:
        pytest.skip("FERROCOPG_TEST_TLS_ROOTCERT is not configured")

    conn_cm = cast(
        Any,
        psycopg.connect(
            dsn,
            impl="ferrocopg",
            sslmode="verify-full",
            sslrootcert=rootcert,
            channel_binding="require",
        ),
    )
    with conn_cm as conn:
        assert conn.execute(
            "select ssl from pg_catalog.pg_stat_ssl where pid = pg_backend_pid()"
        ).fetchone() == (True,)


def test_backend_tls_client_certificate_live(dsn: str) -> None:
    import psycopg

    rootcert = os.environ.get("FERROCOPG_TEST_TLS_ROOTCERT")
    sslcert = os.environ.get("FERROCOPG_TEST_TLS_CERT")
    sslkey = os.environ.get("FERROCOPG_TEST_TLS_KEY")
    if not rootcert or not sslcert or not sslkey:
        pytest.skip("ferrocopg client TLS certificate paths are not configured")

    options = {
        "user": "certuser",
        "dbname": "postgres",
        "sslmode": "verify-full",
        "sslrootcert": rootcert,
    }
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(dsn, impl="ferrocopg", **options)

    conn_cm = cast(
        Any,
        psycopg.connect(
            dsn,
            impl="ferrocopg",
            sslcert=sslcert,
            sslkey=sslkey,
            **options,
        ),
    )
    with conn_cm as conn:
        assert conn.execute(
            "select current_user, ssl "
            "from pg_catalog.pg_stat_ssl where pid = pg_backend_pid()"
        ).fetchone() == ("certuser", True)


def test_backend_connect_no_tls_probe_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    probe = module.connect_no_tls_probe(dsn)
    assert probe is not None
    assert probe.backend_pid > 0
    assert probe.current_database
    assert probe.current_user
    assert probe.server_version_num >= 100000


def test_backend_query_text_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    result = module.query_text_no_tls(
        dsn,
        "select current_user::text as usr, current_database()::text as db",
    )
    assert result is not None
    assert result.columns == ["usr", "db"]
    assert len(result.rows) == 1
    assert result.rows[0][0]
    assert result.rows[0][1]


def test_backend_notice_handler_dispatch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubCancelHandle:
        def cancel(self) -> None:
            pass

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.notices: list[dict[int, bytes | None]] = []

        def cancel_handle(self) -> StubCancelHandle:
            return StubCancelHandle()

        def close(self) -> None:
            self.closed = True

        def simple_query_results(self, query: str) -> list[object]:
            self.notices.append(
                {
                    psycopg.pq.DiagnosticField.SEVERITY: b"NOTICE",
                    psycopg.pq.DiagnosticField.SEVERITY_NONLOCALIZED: b"NOTICE",
                    psycopg.pq.DiagnosticField.SQLSTATE: b"00000",
                    psycopg.pq.DiagnosticField.MESSAGE_PRIMARY: b"price \xa4",
                    psycopg.pq.DiagnosticField.MESSAGE_DETAIL: b"queued safely",
                }
            )
            return [
                SimpleNamespace(
                    columns=[],
                    column_descriptions=[],
                    rows=[],
                    rows_affected=0,
                    is_tuples=False,
                )
            ]

        def drain_notices(self) -> list[dict[int, bytes | None]]:
            notices, self.notices = self.notices, []
            return notices

    session = module.NoTlsSessionAdapter(cast(Any, StubSession()))
    conn = module.NoTlsConnectionAdapter(session)
    conn.pgconn._encoding = "iso8859-15"
    session.encoding = conn.pgconn._encoding
    received: list[psycopg.errors.Diagnostic] = []

    def broken_handler(diag: psycopg.errors.Diagnostic) -> None:
        del diag
        raise RuntimeError("broken notice handler")

    conn.add_notice_handler(broken_handler)
    conn.add_notice_handler(received.append)
    conn.execute("select 1")

    assert len(received) == 1
    assert received[0].severity == "NOTICE"
    assert received[0].severity_nonlocalized == "NOTICE"
    assert received[0].sqlstate == "00000"
    assert received[0].message_primary == "price \u20ac"
    assert received[0].message_detail == "queued safely"
    assert "broken notice handler" in caplog.text

    conn.remove_notice_handler(broken_handler)
    conn.remove_notice_handler(received.append)
    with pytest.raises(ValueError):
        conn.remove_notice_handler(received.append)


def test_backend_error_adapter_compatibility() -> None:
    import pickle

    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    class StubCancelHandle:
        def cancel(self) -> None:
            pass

    class StubSession:
        closed = False

        def __init__(self) -> None:
            self.fail = True

        def cancel_handle(self) -> StubCancelHandle:
            return StubCancelHandle()

        def close(self) -> None:
            self.closed = True

        def simple_query_results(self, query: str) -> list[object]:
            if self.fail:
                raise psycopg.errors.UndefinedTable(
                    'relation "wat" does not exist',
                    info={
                        psycopg.pq.DiagnosticField.SEVERITY: b"ERROR",
                        psycopg.pq.DiagnosticField.SQLSTATE: b"42P01",
                        psycopg.pq.DiagnosticField.MESSAGE_PRIMARY: (
                            b'relation "wat" does not exist'
                        ),
                        psycopg.pq.DiagnosticField.MESSAGE_DETAIL: b"price \xa4",
                        psycopg.pq.DiagnosticField.STATEMENT_POSITION: b"15",
                    },
                )
            return [
                SimpleNamespace(
                    columns=[],
                    column_descriptions=[],
                    rows=[],
                    rows_affected=0,
                    is_tuples=False,
                )
            ]

        def run_text_params(self, query: str, params: list[str | None]) -> object:
            assert params == ["client_encoding"]
            return SimpleNamespace(columns=["value"], rows=[["UTF8"]], rows_affected=1)

    stub = StubSession()
    session = module.NoTlsSessionAdapter(cast(Any, stub))
    conn = module.NoTlsConnectionAdapter(session)
    conn.pgconn._encoding = "iso8859-15"
    session.encoding = conn.pgconn._encoding

    with pytest.raises(psycopg.errors.UndefinedTable) as excinfo:
        conn.execute("select * from wat")

    exc = excinfo.value
    assert isinstance(exc, conn.ProgrammingError)
    assert "LINE 1: select * from wat" in str(exc)
    assert exc.pgresult is not None
    assert exc.pgresult.error_field(psycopg.pq.DiagnosticField.SQLSTATE) == b"42P01"
    assert exc.diag.message_primary == 'relation "wat" does not exist'
    assert exc.diag.message_detail == "price \u20ac"

    pickled = pickle.loads(pickle.dumps(exc))
    assert pickled.pgresult is None
    assert pickled.diag.sqlstate == "42P01"

    assert conn.parameter_status(b"client_encoding") == b"UTF8"
    stub.fail = False
    assert conn.pgconn.exec_(b"set client_min_messages to notice") is not None


def test_backend_notice_handler_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")
    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    received: list[psycopg.errors.Diagnostic] = []
    with cast(Any, psycopg.connect(dsn, impl="ferrocopg")) as conn:
        conn.add_notice_handler(received.append)
        conn.execute("set client_min_messages to notice")
        conn.execute("""
            do $$begin
                raise notice using
                    message = 'hello from ferrocopg',
                    detail = 'queued in rust',
                    hint = 'dispatched in python';
            end$$ language plpgsql
            """)

        assert len(received) == 1
        diag = received[0]
        assert diag.severity == "NOTICE"
        assert diag.severity_nonlocalized == "NOTICE"
        assert diag.sqlstate == "00000"
        assert diag.message_primary == "hello from ferrocopg"
        assert diag.message_detail == "queued in rust"
        assert diag.message_hint == "dispatched in python"
        assert diag.context and "PL/pgSQL" in diag.context
        assert diag.source_file
        assert diag.source_line
        assert diag.source_function

        conn.execute("set client_encoding to latin9")
        assert conn.info.encoding == "iso8859-15"
        conn.execute(
            "do $$begin raise notice 'price %', chr(8364); end$$ language plpgsql"
        )
        assert received[-1].message_primary == "price \u20ac"

        conn.remove_notice_handler(received.append)
        conn.execute("do $$begin raise notice 'ignored'; end$$ language plpgsql")
        assert len(received) == 2


def test_backend_phase35_cursor_copy_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")
    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    with cast(Any, psycopg.connect(dsn, impl="ferrocopg")) as conn:
        conn.execute(
            "create temp table ferrocopg_copy35 (id int4 primary key, label text)"
        )
        with conn.cursor().copy(
            "copy ferrocopg_copy35 from stdin (format binary)"
        ) as copy:
            copy.set_types(["int4", "text"])
            copy.write_row((1, "one"))
            copy.write_row((2, "two"))
            copy.write_row((3, "three"))

        with conn.cursor().copy(
            "copy (select id, label from ferrocopg_copy35 "
            "where id >= %s order by id) to stdout (format binary)",
            (2,),
        ) as copy:
            copy.set_types(["int4", "text"])
            assert list(copy.rows()) == [(2, "two"), (3, "three")]

        with conn.cursor("phase35", scrollable=True) as cur:
            cur.execute("select id, label from ferrocopg_copy35 order by id")
            assert cur.fetchone() == (1, "one")
            cur.scroll(1)
            assert cur.fetchone() == (3, "three")
            cur.scroll(1, mode="absolute")
            assert cur.fetchone() == (2, "two")


def test_backend_phase36_tpc_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")
    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    table_name = f"ferrocopg_tpc36_{uuid.uuid4().hex[:12]}"
    prepared_gid = f"ferrocopg-prepare-{uuid.uuid4().hex}"
    rollback_gid = f"ferrocopg-rollback-{uuid.uuid4().hex}"

    setup = cast(Any, psycopg.connect(dsn, impl="ferrocopg", autocommit=True))
    try:
        if int(setup.execute("show max_prepared_transactions").fetchone()[0]) == 0:
            pytest.skip("prepared transactions are disabled")
        setup.execute(f'create table "{table_name}" (value text primary key)')
    finally:
        setup.close()

    try:
        conn = cast(Any, psycopg.connect(dsn, impl="ferrocopg"))
        conn.tpc_begin(prepared_gid)
        conn.execute(f'insert into "{table_name}" values (%s)', ("committed",))
        conn.tpc_prepare()
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        conn.close()

        recovered = cast(Any, psycopg.connect(dsn, impl="ferrocopg"))
        xids = recovered.tpc_recover()
        prepared = next(xid for xid in xids if xid.gtrid == prepared_gid)
        recovered.tpc_commit(prepared)
        assert recovered.execute(f'select value from "{table_name}"').fetchone() == (
            "committed",
        )
        recovered.rollback()

        recovered.tpc_begin(rollback_gid)
        recovered.execute(f'insert into "{table_name}" values (%s)', ("rolled-back",))
        recovered.tpc_rollback()
        assert recovered.execute(f'select count(*) from "{table_name}"').fetchone() == (
            1,
        )
        recovered.close()
    finally:
        cleanup = cast(Any, psycopg.connect(dsn, impl="ferrocopg", autocommit=True))
        try:
            for xid in cleanup.tpc_recover():
                if xid.gtrid in {prepared_gid, rollback_gid}:
                    cleanup.tpc_rollback(xid)
            cleanup.execute(f'drop table if exists "{table_name}"')
        finally:
            cleanup.close()


def test_backend_phase37_async_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")
    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    async def exercise() -> None:
        async with await psycopg.FerrocopgAsyncConnection.connect(dsn) as conn:
            result = await conn.execute("select %s::int4", (7,))
            assert await result.fetchone() == (7,)

            await conn.execute(
                "create temp table ferrocopg_async37 (id int4 primary key, label text)"
            )
            async with conn.cursor().copy(
                "copy ferrocopg_async37 from stdin (format binary)"
            ) as copy:
                copy.set_types(["int4", "text"])
                await copy.write_row((1, "one"))
                await copy.write_row((2, "two"))

            async with conn.cursor().copy(
                "copy (select id, label from ferrocopg_async37 order by id) "
                "to stdout (format binary)"
            ) as copy:
                copy.set_types(["int4", "text"])
                assert [row async for row in copy.rows()] == [(1, "one"), (2, "two")]

            async with conn.cursor("async37", scrollable=True) as cursor:
                await cursor.execute(
                    "select id, label from ferrocopg_async37 order by id"
                )
                assert await cursor.fetchone() == (1, "one")
                await cursor.scroll(1, "absolute")
                assert await cursor.fetchone() == (2, "two")

            async with conn.pipeline():
                left = await conn.execute("select 'left'::text")
                right = await conn.execute("select 'right'::text")
            assert await left.fetchone() == ("left",)
            assert await right.fetchone() == ("right",)

    asyncio.run(exercise())


def test_backend_no_tls_error_mapping_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    with pytest.raises(psycopg.errors.SyntaxError, match="syntax error") as excinfo:
        module.query_text_no_tls(dsn, "select from")
    assert excinfo.value.sqlstate == "42601"
    assert excinfo.value.diag.sqlstate == "42601"
    assert excinfo.value.diag.message_primary is not None
    assert "syntax error" in excinfo.value.diag.message_primary
    assert excinfo.value.diag.statement_position is not None

    with pytest.raises(psycopg.ProgrammingError, match="expected 1 params but got 0"):
        module.query_text_params_no_tls(dsn, "select $1::text", [])

    with pytest.raises(
        psycopg.ProgrammingError,
        match=r"unsupported parameter type at \$1: timetz",
    ):
        module.query_text_params_no_tls(
            dsn, "select $1::timetz::text", ["03:04:05+02:00"]
        )

    table_name = f"ferrocopg_error_diag_{uuid.uuid4().hex[:12]}"
    quoted_table = f'"{table_name}"'
    module.query_text_no_tls(dsn, f"drop table if exists {quoted_table}")
    try:
        module.query_text_no_tls(
            dsn, f"create table {quoted_table} (id int4 primary key)"
        )
        module.query_text_no_tls(dsn, f"insert into {quoted_table} values (1)")

        with pytest.raises(psycopg.errors.UniqueViolation) as unique_exc:
            module.query_text_no_tls(dsn, f"insert into {quoted_table} values (1)")

        assert unique_exc.value.sqlstate == "23505"
        assert unique_exc.value.diag.sqlstate == "23505"
        assert unique_exc.value.diag.schema_name == "public"
        assert unique_exc.value.diag.table_name == table_name
        assert unique_exc.value.diag.constraint_name == f"{table_name}_pkey"
        assert unique_exc.value.diag.message_detail is not None
    finally:
        module.query_text_no_tls(dsn, f"drop table if exists {quoted_table}")


def test_backend_simple_query_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    messages = module.simple_query_no_tls(
        dsn,
        "select 'alpha'::text as label; select 'beta'::text as label",
    )
    assert messages is not None
    assert [
        (message.kind, message.columns, message.values, message.rows_affected)
        for message in messages
    ] == [
        ("row_description", ["label"], [], None),
        ("row", ["label"], ["alpha"], None),
        ("command_complete", [], [], 1),
        ("row_description", ["label"], [], None),
        ("row", ["label"], ["beta"], None),
        ("command_complete", [], [], 1),
    ]


def test_backend_simple_query_results_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    results = module.simple_query_results_no_tls(
        dsn,
        "select 'alpha'::text as label; select 'beta'::text as label",
    )
    assert results is not None
    assert [
        (result.columns, result.rows, result.rows_affected) for result in results
    ] == [
        (["label"], [["alpha"]], 1),
        (["label"], [["beta"]], 1),
    ]


def test_backend_pipeline_simple_query_results_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    batches = module.pipeline_simple_query_results_no_tls(
        dsn,
        [
            "select 'alpha'::text as label",
            "select 'beta'::text as label; select 'gamma'::text as label",
        ],
    )
    assert batches is not None
    assert [
        [(result.columns, result.rows, result.rows_affected) for result in batch]
        for batch in batches
    ] == [
        [(["label"], [["alpha"]], 1)],
        [(["label"], [["beta"]], 1), (["label"], [["gamma"]], 1)],
    ]


def test_backend_query_text_params_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    marker = uuid.UUID("12345678-1234-5678-1234-567812345678")
    result = module.query_text_params_no_tls(
        dsn,
        "select "
        "($1::int4 + $2::int4)::text as total, "
        "$3::text as label, "
        "$4::text as nullable, "
        "$5::date::text as day, "
        "$6::uuid::text as marker, "
        "to_char($7::time, 'HH24:MI:SS.US') as clock, "
        "to_char($8::timestamp, 'YYYY-MM-DD HH24:MI:SS.US') as ts, "
        "to_char($9::timestamptz at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') as ts_utc, "
        "($10::interval = '3 days 01:01:01.000042'::interval)::text as span_ok",
        [
            "2",
            "5",
            "sum",
            None,
            "2024-01-02",
            str(marker),
            "03:04:05.678901",
            "2024-01-02 03:04:05.678901",
            "2024-01-02 03:04:05.678901+02:30",
            "3 days 1:01:01.000042",
        ],
    )
    assert result is not None
    assert result.columns == [
        "total",
        "label",
        "nullable",
        "day",
        "marker",
        "clock",
        "ts",
        "ts_utc",
        "span_ok",
    ]
    assert result.rows == [
        [
            "7",
            "sum",
            None,
            "2024-01-02",
            str(marker),
            "03:04:05.678901",
            "2024-01-02 03:04:05.678901",
            "2024-01-02 00:34:05.678901",
            "true",
        ]
    ]


def test_backend_run_text_params_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    select_result = module.run_text_params_no_tls(
        dsn,
        "select ($1::int4 + $2::int4)::text as total, $3::text as label",
        ["2", "5", "sum"],
    )
    assert select_result is not None
    assert (select_result.columns, select_result.rows, select_result.rows_affected) == (
        ["total", "label"],
        [[b"7", b"sum"]],
        1,
    )
    assert [
        (column.name, column.oid, column.type_name)
        for column in select_result.column_descriptions
    ] == [
        ("total", 25, "text"),
        ("label", 25, "text"),
    ]

    command_result = module.run_text_params_no_tls(
        dsn,
        "create temporary table ferrocopg_run_result_test (id int4, label text)",
        [],
    )
    assert (
        command_result.columns,
        command_result.rows,
        command_result.rows_affected,
    ) == (
        [],
        [],
        0,
    )
    assert command_result.column_descriptions == []


def test_backend_execute_text_params_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    result = module.execute_text_params_no_tls(
        dsn,
        "create temporary table ferrocopg_execute_test (id int4, label text)",
        [],
    )
    assert result is not None
    assert result.rows_affected == 0


def test_backend_describe_text_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    description = module.describe_text_no_tls(
        dsn,
        "select $1::int4 as n, $2::text as t",
    )
    assert description is not None
    assert [(param.oid, param.type_name) for param in description.params] == [
        (23, "int4"),
        (25, "text"),
    ]
    assert [
        (column.name, column.oid, column.type_name) for column in description.columns
    ] == [
        ("n", 23, "int4"),
        ("t", 25, "text"),
    ]


def test_backend_no_tls_session_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    session = module.no_tls_session(dsn)
    assert session is not None
    assert session.closed is False

    probe = session.probe()
    assert probe.backend_pid > 0

    result = session.query_text("select current_database()::text as db")
    assert result.columns == ["db"]
    assert len(result.rows) == 1
    assert result.rows[0][0]

    simple_messages = session.simple_query(
        "select 'first'::text as label; select 'second'::text as label"
    )
    assert [
        (message.kind, message.columns, message.values, message.rows_affected)
        for message in simple_messages
    ] == [
        ("row_description", ["label"], [], None),
        ("row", ["label"], ["first"], None),
        ("command_complete", [], [], 1),
        ("row_description", ["label"], [], None),
        ("row", ["label"], ["second"], None),
        ("command_complete", [], [], 1),
    ]

    simple_results = session.simple_query_results(
        "select 'first'::text as label; select 'second'::text as label"
    )
    assert [
        (result.columns, result.rows, result.rows_affected) for result in simple_results
    ] == [
        (["label"], [["first"]], 1),
        (["label"], [["second"]], 1),
    ]

    pipeline_results = session.pipeline_simple_query_results(
        [
            "select 'alpha'::text as label",
            "select 'beta'::text as label; select 'gamma'::text as label",
        ]
    )
    assert [
        [(result.columns, result.rows, result.rows_affected) for result in batch]
        for batch in pipeline_results
    ] == [
        [(["label"], [["alpha"]], 1)],
        [(["label"], [["beta"]], 1), (["label"], [["gamma"]], 1)],
    ]

    bound = session.query_text_params(
        "select ($1::int4 + $2::int4)::text as total, $3::text as label, $4::text as nullable",
        ["3", "4", "session", None],
    )
    assert bound.columns == ["total", "label", "nullable"]
    assert bound.rows == [["7", "session", None]]

    bound_result = session.run_text_params(
        "select ($1::int4 + $2::int4)::text as total, $3::text as label, $4::text as nullable",
        ["3", "4", "session", None],
    )
    assert (
        bound_result.columns,
        bound_result.rows,
        bound_result.rows_affected,
    ) == (["total", "label", "nullable"], [[b"7", b"session", None]], 1)

    marker = uuid.UUID("12345678-1234-5678-1234-567812345678")
    typed_bound = session.query_text_params(
        "select "
        "$1::date::text as day, "
        "$2::uuid::text as marker, "
        "$3::date::text as missing_day, "
        "$4::uuid::text as missing_marker, "
        "to_char($5::time, 'HH24:MI:SS.US') as clock, "
        "to_char($6::timestamp, 'YYYY-MM-DD HH24:MI:SS.US') as ts, "
        "to_char($7::timestamptz at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') as ts_utc, "
        "($8::interval = '3 days 01:01:01.000042'::interval)::text as span_ok",
        [
            "2024-01-02",
            str(marker),
            None,
            None,
            "03:04:05.678901",
            "2024-01-02 03:04:05.678901",
            "2024-01-02 03:04:05.678901+02:30",
            "3 days 1:01:01.000042",
        ],
    )
    assert typed_bound.columns == [
        "day",
        "marker",
        "missing_day",
        "missing_marker",
        "clock",
        "ts",
        "ts_utc",
        "span_ok",
    ]
    assert typed_bound.rows == [
        [
            "2024-01-02",
            str(marker),
            None,
            None,
            "03:04:05.678901",
            "2024-01-02 03:04:05.678901",
            "2024-01-02 00:34:05.678901",
            "true",
        ]
    ]

    ddl = session.execute_text_params(
        "create temporary table ferrocopg_session_test (id int4, label text)",
        [],
    )
    assert ddl.rows_affected == 0
    ddl_result = session.run_text_params(
        "create temporary table ferrocopg_session_run_test (id int4, label text)",
        [],
    )
    assert (ddl_result.columns, ddl_result.rows, ddl_result.rows_affected) == (
        [],
        [],
        0,
    )
    inserted = session.execute_text_params(
        "insert into ferrocopg_session_test (id, label) values ($1::int4, $2::text)",
        ["10", "row"],
    )
    assert inserted.rows_affected == 1
    inserted_null = session.execute_text_params(
        "insert into ferrocopg_session_test (id, label) values ($1::int4, $2::text)",
        ["11", None],
    )
    assert inserted_null.rows_affected == 1
    stored = session.query_text(
        "select id::text as id, label from ferrocopg_session_test order by id"
    )
    assert stored.columns == ["id", "label"]
    assert stored.rows == [["10", "row"], ["11", None]]

    session.begin()
    tx_inserted = session.execute_text_params(
        "insert into ferrocopg_session_test (id, label) values ($1::int4, $2::text)",
        ["12", "rolled back"],
    )
    assert tx_inserted.rows_affected == 1
    session.rollback()
    after_rollback = session.query_text(
        "select id::text as id, label from ferrocopg_session_test order by id"
    )
    assert after_rollback.rows == [["10", "row"], ["11", None]]

    session.begin()
    tx_committed = session.execute_text_params(
        "insert into ferrocopg_session_test (id, label) values ($1::int4, $2::text)",
        ["13", "committed"],
    )
    assert tx_committed.rows_affected == 1
    session.commit()
    after_commit = session.query_text(
        "select id::text as id, label from ferrocopg_session_test order by id"
    )
    assert after_commit.rows == [["10", "row"], ["11", None], ["13", "committed"]]

    prepared_insert = session.prepare_text(
        "insert into ferrocopg_session_test (id, label) values ($1::int4, $2::text)"
    )
    assert prepared_insert.statement_id > 0
    assert [
        (param.oid, param.type_name) for param in prepared_insert.description.params
    ] == [
        (23, "int4"),
        (25, "text"),
    ]
    assert prepared_insert.description.columns == []
    described_insert = session.describe_prepared(prepared_insert.statement_id)
    assert [(param.oid, param.type_name) for param in described_insert.params] == [
        (23, "int4"),
        (25, "text"),
    ]
    inserted_prepared = session.execute_prepared_text_params(
        prepared_insert.statement_id,
        ["14", "prepared"],
    )
    assert inserted_prepared.rows_affected == 1

    prepared_query = session.prepare_text(
        "select id::text as id, label from ferrocopg_session_test where id >= $1::int4 order by id"
    )
    assert prepared_query.statement_id > prepared_insert.statement_id
    assert [
        (param.oid, param.type_name) for param in prepared_query.description.params
    ] == [
        (23, "int4"),
    ]
    queried_prepared = session.query_prepared_text_params(
        prepared_query.statement_id, ["13"]
    )
    assert queried_prepared.columns == ["id", "label"]
    assert queried_prepared.rows == [["13", "committed"], ["14", "prepared"]]
    queried_prepared_result = session.run_prepared_text_params(
        prepared_query.statement_id,
        ["13"],
    )
    assert (
        queried_prepared_result.columns,
        queried_prepared_result.rows,
        queried_prepared_result.rows_affected,
    ) == (["id", "label"], [[b"13", b"committed"], [b"14", b"prepared"]], 2)
    session.close_prepared(prepared_query.statement_id)
    with pytest.raises(psycopg.ProgrammingError, match="unknown prepared statement id"):
        session.describe_prepared(prepared_query.statement_id)

    listener_channel = f"ferrocopg_backend_notify_{uuid.uuid4().hex}"
    sender = module.no_tls_session(dsn)
    assert sender is not None
    listener_probe = session.probe()

    session.listen(listener_channel)
    sender.notify(listener_channel, "first")
    first_notification = session.wait_for_notification(1_000)
    assert first_notification is not None
    assert first_notification.channel == listener_channel
    assert first_notification.payload == "first"
    assert first_notification.process_id == sender.probe().backend_pid

    sender.notify(listener_channel, "second")
    sender.notify(listener_channel, "third")
    second_notification = session.wait_for_notification(1_000)
    third_notification = session.wait_for_notification(1_000)
    assert second_notification is not None
    assert third_notification is not None
    observed_payloads = sorted(
        [second_notification.payload, third_notification.payload]
    )
    assert observed_payloads == ["second", "third"]
    assert all(
        notification.channel == listener_channel
        for notification in [second_notification, third_notification]
    )
    assert all(
        notification.process_id == sender.probe().backend_pid
        for notification in [second_notification, third_notification]
    )

    sender.notify(listener_channel, "drained")
    drained_notification = session.wait_for_notification(1_000)
    assert drained_notification is not None
    assert drained_notification.payload == "drained"
    assert session.drain_notifications() == []

    session.unlisten(listener_channel)
    sender.notify(listener_channel, "ignored")
    assert session.wait_for_notification(150) is None
    assert session.probe().backend_pid == listener_probe.backend_pid
    sender.close()

    description = session.describe_text("select $1::int4 as n, $2::text as t")
    assert [(param.oid, param.type_name) for param in description.params] == [
        (23, "int4"),
        (25, "text"),
    ]
    assert [
        (column.name, column.oid, column.type_name) for column in description.columns
    ] == [
        ("n", 23, "int4"),
        ("t", 25, "text"),
    ]

    copy_in_count = session.copy_from_stdin(
        "copy ferrocopg_session_test (id, label) from stdin",
        b"15\tcopied in\n16\tcopied out\n",
    )
    assert copy_in_count == 2
    copied_out = session.copy_to_stdout(
        "copy (select id, label from ferrocopg_session_test where id >= 15 order by id) to stdout"
    )
    assert copied_out.data == b"15\tcopied in\n16\tcopied out\n"

    session.close()
    assert session.closed is True

    with pytest.raises(psycopg.OperationalError, match="closed"):
        session.query_text("select 1")
    with pytest.raises(psycopg.OperationalError, match="closed"):
        session.describe_text("select 1")


def test_backend_no_tls_session_adapter_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    adapter = module.no_tls_session_adapter(dsn)
    assert adapter is not None
    assert adapter.closed is False

    simple = adapter.execute_simple(
        "select 'first'::text as label; select 'second'::text as label"
    )
    assert simple.fetchall() == [["first"]]
    assert simple.nextset() is True
    assert simple.fetchall() == [["second"]]
    assert simple.nextset() is None

    pipeline = adapter.execute_pipeline_simple(
        [
            "select 'alpha'::text as label",
            "select 'beta'::text as label; select 'gamma'::text as label",
        ]
    )
    assert pipeline[0].fetchall() == [["alpha"]]
    assert [res.fetchall() for res in pipeline[1].results()] == [
        [["beta"]],
        [["gamma"]],
    ]

    bound = adapter.execute_params(
        "select ($1::int4 + $2::int4)::text as total, $3::text as label",
        ["2", "5", "sum"],
    )
    assert bound.columns == ["total", "label"]
    assert bound.rows_affected == 1
    assert bound.fetchall() == [[b"7", b"sum"]]

    prepared = adapter.prepare_text(
        "select id::text as id, label from (values (1, 'one'), (2, 'two')) as t(id, label) where id >= $1::int4 order by id"
    )
    prepared_cur = adapter.execute_prepared(prepared.statement_id, ["2"])
    assert prepared_cur.fetchall() == [[b"2", b"two"]]

    adapter.close()
    assert adapter.closed is True


def test_backend_no_tls_connection_adapter_live(dsn: str) -> None:
    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    conn = module.no_tls_connection_adapter(dsn)
    assert conn is not None
    assert conn.closed is False
    assert conn.info.vendor == "PostgreSQL"
    assert conn.info.dbname
    assert conn.info.user
    assert conn.info.server_version >= 100000
    assert conn.info.backend_pid > 0
    assert conn.info.parameter_status("client_encoding") == "UTF8"
    assert conn.info.parameter_status("TimeZone") is not None
    assert conn.info.encoding == "utf-8"
    assert conn.info.timezone is not None
    assert conn.info.password == "password"
    assert conn.info.options == ""
    assert conn.info.status == psycopg.pq.ConnStatus.OK
    assert conn.info.full_protocol_version == 30000
    assert conn.info.error_message == ""
    assert "password" not in conn.info.get_parameters()
    assert "password" not in conn.info.dsn
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert conn.info.pipeline_status == psycopg.pq.PipelineStatus.OFF

    cur = conn.execute("select 'first'::text as label; select 'second'::text as label")
    assert cur.statusmessage == "SELECT 1"
    assert cur.fetchall() == [["first"]]
    assert cur.nextset() is True
    assert cur.statusmessage == "SELECT 1"
    assert cur.fetchall() == [["second"]]
    assert cur.set_result(0) is cur
    assert [res.fetchall() for res in cur.results()] == [[["first"]], [["second"]]]

    pipeline = conn.execute_pipeline_simple(
        [
            "select 'alpha'::text as label",
            "select 'beta'::text as label; select 'gamma'::text as label",
        ],
        row_factory=module.scalar_row,
    )
    assert pipeline[0].fetchall() == ["alpha"]
    assert [res.fetchall() for res in pipeline[1].results()] == [["beta"], ["gamma"]]

    with conn.pipeline() as pipeline_ctx:
        assert conn.info.pipeline_status == psycopg.pq.PipelineStatus.ON
        queued1 = pipeline_ctx.execute(
            "select 'pipeline-a'::text as label",
            row_factory=module.scalar_row,
        )
        queued2 = pipeline_ctx.execute("select 'pipeline-b'::text as label")
        queued_params = pipeline_ctx.execute(
            "select $1::text as label",
            params=["pipeline-param"],
            row_factory=module.scalar_row,
        )
        with pytest.raises(psycopg.ProgrammingError, match="no result available"):
            queued1.fetchall()
        pipeline_ctx.sync()
        assert queued1.fetchall() == ["pipeline-a"]
        assert queued2.fetchall() == [["pipeline-b"]]
        assert queued_params.fetchall() == ["pipeline-param"]
        queued3 = pipeline_ctx.execute(
            "select 'pipeline-c'::text as label",
            row_factory=module.scalar_row,
        )

    assert queued3.fetchall() == ["pipeline-c"]
    assert conn.info.pipeline_status == psycopg.pq.PipelineStatus.OFF

    conn.set_isolation_level(psycopg.IsolationLevel.SERIALIZABLE)
    conn.set_read_only(True)
    conn.set_deferrable(False)
    with conn.transaction():
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
        tx_params = conn.execute(
            "select current_setting('transaction_isolation'), "
            "current_setting('transaction_read_only'), "
            "current_setting('transaction_deferrable')"
        )
        assert tx_params.fetchall() == [["serializable", "on", "off"]]
    conn.rollback()
    conn.set_isolation_level(None)
    conn.set_read_only(None)
    conn.set_deferrable(None)

    listener_channel = f"ferrocopg_conn_notify_{uuid.uuid4().hex}"
    sender = module.no_tls_connection_adapter(dsn)
    assert sender is not None
    conn.listen(listener_channel)
    sender.notify(listener_channel, "alpha")
    got = conn.wait_for_notification(1.0)
    assert got is not None
    assert got.channel == listener_channel
    assert got.payload == "alpha"
    drained = conn.drain_notifications()
    assert drained == []
    sender.notify(listener_channel, "beta")
    streamed = list(conn.notifies(timeout=1.0, stop_after=1))
    assert len(streamed) == 1
    assert streamed[0].channel == listener_channel
    assert streamed[0].payload == "beta"
    seen: list[psycopg.Notify] = []
    conn.add_notify_handler(lambda n: seen.append(n))
    sender.notify(listener_channel, "gamma")
    assert conn.wait_for_notification(1.0) == seen[-1]
    conn.unlisten(listener_channel)
    sender.close()

    with conn.cursor() as cur2:
        cur2.execute(
            "select ($1::int4 + $2::int4)::text as total, $3::text as label",
            ["2", "5", "sum"],
        )
        assert cur2.description == [
            module.BackendColumn("total", 25),
            module.BackendColumn("label", 25),
        ]
        assert cur2.rowcount == 1
        assert cur2.statusmessage == "SELECT 1"
        assert cur2.fetchmany(1) == [["7", "sum"]]
        assert cur2.fetchone() is None
        cur2.execute(
            "select ($1::int4 + $2::int4)::text as total, $3::text as label",
            ["2", "5", "sum"],
        )
        assert cur2.fetchall() == [["7", "sum"]]
        assert cur2.rownumber == 1

    marker = uuid.UUID("12345678-1234-5678-1234-567812345678")
    psycopg_style_params = conn.execute(
        "select "
        "(%s::int4 + %s::int4)::text as total, "
        "%s::date::text as day, "
        "%s::uuid::text as marker, "
        "%s::text as label",
        [2, 5, date(2024, 1, 2), marker, "sum"],
    )
    assert psycopg_style_params.fetchall() == [["7", "2024-01-02", str(marker), "sum"]]

    temporal_params = conn.execute(
        "select "
        "to_char(%s::time, 'HH24:MI:SS.US') as clock, "
        "to_char(%s::timestamp, 'YYYY-MM-DD HH24:MI:SS.US') as ts, "
        "to_char(%s::timestamptz at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') as ts_utc, "
        "(%s::interval = '3 days 01:01:01.000042'::interval)::text as span_ok",
        [
            time(3, 4, 5, 678901),
            datetime(2024, 1, 2, 3, 4, 5, 678901),
            datetime(
                2024,
                1,
                2,
                3,
                4,
                5,
                678901,
                tzinfo=timezone(timedelta(hours=2, minutes=30)),
            ),
            timedelta(days=3, seconds=3661, microseconds=42),
        ],
    )
    assert temporal_params.fetchall() == [
        [
            "03:04:05.678901",
            "2024-01-02 03:04:05.678901",
            "2024-01-02 00:34:05.678901",
            "true",
        ]
    ]

    prep_query = (
        "select id::text as id, label from "
        "(values (1, 'one'), (2, 'two')) as t(id, label) "
        "where id >= $1::int4 order by id"
    )
    first = conn.execute(prep_query, ["1"], prepare=True)
    second = conn.execute(prep_query, ["2"], prepare=True)
    assert first.fetchall() == [["1", "one"], ["2", "two"]]
    assert second.fetchall() == [["2", "two"]]

    dict_cur = conn.execute(
        "select 10::text as id, 'ten'::text as label",
        row_factory=module.dict_row,
    )
    assert dict_cur.fetchall() == [{"id": "10", "label": "ten"}]

    tuple_cur = conn.execute(
        "select 1::text as id, 'one'::text as label union all select 2::text, 'two'::text order by id",
        row_factory=module.tuple_row,
    )
    assert list(tuple_cur) == [("1", "one"), ("2", "two")]

    scalar_cur = conn.execute(
        "select 42::text as answer",
        row_factory=module.scalar_row,
    )
    assert scalar_cur.fetchone() == "42"

    conn.execute("create temporary table ferrocopg_conn_execmany_test (id int4)")
    with conn.cursor() as cur3:
        rv = cur3.executemany(
            "insert into ferrocopg_conn_execmany_test (id) values ($1::int4)",
            [["1"], ["2"], ["3"]],
        )
        assert rv is None
        assert cur3.rowcount == 3
        assert cur3.statusmessage == "INSERT 0 3"
        assert cur3.rownumber is None

    verify_many = conn.execute(
        "select id::text as id from ferrocopg_conn_execmany_test order by id"
    )
    assert verify_many.fetchall() == [["1"], ["2"], ["3"]]

    with conn.cursor(row_factory=module.scalar_row) as cur4:
        rv = cur4.executemany(
            "select $1::text as label",
            [["alpha"], ["beta"]],
            returning=True,
            prepare=True,
        )
        assert rv is None
        assert cur4.fetchone() == "alpha"
        assert cur4.nextset() is True
        assert cur4.fetchone() == "beta"
        assert cur4.nextset() is None

    conn.execute(
        "create temporary table ferrocopg_conn_copy_test (id int4, label text)"
    )
    with conn.cursor() as cur_copy:
        with cur_copy.copy(
            "copy ferrocopg_conn_copy_test (id, label) from stdin"
        ) as copy:
            copy.write("1\tone\n")
            copy.write(b"2\ttwo\n")
            copy.write_row(["3", "three"])
        assert cur_copy.rowcount == 3
        assert cur_copy.statusmessage == "COPY 3"

    copied_rows = conn.execute(
        "select id::text as id, label from ferrocopg_conn_copy_test order by id"
    )
    assert copied_rows.fetchall() == [["1", "one"], ["2", "two"], ["3", "three"]]

    with conn.cursor() as cur_copy_out:
        with cur_copy_out.copy(
            "copy (select id::text, label from ferrocopg_conn_copy_test order by id) to stdout"
        ) as copy:
            assert copy.read(6) == b"1\tone\n"
            assert copy.read() == b"2\ttwo\n"
            assert copy.read() == b"3\tthree\n"
            assert copy.read() == b""

    with conn.cursor() as cur_copy_rows:
        with cur_copy_rows.copy(
            "copy (select id::text, label from ferrocopg_conn_copy_test order by id) to stdout"
        ) as copy:
            assert copy.read_row() == ("1", "one")
            assert list(copy.rows()) == [("2", "two"), ("3", "three")]

    conn.begin()
    conn.execute("create temporary table ferrocopg_conn_adapter_test (id int4)")
    conn.execute(
        "insert into ferrocopg_conn_adapter_test (id) values ($1::int4)", ["1"]
    )
    conn.rollback()
    check = conn.execute(
        "select count(*)::text as n from pg_tables where tablename = 'ferrocopg_conn_adapter_test'"
    )
    assert check.fetchall() == [["0"]]

    with conn.transaction():
        conn.execute("create temporary table ferrocopg_conn_tx_test (id int4)")
        conn.execute("insert into ferrocopg_conn_tx_test (id) values ($1::int4)", ["1"])
        with conn.transaction("inner_tx"):
            conn.execute(
                "insert into ferrocopg_conn_tx_test (id) values ($1::int4)", ["2"]
            )

    committed = conn.execute(
        "select id::text as id from ferrocopg_conn_tx_test order by id"
    )
    assert committed.fetchall() == [["1"], ["2"]]

    with conn.transaction():
        conn.execute("insert into ferrocopg_conn_tx_test (id) values ($1::int4)", ["3"])
        with conn.transaction():
            conn.execute(
                "insert into ferrocopg_conn_tx_test (id) values ($1::int4)", ["4"]
            )
            raise module.Rollback()

    after_inner_rollback = conn.execute(
        "select id::text as id from ferrocopg_conn_tx_test order by id"
    )
    assert after_inner_rollback.fetchall() == [["1"], ["2"], ["3"]]

    with conn.transaction(force_rollback=True):
        conn.execute("insert into ferrocopg_conn_tx_test (id) values ($1::int4)", ["5"])

    after_force_rollback = conn.execute(
        "select id::text as id from ferrocopg_conn_tx_test order by id"
    )
    assert after_force_rollback.fetchall() == [["1"], ["2"], ["3"]]

    conn.close()
    assert conn.closed is True

    with pytest.raises(psycopg.OperationalError, match="connection is closed"):
        conn.execute("select 1")


def test_backend_package_connect_ferrocopg_live(dsn: str) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    conn = cast(Any, psycopg.connect_ferrocopg(dsn))
    assert conn is not None
    assert conn.execute("select 42::int4 as answer").fetchall() == [(42,)]
    assert conn.execute("select %s, %s", [10, 20]).fetchall() == [(10, 20)]
    binary_cursor = conn.cursor(binary=True)
    binary_cursor.execute("select 42::int4, '\\x0001ff'::bytea")
    assert binary_cursor.fetchall() == [(42, b"\x00\x01\xff")]
    binary_cursor.close()
    assert conn.execute("select %s as answer", [42], prepare=True).fetchall() == [(42,)]
    assert conn.execute("select %s as answer", [43], prepare=True).fetchall() == [(43,)]
    assert conn.execute("select 'semi;colon'::text as label").fetchall() == [
        ("semi;colon",)
    ]

    cur = conn.execute(
        "select 'ferrocopg'::text as label", row_factory=module.scalar_row
    )
    assert cur.fetchall() == ["ferrocopg"]

    TrackingCursor = cast(Any, type("TrackingCursor", (module.NoTlsCursorAdapter,), {}))

    custom_cursor_conn = cast(
        Any,
        psycopg.connect_ferrocopg(dsn, cursor_factory=TrackingCursor),
    )
    custom_cur = cast(Any, custom_cursor_conn.cursor(row_factory=module.scalar_row))
    assert isinstance(custom_cur, TrackingCursor)
    assert custom_cur.execute("select 'custom-cursor'::text").fetchall() == [
        "custom-cursor"
    ]
    custom_cur.close()
    custom_cursor_conn.close()

    scalar_conn = cast(
        Any, psycopg.connect_ferrocopg(dsn, row_factory=module.scalar_row)
    )
    scalar_cur = scalar_conn.execute("select 'default-row-factory'::text as label")
    assert scalar_cur.fetchall() == ["default-row-factory"]
    with scalar_conn.pipeline() as p:
        queued = p.execute("select 'selector-pipeline'::text as label")
    assert queued.fetchall() == ["selector-pipeline"]
    scalar_conn.set_isolation_level(psycopg.IsolationLevel.SERIALIZABLE)
    scalar_conn.read_only = True
    with scalar_conn.transaction():
        tx_settings = scalar_conn.execute(
            "select current_setting('transaction_isolation'), "
            "current_setting('transaction_read_only')",
            row_factory=module.tuple_row,
        )
        assert tx_settings.fetchall() == [("serializable", "on")]
    scalar_conn.rollback()
    scalar_conn.close()

    configured_conn = cast(
        Any,
        psycopg.connect_ferrocopg(
            dsn,
            isolation_level=psycopg.IsolationLevel.SERIALIZABLE,
            read_only=True,
            deferrable=False,
        ),
    )
    with configured_conn.transaction():
        configured = configured_conn.execute(
            "select current_setting('transaction_isolation'), "
            "current_setting('transaction_read_only'), "
            "current_setting('transaction_deferrable')",
            row_factory=module.tuple_row,
        )
        assert configured.fetchall() == [("serializable", "on", "off")]
    configured_conn.rollback()
    configured_conn.close()

    tx_conn = cast(Any, psycopg.connect_ferrocopg(dsn, autocommit=False))
    tx_conn.execute("create temporary table ferrocopg_connect_tx_test (id int4)")
    tx_conn.execute(
        "insert into ferrocopg_connect_tx_test (id) values ($1::int4)", ["1"]
    )
    tx_conn.rollback()
    check = tx_conn.execute(
        "select count(*)::text as n from pg_tables where tablename = 'ferrocopg_connect_tx_test'"
    )
    assert check.fetchall() == [("0",)]
    tx_conn.close()

    table_name = f"ferrocopg_connect_ctx_{uuid.uuid4().hex[:12]}"
    with cast(Any, psycopg.connect_ferrocopg(dsn, autocommit=False)) as ctx_conn:
        ctx_conn.execute(f'create table "{table_name}" (id int4)')
        ctx_conn.execute(f'insert into "{table_name}" (id) values ($1::int4)', ["1"])

    verify_ctx_commit = cast(Any, psycopg.connect_ferrocopg(dsn))
    committed = verify_ctx_commit.execute(
        f'select id::text as id from "{table_name}" order by id'
    )
    assert committed.fetchall() == [("1",)]
    verify_ctx_commit.execute(f'drop table "{table_name}"')
    verify_ctx_commit.close()

    conn.close()
    assert conn.closed is True


def test_backend_package_connect_impl_ferrocopg_live(dsn: str) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    conn = cast(
        Any, psycopg.connect(dsn, impl="ferrocopg", row_factory=module.scalar_row)
    )
    assert conn.autocommit is False
    conn.execute("create temporary table ferrocopg_connect_impl_test (id int4)")
    conn.execute(
        "insert into ferrocopg_connect_impl_test (id) values ($1::int4)", ["1"]
    )
    inside = conn.execute(
        "select id::text as id from ferrocopg_connect_impl_test order by id"
    )
    assert inside.fetchall() == ["1"]
    conn.rollback()
    check = conn.execute(
        "select count(*)::text as n from pg_tables where tablename = "
        "'ferrocopg_connect_impl_test'"
    )
    assert check.fetchall() == ["0"]
    conn.close()


def test_backend_package_connect_default_live(
    dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import psycopg

    module = cast(Any, importlib.import_module("psycopg._ferrocopg"))
    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    monkeypatch.delenv("PSYCOPG_SOURCE_IMPL", raising=False)
    conn = cast(Any, psycopg.connect(dsn, row_factory=module.scalar_row))
    assert isinstance(conn, module.NoTlsConnectionAdapter)
    assert conn.autocommit is False
    assert conn.execute("select %s::int4", (42,)).fetchone() == 42
    conn.close()


def test_backend_no_tls_cancel_handle_live(dsn: str) -> None:
    import time as pytime

    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    session = module.no_tls_session(dsn)
    blocker = module.no_tls_session(dsn)
    assert session is not None
    assert blocker is not None

    cancel_handle = session.cancel_handle()
    errors: deque[tuple[str, str | None, str | None, str | None]] = deque()
    lock_id = uuid.uuid4().int % (2**31)

    try:
        blocker.query_text(
            f"select 'locked'::text from (select pg_advisory_lock({lock_id})) as _"
        )

        def run_sleep_query() -> None:
            try:
                session.query_text(
                    f"select 'done'::text from (select pg_advisory_lock({lock_id})) as _"
                )
            except psycopg.errors.QueryCanceled as exc:
                errors.append(
                    (
                        str(exc),
                        exc.sqlstate,
                        exc.diag.sqlstate,
                        exc.diag.message_primary,
                    )
                )
            else:
                errors.append(("query unexpectedly completed", None, None, None))

        worker = threading.Thread(target=run_sleep_query)
        worker.start()

        for _ in range(20):
            pytime.sleep(0.05)
            cancel_handle.cancel()
            worker.join(timeout=0.1)
            if not worker.is_alive():
                break
        else:
            worker.join(timeout=5)

        assert not worker.is_alive()
        assert errors
        assert errors[0][1] == "57014"
        assert errors[0][2] == "57014"
        assert errors[0][3] == "canceling statement due to user request"
        assert "canceling statement due to user request" in errors[0][0]
    finally:
        blocker.query_text(f"select pg_advisory_unlock({lock_id})::text as unlocked")
        blocker.close()
        session.close()


def test_backend_no_tls_connection_cancel_live(dsn: str) -> None:
    import time as pytime

    import psycopg

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    conn = module.no_tls_connection_adapter(dsn)
    blocker = module.no_tls_connection_adapter(dsn)
    assert conn is not None
    assert blocker is not None

    errors: deque[tuple[str, str | None, str | None, str | None]] = deque()
    lock_id = uuid.uuid4().int % (2**31)

    try:
        blocker.execute(
            f"select 'locked'::text from (select pg_advisory_lock({lock_id})) as _"
        )

        def run_blocked_query() -> None:
            try:
                conn.execute(
                    f"select 'done'::text from (select pg_advisory_lock({lock_id})) as _"
                ).fetchall()
            except psycopg.errors.QueryCanceled as exc:
                errors.append(
                    (
                        str(exc),
                        exc.sqlstate,
                        exc.diag.sqlstate,
                        exc.diag.message_primary,
                    )
                )
            else:
                errors.append(("query unexpectedly completed", None, None, None))

        worker = threading.Thread(target=run_blocked_query)
        worker.start()

        for _ in range(20):
            pytime.sleep(0.05)
            conn.cancel_safe(timeout=1.0)
            worker.join(timeout=0.1)
            if not worker.is_alive():
                break
        else:
            worker.join(timeout=5)

        assert not worker.is_alive()
        assert errors
        assert errors[0][1] == "57014"
        assert errors[0][2] == "57014"
        assert errors[0][3] == "canceling statement due to user request"
        assert "canceling statement due to user request" in errors[0][0]
    finally:
        blocker.execute(f"select pg_advisory_unlock({lock_id})::text as unlocked")
        blocker.close()
        conn.close()


def test_copy_base_prefers_c_copy_optimizations(monkeypatch):
    module = importlib.import_module("psycopg._copy_base")

    class StubCModule:
        @staticmethod
        def format_row_text(*args: object) -> None:
            pass

        @staticmethod
        def format_row_binary(*args: object) -> None:
            pass

        @staticmethod
        def parse_row_text(*args: object) -> tuple[()]:
            return ()

        @staticmethod
        def parse_row_binary(*args: object) -> tuple[()]:
            return ()

    class StubRustModule:
        @staticmethod
        def format_row_text(*args: object) -> None:
            pass

        @staticmethod
        def format_row_binary(*args: object) -> None:
            pass

        @staticmethod
        def parse_row_text(*args: object) -> tuple[()]:
            return ()

        @staticmethod
        def parse_row_binary(*args: object) -> tuple[()]:
            return ()

    monkeypatch.setattr(module, "_psycopg", StubCModule)
    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)

    format_row_text, format_row_binary, parse_row_text, parse_row_binary = (
        module._load_copy_impl()
    )

    assert format_row_text is StubCModule.format_row_text
    assert format_row_binary is StubCModule.format_row_binary
    assert parse_row_text is StubCModule.parse_row_text
    assert parse_row_binary is StubCModule.parse_row_binary


def test_copy_base_uses_ferrocopg_copy_optimizations(monkeypatch):
    module = importlib.import_module("psycopg._copy_base")

    class StubRustModule:
        @staticmethod
        def format_row_text(*args: object) -> None:
            pass

        @staticmethod
        def format_row_binary(*args: object) -> None:
            pass

        @staticmethod
        def parse_row_text(*args: object) -> tuple[()]:
            return ()

        @staticmethod
        def parse_row_binary(*args: object) -> tuple[()]:
            return ()

    monkeypatch.setattr(module, "_psycopg", None)
    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)

    format_row_text, format_row_binary, parse_row_text, parse_row_binary = (
        module._load_copy_impl()
    )

    assert format_row_text is StubRustModule.format_row_text
    assert format_row_binary is StubRustModule.format_row_binary
    assert parse_row_text is StubRustModule.parse_row_text
    assert parse_row_binary is StubRustModule.parse_row_binary


def test_installed_ferrocopg_copy_helpers_roundtrip():
    ferrocopg = pytest.importorskip("ferrocopg_rust")

    tx = StubCopyTransformer(
        [
            b"alpha\tbeta",
            None,
            b"line1\nline2",
        ]
    )

    text_out = bytearray()
    ferrocopg.format_row_text(("a", "b", "c"), tx, text_out)
    assert bytes(text_out) == b"alpha\\tbeta\t\\N\tline1\\nline2\n"
    assert ferrocopg.parse_row_text(text_out, tx) == (
        b"alpha\tbeta",
        None,
        b"line1\nline2",
    )

    binary_out = bytearray()
    ferrocopg.format_row_binary(("a", "b", "c"), tx, binary_out)
    assert ferrocopg.parse_row_binary(binary_out, tx) == (
        b"alpha\tbeta",
        None,
        b"line1\nline2",
    )
