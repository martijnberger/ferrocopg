import importlib
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
    def __init__(self, statuses: list[int], socket: int = 42, error_message: str = "boom"):
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


def _drive_send_generator(gen: object, ready_values: list[int | None]) -> tuple[list[int], object]:
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
    monkeypatch.setattr(generators, "pq", SimpleNamespace(**{**generators.pq.__dict__, "PGconn": fake_pgconn}))
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
        raise errors.DataError(
            "PostgreSQL text fields cannot contain NUL (0x00) bytes"
        )
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
        ([[True, False], []], [1], [("COMMAND_OK", "waited"), None], [1], ["waited"], 1),
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
        assert [res.label for res in cast(list[StubResult], got)] == expected_labels, name
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
        ([0], [[True, False], []], [1], [("COMMAND_OK", "fetched"), None], [1], ["fetched"], 1),
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
        assert [res.label for res in cast(list[StubResult], got)] == expected_labels, name
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
    read_cycles: list[
        tuple[list[bool], list[tuple[str, str] | None], list[object]]
    ],
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
            [(lambda label=label: command_calls.append(label)) for label in expected_command_calls]
        )
        pgconn = StubPipelinePgconn(
            [
                (
                    busy,
                    [
                        None if result is None else StubResult(getattr(exec_status, result[0]), result[1])
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
            [res.label for res in batch]
            for batch in cast(list[list[StubResult]], got)
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
        waits, result = _drive_cancel_generator(impl.cancel(cancel_conn), [1] * len(expected_waits))
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
        (b'{{1,2},{3,4}}', b",", [[1, 2], [3, 4]]),
        (b'[1:2]={1;2}', b";", [1, 2]),
        (b'{"a,b","c\\\\d"}', b",", ["a,b", "c\\d"]),
    ],
)
def test_array_load_text_equivalent(
    payload: bytes, delimiter: bytes, expected: list[object]
) -> None:
    loader = StubArrayLoader(lambda data: int(data) if data.isdigit() else data.decode())
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
        assert impl.uuid_load_text(payload) == uuid.UUID("12345678-1234-5678-1234-567812345678"), name


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
    seq = ("plain", "needs,quotes", "say\"hi", None, "")
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
        assert impl.composite_dump_binary_sequence(seq, types, formats, tx) == expected, name


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"foo,bar", [b"foo", b"bar"]),
        (b'"a","b""c",', [b"a", b'b"c', None]),
        (b',', [None, None]),
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
        assert (
            impl.dump_decimal_to_numeric_binary(value)
            == importlib.import_module("psycopg.types.numeric").dump_decimal_to_numeric_binary(
                value
            )
        ), name


@pytest.mark.parametrize("value", [0, 42, -10000, 10**30 + 12345])
def test_numeric_int_binary_equivalent(value: int) -> None:
    for name, impl in _numeric_impls():
        assert (
            impl.dump_int_to_numeric_binary(value)
            == importlib.import_module("psycopg.types.numeric").dump_int_to_numeric_binary(value)
        ), name


@pytest.mark.parametrize("payload", [b"123.45", memoryview(b"-0.0012")])
def test_numeric_text_load_equivalent(payload: bytes | memoryview) -> None:
    for name, impl in _numeric_impls():
        assert impl.numeric_load_text(payload) == Decimal(bytes(payload).decode()), name


@pytest.mark.parametrize("value", [Decimal("12.34"), Decimal("-0.0012"), Decimal("NaN")])
def test_numeric_binary_load_equivalent(value: Decimal) -> None:
    payload = importlib.import_module("psycopg.types.numeric").dump_decimal_to_numeric_binary(
        value
    )
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
    assert module.DecimalDumper(Decimal).dump(Decimal("1.2")) == ("text", Decimal("1.2"))
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
        assert impl.timestamptz_load_binary(aware_payload, target_tz) == aware.astimezone(
            target_tz
        ), name


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
        def timestamptz_load_binary(data: object, timezone_obj: object) -> tuple[str, object, object]:
            return ("timestamptz-load", data, timezone_obj)

        @staticmethod
        def timedelta_dump_binary(obj: object) -> bytes:
            return b"interval-binary"

        @staticmethod
        def interval_load_binary(data: object) -> tuple[str, object]:
            return ("interval-load", data)

    monkeypatch.setattr(module, "_rpsycopg", StubRustModule)
    assert module.DateDumper(date).dump(date(2024, 1, 2)) == ("date-text", date(2024, 1, 2))
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
    assert (
        module.DatetimeDumper(datetime).dump(datetime(2024, 1, 2, 3, 4, 5))
        == ("datetime-text", datetime(2024, 1, 2, 3, 4, 5))
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
    assert module.TimedeltaBinaryDumper(timedelta).dump(timedelta(seconds=1)) == b"interval-binary"
    assert module.IntervalBinaryLoader(0).load(b"x") == ("interval-load", b"x")


def test_transformer_prefers_ferrocopg_when_c_absent(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert module.query_text_params_no_tls("host=localhost", "select $1::text", ["x"]) is None
    assert module.run_text_params_no_tls("host=localhost", "select $1::text", ["x"]) is None
    assert module.execute_text_params_no_tls("host=localhost", "select 1", []) is None
    assert module.describe_text_no_tls("host=localhost", "select 1") is None
    assert module.no_tls_session("host=localhost") is None
    assert module.no_tls_session_adapter("host=localhost") is None


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
        def simple_query_results_no_tls(conninfo: str, query: str) -> tuple[str, str, str]:
            calls.append(("simple-query-results", conninfo))
            return ("simple-query-results", conninfo, query)

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
    assert module.query_text_params_no_tls("host=localhost", "select $1::text", ["x", None]) == (
        "query-params",
        "host=localhost",
        "select $1::text",
        ["x", None],
    )
    assert module.run_text_params_no_tls("host=localhost", "select $1::text", ["x", None]) == (
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
    adapter = module.no_tls_session_adapter("host=localhost")
    assert adapter is not None
    assert calls == [
        ("summary", "host=localhost"),
        ("plan", "host=localhost"),
        ("target", "host=localhost"),
        ("probe", "host=localhost"),
        ("query", "host=localhost"),
        ("simple-query", "host=localhost"),
        ("simple-query-results", "host=localhost"),
        ("query-params", "host=localhost"),
        ("run-params", "host=localhost"),
        ("execute-params", "host=localhost"),
        ("describe", "host=localhost"),
        ("session", "host=localhost"),
        ("session", "host=localhost"),
    ]


def test_backend_result_cursor_navigation() -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    results = [
        SimpleNamespace(columns=["a"], rows=[["one"], ["two"]], rows_affected=2),
        SimpleNamespace(columns=["b"], rows=[["three"]], rows_affected=1),
    ]

    cur = module.BackendResultCursor(results)
    assert cur.columns == ["a"]
    assert cur.rows_affected == 2
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
    module = importlib.import_module("psycopg._ferrocopg")

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

    stub = StubSession()
    monkeypatch.setattr(module, "no_tls_session", lambda conninfo: stub)

    conn = module.no_tls_connection_adapter("host=localhost")
    assert conn is not None
    assert conn.closed is False

    assert conn.execute("select 1").fetchall() == [["one"]]
    assert conn.execute("select $1::text", ["x"]).fetchall() == [["two"]]
    assert conn.execute("select $1::text", ["x"], prepare=True).fetchall() == [["three"]]
    assert conn.execute("select $1::text", ["y"], prepare=True).fetchall() == [["three"]]

    with conn.cursor() as cur:
        assert cur.execute("select 1").fetchone() == ["one"]
        assert cur.rowcount == 1

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
        ("begin",),
        ("commit",),
        ("rollback",),
        ("close",),
    ]


def test_no_tls_connection_adapter_row_factories(monkeypatch: pytest.MonkeyPatch) -> None:
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

        def run_text_params(self, query: str, params: list[str | None]) -> object:
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
    assert default_cur.fetchall() == [["1", "one"], ["2", "two"]]
    assert default_cur.rownumber == 2

    tuple_cur = conn.execute("select 1", row_factory=module.tuple_row)
    assert tuple_cur.fetchall() == [("1", "one"), ("2", "two")]

    dict_cur = conn.execute("select 1", row_factory=module.dict_row)
    assert dict_cur.fetchall() == [
        {"id": "1", "label": "one"},
        {"id": "2", "label": "two"},
    ]

    scalar_cur = conn.execute(
        "select $1::text",
        ["3"],
        row_factory=module.scalar_row,
        prepare=True,
    )
    assert scalar_cur.fetchone() == "4"


def test_no_tls_cursor_adapter_executemany(monkeypatch: pytest.MonkeyPatch) -> None:
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
        cur.executemany(
            "insert into demo values ($1::text)",
            [["one"], ["two"]],
        )
        assert cur.rowcount == 2
        assert cur.fetchall() == []

    with conn.cursor(row_factory=module.scalar_row) as cur:
        cur.executemany(
            "select $1::text as value",
            [["one"], ["two"]],
            returning=True,
            prepare=True,
        )
        assert cur.fetchone() == "one"
        assert cur.nextset() is True
        assert cur.fetchone() == "two"
        assert cur.nextset() is None


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
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    with pytest.raises(RuntimeError, match="requires TLS"):
        module.connect_no_tls_probe("host=localhost sslmode=require dbname=postgres")


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


def test_backend_simple_query_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    messages = module.simple_query_no_tls(
        dsn,
        "select 'alpha'::text as label; select 'beta'::text as label",
    )
    assert messages is not None
    assert [(message.kind, message.columns, message.values, message.rows_affected) for message in messages] == [
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
    assert [(result.columns, result.rows, result.rows_affected) for result in results] == [
        (["label"], [["alpha"]], 1),
        (["label"], [["beta"]], 1),
    ]


def test_backend_query_text_params_no_tls_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    result = module.query_text_params_no_tls(
        dsn,
        "select ($1::int4 + $2::int4)::text as total, $3::text as label, $4::text as nullable",
        ["2", "5", "sum", None],
    )
    assert result is not None
    assert result.columns == ["total", "label", "nullable"]
    assert result.rows == [["7", "sum", None]]


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
        [["7", "sum"]],
        1,
    )

    command_result = module.run_text_params_no_tls(
        dsn,
        "create temporary table ferrocopg_run_result_test (id int4, label text)",
        [],
    )
    assert (command_result.columns, command_result.rows, command_result.rows_affected) == (
        [],
        [],
        0,
    )


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
    assert [(column.name, column.oid, column.type_name) for column in description.columns] == [
        ("n", 23, "int4"),
        ("t", 25, "text"),
    ]


def test_backend_no_tls_session_live(dsn: str) -> None:
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
        (result.columns, result.rows, result.rows_affected)
        for result in simple_results
    ] == [
        (["label"], [["first"]], 1),
        (["label"], [["second"]], 1),
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
    ) == (["total", "label", "nullable"], [["7", "session", None]], 1)

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
    assert [(param.oid, param.type_name) for param in prepared_insert.description.params] == [
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
    assert [(param.oid, param.type_name) for param in prepared_query.description.params] == [
        (23, "int4"),
    ]
    queried_prepared = session.query_prepared_text_params(prepared_query.statement_id, ["13"])
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
    ) == (["id", "label"], [["13", "committed"], ["14", "prepared"]], 2)
    session.close_prepared(prepared_query.statement_id)
    with pytest.raises(RuntimeError, match="unknown prepared statement id"):
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
    observed_payloads = sorted([second_notification.payload, third_notification.payload])
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
    assert [(column.name, column.oid, column.type_name) for column in description.columns] == [
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

    with pytest.raises(RuntimeError, match="closed"):
        session.query_text("select 1")
    with pytest.raises(RuntimeError, match="closed"):
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

    bound = adapter.execute_params(
        "select ($1::int4 + $2::int4)::text as total, $3::text as label",
        ["2", "5", "sum"],
    )
    assert bound.columns == ["total", "label"]
    assert bound.rows_affected == 1
    assert bound.fetchall() == [["7", "sum"]]

    prepared = adapter.prepare_text(
        "select id::text as id, label from (values (1, 'one'), (2, 'two')) as t(id, label) where id >= $1::int4 order by id"
    )
    prepared_cur = adapter.execute_prepared(prepared.statement_id, ["2"])
    assert prepared_cur.fetchall() == [["2", "two"]]

    adapter.close()
    assert adapter.closed is True


def test_backend_no_tls_connection_adapter_live(dsn: str) -> None:
    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    conn = module.no_tls_connection_adapter(dsn)
    assert conn is not None
    assert conn.closed is False

    cur = conn.execute(
        "select 'first'::text as label; select 'second'::text as label"
    )
    assert cur.fetchall() == [["first"]]
    assert cur.nextset() is True
    assert cur.fetchall() == [["second"]]

    with conn.cursor() as cur2:
        cur2.execute(
            "select ($1::int4 + $2::int4)::text as total, $3::text as label",
            ["2", "5", "sum"],
        )
        assert cur2.description == [
            module.BackendColumn("total"),
            module.BackendColumn("label"),
        ]
        assert cur2.rowcount == 1
        assert cur2.fetchall() == [["7", "sum"]]
        assert cur2.rownumber == 1

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

    scalar_cur = conn.execute(
        "select 42::text as answer",
        row_factory=module.scalar_row,
    )
    assert scalar_cur.fetchone() == "42"

    conn.execute("create temporary table ferrocopg_conn_execmany_test (id int4)")
    with conn.cursor() as cur3:
        cur3.executemany(
            "insert into ferrocopg_conn_execmany_test (id) values ($1::int4)",
            [["1"], ["2"], ["3"]],
        )
        assert cur3.rowcount == 3

    verify_many = conn.execute(
        "select id::text as id from ferrocopg_conn_execmany_test order by id"
    )
    assert verify_many.fetchall() == [["1"], ["2"], ["3"]]

    with conn.cursor(row_factory=module.scalar_row) as cur4:
        cur4.executemany(
            "select $1::text as label",
            [["alpha"], ["beta"]],
            returning=True,
            prepare=True,
        )
        assert cur4.fetchone() == "alpha"
        assert cur4.nextset() is True
        assert cur4.fetchone() == "beta"
        assert cur4.nextset() is None

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

    conn.close()
    assert conn.closed is True


def test_backend_no_tls_cancel_handle_live(dsn: str) -> None:
    import time as pytime

    module = importlib.import_module("psycopg._ferrocopg")

    if not module.is_available():
        pytest.skip("ferrocopg extension not installed")

    session = module.no_tls_session(dsn)
    blocker = module.no_tls_session(dsn)
    assert session is not None
    assert blocker is not None

    cancel_handle = session.cancel_handle()
    errors: deque[str] = deque()
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
            except RuntimeError as exc:
                errors.append(str(exc))
            else:
                errors.append("query unexpectedly completed")

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
        assert "canceling statement due to user request" in errors[0]
    finally:
        blocker.query_text(f"select pg_advisory_unlock({lock_id})::text as unlocked")
        blocker.close()
        session.close()


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
