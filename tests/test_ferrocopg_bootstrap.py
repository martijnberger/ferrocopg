import importlib
import socket
from collections import deque
from collections.abc import Generator
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


def test_ferrocopg_unavailable(monkeypatch):
    module = importlib.import_module("psycopg._ferrocopg")

    monkeypatch.setattr(module, "_ferrocopg", None)

    assert module.is_available() is False
    assert module.conninfo_summary("host=localhost") is None
    assert module.connect_plan("host=localhost") is None


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

    monkeypatch.setattr(module, "_ferrocopg", StubRustModule)

    assert module.is_available() is True
    assert module.conninfo_summary("host=localhost") == ("summary", "host=localhost")
    assert module.connect_plan("host=localhost") == ("plan", "host=localhost")
    assert calls == [
        ("summary", "host=localhost"),
        ("plan", "host=localhost"),
    ]


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
