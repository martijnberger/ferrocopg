import importlib
import socket
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


def _send_impls() -> list[tuple[str, GeneratorImpl]]:
    ferrocopg = cast(GeneratorImpl, pytest.importorskip("ferrocopg_rust"))
    generators = importlib.import_module("psycopg.generators")
    python_impl = cast(GeneratorImpl, SimpleNamespace(send=generators._send))
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
