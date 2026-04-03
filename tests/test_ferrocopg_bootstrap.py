import importlib
from types import SimpleNamespace

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


def _copy_impls() -> list[tuple[str, object]]:
    ferrocopg = pytest.importorskip("ferrocopg_rust")
    copy_base = importlib.import_module("psycopg._copy_base")
    python_impl = SimpleNamespace(
        format_row_text=copy_base._format_row_text,
        format_row_binary=copy_base._format_row_binary,
        parse_row_text=_expected_text_row,
        parse_row_binary=copy_base._parse_row_binary,
    )

    return [
        ("python", python_impl),
        ("rust", ferrocopg),
    ]


def _expected_text_row(data: bytearray, tx: StubCopyTransformer) -> tuple[bytes | None, ...]:
    if not tx._nfields and bytes(data) == b"\n":
        return ()

    return importlib.import_module("psycopg._copy_base")._parse_row_text(data, tx)


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


def _make_text_transformer(impl_name: str, nfields: int):
    pq = importlib.import_module("psycopg.pq")
    text_oid = 25

    if impl_name == "c":
        tx = importlib.import_module("psycopg_c._psycopg").Transformer()
    else:
        tx = importlib.import_module("psycopg._py_transformer").Transformer()

    tx.set_dumper_types([text_oid] * nfields, pq.Format.TEXT)
    tx.set_loader_types([text_oid] * nfields, pq.Format.TEXT)
    return tx


def _make_int4_binary_transformer(impl_name: str, nfields: int):
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
    cmodule = pytest.importorskip("psycopg_c._psycopg")
    baseline = importlib.import_module("psycopg._copy_base")

    py_tx = _make_text_transformer("python", len(row))
    expected_out = bytearray()
    baseline._format_row_text(row, py_tx, expected_out)
    expected_row = () if not row else baseline._parse_row_text(expected_out, py_tx)

    rust = importlib.import_module("ferrocopg_rust")
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
    cmodule = pytest.importorskip("psycopg_c._psycopg")
    baseline = importlib.import_module("psycopg._copy_base")

    py_tx = _make_int4_binary_transformer("python", len(row))
    expected_out = bytearray()
    baseline._format_row_binary(row, py_tx, expected_out)
    expected_row = baseline._parse_row_binary(expected_out, py_tx)

    rust = importlib.import_module("ferrocopg_rust")
    for name, impl in [("rust", rust), ("c", cmodule)]:
        tx = _make_int4_binary_transformer(name, len(row))
        out = bytearray()
        impl.format_row_binary(row, tx, out)
        assert bytes(out) == bytes(expected_out), name
        assert impl.parse_row_binary(out, tx) == expected_row, name


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
