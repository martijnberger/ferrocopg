import importlib

import pytest


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

    class StubTransformer:
        def dump_sequence(
            self, params: tuple[object, ...], formats: list[object]
        ) -> list[bytes | None]:
            assert len(params) == len(formats)
            return [
                b"alpha\tbeta",
                None,
                b"line1\nline2",
            ]

        def load_sequence(
            self, record: list[bytes | None]
        ) -> tuple[bytes | None, ...]:
            return tuple(record)

    tx = StubTransformer()

    text_out = bytearray()
    ferrocopg.format_row_text(("ignored",), tx, text_out)
    assert bytes(text_out) == b"alpha\\tbeta\t\\N\tline1\\nline2\n"
    assert ferrocopg.parse_row_text(text_out, tx) == (
        b"alpha\tbeta",
        None,
        b"line1\nline2",
    )

    binary_out = bytearray()
    ferrocopg.format_row_binary(("ignored",), tx, binary_out)
    assert ferrocopg.parse_row_binary(binary_out, tx) == (
        b"alpha\tbeta",
        None,
        b"line1\nline2",
    )
