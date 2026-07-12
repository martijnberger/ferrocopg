#!/usr/bin/env python3
"""Stage the vendored Psycopg package under the ferrocopg namespace."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = ROOT / "psycopg" / "psycopg"
DEFAULT_LICENSE = ROOT / "psycopg" / "LICENSE.txt"
DEFAULT_MANIFEST = ROOT / "crates" / "ferrocopg-python" / "Cargo.toml"
DEFAULT_UPSTREAM_REVISION = ROOT / "UPSTREAM_REVISION"
NAMESPACE_RE = re.compile(r"\bpsycopg\b")
RUST_IMPORT = """\
    import ferrocopg_rust._ferrocopg

    _ferrocopg = ferrocopg_rust._ferrocopg
"""
STAGED_RUST_IMPORT = """\
    from ._rust import _ferrocopg as rust_extension

    _ferrocopg = rust_extension
"""
SOURCE_ASYNC_IMPORT = "from .connection_async import AsyncConnection"
STAGED_ASYNC_IMPORT = "from ._official import AsyncConnection"
SOURCE_CONNECTION_IMPORT = "from .connection import Connection"
STAGED_CONNECTION_IMPORT = """\
from .connection import Connection
from ._ferrocopg import NoTlsConnectionAdapter as RustConnection
""".rstrip()
SOURCE_CONNECTION_EXPORT = '    "Connection",\n'
STAGED_CONNECTION_EXPORT = '    "Connection",\n    "RustConnection",\n'
SOURCE_VERSION_IMPORT = "from .version import __version__ as __version__  # noqa: F401"
STAGED_VERSION_IMPORT = """\
from .version import __version__ as __version__  # noqa: F401
from ._vendored import PSYCOPG_REVISION as __vendored_psycopg_revision__
""".rstrip()
SOURCE_PQ_IMPL = '    impl = os.environ.get("PSYCOPG_IMPL", "").lower()\n'
STAGED_PQ_IMPL = '    impl = "ferrocopg"\n'
PQ_ATTEMPTS = "    attempts: list[str] = []\n"
STAGED_PQ_ATTEMPTS = """\
    attempts: list[str] = []

    if impl == "ferrocopg":
        from .. import _pq_compat as module  # type: ignore[assignment]
"""
SOURCE_CMODULE_PYTHON = 'elif pq.__impl__ == "python":\n'
STAGED_CMODULE_PYTHON = 'elif pq.__impl__ in ("python", "ferrocopg"):\n'
SOURCE_SELECTOR_DOC = """\
    `impl="libpq"` selects the temporary source-tree comparison path. The
    `PSYCOPG_SOURCE_IMPL` environment variable exists only for upstream
    comparison automation and will not be part of the staged package contract.
"""
STAGED_SELECTOR_DOC = """\
    `impl="libpq"` lazily delegates to an installed official Psycopg package.
"""
SOURCE_SELECTOR = """\
    selected_impl = (
        impl if impl is not None else os.environ.get("PSYCOPG_SOURCE_IMPL", "ferrocopg")
    )
"""
STAGED_SELECTOR = '    selected_impl = impl if impl is not None else "ferrocopg"\n'
SOURCE_CONNECT_OVERLOADS = """\
@overload
def connect(
    conninfo: str = "",
    /,
    *,
    autocommit: bool = False,
    prepare_threshold: int | None = 5,
    context: AdaptContext | None = None,
    row_factory: RowFactory[Row] | None = None,
    cursor_factory: type[Cursor[Row]] | None = None,
    impl: Literal["libpq"] | None = None,
    **kwargs: ConnParam,
) -> Connection[Row]: ...


@overload
def connect(
    conninfo: str = "",
    /,
    *,
    impl: Literal["ferrocopg"],
    **kwargs: Any,
) -> object: ...
"""
STAGED_CONNECT_OVERLOADS = """\
@overload
def connect(
    conninfo: str = "",
    /,
    *,
    impl: Literal["libpq"],
    **kwargs: Any,
) -> Any: ...


@overload
def connect(
    conninfo: str = "",
    /,
    *,
    impl: Literal["ferrocopg"] | None = None,
    **kwargs: Any,
) -> RustConnection: ...
"""
SOURCE_LIBPQ_CONNECT = """\
    if selected_impl == "libpq":
        return Connection.connect(conninfo, **kwargs)
"""
STAGED_LIBPQ_CONNECT = """\
    if selected_impl == "libpq":
        from ._official import connect as connect_official

        return connect_official(conninfo, **kwargs)
"""
OFFICIAL_MODULE = '''\
"""Lazy delegation to an installed official Psycopg package."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any


def _package() -> ModuleType:
    try:
        return import_module("psycopg")
    except ImportError as ex:
        raise ImportError(
            "official Psycopg is required for async or impl='libpq'; "
            "install the ferrocopg fallback extra with `pip install "
            "ferrocopg[libpq]`"
        ) from ex


def connect(conninfo: str = "", **kwargs: Any) -> Any:
    return _package().connect(conninfo, **kwargs)


class AsyncConnection:
    __module__ = "ferrocopg"

    @classmethod
    async def connect(cls, conninfo: str = "", **kwargs: Any) -> Any:
        return await _package().AsyncConnection.connect(conninfo, **kwargs)
'''

PQ_COMPAT_MODULE = '''\
"""libpq-free compatibility surface used by the Rust backend."""

from __future__ import annotations

from typing import Any, NoReturn

from .pq.misc import ConninfoOption

__impl__ = "ferrocopg"
__build_version__ = 0


def version() -> int:
    """Return zero because this implementation is not backed by libpq."""
    return 0


def _unsupported() -> NoReturn:
    from .errors import NotSupportedError

    raise NotSupportedError(
        "raw libpq operations are unavailable in the ferrocopg Rust backend; "
        "use impl='libpq' with ferrocopg[libpq]"
    )


class _Unavailable:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        _unsupported()


class Conninfo:
    @classmethod
    def get_defaults(cls) -> list[ConninfoOption]:
        _unsupported()

    @classmethod
    def parse(cls, _conninfo: bytes) -> list[ConninfoOption]:
        _unsupported()


class Escaping:
    """Pure-Python SQL escaping needed by the vendored adaptation layer."""

    def __init__(self, _conn: object | None = None) -> None:
        pass

    def escape_string(self, data: Any) -> bytes:
        return bytes(data).replace(b"'", b"''").replace(b"\\\\", b"\\\\\\\\")

    def escape_literal(self, data: Any) -> bytes:
        escaped = self.escape_string(data)
        prefix = b" E" if b"\\\\" in escaped else b""
        return prefix + b"'" + escaped + b"'"

    def escape_identifier(self, data: Any) -> bytes:
        return b'"' + bytes(data).replace(b'"', b'""') + b'"'

    def escape_bytea(self, data: Any) -> bytes:
        return b"\\\\x" + bytes(data).hex().encode("ascii")

    def unescape_bytea(self, data: Any) -> bytes:
        value = bytes(data)
        if value.startswith(b"\\\\x"):
            return bytes.fromhex(value[2:].decode("ascii"))
        return value


PGconn = _Unavailable
PGresult = _Unavailable
PGcancel = _Unavailable
PGcancelConn = _Unavailable
'''


def stage_package(
    output: Path,
    *,
    source: Path = DEFAULT_SOURCE,
    license_file: Path = DEFAULT_LICENSE,
    manifest: Path = DEFAULT_MANIFEST,
    upstream_revision_file: Path = DEFAULT_UPSTREAM_REVISION,
) -> Path:
    """Create a fresh package staging tree and return its package path."""
    if output.exists():
        raise FileExistsError(f"staging output already exists: {output}")
    if not (source / "__init__.py").is_file():
        raise FileNotFoundError(f"Psycopg package source not found: {source}")
    if not license_file.is_file():
        raise FileNotFoundError(f"Psycopg license not found: {license_file}")
    if not manifest.is_file():
        raise FileNotFoundError(f"ferrocopg Rust manifest not found: {manifest}")
    revision = upstream_revision_file.read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError(
            f"invalid vendored Psycopg revision in {upstream_revision_file}"
        )

    output.mkdir(parents=True)
    package = output / "ferrocopg"
    shutil.copytree(
        source,
        package,
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "*.so", "*.pyd", "*.dylib"
        ),
    )
    shutil.copy2(license_file, output / "LICENSE.txt")
    (output / "pyproject.toml").write_text(_build_pyproject(output, manifest))
    rust_package = package / "_rust"
    rust_package.mkdir()
    (rust_package / "__init__.py").write_text(
        '"""Private package containing the ferrocopg Rust extension."""\n'
    )

    for path in package.rglob("*"):
        if path.suffix in {".py", ".pyi"}:
            path.write_text(_transform_source(path, path.read_text()))
    (package / "_official.py").write_text(OFFICIAL_MODULE)
    (package / "_pq_compat.py").write_text(PQ_COMPAT_MODULE)
    (package / "_vendored.py").write_text(
        f'"""Vendored Psycopg source provenance."""\n\nPSYCOPG_REVISION = "{revision}"\n'
    )

    return package


def _build_pyproject(output: Path, manifest: Path) -> str:
    try:
        staged_manifest = Path(os.path.relpath(manifest, output)).as_posix()
    except ValueError:
        # Windows cannot express relative paths across drive letters.
        staged_manifest = manifest.resolve().as_posix()
    return f"""\
[build-system]
requires = ["maturin>=1.8,<2.0"]
build-backend = "maturin"

[project]
name = "ferrocopg"
version = "0.1.0"
description = "Rust-native PostgreSQL backend for the Psycopg 3 API"
requires-python = ">=3.11"
license = "LGPL-3.0-only"
license-files = ["LICENSE.txt"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Rust",
]
dependencies = [
    "typing-extensions >= 4.6; python_version < '3.13'",
    "tzdata >= 2022.1; sys_platform == 'win32'",
]

[project.optional-dependencies]
libpq = ["psycopg >= 3.3, < 3.4"]

[project.urls]
Source = "https://github.com/martijnberger/ferrocopg"

[tool.maturin]
python-source = "."
module-name = "ferrocopg._rust._ferrocopg"
manifest-path = {json.dumps(staged_manifest)}
"""


def _transform_source(path: Path, source: str) -> str:
    transformed = NAMESPACE_RE.sub("ferrocopg", source)
    if path.name == "_rmodule.py":
        if RUST_IMPORT not in transformed:
            raise RuntimeError("cannot locate the Rust extension import during staging")
        transformed = transformed.replace(RUST_IMPORT, STAGED_RUST_IMPORT, 1)
    elif path.name == "__init__.py" and path.parent.name == "ferrocopg":
        replacements = (
            (SOURCE_ASYNC_IMPORT, STAGED_ASYNC_IMPORT),
            (SOURCE_CONNECTION_IMPORT, STAGED_CONNECTION_IMPORT),
            (SOURCE_CONNECTION_EXPORT, STAGED_CONNECTION_EXPORT),
            (SOURCE_VERSION_IMPORT, STAGED_VERSION_IMPORT),
            (SOURCE_SELECTOR_DOC, STAGED_SELECTOR_DOC),
            (SOURCE_SELECTOR, STAGED_SELECTOR),
            (SOURCE_CONNECT_OVERLOADS, STAGED_CONNECT_OVERLOADS),
            (SOURCE_LIBPQ_CONNECT, STAGED_LIBPQ_CONNECT),
        )
        for original, replacement in replacements:
            if original not in transformed:
                raise RuntimeError(
                    f"cannot locate package-boundary source in {path.name}: {original!r}"
                )
            transformed = transformed.replace(original, replacement, 1)
    elif path.name == "__init__.py" and path.parent.name == "pq":
        replacements = (
            (SOURCE_PQ_IMPL, STAGED_PQ_IMPL),
            (PQ_ATTEMPTS, STAGED_PQ_ATTEMPTS),
        )
        for original, replacement in replacements:
            if original not in transformed:
                raise RuntimeError(
                    f"cannot locate pq source in {path.name}: {original!r}"
                )
            transformed = transformed.replace(original, replacement, 1)
    elif path.name == "_cmodule.py":
        if SOURCE_CMODULE_PYTHON not in transformed:
            raise RuntimeError("cannot locate pure-Python cmodule branch")
        transformed = transformed.replace(
            SOURCE_CMODULE_PYTHON, STAGED_CMODULE_PYTHON, 1
        )
    return transformed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new staging directory to create")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--license", type=Path, default=DEFAULT_LICENSE)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--upstream-revision", type=Path, default=DEFAULT_UPSTREAM_REVISION
    )
    args = parser.parse_args()
    package = stage_package(
        args.output.resolve(),
        source=args.source.resolve(),
        license_file=args.license.resolve(),
        manifest=args.manifest.resolve(),
        upstream_revision_file=args.upstream_revision.resolve(),
    )
    print(package)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
