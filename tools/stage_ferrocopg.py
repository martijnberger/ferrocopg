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

import re
from typing import Any, NoReturn
from urllib.parse import unquote

from .pq.misc import ConninfoOption

__impl__ = "ferrocopg"
__build_version__ = 0

_DEFAULTS = (
    ("service", "PGSERVICE", None),
    ("user", "PGUSER", None),
    ("password", "PGPASSWORD", None),
    ("passfile", "PGPASSFILE", None),
    ("channel_binding", "PGCHANNELBINDING", "prefer"),
    ("connect_timeout", "PGCONNECT_TIMEOUT", None),
    ("dbname", "PGDATABASE", None),
    ("host", "PGHOST", None),
    ("hostaddr", "PGHOSTADDR", None),
    ("port", "PGPORT", "5432"),
    ("client_encoding", "PGCLIENTENCODING", None),
    ("options", "PGOPTIONS", None),
    ("application_name", "PGAPPNAME", None),
    ("fallback_application_name", "", None),
    ("keepalives", "", None),
    ("keepalives_idle", "", None),
    ("keepalives_interval", "", None),
    ("keepalives_count", "", None),
    ("keepalives_retries", "", None),
    ("tcp_user_timeout", "", None),
    ("sslmode", "PGSSLMODE", "prefer"),
    ("sslnegotiation", "PGSSLNEGOTIATION", "postgres"),
    ("sslcompression", "PGSSLCOMPRESSION", "0"),
    ("sslcert", "PGSSLCERT", None),
    ("sslkey", "PGSSLKEY", None),
    ("sslpassword", "", None),
    ("sslrootcert", "PGSSLROOTCERT", None),
    ("sslcrl", "PGSSLCRL", None),
    ("sslcrldir", "PGSSLCRLDIR", None),
    ("sslsni", "PGSSLSNI", "1"),
    ("ssl_min_protocol_version", "PGSSLMINPROTOCOLVERSION", "TLSv1.2"),
    ("ssl_max_protocol_version", "PGSSLMAXPROTOCOLVERSION", None),
    ("requirepeer", "PGREQUIREPEER", None),
    ("gssencmode", "PGGSSENCMODE", "prefer"),
    ("krbsrvname", "PGKRBSRVNAME", "postgres"),
    ("gsslib", "PGGSSLIB", None),
    ("replication", "", None),
    ("target_session_attrs", "PGTARGETSESSIONATTRS", "any"),
    ("load_balance_hosts", "PGLOADBALANCEHOSTS", "disable"),
)
_KNOWN_OPTIONS = frozenset(item[0] for item in _DEFAULTS)
_BAD_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


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
        return [
            _conninfo_option(keyword, None, envvar, compiled)
            for keyword, envvar, compiled in _DEFAULTS
        ]

    @classmethod
    def parse(cls, conninfo: bytes) -> list[ConninfoOption]:
        from .errors import OperationalError

        try:
            value = conninfo.decode("utf-8")
        except UnicodeDecodeError as ex:
            raise OperationalError("connection string is not valid UTF-8") from ex

        try:
            params = (
                _parse_uri_conninfo(value)
                if value.startswith(("postgres://", "postgresql://"))
                else _parse_keyword_conninfo(value)
            )
            unknown = next(
                (key for key, _value in params if key not in _KNOWN_OPTIONS), None
            )
            if unknown is not None:
                raise ValueError(f'invalid connection option "{unknown}"')
        except ValueError as ex:
            raise OperationalError(str(ex)) from None

        return [_conninfo_option(key, item) for key, item in params]


def _conninfo_option(
    keyword: str,
    value: str | None,
    envvar: str = "",
    compiled: str | None = None,
) -> ConninfoOption:
    return ConninfoOption(
        keyword.encode(),
        envvar.encode() or None,
        compiled.encode() if compiled is not None else None,
        value.encode() if value is not None else None,
        b"",
        b"",
        0,
    )


def _parse_keyword_conninfo(conninfo: str) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = []
    index = 0
    size = len(conninfo)

    while True:
        while index < size and conninfo[index].isspace():
            index += 1
        if index == size:
            return params

        key_start = index
        while index < size and not conninfo[index].isspace() and conninfo[index] != "=":
            index += 1
        key = conninfo[key_start:index]
        while index < size and conninfo[index].isspace():
            index += 1
        if not key or index == size or conninfo[index] != "=":
            raise ValueError(f'missing "=" after "{key or conninfo[key_start:]}"')
        index += 1
        while index < size and conninfo[index].isspace():
            index += 1

        quoted = index < size and conninfo[index] == "'"
        if quoted:
            index += 1
        chars: list[str] = []
        while index < size:
            char = conninfo[index]
            if quoted and char == "'":
                index += 1
                break
            if not quoted and char.isspace():
                break
            if char == "\\\\":
                index += 1
                if index == size:
                    raise ValueError("unterminated escape in connection string")
                char = conninfo[index]
            chars.append(char)
            index += 1
        else:
            if quoted:
                raise ValueError("unterminated quoted string in connection string")

        if quoted and index < size and not conninfo[index].isspace():
            raise ValueError("unexpected character after quoted connection value")
        params.append((key, "".join(chars)))


def _parse_uri_conninfo(conninfo: str) -> list[tuple[str, str]]:
    _scheme, rest = conninfo.split("://", 1)
    location, separator, query = rest.partition("?")
    authority, path_separator, path = location.partition("/")
    params: list[tuple[str, str]] = []

    if "@" in authority:
        userinfo, authority = authority.rsplit("@", 1)
        user, password_separator, password = userinfo.partition(":")
        if user:
            params.append(("user", _uri_unquote(user)))
        if password_separator:
            params.append(("password", _uri_unquote(password)))

    hosts: list[str] = []
    ports: list[str] = []
    has_port = False
    for endpoint in authority.split(",") if authority else ():
        host, port = _split_uri_endpoint(endpoint)
        hosts.append(_uri_unquote(host))
        ports.append(port)
        has_port = has_port or bool(port)
    if hosts:
        params.append(("host", ",".join(hosts)))
    if has_port:
        params.append(("port", ",".join(ports)))
    if path_separator and path:
        params.append(("dbname", _uri_unquote(path)))
    if separator:
        for item in query.split("&"):
            key, value_separator, item_value = item.partition("=")
            if not key or not value_separator:
                raise ValueError("invalid query parameter in connection URI")
            params.append((_uri_unquote(key), _uri_unquote(item_value)))
    return params


def _split_uri_endpoint(endpoint: str) -> tuple[str, str]:
    if endpoint.startswith("["):
        end = endpoint.find("]")
        if end < 0:
            raise ValueError("unterminated IPv6 address in connection URI")
        suffix = endpoint[end + 1 :]
        if suffix and not suffix.startswith(":"):
            raise ValueError("invalid host in connection URI")
        return endpoint[1:end], suffix[1:]

    if endpoint.count(":") > 1:
        raise ValueError("IPv6 addresses in connection URIs must use brackets")
    host, separator, port = endpoint.rpartition(":")
    if separator and port and not port.isdigit():
        raise ValueError("invalid port in connection URI")
    return (host, port) if separator else (endpoint, "")


def _uri_unquote(value: str) -> str:
    if _BAD_PERCENT_ESCAPE.search(value):
        raise ValueError("invalid percent escape in connection URI")
    try:
        return unquote(value, errors="strict")
    except UnicodeDecodeError as ex:
        raise ValueError("connection URI is not valid UTF-8") from ex


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
