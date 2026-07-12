from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

from tools import stage_ferrocopg as staging

ROOT = Path(__file__).parent.parent


def test_stage_package_rewrites_namespace_and_preserves_accelerator_names(
    tmp_path: Path,
) -> None:
    output = tmp_path / "stage"
    package = staging.stage_package(output)

    assert package == output / "ferrocopg"
    assert (package / "__init__.py").is_file()
    assert (package / "_ferrocopg.py").is_file()
    assert (package / "_rust" / "__init__.py").is_file()
    assert (package / "py.typed").is_file()
    assert "GNU LESSER GENERAL PUBLIC LICENSE" in (output / "LICENSE.txt").read_text()
    assert not (output / "psycopg").exists()
    assert not list(package.rglob("*.so"))
    assert not list(package.rglob("*.pyc"))

    project = (output / "pyproject.toml").read_text()
    assert 'name = "ferrocopg"' in project
    assert 'version = "0.1.0"' in project
    assert 'requires-python = ">=3.11"' in project
    assert 'libpq = ["psycopg >= 3.3, < 3.4"]' in project
    assert 'module-name = "ferrocopg._rust._ferrocopg"' in project
    match = re.search(r'^manifest-path = "([^"]+)"$', project, re.MULTILINE)
    assert match
    manifest = output / match.group(1)
    assert manifest.resolve().is_file()

    connection = (package / "connection.py").read_text()
    version = (package / "version.py").read_text()
    cmodule = (package / "_cmodule.py").read_text()
    pq_init = (package / "pq" / "__init__.py").read_text()
    rmodule = (package / "_rmodule.py").read_text()
    init = (package / "__init__.py").read_text()
    official = (package / "_official.py").read_text()
    vendored = (package / "_vendored.py").read_text()
    assert '__module__ = "ferrocopg"' in connection
    assert 'metadata.version("ferrocopg")' in version
    assert "import psycopg_c._psycopg" in cmodule
    assert "import psycopg_binary._psycopg" in cmodule
    assert 'pq.__impl__ in ("python", "ferrocopg")' in cmodule
    assert 'impl = "ferrocopg"' in pq_init
    assert "from .. import _pq_compat as module" in pq_init
    assert "from ._rust import _ferrocopg as rust_extension" in rmodule
    assert "ferrocopg_rust" not in rmodule
    assert "from ._official import AsyncConnection" in init
    assert "NoTlsConnectionAdapter as RustConnection" in init
    assert '"RustConnection",' in init
    assert ") -> RustConnection: ..." in init
    assert 'impl: Literal["libpq"]' in init
    assert ") -> Any: ..." in init
    assert "connect as connect_official" in init
    assert "PSYCOPG_SOURCE_IMPL" not in init
    assert 'import_module("psycopg")' in official
    assert "ferrocopg[libpq]" in official
    revision = staging.DEFAULT_UPSTREAM_REVISION.read_text().strip()
    assert f'PSYCOPG_REVISION = "{revision}"' in vendored
    assert "__vendored_psycopg_revision__" in init

    fallback = Requirement("psycopg >= 3.3, < 3.4")
    assert Version("3.3") in fallback.specifier
    assert Version("3.3.99") in fallback.specifier
    assert Version("3.2.99") not in fallback.specifier
    assert Version("3.4") not in fallback.specifier


def test_staged_package_imports_with_ferrocopg_identities(tmp_path: Path) -> None:
    output = tmp_path / "stage"
    staging.stage_package(output)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import ferrocopg; "
            "print(ferrocopg.__name__); "
            "print(ferrocopg.Connection.__module__); "
            "print(ferrocopg.Error.__module__); "
            "print(ferrocopg.pq.__impl__); "
            'assert ferrocopg.sql.Identifier(\'a\\"b\').as_string() == \'"a\\"\\"b"\'; '
            "print(ferrocopg.__vendored_psycopg_revision__)",
        ],
        cwd=tmp_path,
        env={k: v for k, v in os.environ.items() if k != "PSYCOPG_IMPL"}
        | {"PYTHONPATH": str(output)},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "ferrocopg",
        "ferrocopg",
        "ferrocopg",
        "ferrocopg",
        staging.DEFAULT_UPSTREAM_REVISION.read_text().strip(),
    ]


def test_staged_package_delegates_libpq_and_async_to_official_psycopg(
    tmp_path: Path,
) -> None:
    output = tmp_path / "stage"
    staging.stage_package(output)
    script = """\
import asyncio
import ferrocopg
import psycopg
import ferrocopg._official as official

sentinel = object()
psycopg.connect = lambda conninfo, **kwargs: (sentinel, conninfo, kwargs)
assert ferrocopg.connect("dbname=test", impl="libpq", autocommit=True) == (
    sentinel, "dbname=test", {"autocommit": True}
)

class StubAsyncConnection:
    @classmethod
    async def connect(cls, conninfo, **kwargs):
        return sentinel, conninfo, kwargs

psycopg.AsyncConnection = StubAsyncConnection
assert asyncio.run(ferrocopg.AsyncConnection.connect("dbname=async")) == (
    sentinel, "dbname=async", {}
)

official.import_module = lambda name: (_ for _ in ()).throw(
    ModuleNotFoundError("No module named 'psycopg'", name="psycopg")
)
try:
    ferrocopg.connect("dbname=missing", impl="libpq")
except ImportError as ex:
    assert "ferrocopg[libpq]" in str(ex)
else:
    raise AssertionError("missing official Psycopg didn't fail")
"""
    pythonpath = os.pathsep.join((str(output), str(ROOT / "psycopg")))

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": pythonpath, "PSYCOPG_IMPL": "python"},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_staged_package_exposes_honest_connect_types(tmp_path: Path) -> None:
    output = tmp_path / "stage"
    staging.stage_package(output)
    probe = tmp_path / "typing_probe.py"
    probe.write_text(
        "import ferrocopg\n"
        "reveal_type(ferrocopg.connect())\n"
        'reveal_type(ferrocopg.connect(impl="ferrocopg"))\n'
        'reveal_type(ferrocopg.connect(impl="libpq"))\n'
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--no-incremental",
            "--ignore-missing-imports",
            str(probe),
        ],
        cwd=tmp_path,
        env={**os.environ, "MYPYPATH": str(output)},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    rust_type = "ferrocopg._ferrocopg.NoTlsConnectionAdapter"
    assert result.stdout.count(f'Revealed type is "{rust_type}"') == 2
    assert 'Revealed type is "Any"' in result.stdout


def test_stage_package_refuses_existing_output(tmp_path: Path) -> None:
    output = tmp_path / "stage"
    output.mkdir()

    with pytest.raises(FileExistsError, match="already exists"):
        staging.stage_package(output)


def test_generated_project_uses_absolute_manifest_across_drives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def different_drive(*_args: object) -> str:
        raise ValueError("path is on a different drive")

    monkeypatch.setattr(os.path, "relpath", different_drive)
    project = staging._build_pyproject(tmp_path, staging.DEFAULT_MANIFEST)

    assert staging.DEFAULT_MANIFEST.resolve().as_posix() in project
