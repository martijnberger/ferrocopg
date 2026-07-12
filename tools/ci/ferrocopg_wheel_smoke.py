#!/usr/bin/env python3
"""Smoke-test an installed staged ferrocopg wheel."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn")
    parser.add_argument("--after-uninstall", action="store_true")
    args = parser.parse_args()

    if args.after_uninstall:
        return _check_uninstalled()
    if not args.dsn:
        parser.error("--dsn is required before uninstall")
    return _check_installed(args.dsn)


def _check_installed(dsn: str) -> int:
    import ferrocopg
    from ferrocopg import _ferrocopg as adapter
    from ferrocopg._rust import _ferrocopg as rust

    import psycopg

    distribution = importlib.metadata.distribution("ferrocopg")
    installed_files = {str(path) for path in distribution.files or ()}

    assert distribution.version == "0.1.0"
    assert not any(path.startswith("psycopg/") for path in installed_files)
    assert ferrocopg is not psycopg
    assert ferrocopg.Connection.__module__ == "ferrocopg"
    assert psycopg.Connection.__module__ == "psycopg"
    assert adapter.is_available()
    assert rust.__version__ == "0.1.0"
    assert (
        ferrocopg.__vendored_psycopg_revision__
        == (ROOT / "UPSTREAM_REVISION").read_text().strip()
    )

    conn = ferrocopg.connect(dsn, connect_timeout=10)
    try:
        assert conn.execute("select %s::int4", (42,)).fetchone() == (42,)
    finally:
        conn.close()
    return 0


def _check_uninstalled() -> int:
    assert importlib.util.find_spec("ferrocopg") is None
    assert importlib.util.find_spec("psycopg") is not None

    import psycopg

    assert psycopg.Connection.__module__ == "psycopg"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
