"""
Helpers to access the bootstrap ferrocopg Rust module from Python code.

This module is intentionally small and optional. It gives the Python package a
stable place to reach future Rust-backed ferrocopg helpers without forcing the
extension to be present in every environment.
"""

from __future__ import annotations

from ._rmodule import __version__ as __version__
from ._rmodule import _ferrocopg


def is_available() -> bool:
    """Return `True` if the bootstrap ferrocopg Rust extension is importable."""
    return _ferrocopg is not None


def conninfo_summary(conninfo: str) -> object | None:
    """
    Return a Rust-backed conninfo summary if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return _ferrocopg.parse_conninfo_summary(conninfo)


def connect_plan(conninfo: str) -> object | None:
    """
    Return a Rust-backed connect plan if the ferrocopg extension is loaded.
    """
    if not _ferrocopg:
        return None
    return _ferrocopg.parse_connect_plan(conninfo)
