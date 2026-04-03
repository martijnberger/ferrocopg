# mypy: disable-error-code="import-not-found, attr-defined"
"""
Simplify access to the bootstrap ferrocopg Rust module.

This module intentionally does not participate in implementation selection yet.
It only provides one place for future Rust-backed helpers to import from while
the ferrocopg port is still in flight.
"""

from __future__ import annotations

from types import ModuleType

__version__: str | None = None
_ferrocopg: ModuleType | None

try:
    import ferrocopg_rust._ferrocopg

    _ferrocopg = ferrocopg_rust._ferrocopg
    __version__ = _ferrocopg.__version__
except Exception:
    _ferrocopg = None
