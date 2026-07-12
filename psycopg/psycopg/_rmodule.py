# mypy: disable-error-code="import-not-found, import-untyped, attr-defined"
"""
Simplify access to the bootstrap ferrocopg Rust module.

This module keeps the optional source-tree extension import and its failure in
one place. Public connection selection turns a missing extension into an
actionable error; helper fast paths may still probe `_ferrocopg` directly.
"""

from __future__ import annotations

from types import ModuleType

__version__: str | None = None
_ferrocopg: ModuleType | None
_import_error: BaseException | None

try:
    import ferrocopg_rust._ferrocopg

    _ferrocopg = ferrocopg_rust._ferrocopg
    __version__ = _ferrocopg.__version__
    _import_error = None
except Exception as ex:
    _ferrocopg = None
    _import_error = ex
