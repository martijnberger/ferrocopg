"""
Bootstrap Rust extension package for ferrocopg.
"""

from ._ferrocopg import (
    __version__,
    libpq_binding,
    libpq_version,
    milestone,
    scaffold_status,
)

__all__ = [
    "__version__",
    "libpq_binding",
    "libpq_version",
    "milestone",
    "scaffold_status",
]
