"""
Adapters for booleans.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from typing import cast

from .. import _oids
from .._rmodule import _ferrocopg as _rpsycopg
from ..abc import AdaptContext
from ..adapt import Buffer, Dumper, Loader
from ..pq import Format


class BoolDumper(Dumper):
    oid = _oids.BOOL_OID

    def dump(self, obj: bool) -> Buffer | None:
        if _rpsycopg and hasattr(_rpsycopg, "bool_dump_text"):
            return cast(Buffer, _rpsycopg.bool_dump_text(obj))
        return b"t" if obj else b"f"

    def quote(self, obj: bool) -> Buffer:
        return b"true" if obj else b"false"


class BoolBinaryDumper(Dumper):
    format = Format.BINARY
    oid = _oids.BOOL_OID

    def dump(self, obj: bool) -> Buffer | None:
        if _rpsycopg and hasattr(_rpsycopg, "bool_dump_binary"):
            return cast(Buffer, _rpsycopg.bool_dump_binary(obj))
        return b"\x01" if obj else b"\x00"


class BoolLoader(Loader):
    def load(self, data: Buffer) -> bool:
        if _rpsycopg and hasattr(_rpsycopg, "bool_load_text"):
            return cast(bool, _rpsycopg.bool_load_text(data))
        return data == b"t"


class BoolBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> bool:
        if _rpsycopg and hasattr(_rpsycopg, "bool_load_binary"):
            return cast(bool, _rpsycopg.bool_load_binary(data))
        return data != b"\x00"


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters
    adapters.register_dumper(bool, BoolDumper)
    adapters.register_dumper(bool, BoolBinaryDumper)
    adapters.register_loader("bool", BoolLoader)
    adapters.register_loader("bool", BoolBinaryLoader)
