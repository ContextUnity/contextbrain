"""JSON and vector serialization helpers for SQLite storage."""

from __future__ import annotations

import json
import sqlite3
import struct

from contextunity.core.parsing import json_loads as parse_wire_json
from contextunity.core.types import JsonDict, JsonValue, is_json_dict, is_json_value


def fetchone_row(cur: sqlite3.Cursor) -> sqlite3.Row | None:
    """Return a typed ``sqlite3.Row`` from ``fetchone()``, or ``None``."""
    rows: list[sqlite3.Row] = list(cur.fetchmany(1))
    return rows[0] if rows else None


def _row_key_index(row: sqlite3.Row, key: str) -> int | None:
    for idx, name in enumerate(row.keys()):
        if name == key:
            return idx
    return None


def _row_index_value(row: sqlite3.Row, index: int) -> object:
    packed: tuple[object, ...] = tuple(row)
    return packed[index]


def sqlite_cell(row: sqlite3.Row, key: str) -> object:
    """Read one column from a ``sqlite3.Row``."""
    index = _row_key_index(row, key)
    if index is None:
        return ""
    return _row_index_value(row, index)


def vec_to_bytes(embedding: list[float] | None) -> bytes | None:
    """Serialize float list to raw bytes for sqlite-vec."""
    if not embedding:
        return None
    return struct.pack(f"<{len(embedding)}f", *embedding)


def json_dumps(data: object) -> str | None:
    """Serialize to JSON string, None-safe."""
    if data is None:
        return None
    return json.dumps(data, ensure_ascii=False, default=str)


def json_loads(raw: str | None) -> JsonValue:
    """Deserialize JSON string, None-safe."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw if is_json_dict(raw) else {}
    try:
        decoded = parse_wire_json(raw)
    except Exception:
        return {}
    return decoded if is_json_value(decoded) else {}


def json_dict_field(raw: object) -> JsonDict:
    """Parse a DB text/JSON column into ``JsonDict``."""
    parsed = json_loads(raw if isinstance(raw, str) else None)
    return parsed if is_json_dict(parsed) else {}


def row_to_dict(row: sqlite3.Row) -> JsonDict:
    """Convert sqlite3.Row to dict with JSON deserialization for known fields."""
    d: dict[str, object] = {key: sqlite_cell(row, key) for key in row.keys()}
    for key in (
        "metadata",
        "struct_data",
        "tool_calls",
        "token_usage",
        "security_flags",
        "provenance",
        "input_data",
        "output_data",
    ):
        raw_cell = d.get(key)
        if isinstance(raw_cell, str):
            d[key] = json_loads(raw_cell)
    out: JsonDict = {}
    for key, value in d.items():
        if is_json_value(value):
            out[key] = value
        elif value is None:
            out[key] = None
        else:
            out[key] = str(value)
    return out


__all__ = [
    "fetchone_row",
    "json_dict_field",
    "sqlite_cell",
    "vec_to_bytes",
    "json_dumps",
    "json_loads",
    "row_to_dict",
]
