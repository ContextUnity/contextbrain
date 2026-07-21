"""Payload decoding helpers shared by SQLite admin query modules."""

from __future__ import annotations

from contextunity.core.types import JsonDict, is_json_dict

from .codecs import json_loads


def _json_dict_field(raw: object) -> JsonDict:
    if is_json_dict(raw):
        return raw
    if isinstance(raw, str):
        parsed = json_loads(raw)
        if is_json_dict(parsed):
            return parsed
    return {}


__all__ = ["_json_dict_field"]
