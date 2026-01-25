"""Core type definitions and utilities for ContextBrain.

This module provides shared type definitions and utilities used across
the ContextBrain codebase, including StructData for JSON-serializable data.
"""

from __future__ import annotations

from typing import Any, Union

# Type aliases for JSON-serializable data structures
StructDataPrimitive = Union[str, int, float, bool, None]
StructDataValue = Union[
    StructDataPrimitive,
    list["StructDataValue"],
    dict[str, "StructDataValue"],
]
StructData = dict[str, StructDataValue]


def coerce_struct_data(obj: Any) -> StructDataValue:
    """Coerce an object to StructDataValue format.

    This function normalizes data at integration boundaries to ensure
    it's JSON-serializable and matches the StructData contract.

    Args:
        obj: Any object to coerce

    Returns:
        StructDataValue: Coerced value that is JSON-serializable
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): coerce_struct_data(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [coerce_struct_data(item) for item in obj]
    # For other types, convert to string as fallback
    return str(obj)


__all__ = [
    "StructData",
    "StructDataPrimitive",
    "StructDataValue",
    "coerce_struct_data",
]
