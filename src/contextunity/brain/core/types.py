"""Core type definitions and utilities for contextunity.brain.

Re-exports canonical StructData types from ``contextunity.core.sdk.types``.
"""

from contextunity.core.sdk.types import (
    StructData,
    StructDataPrimitive,
    StructDataValue,
    coerce_struct_data,
)

__all__ = [
    "StructData",
    "StructDataPrimitive",
    "StructDataValue",
    "coerce_struct_data",
]
