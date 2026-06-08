"""Record creation and JSONL utilities."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import TypedDict

from contextunity.core.types import JsonValue

from contextunity.brain.core.types import StructData, coerce_struct_data


class VertexImportContent(TypedDict):
    """Represent and manage Vertex Import Content logic within the system."""

    mimeType: str
    rawBytes: str


class VertexImportRecord(TypedDict):
    """Represent and manage Vertex Import Record logic within the system."""

    id: str
    content: VertexImportContent
    structData: StructData


def generate_id(*parts: str) -> str:
    """Generate a stable MD5 hash ID from string parts.

    Returns:
        str: The resulting string value.
    """
    combined = "_".join(str(p) for p in parts)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def slugify(value: str, max_length: int = 50) -> str:
    """Convert string to URL-safe slug.

    Args:
        value (str): The value to store or update.
        max_length (int): The max length parameter.

    Returns:
        str: The resulting string value.
    """
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    if max_length and len(value) > max_length:
        value = value[:max_length].rstrip("_")
    return value or "item"


def create_record(
    record_id: str,
    content: str,
    source_type: str,
    title: str,
    **metadata: JsonValue,
) -> VertexImportRecord:
    """Create a JSONL record for Vertex AI Search.

    Args:
        record_id (str): The record id parameter.
        content (str): The content parameter.
        source_type (str): The source type parameter.
        title (str): The title parameter.

    Returns:
        VertexImportRecord: An instance of VertexImportRecord.
    """
    struct_data: StructData = {"source_type": source_type, "title": title}
    for key, value in metadata.items():
        struct_data[key] = coerce_struct_data(value)

    return {
        "id": record_id,
        "content": {
            "mimeType": "text/plain",
            "rawBytes": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        },
        "structData": struct_data,
    }


def write_jsonl(records: Iterable[VertexImportRecord], destination: Path) -> int:
    """Write jsonl.

    Args:
        records (Iterable[VertexImportRecord]): The records parameter.
        destination (Path): The destination parameter.

    Returns:
        int: The resulting integer value.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with destination.open("w", encoding="utf-8") as f:
        for record in records:
            _ = f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def format_timestamp(seconds: float) -> str:
    """Format timestamp.

    Args:
        seconds (float): The seconds parameter.

    Returns:
        str: The resulting string value.
    """
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"
