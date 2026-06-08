"""Persistence helpers for staged ingestion artifacts (JSONL)."""

from __future__ import annotations

import json
from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_json_dict, as_str
from contextunity.core.parsing import json_loads
from contextunity.core.types import is_json_dict

from ..core import RawData, ShadowRecord

logger = get_contextunit_logger(__name__)


def write_raw_data_jsonl(items: list[RawData], path: Path, *, overwrite: bool = True) -> int:
    """Write raw data jsonl."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "a"
    with open(path, mode, encoding="utf-8") as f:
        for item in items:
            payload = {
                "content": item.content,
                "source_type": item.source_type,
                "metadata": item.metadata,
            }
            _ = f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return len(items)


def read_raw_data_jsonl(path: Path) -> list[RawData]:
    """Read raw data jsonl."""
    out: list[RawData] = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json_loads(line)
        except Exception as e:
            logger.debug("Failed to parse JSON line: %s", e)
            continue
        if not is_json_dict(obj):
            continue
        content = as_str(obj.get("content"))
        source_type = as_str(obj.get("source_type"))
        metadata = as_json_dict(obj.get("metadata"))
        if content and source_type:
            out.append(RawData(content=content, source_type=source_type, metadata=metadata))
    return out


def write_shadow_records_jsonl(
    records: list[ShadowRecord],
    path: Path,
    *,
    overwrite: bool = True,
) -> int:
    """Write shadow records jsonl."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "a"
    with open(path, mode, encoding="utf-8") as f:
        for r in records:
            payload = {
                "id": r.id,
                "input_text": r.input_text,
                "struct_data": r.struct_data,
                "citation_label": r.citation_label,
                "title": r.title,
                "source_type": r.source_type,
            }
            _ = f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return len(records)


def read_shadow_records_jsonl(path: Path) -> list[ShadowRecord]:
    """Read shadow records jsonl."""
    out: list[ShadowRecord] = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json_loads(line)
        except Exception as e:
            logger.debug("Failed to parse JSON line: %s", e)
            continue
        if not is_json_dict(obj):
            continue
        rid = as_str(obj.get("id"))
        input_text = as_str(obj.get("input_text"))
        struct_data_raw = obj.get("struct_data")
        if not rid or not input_text or not is_json_dict(struct_data_raw):
            continue
        sd: dict[str, object] = dict(struct_data_raw)
        out.append(
            ShadowRecord(
                id=rid,
                input_text=input_text,
                struct_data=sd,
                citation_label=as_str(obj.get("citation_label")) or None,
                title=as_str(obj.get("title")) or None,
                source_type=as_str(obj.get("source_type")) or None,
            )
        )
    return out
