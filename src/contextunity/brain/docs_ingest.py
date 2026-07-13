"""Brain-owned persistence adapter for documentation BrainCells."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import PurePosixPath

from contextunity.core.documentation import (
    ALLOWED_DOC_TYPES,
    DEFAULT_LIFECYCLE,
    DEFAULT_VISIBILITY,
    DOCUMENTATION_CELL_KIND,
    DocumentationCellSource,
    build_test_generated_documentation_cell,
    content_hash_of,
    deterministic_document_id,
    extract_doc_comment_cells,
    extract_documentation_cells,
    extract_proto_documentation_cells,
    extract_pydantic_config_cells,
    extract_yaml_config_cells,
    stable_document_identity,
    validate_documentation_type,
)
from contextunity.core.tenant_policy import DOC_TENANT_ID
from contextunity.core.types import JsonDict

from contextunity.brain.storage.contracts import BrainStorageProtocol


def _metadata_for(source: DocumentationCellSource, source_hash: str) -> JsonDict:
    return {
        **source.metadata,
        "doc_type": source.doc_type,
        "source_path": source.source_path,
        "source_hash": source_hash,
        "symbol": source.symbol,
        "phase": source.phase,
        "visibility": source.visibility,
        "lifecycle": source.lifecycle,
        "lifecycle_state": source.lifecycle,
    }


async def ingest_documentation_cell(
    storage: BrainStorageProtocol,
    *,
    content: str,
    source_path: str,
    doc_type: str,
    phase: int,
    symbol: str | None = None,
    visibility: str = DEFAULT_VISIBILITY,
    lifecycle: str = DEFAULT_LIFECYCLE,
    metadata: JsonDict | None = None,
) -> str:
    """Upsert one documentation BrainCell under ``_doc``."""
    clean_type = validate_documentation_type(doc_type)
    clean_symbol = symbol or PurePosixPath(source_path).name
    source = DocumentationCellSource(
        content=content,
        source_path=PurePosixPath(source_path).as_posix(),
        doc_type=clean_type,
        symbol=clean_symbol,
        phase=phase,
        visibility=visibility,
        lifecycle=lifecycle,
        metadata=dict(metadata or {}),
    )
    source_hash = content_hash_of(content)
    document_id = deterministic_document_id(source.source_path, source.symbol)
    await storage.upsert_cell(
        tenant_id=DOC_TENANT_ID,
        cell_id=document_id,
        cell_kind=DOCUMENTATION_CELL_KIND,
        content=content,
        metadata=_metadata_for(source, source_hash),
        content_hash=source_hash,
        source_type="documentation",
        source_ref=stable_document_identity(source.source_path, source.symbol),
        confidence=1.0,
        visibility=visibility,
    )
    return document_id


async def ingest_documentation_sources(
    storage: BrainStorageProtocol, sources: Sequence[DocumentationCellSource]
) -> list[str]:
    """Write validated documentation source records as BrainCells."""
    return [
        await ingest_documentation_cell(
            storage,
            content=source.content,
            source_path=source.source_path,
            doc_type=source.doc_type,
            phase=source.phase,
            symbol=source.symbol,
            visibility=source.visibility,
            lifecycle=source.lifecycle,
            metadata=source.metadata,
        )
        for source in sources
    ]


__all__ = [
    "ALLOWED_DOC_TYPES",
    "DOC_TENANT_ID",
    "DOCUMENTATION_CELL_KIND",
    "DocumentationCellSource",
    "build_test_generated_documentation_cell",
    "content_hash_of",
    "extract_doc_comment_cells",
    "extract_documentation_cells",
    "extract_proto_documentation_cells",
    "extract_pydantic_config_cells",
    "extract_yaml_config_cells",
    "ingest_documentation_cell",
    "ingest_documentation_sources",
]
