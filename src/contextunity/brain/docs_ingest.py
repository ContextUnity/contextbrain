"""Documentation-as-memory ingestion under the canonical ``_doc`` tenant.

Wraps ``BrainStorageProtocol.upsert_graph`` with the metadata shape and
idempotency behavior documentation BrainCells require, so callers never
construct the raw ``GraphNode``/metadata dict by hand. The BrainCell ``id``
is derived deterministically from ``source_path`` — re-ingesting the same
source, changed or not, always updates that one row rather than creating a
duplicate; ``source_hash`` is stored alongside for change detection.

``source_hash`` is written to ``metadata`` only, never to ``GraphNode``'s
``content_hash`` column: that column backs a real ``(node_kind='chunk',
content_hash)`` unique index (shared with the RAG chunk-upload path), which
would reject two genuinely different documentation sources that happen to
have byte-identical content — a real scenario for e.g. shared boilerplate
or license headers.

This is a trusted internal ingestion path (mirrors ``IngestionService``): it
calls storage directly and performs no token/tenant-access check itself.
Writes to ``_doc`` made through the ``Upsert`` gRPC RPC instead go through
the normal ``validate_tenant_access`` check, which already rejects a token
lacking ``_doc`` in ``allowed_tenants`` with a ``policy_fault``-classified
``SecurityError``.
"""

from __future__ import annotations

import hashlib
import uuid

from contextunity.brain.storage.contracts import BrainStorageProtocol
from contextunity.brain.storage.postgres.models import GraphNode

DOC_TENANT_ID = "_doc"


def _deterministic_doc_id(source_path: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"contextunity.doc.{source_path}"))


def content_hash_of(content: str) -> str:
    """Stable content hash used both as the stored ``source_hash`` and for
    change detection between ingestion runs."""
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


async def ingest_documentation_cell(
    storage: BrainStorageProtocol,
    *,
    content: str,
    source_path: str,
    doc_type: str,
    phase: int,
    visibility: str = "internal",
) -> str:
    """Upsert one documentation BrainCell under the ``_doc`` tenant.

    Args:
        storage: Backend to write through (Postgres or SQLite).
        content: Documentation text.
        source_path: Stable identifier for this piece of documentation
            (e.g. a repo-relative file path or proto RPC name) — the
            BrainCell id is derived from this, not from content, so the
            row updates in place across re-runs even when content changes.
        doc_type: Category, e.g. ``"rpc_reference"``, ``"config_ref"``.
        phase: Numeric rollout-phase tag for this documentation record.
        visibility: Access tier metadata (enforcement is not yet wired
            into RLS — see ``docs/policy/docs_as_memory.md``).

    Returns:
        The deterministic BrainCell id.
    """
    doc_id = _deterministic_doc_id(source_path)
    source_hash = content_hash_of(content)

    node = GraphNode(
        id=doc_id,
        content=content,
        node_kind="chunk",
        source_type="documentation",
        metadata={
            "doc_type": doc_type,
            "source_path": source_path,
            "source_hash": source_hash,
            "phase": phase,
            "visibility": visibility,
        },
        tenant_id=DOC_TENANT_ID,
    )
    await storage.upsert_graph(nodes=[node], edges=[], tenant_id=DOC_TENANT_ID)
    return doc_id


__all__ = ["DOC_TENANT_ID", "content_hash_of", "ingest_documentation_cell"]
