"""Postgres provider (storage + retrieval)."""

from __future__ import annotations

from typing import Any

from contextcore import ContextUnit

from contextbrain.core import get_core_config
from contextbrain.core.interfaces import BaseProvider, IRead, IWrite
from contextbrain.core.models import RetrievedDoc
from contextbrain.core.tokens import AccessManager, ContextToken
from contextbrain.core.types import coerce_struct_data

# NOTE: Router-specific logic (model_registry, get_rag_retrieval_settings) removed.
# Brain should use a simpler embedding interface when implemented.
from .store import GraphNode, PostgresKnowledgeStore


def _flatten_keywords(metadata: dict[str, Any]) -> str | None:
    keywords = metadata.get("keywords")
    keyphrases = metadata.get("keyphrase_texts")
    parts: list[str] = []
    for raw in (keywords, keyphrases):
        if isinstance(raw, list):
            for item in raw:
                text = str(item).strip()
                if text:
                    parts.append(text)
    if not parts:
        return None
    seen: set[str] = set()
    uniq: list[str] = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return " ".join(uniq)


class PostgresProvider(BaseProvider, IRead, IWrite):
    def __init__(self, *, store: PostgresKnowledgeStore | None = None) -> None:
        cfg = get_core_config()
        self._access = AccessManager.from_core_config()
        if store is not None:
            self._store = store
        else:
            if not getattr(cfg, "postgres", None):
                raise RuntimeError("Postgres config is missing from core config")
            self._store = PostgresKnowledgeStore(
                dsn=cfg.postgres.dsn,
                pool_min_size=cfg.postgres.pool_min_size,
                pool_max_size=cfg.postgres.pool_max_size,
            )

    async def read(
        self,
        query: str,
        *,
        limit: int = 5,
        filters: dict[str, Any] | None = None,
        token: ContextToken,
    ) -> list[ContextUnit]:
        """Read/search from Postgres knowledge store.

        NOTE: This is a simplified version for brain. Full retrieval logic
        (reranking, fusion, etc.) belongs in router.
        """
        self._access.verify_read(token)
        cfg = get_core_config()

        tenant_id = (filters or {}).get("tenant_id")
        if cfg.security.enabled and not tenant_id:
            raise PermissionError("tenant_id is required for Postgres retrieval")
        if not tenant_id:
            tenant_id = "public"

        # TODO: Embeddings should come from a simpler interface in brain
        # Router-specific logic (model_registry, rag_cfg) removed.
        # Brain needs its own embedding interface when implemented.
        raise NotImplementedError(
            "PostgresProvider.read requires embeddings which are not yet implemented in brain. "
            "This functionality should use a simpler interface than router's model_registry."
        )

    async def write(self, data: ContextUnit, *, token: ContextToken) -> None:
        self._access.verify_envelope_write(data, token)
        cfg = get_core_config()
        payload = data.payload or {}
        content = payload.get("content")
        if isinstance(content, RetrievedDoc):
            doc = content
        elif isinstance(content, dict):
            doc = RetrievedDoc.model_validate(content)
        else:
            raise ValueError("PostgresProvider.write expects RetrievedDoc content")

        metadata = payload.get("metadata", {})
        tenant_id = metadata.get("tenant_id") if isinstance(metadata, dict) else None
        if cfg.security.enabled and not tenant_id:
            raise PermissionError("tenant_id is required for Postgres write")
        if not tenant_id:
            tenant_id = "public"
        user_id = metadata.get("user_id") if isinstance(metadata, dict) else None

        node_id = str(data.unit_id)
        doc_metadata = coerce_struct_data(doc.metadata or {})
        if not isinstance(doc_metadata, dict):
            doc_metadata = {}
        keywords_text = _flatten_keywords(doc_metadata)
        node = GraphNode(
            id=node_id,
            content=str(doc.content or ""),
            node_kind="chunk",
            source_type=str(doc.source_type or "unknown"),
            source_id=str(doc.url or ""),
            title=doc.title,
            metadata=doc_metadata,
            keywords_text=keywords_text,
            tenant_id=str(tenant_id),
            user_id=str(user_id) if user_id else None,
        )
        await self._store.upsert_graph([node], [], tenant_id=str(tenant_id), user_id=user_id)

    async def sink(self, envelope: ContextUnit, *, token: ContextToken) -> Any:
        await self.write(envelope, token=token)
        return None

    def _to_retrieved_doc(self, node: GraphNode, *, score: float) -> RetrievedDoc:
        metadata = coerce_struct_data(node.metadata or {})
        if not isinstance(metadata, dict):
            metadata = {}
        doc_data = {
            "source_type": node.source_type or "unknown",
            "content": node.content,
            "title": node.title,
            "metadata": metadata,
            "relevance": score,
        }
        for key in (
            "url",
            "snippet",
            "keywords",
            "summary",
            "quote",
            "book_title",
            "chapter",
            "chapter_number",
            "page_start",
            "page_end",
            "video_id",
            "video_url",
            "video_name",
            "timestamp",
            "timestamp_seconds",
            "session_title",
            "question",
            "answer",
            "filename",
            "description",
        ):
            if key in metadata:
                doc_data[key] = metadata[key]
        return RetrievedDoc.model_validate(doc_data)


__all__ = ["PostgresProvider"]
