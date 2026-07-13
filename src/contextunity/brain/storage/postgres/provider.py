"""Postgres provider (storage + retrieval)."""

from __future__ import annotations

from typing import override

from contextunity.core import ContextToken, ContextUnit
from contextunity.core.exceptions import ConfigurationError
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core import get_core_config
from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.core.interfaces import BaseProvider, IRead, IWrite
from contextunity.brain.core.models import RetrievedDoc
from contextunity.brain.core.types import coerce_struct_data

# NOTE: Router-specific logic (model_registry, get_rag_retrieval_settings) removed.
# Brain should use a simpler embedding interface when implemented.
from .models import GraphNode
from .store import PostgresBrainStore


def _flatten_keywords(metadata: JsonDict) -> str | None:
    """flatten keywords.

    Args:
        metadata: Keyword metadata map.

    Returns:
        str | None: An instance of str | None.
    """
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
    """Represent and manage Postgres Provider logic within the system."""

    _store: PostgresBrainStore

    def __init__(self, *, store: PostgresBrainStore | None = None) -> None:
        """Initialize a new instance of PostgresProvider.

        Raises:
            ConfigurationError: If a validation error occurs.
        """
        cfg = get_core_config()
        if store is not None:
            self._store = store
        else:
            if not getattr(cfg, "postgres", None):
                raise ConfigurationError("Postgres config is missing from core config")
            self._store = PostgresBrainStore(
                dsn=cfg.postgres.dsn,
                pool_min_size=cfg.postgres.pool_min_size,
                pool_max_size=cfg.postgres.pool_max_size,
            )

    @override
    async def read(
        self,
        query: str,
        *,
        limit: int = 5,
        filters: JsonDict | None = None,
        token: ContextToken,
    ) -> list[ContextUnit]:
        """Read/search from Postgres knowledge store.

        Args:
            query (str): The query parameter.

        Returns:
            list[ContextUnit]: A list of list[ContextUnit].

        Raises:
            NotImplementedError: If a validation error occurs.
            SecurityError: If security credentials fail or permissions are insufficient.
        """
        filter_map = filters or {}
        tenant_id = filter_map.get("tenant_id")
        if not isinstance(tenant_id, str):
            tenant_id = None
        if not tenant_id:
            from contextunity.core.exceptions import SecurityError

            raise SecurityError("tenant_id is required for Postgres retrieval")

        # TODO: Embeddings should come from a simpler interface in brain
        # Router-specific logic (model_registry, rag_cfg) removed.
        # Brain needs its own embedding interface when implemented.
        raise NotImplementedError(
            (
                "PostgresProvider.read requires embeddings which are not yet implemented in brain. "
                "This functionality should use a simpler interface than router's model_registry."
            )
        )

    @override
    async def write(self, data: ContextUnit, *, token: ContextToken) -> None:
        """Write.

        Args:
            data (ContextUnit): The raw data dictionary or object.

        Raises:
            SecurityError: If security credentials fail or permissions are insufficient.
            BrainValidationError: If parameter values are invalid.
        """
        payload = data.payload or {}
        content = payload.get("content")
        if isinstance(content, RetrievedDoc):
            doc = content
        elif isinstance(content, dict):
            doc = RetrievedDoc.model_validate(content)
        else:
            raise BrainValidationError("PostgresProvider.write expects RetrievedDoc content")

        metadata_raw = payload.get("metadata", {})
        metadata = metadata_raw if is_json_dict(metadata_raw) else {}
        tenant_id = metadata.get("tenant_id")
        if not isinstance(tenant_id, str) or not tenant_id:
            from contextunity.core.exceptions import SecurityError

            raise SecurityError("tenant_id is required for Postgres write")
        user_id_raw = metadata.get("user_id")
        user_id = user_id_raw if isinstance(user_id_raw, str) else None

        node_id = str(data.unit_id)
        doc_metadata = coerce_struct_data(doc.metadata or {})
        if not isinstance(doc_metadata, dict):
            doc_metadata = {}
        doc_metadata["title"] = doc.title or ""
        doc_metadata["keywords_text"] = _flatten_keywords(doc_metadata)
        await self._store.upsert_cell(
            tenant_id=tenant_id,
            user_id=user_id,
            cell_id=node_id,
            cell_kind="document",
            content=str(doc.content or ""),
            metadata=doc_metadata,
            source_type=str(doc.source_type or "unknown"),
            source_ref=str(doc.url or ""),
        )

    @override
    async def sink(self, envelope: ContextUnit, *, token: ContextToken) -> None:
        """Sink.

        Args:
            envelope (ContextUnit): The envelope parameter.

        """
        await self.write(envelope, token=token)

    def _to_retrieved_doc(self, node: GraphNode, *, score: float) -> RetrievedDoc:
        """to retrieved doc.

        Args:
            node (GraphNode): The node parameter.

        Returns:
            RetrievedDoc: An instance of RetrievedDoc.
        """
        metadata = coerce_struct_data(node.metadata or {})
        if not isinstance(metadata, dict):
            metadata = {}
        doc_data: dict[str, object] = {
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
