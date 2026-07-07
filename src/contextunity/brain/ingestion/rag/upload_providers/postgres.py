"""Postgres upload provider: load JSONL into pgvector store."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from pathlib import Path
from typing import override

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError
from contextunity.core.narrowing import as_json_dict, as_str
from contextunity.core.types import JsonDict, is_json_dict
from psycopg_pool import AsyncConnectionPool

from contextunity.brain.core import get_core_config
from contextunity.brain.core.types import coerce_struct_data

# TODO: Model registry not yet implemented
# from contextunity.brain.modules.models import model_registry
from .base import UploadProvider, UploadResult

logger = get_contextunit_logger(__name__)


class PostgresUploadProvider(UploadProvider):
    """Represent and manage Postgres Upload Provider logic within the system."""

    def __init__(self, config: JsonDict):
        """Initialize a new instance of PostgresUploadProvider.

        Args:
            config (JsonDict): The configuration settings dict or object.
        """
        super().__init__(config)
        self._pool: AsyncConnectionPool | None = None

    @property
    @override
    def name(self) -> str:
        """Name.

        Returns:
            str: The resulting string value.
        """
        return "postgres"

    @override
    def upload_and_index(self, local_path: Path, *, wait: bool = False) -> UploadResult:
        """Upload and index.

        Args:
            local_path (Path): The local path parameter.

        Returns:
            UploadResult: An instance of UploadResult.
        """
        _ = wait
        try:
            asyncio.run(self._upload_async(local_path))
            return UploadResult(success=True, provider=self.name, details={})
        except Exception as e:
            logger.exception("Postgres upload failed")
            return UploadResult(success=False, provider=self.name, error=str(e), details={})

    @override
    def get_config_summary(self) -> dict[str, str]:
        """Retrieve the config summary information.

        Returns:
            dict[str, str]: A dictionary containing the results.
        """
        return {"provider": self.name, "dsn": "***" if self._config.get("dsn") else "missing"}

    async def _upload_async(self, _local_path: Path) -> None:
        """upload async.

        Args:
            local_path (Path): The local path parameter.

        Raises:
            NotImplementedError: If a validation error occurs.
            ValueError: If parameter values are invalid.
        """
        cfg = get_core_config()
        dsn = as_str(self._config.get("dsn")) or cfg.postgres.dsn
        if not dsn:
            raise ConfigurationError("Postgres DSN is required")

        _ = await self._get_pool(dsn=dsn)
        _ = as_str(self._config.get("tenant_id")) or "public"
        _ = self._config.get("user_id")
        embeddings_key = (
            as_str(self._config.get("embeddings_model")) or cfg.models.default_embeddings
        )
        raise NotImplementedError(
            "Embeddings require model registry which is not yet implemented. "
            + f"Requested embeddings: {embeddings_key}"
        )

        # Placeholder code (will not execute):
        # with open(local_path, encoding="utf-8") as f:
        #     records = [json.loads(line) for line in f if line.strip()]
        #
        # batch_size = 64
        # for batch in _chunks(records, batch_size):
        #     texts, payloads = self._prepare_batch(batch)
        #     embeddings = await embedder.embed_documents(texts)
        #     await self._insert_batch(
        #         pool=pool,
        #         tenant_id=tenant_id,
        #         user_id=user_id,
        #         payloads=payloads,
        #         embeddings=embeddings,
        #     )

    async def _get_pool(self, *, dsn: str) -> AsyncConnectionPool:
        """get pool.

        Returns:
            AsyncConnectionPool: An instance of AsyncConnectionPool.
        """
        if self._pool is None:
            self._pool = AsyncConnectionPool(dsn, min_size=2, max_size=10, open=False)
        if not self._pool.closed:
            await self._pool.open()
        return self._pool

    def _prepare_batch(self, batch: list[JsonDict]) -> tuple[list[str], list[JsonDict]]:
        """prepare batch.

        Args:
            batch (list[JsonDict]): The batch parameter.

        Returns:
            tuple[list[str], list[JsonDict]]: A list of tuple[list[str], list[JsonDict]].
        """
        texts: list[str] = []
        payloads: list[JsonDict] = []
        for record in batch:
            content_obj = as_json_dict(record.get("content"))
            raw = content_obj.get("rawBytes")
            if not raw:
                continue
            content = base64.b64decode(as_str(raw)).decode("utf-8", errors="ignore")
            struct_raw = record.get("structData") or {}
            struct_data = coerce_struct_data(struct_raw)
            if not is_json_dict(struct_data):
                struct_data = {}
            keywords_text = _flatten_keywords(struct_data)
            payloads.append(
                {
                    "id": record.get("id"),
                    "content": content,
                    "struct_data": struct_data,
                    "source_type": struct_data.get("source_type"),
                    "source_id": struct_data.get("source_id"),
                    "title": struct_data.get("title"),
                    "keywords_text": keywords_text,
                    "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                }
            )
            texts.append(content)
        return texts, payloads

    async def _insert_batch(
        self,
        *,
        pool: AsyncConnectionPool,
        tenant_id: str,
        user_id: str | None,
        payloads: list[JsonDict],
        embeddings: list[list[float]],
    ) -> None:
        """insert batch."""
        if not payloads:
            return
        async with pool.connection() as conn:
            async with conn.transaction():
                for payload, embedding in zip(payloads, embeddings):
                    _ = await conn.execute(
                        """
                        INSERT INTO cells (
                            id, tenant_id, user_id, node_kind, source_type, source_id, title,
                            content, struct_data, keywords_text, content_hash, embedding
                        )
                        VALUES (
                            %(id)s, %(tenant_id)s, %(user_id)s, 'chunk', %(source_type)s, %(source_id)s,
                            %(title)s, %(content)s, %(struct_data)s, %(keywords_text)s, %(content_hash)s, %(embedding)s
                        )
                        ON CONFLICT (node_kind, content_hash) DO NOTHING
                        """,
                        {
                            "id": payload.get("id"),
                            "tenant_id": tenant_id,
                            "user_id": user_id,
                            "source_type": payload.get("source_type"),
                            "source_id": payload.get("source_id"),
                            "title": payload.get("title"),
                            "content": payload.get("content"),
                            "struct_data": payload.get("struct_data"),
                            "keywords_text": payload.get("keywords_text"),
                            "content_hash": payload.get("content_hash"),
                            "embedding": "[" + ",".join(f"{float(x):.8f}" for x in embedding) + "]",
                        },
                    )


def _flatten_keywords(struct_data: JsonDict) -> str | None:
    """flatten keywords.

    Args:
        struct_data (JsonDict): The struct data parameter.

    Returns:
        str | None: An instance of str | None.
    """
    keywords = struct_data.get("keywords")
    keyphrases = struct_data.get("keyphrase_texts")
    parts: list[str] = []
    for raw in (keywords, keyphrases):
        if isinstance(raw, list):
            for item in raw:
                text = str(item).strip()
                if text:
                    parts.append(text)
    if not parts:
        return None
    # Deduplicate while preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return " ".join(uniq)


__all__ = ["PostgresUploadProvider"]
