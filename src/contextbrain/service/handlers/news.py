"""News handlers - news items and posts."""

from __future__ import annotations

from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler

from ...payloads import (
    CheckNewsPostExistsPayload,
    GetNewsItemsPayload,
    UpsertNewsItemPayload,
    UpsertNewsPostPayload,
)
from ..helpers import make_response, parse_unit

logger = get_context_unit_logger(__name__)


class NewsHandlersMixin:
    """Mixin for NewsEngine operations."""

    @grpc_error_handler
    async def UpsertNewsItem(self, request, context):
        """Upsert news item (raw or fact) to Brain storage."""
        unit = parse_unit(request)
        params = UpsertNewsItemPayload(**unit.payload)

        try:
            if params.item_type == "raw":
                item_id = await self.news_store.upsert_raw(
                    id=str(unit.unit_id),
                    tenant_id=params.tenant_id,
                    url=params.url,
                    headline=params.headline,
                    summary=params.summary,
                    category=params.category,
                    source_api=params.source_api,
                    metadata=params.metadata,
                )
            else:
                embedding = await self.embedder.embed_async(f"{params.headline} {params.summary}")
                item_id = await self.news_store.upsert_fact(
                    id=str(unit.unit_id),
                    tenant_id=params.tenant_id,
                    url=params.url,
                    headline=params.headline,
                    summary=params.summary,
                    category=params.category,
                    embedding=embedding,
                    metadata=params.metadata,
                )

            logger.info(f"Upserted news {params.item_type}: {item_id}")
            return make_response(
                payload={"id": item_id, "success": True, "message": "OK"},
                parent_unit=unit,
                provenance=["brain:upsert_news_item"],
            )
        except Exception as e:
            logger.error(f"UpsertNewsItem failed: {e}")
            return make_response(
                payload={"id": "", "success": False, "message": str(e)},
                parent_unit=unit,
                provenance=["brain:upsert_news_item:error"],
            )

    @grpc_stream_error_handler
    async def GetNewsItems(self, request, context):
        """Get news items (raw or facts) from Brain."""
        unit = parse_unit(request)
        params = GetNewsItemsPayload(**unit.payload)

        try:
            if params.item_type == "fact":
                rows = await self.news_store.get_facts(
                    tenant_id=params.tenant_id,
                    limit=params.limit,
                    since=params.since,
                )
            else:
                rows = []

            for row in rows:
                yield make_response(
                    payload={
                        "id": row.get("id", ""),
                        "url": row.get("url", ""),
                        "headline": row.get("headline", ""),
                        "summary": row.get("summary", ""),
                        "category": row.get("category", ""),
                        "metadata": {k: str(v) for k, v in (row.get("metadata") or {}).items()},
                    },
                    parent_unit=unit,
                    provenance=["brain:get_news_items"],
                )
        except Exception as e:
            logger.error(f"GetNewsItems failed: {e}")

    @grpc_error_handler
    async def UpsertNewsPost(self, request, context):
        """Upsert a generated post to Brain storage."""
        unit = parse_unit(request)
        params = UpsertNewsPostPayload(**unit.payload)

        try:
            embedding = await self.embedder.embed_async(f"{params.headline} {params.content}")

            post_id = await self.news_store.upsert_post(
                id=str(unit.unit_id),
                tenant_id=params.tenant_id,
                fact_id=params.fact_id if params.fact_id else None,
                agent=params.agent,
                headline=params.headline,
                content=params.content,
                emoji=params.emoji,
                fact_url=params.fact_url,
                embedding=embedding,
                scheduled_at=params.scheduled_at,
            )

            logger.info(f"Upserted news post: {post_id}")
            return make_response(
                payload={"id": post_id, "success": True},
                parent_unit=unit,
                provenance=["brain:upsert_news_post"],
            )
        except Exception as e:
            logger.error(f"UpsertNewsPost failed: {e}")
            return make_response(
                payload={"id": "", "success": False},
                parent_unit=unit,
                provenance=["brain:upsert_news_post:error"],
            )

    @grpc_error_handler
    async def CheckNewsPostExists(self, request, context):
        """Check if a news post with given URL already exists."""
        unit = parse_unit(request)
        params = CheckNewsPostExistsPayload(**unit.payload)

        try:
            exists = await self.news_store.check_url_exists(
                tenant_id=params.tenant_id,
                fact_url=params.fact_url,
            )

            logger.debug(f"CheckNewsPostExists: {params.fact_url} -> {exists}")
            return make_response(
                payload={"exists": exists},
                parent_unit=unit,
                provenance=["brain:check_news_post_exists"],
            )
        except Exception as e:
            logger.error(f"CheckNewsPostExists failed: {e}")
            return make_response(
                payload={"exists": False},
                parent_unit=unit,
                provenance=["brain:check_news_post_exists:error"],
            )


__all__ = ["NewsHandlersMixin"]
