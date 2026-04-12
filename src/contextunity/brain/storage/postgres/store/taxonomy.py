"""Taxonomy operations."""

from __future__ import annotations

from typing import List, Optional

from .helpers import Json, execute, fetch_all


class TaxonomyMixin:
    """Mixin for taxonomy operations."""

    async def upsert_taxonomy(
        self,
        *,
        tenant_id: str,
        domain: str,
        name: str,
        path: str,
        keywords: List[str],
        metadata: dict = None,
    ) -> None:
        """Upsert a taxonomy node."""
        async with await self.tenant_connection(tenant_id) as conn:
            await execute(
                conn,
                """
                INSERT INTO catalog_taxonomy (tenant_id, domain, name, path, keywords, metadata, updated_at)
                VALUES (%(tenant_id)s, %(domain)s, %(name)s, %(path)s, %(keywords)s, %(metadata)s, now())
                ON CONFLICT (tenant_id, domain, path) DO UPDATE SET
                    name = EXCLUDED.name, keywords = EXCLUDED.keywords,
                    metadata = EXCLUDED.metadata, updated_at = now()
            """,
                {
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "name": name,
                    "path": path,
                    "keywords": keywords,
                    "metadata": Json(metadata or {}),
                },
            )

    async def get_all_taxonomy(self, *, tenant_id: str, domain: Optional[str] = None) -> List[dict]:
        """Get all taxonomy nodes."""
        async with await self.tenant_connection(tenant_id) as conn:
            query = "SELECT * FROM catalog_taxonomy WHERE tenant_id = %(tenant_id)s"
            params = {"tenant_id": tenant_id}
            if domain:
                query += " AND domain = %(domain)s"
                params["domain"] = domain
            return await fetch_all(conn, query, params)
