"""Knowledge graph operations."""

from __future__ import annotations

from typing import List

from ..models import GraphEdge, GraphNode
from .helpers import Json, execute, vec


class GraphMixin:
    """Mixin for knowledge graph operations."""

    async def upsert_graph(
        self,
        nodes: List[GraphNode],
        edges: List[GraphEdge],
        *,
        tenant_id: str,
        user_id: str | None = None,
    ) -> None:
        """Upsert knowledge graph nodes and edges."""
        if not tenant_id:
            raise ValueError("tenant_id is required")

        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.transaction():
                for node in nodes:
                    await execute(
                        conn,
                        """
                        INSERT INTO knowledge_nodes (
                            id, tenant_id, user_id, node_kind, source_type, source_id,
                            title, content, struct_data, keywords_text, taxonomy_path, embedding
                        ) VALUES (
                            %(id)s, %(tenant_id)s, %(user_id)s, %(node_kind)s, %(source_type)s,
                            %(source_id)s, %(title)s, %(content)s, %(struct_data)s,
                            %(keywords_text)s, %(taxonomy_path)s, %(embedding)s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title, content = EXCLUDED.content,
                            struct_data = EXCLUDED.struct_data, keywords_text = EXCLUDED.keywords_text,
                            taxonomy_path = EXCLUDED.taxonomy_path, embedding = EXCLUDED.embedding
                    """,
                        {
                            "id": node.id,
                            "tenant_id": tenant_id,
                            "user_id": user_id,
                            "node_kind": node.node_kind,
                            "source_type": node.source_type,
                            "source_id": node.source_id,
                            "title": node.title,
                            "content": node.content,
                            "struct_data": Json(node.metadata),
                            "keywords_text": node.keywords_text,
                            "taxonomy_path": node.taxonomy_path,
                            "embedding": vec(node.embedding) if node.embedding else None,
                        },
                    )

                for edge in edges:
                    await execute(
                        conn,
                        """
                        INSERT INTO knowledge_edges (tenant_id, source_id, target_id, relation, weight, metadata)
                        VALUES (%(tenant_id)s, %(source_id)s, %(target_id)s, %(relation)s, %(weight)s, %(metadata)s)
                        ON CONFLICT (tenant_id, source_id, target_id, relation) DO UPDATE SET
                            weight = EXCLUDED.weight, metadata = EXCLUDED.metadata
                    """,
                        {
                            "tenant_id": tenant_id,
                            "source_id": edge.source_id,
                            "target_id": edge.target_id,
                            "relation": edge.relation,
                            "weight": edge.weight,
                            "metadata": edge.metadata,
                        },
                    )
