"""Taxonomy storage (SQLite implementation).

Contract-compatible with ``postgres/store/taxonomy.py``.
"""

from __future__ import annotations

import sqlite3

from contextunity.core.narrowing import as_str_list
from contextunity.core.types import JsonDict, JsonValue

from .codecs import json_dict_field, json_dumps, json_loads, row_to_dict
from .connection import SqliteConnectionMixin


class TaxonomyMixin(SqliteConnectionMixin):
    """SQLite taxonomy operations matching Postgres contract."""

    async def upsert_taxonomy(
        self,
        *,
        tenant_id: str,
        domain: str,
        name: str,
        path: str,
        keywords: list[str],
        metadata: JsonDict | None = None,
    ) -> None:
        """Upsert a taxonomy node."""
        with self._get_connection() as db:
            _ = db.execute(
                """
                INSERT INTO catalog_taxonomy
                    (tenant_id, domain, name, path, keywords, metadata, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT (tenant_id, domain, path) DO UPDATE SET
                    name = excluded.name,
                    keywords = excluded.keywords,
                    metadata = excluded.metadata,
                    updated_at = datetime('now')
                """,
                (
                    tenant_id,
                    domain,
                    name,
                    path,
                    json_dumps(keywords),
                    json_dumps(metadata or {}),
                ),
            )
            db.commit()

    async def get_all_taxonomy(
        self, *, tenant_id: str, domain: str | None = None
    ) -> list[JsonDict]:
        """Get all taxonomy nodes, optionally filtered by domain."""
        query = "SELECT * FROM catalog_taxonomy WHERE tenant_id = ?"
        params: list[object] = [tenant_id]

        if domain:
            query += " AND domain = ?"
            params.append(domain)

        with self._get_connection() as db:
            cursor = db.execute(query, params)
            rows: list[sqlite3.Row] = list(cursor.fetchall())

        results: list[JsonDict] = []
        for r in rows:
            d = row_to_dict(r)
            keywords_raw = d.get("keywords")
            kw_parsed = json_loads(keywords_raw if isinstance(keywords_raw, str) else None)
            keywords: list[JsonValue] = []
            for s in as_str_list(kw_parsed):
                keywords.append(s)
            d["keywords"] = keywords
            d["metadata"] = json_dict_field(d.get("metadata"))
            results.append(d)
        return results


__all__ = ["TaxonomyMixin"]
