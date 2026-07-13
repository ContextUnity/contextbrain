"""Composed SQLite-Vec storage backend for local Brain.
Assembles domain mixins into a single class implementing
``BrainStorageProtocol``.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.braincell_identity import source_owned_content_hash
from contextunity.core.types import JsonDict

from contextunity.brain.cell_confidence import cap_confidence
from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION

from ..user_facts_guard import guard_and_drop_sqlite_user_facts
from .blackboard import BlackboardMixin
from .connection import SqliteConnectionMixin
from .embedding_jobs import EmbeddingJobsMixin
from .episodes import EpisodesMixin
from .graph import GraphMixin
from .schema import SCHEMA_VERSION, apply_preflight_renames, build_core_ddl, build_vector_ddl
from .search import SearchMixin
from .synapses import SynapsesMixin
from .taxonomy import TaxonomyMixin
from .traces import TracesMixin

logger = get_contextunit_logger(__name__)


class SqliteBrainStore(
    GraphMixin,
    EpisodesMixin,
    TracesMixin,
    TaxonomyMixin,
    SearchMixin,
    BlackboardMixin,
    SynapsesMixin,
    EmbeddingJobsMixin,
    SqliteConnectionMixin,
):
    """Local SQLite backend for Brain Service.

    Mirrors ``PostgresBrainStore`` mixin composition.
    Uses sqlite-vec for vector similarity when available.
    """

    db_path: Path
    vector_dim: int

    def __init__(
        self,
        db_path: str = "~/.contextunity/brain_local.sqlite3",
        vector_dim: int = DEFAULT_EMBEDDING_DIMENSION,
    ):
        """Initialize a new instance of SqliteBrainStore.

        Args:
            db_path (str): The db path parameter.
            vector_dim (int): The vector dim parameter.
        """
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.vector_dim = vector_dim
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        if not self.has_sqlite_vec():
            logger.warning(
                (
                    "sqlite-vec not installed — vector search disabled. "
                    "Install with: pip install sqlite-vec"
                )
            )

        with self._get_connection() as db:
            apply_preflight_renames(db)
            guard_and_drop_sqlite_user_facts(db)

            for stmt in build_core_ddl():
                _ = db.execute(stmt)

            if self.has_sqlite_vec():
                for stmt in build_vector_ddl(self.vector_dim):
                    try:
                        _ = db.execute(stmt)
                    except sqlite3.OperationalError as e:
                        logger.debug("Vector table init: %s", e)

            # Track schema version
            _ = db.execute(
                "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            db.commit()

        logger.info(
            "SQLite backend ready: %s (schema_v%d, vec=%s)",
            self.db_path,
            SCHEMA_VERSION,
            self.has_sqlite_vec(),
        )

    async def ensure_schema(
        self,
        *,
        include_commerce: bool = False,
        vector_dim: int = DEFAULT_EMBEDDING_DIMENSION,
    ) -> None:
        """Schema is ensured on ``__init__``."""
        _ = include_commerce, vector_dim
        logger.info("SQLite schema already initialized at %s", self.db_path)

    async def close(self) -> None:
        """No-op — SQLite connections are per-call."""
        pass

    async def upsert_cell(
        self,
        *,
        tenant_id: str,
        cell_kind: str,
        content: str,
        metadata: JsonDict | None = None,
        cell_id: str | None = None,
        user_id: str | None = None,
        scope_path: str | None = None,
        content_hash: str | None = None,
        source_type: str = "manual",
        source_ref: str | None = None,
        confidence: float = 0.5,
        visibility: str = "tenant",
    ) -> JsonDict:
        """Upsert BrainCell. Idempotent on content_hash if provided."""
        conn = self._get_connection()
        cur = conn.cursor()
        capped = cap_confidence(source_type, confidence)
        meta = dict(metadata or {})
        meta.update(
            {
                "cell_kind": cell_kind,
                "confidence": capped,
                "visibility": visibility,
            }
        )
        if source_ref is not None:
            meta["source_ref"] = source_ref
        if content_hash is None:
            content_hash = source_owned_content_hash(
                producer=source_type,
                tenant_id=tenant_id,
                user_id=user_id,
                cell_kind=cell_kind,
                content=content,
            )
        if cell_id is None:
            cell_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{tenant_id}:{content_hash}"))
        cur.execute(
            """
            INSERT INTO cells (id, tenant_id, user_id, cell_kind, source_type, source_id,
                               source_ref, content, struct_data, scope_path, content_hash,
                               confidence, visibility)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                content = excluded.content,
                struct_data = excluded.struct_data,
                scope_path = excluded.scope_path,
                content_hash = excluded.content_hash,
                source_id = excluded.source_id,
                source_ref = excluded.source_ref,
                source_type = excluded.source_type,
                confidence = excluded.confidence,
                visibility = excluded.visibility,
                updated_at = datetime('now')
            RETURNING id, tenant_id, cell_kind, source_type,
                      scope_path, content_hash, confidence, visibility, created_at, updated_at
            """,
            (
                cell_id,
                tenant_id,
                user_id,
                cell_kind,
                source_type,
                source_ref,
                source_ref,
                content,
                json.dumps(meta),
                scope_path,
                content_hash,
                capped,
                visibility,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return JsonDict(
            dict(row)
            if row
            else {
                "id": cell_id,
                "tenant_id": tenant_id,
                "cell_kind": cell_kind,
                "content_hash": content_hash,
            }
        )

    async def query_cells(
        self,
        *,
        tenant_id: str,
        query_text: str | None = None,
        cell_kind: str | None = None,
        source_type: str | None = None,
        scope_path: str | None = None,
        metadata_filter: JsonDict | None = None,
        limit: int = 10,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[JsonDict]:
        conn = self._get_connection()
        cur = conn.cursor()
        sql = """
            SELECT id, tenant_id, cell_kind, content, struct_data as metadata,
                   content_hash, scope_path, source_type,
                   COALESCE(source_ref, source_id) as source_ref,
                   confidence, visibility
            FROM cells WHERE tenant_id = ?
        """
        params: list[object] = [tenant_id]
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        if cell_kind:
            sql += " AND cell_kind = ?"
            params.append(cell_kind)
        if source_type:
            sql += " AND source_type = ?"
            params.append(source_type)
        if scope_path:
            sql += " AND scope_path = ?"
            params.append(scope_path)
        if query_text:
            sql += " AND content LIKE ?"
            params.append(f"%{query_text}%")
        if metadata_filter:
            import json as _json

            for key, value in metadata_filter.items():
                sql += " AND json_extract(struct_data, ?) = json_extract(?, '$')"
                params.extend((f'$."{key}"', _json.dumps(value)))
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend((limit, offset))
        cur.execute(sql, params)
        rows = cur.fetchall()
        results: list[JsonDict] = []
        import json as _json

        for row in rows:
            d = dict(row)
            meta = d.get("metadata")
            if isinstance(meta, str):
                try:
                    d["metadata"] = _json.loads(meta)
                except Exception:
                    d["metadata"] = {}
            results.append(JsonDict(d))
        return results

    async def get_cell(
        self, *, tenant_id: str, cell_id: str, user_id: str | None = None
    ) -> JsonDict | None:
        conn = self._get_connection()
        cur = conn.cursor()
        sql = """
            SELECT id, tenant_id, cell_kind, content, struct_data as metadata,
                   content_hash, source_type, COALESCE(source_ref, source_id) as source_ref,
                   scope_path, confidence, visibility, created_at, updated_at
            FROM cells WHERE tenant_id = ? AND id = ?
        """
        params: list[object] = [tenant_id, cell_id]
        if user_id:
            sql += " AND (user_id = ? OR user_id IS NULL)"
            params.append(user_id)
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        d = dict(row)
        m = d.get("metadata")
        if isinstance(m, str):
            import json as _json

            try:
                d["metadata"] = _json.loads(m)
            except Exception:
                d["metadata"] = {}
        return JsonDict(d)


__all__ = ["SqliteBrainStore"]
