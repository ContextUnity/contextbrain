"""Postgres schema DDL for knowledge store (pgvector + ltree).

Modules:
- core: Knowledge nodes, edges, aliases, episodes, user_facts (always included)
- commerce: Dealer products, taxonomy (for e-commerce)
- news_engine: Raw news, facts, posts (for Pink Pony)
"""

from __future__ import annotations

from typing import List, Sequence


def _core_schema(vector_dim: int) -> List[str]:
    """Core Brain tables - always required."""
    return [
        # Extensions
        "CREATE EXTENSION IF NOT EXISTS vector;",
        "CREATE EXTENSION IF NOT EXISTS ltree;",
        # Nodes table
        """
        CREATE TABLE IF NOT EXISTS knowledge_nodes (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            user_id         TEXT NULL,
            node_kind       TEXT NOT NULL CHECK (node_kind IN ('chunk', 'concept')),

            source_type     TEXT NULL CHECK (source_type IN ('video','book','qa','web','knowledge')),
            source_id       TEXT NULL,
            title           TEXT NULL,
            content         TEXT NOT NULL,
            struct_data     JSONB NOT NULL DEFAULT '{}'::jsonb,
            keywords_text   TEXT NULL,

            content_hash    TEXT NULL,
            taxonomy_path   LTREE NULL,

            search_vector   TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
            keywords_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', COALESCE(keywords_text, ''))) STORED,
            embedding       VECTOR(%d) NULL,

            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        # Indexes
        """
        CREATE INDEX IF NOT EXISTS knowledge_nodes_taxonomy_path_gist
          ON knowledge_nodes USING GIST (taxonomy_path);
        """,
        """
        CREATE INDEX IF NOT EXISTS knowledge_nodes_embedding_hnsw
          ON knowledge_nodes USING hnsw (embedding vector_cosine_ops);
        """,
        """
        CREATE INDEX IF NOT EXISTS knowledge_nodes_search_vector_gin
          ON knowledge_nodes USING GIN (search_vector);
        """,
        """
        CREATE INDEX IF NOT EXISTS knowledge_nodes_keywords_vector_gin
          ON knowledge_nodes USING GIN (keywords_vector);
        """,
        "CREATE INDEX IF NOT EXISTS knowledge_nodes_source_type_idx ON knowledge_nodes (source_type);",
        "CREATE INDEX IF NOT EXISTS knowledge_nodes_source_id_idx ON knowledge_nodes (source_id);",
        "CREATE INDEX IF NOT EXISTS knowledge_nodes_node_kind_idx ON knowledge_nodes (node_kind);",
        "CREATE INDEX IF NOT EXISTS knowledge_nodes_tenant_idx ON knowledge_nodes (tenant_id);",
        """
        CREATE INDEX IF NOT EXISTS knowledge_nodes_struct_data_gin
          ON knowledge_nodes USING GIN (struct_data);
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS knowledge_nodes_chunk_content_hash_uq
          ON knowledge_nodes (node_kind, content_hash)
          WHERE node_kind = 'chunk' AND content_hash IS NOT NULL;
        """,
        # Edges table
        """
        CREATE TABLE IF NOT EXISTS knowledge_edges (
            tenant_id   TEXT NOT NULL,
            source_id   TEXT NOT NULL REFERENCES knowledge_nodes(id) ON DELETE CASCADE,
            target_id   TEXT NOT NULL REFERENCES knowledge_nodes(id) ON DELETE CASCADE,
            relation    TEXT NOT NULL,
            weight      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (tenant_id, source_id, target_id, relation)
        );
        """,
        "CREATE INDEX IF NOT EXISTS knowledge_edges_source_idx ON knowledge_edges (source_id);",
        "CREATE INDEX IF NOT EXISTS knowledge_edges_target_idx ON knowledge_edges (target_id);",
        "CREATE INDEX IF NOT EXISTS knowledge_edges_relation_idx ON knowledge_edges (relation);",
        "CREATE INDEX IF NOT EXISTS knowledge_edges_tenant_idx ON knowledge_edges (tenant_id);",
        # Aliases table
        """
        CREATE TABLE IF NOT EXISTS knowledge_aliases (
            tenant_id   TEXT NOT NULL,
            alias       TEXT NOT NULL,
            node_id     TEXT NOT NULL REFERENCES knowledge_nodes(id) ON DELETE CASCADE,
            source      TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, alias)
        );
        """,
        "CREATE INDEX IF NOT EXISTS knowledge_aliases_node_id_idx ON knowledge_aliases (node_id);",
        "CREATE INDEX IF NOT EXISTS knowledge_aliases_tenant_idx ON knowledge_aliases (tenant_id);",
        # Episodic Memory (The Journal)
        """
        CREATE TABLE IF NOT EXISTS episodic_events (
            id          UUID PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            session_id  TEXT NULL,
            content     TEXT NOT NULL,
            embedding   VECTOR(%d) NULL,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        "CREATE INDEX IF NOT EXISTS episodic_events_user_idx ON episodic_events (user_id);",
        "CREATE INDEX IF NOT EXISTS episodic_events_session_idx ON episodic_events (session_id);",
        """
        CREATE INDEX IF NOT EXISTS episodic_events_embedding_hnsw
          ON episodic_events USING hnsw (embedding vector_cosine_ops);
        """,
        # Entity Memory (User Facts / Profile)
        """
        CREATE TABLE IF NOT EXISTS user_facts (
            user_id     TEXT NOT NULL,
            fact_key    TEXT NOT NULL,
            fact_value  JSONB NOT NULL,
            confidence  DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            source_id   UUID NULL REFERENCES episodic_events(id) ON DELETE SET NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, fact_key)
        );
        """,
        "CREATE INDEX IF NOT EXISTS user_facts_user_idx ON user_facts (user_id);",
    ]


def _commerce_schema(vector_dim: int) -> List[str]:
    """Commerce/Taxonomy tables - for e-commerce integrations."""
    return [
        # Catalog Taxonomy (The Gold Standard)
        # Note: Support both singular and plural domain names for compatibility
        """
        CREATE TABLE IF NOT EXISTS catalog_taxonomy (
            tenant_id   TEXT NOT NULL,
            domain      TEXT NOT NULL CHECK (domain IN (
                'category', 'categories',
                'color', 'colors',
                'size', 'sizes',
                'gender', 'genders'
            )),
            name        TEXT NOT NULL,
            path        LTREE NOT NULL,
            keywords    TEXT[] NOT NULL DEFAULT '{}',
            embedding   VECTOR(%d) NULL,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, domain, path)
        );
        """
        % int(vector_dim),
        "CREATE INDEX IF NOT EXISTS catalog_taxonomy_path_gist ON catalog_taxonomy USING GIST (path);",
        "CREATE INDEX IF NOT EXISTS catalog_taxonomy_domain_idx ON catalog_taxonomy (domain);",
        """
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_embedding_hnsw
          ON catalog_taxonomy USING hnsw (embedding vector_cosine_ops);
        """,
    ]


def _news_engine_schema(vector_dim: int) -> List[str]:
    """NewsEngine tables - for Pink Pony news pipeline."""
    return [
        # Raw news items (direct from harvest)
        """
        CREATE TABLE IF NOT EXISTS news_raw (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            url             TEXT NOT NULL,
            headline        TEXT NOT NULL,
            summary         TEXT NOT NULL,
            category        TEXT NULL,
            source_api      TEXT NOT NULL,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            harvested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, url)
        );
        """,
        "CREATE INDEX IF NOT EXISTS news_raw_tenant_idx ON news_raw (tenant_id);",
        "CREATE INDEX IF NOT EXISTS news_raw_harvested_idx ON news_raw (harvested_at DESC);",
        "CREATE INDEX IF NOT EXISTS news_raw_category_idx ON news_raw (category);",
        # Validated facts (after archivist filter)
        """
        CREATE TABLE IF NOT EXISTS news_facts (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            url             TEXT NOT NULL,
            headline        TEXT NOT NULL,
            summary         TEXT NOT NULL,
            category        TEXT NULL,
            suggested_agent TEXT NULL,
            significance    DOUBLE PRECISION DEFAULT 0.5,
            atomic_facts    TEXT[] DEFAULT '{}',
            irony_potential TEXT NULL,
            embedding       VECTOR(%d) NULL,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            raw_id          TEXT NULL REFERENCES news_raw(id) ON DELETE SET NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, url)
        );
        """
        % int(vector_dim),
        "CREATE INDEX IF NOT EXISTS news_facts_tenant_idx ON news_facts (tenant_id);",
        "CREATE INDEX IF NOT EXISTS news_facts_created_idx ON news_facts (created_at DESC);",
        "CREATE INDEX IF NOT EXISTS news_facts_category_idx ON news_facts (category);",
        """
        CREATE INDEX IF NOT EXISTS news_facts_embedding_hnsw
          ON news_facts USING hnsw (embedding vector_cosine_ops);
        """,
        # Generated posts (ready for publish)
        """
        CREATE TABLE IF NOT EXISTS news_posts (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            fact_id         TEXT NULL REFERENCES news_facts(id) ON DELETE SET NULL,
            agent           TEXT NOT NULL,
            headline        TEXT NOT NULL,
            content         TEXT NOT NULL,
            emoji           TEXT DEFAULT '📰',
            fact_url        TEXT NULL,
            embedding       VECTOR(%d) NULL,
            scheduled_at    TIMESTAMPTZ NULL,
            published_at    TIMESTAMPTZ NULL,
            telegram_msg_id BIGINT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        "CREATE INDEX IF NOT EXISTS news_posts_tenant_idx ON news_posts (tenant_id);",
        "CREATE INDEX IF NOT EXISTS news_posts_scheduled_idx ON news_posts (scheduled_at);",
        "CREATE INDEX IF NOT EXISTS news_posts_published_idx ON news_posts (published_at);",
        """
        CREATE INDEX IF NOT EXISTS news_posts_embedding_hnsw
          ON news_posts USING hnsw (embedding vector_cosine_ops);
        """,
    ]


def build_schema_sql(
    *,
    vector_dim: int,
    include_commerce: bool = False,
    include_news_engine: bool = False,
) -> Sequence[str]:
    """Build schema SQL statements.
    
    Args:
        vector_dim: Dimension of embedding vectors:
            - 768 for all-mpnet-base-v2 (local)
            - 1536 for OpenAI text-embedding-3-small
            - 3072 for OpenAI text-embedding-3-large
        include_commerce: Include commerce/taxonomy tables
        include_news_engine: Include Pink Pony news tables
        
    Returns:
        List of SQL statements to execute
    """
    if vector_dim <= 0:
        raise ValueError("vector_dim must be positive")

    statements = _core_schema(vector_dim)
    
    if include_commerce:
        statements.extend(_commerce_schema(vector_dim))
    
    if include_news_engine:
        statements.extend(_news_engine_schema(vector_dim))
    
    return statements


__all__ = ["build_schema_sql"]
