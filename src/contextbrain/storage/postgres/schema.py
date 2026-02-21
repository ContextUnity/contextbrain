"""Postgres schema DDL for knowledge store (pgvector + ltree).

Modules:
- core: Knowledge nodes, edges, aliases, episodes, user_facts (always included)
- commerce: Dealer products, taxonomy (for e-commerce)
- news_engine: Raw news, facts, posts (for news pipeline)
"""

from __future__ import annotations

from typing import List, Sequence


def _extension_statements() -> List[str]:
    """Extensions required by Brain — need superuser privileges.

    These are separated from table DDL because they may require
    elevated privileges. If they fail, ensure_schema logs a clear
    message instead of aborting all table creation.
    """
    return [
        "CREATE EXTENSION IF NOT EXISTS vector;",
        "CREATE EXTENSION IF NOT EXISTS ltree;",
    ]


def _core_schema(vector_dim: int) -> List[str]:
    """Core Brain tables - always required."""
    return [
        # Nodes table
        """
        CREATE TABLE IF NOT EXISTS knowledge_nodes (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            user_id         TEXT NULL,
            node_kind       TEXT NOT NULL CHECK (node_kind IN ('chunk', 'concept')),

            source_type     TEXT NULL CHECK (source_type IN ('video','book','qa','web','knowledge','documentation')),
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
        "CREATE INDEX IF NOT EXISTS episodic_events_tenant_idx ON episodic_events (tenant_id);",
        """
        CREATE INDEX IF NOT EXISTS episodic_events_embedding_hnsw
          ON episodic_events USING hnsw (embedding vector_cosine_ops);
        """,
        # Entity Memory (User Facts / Profile)
        """
        CREATE TABLE IF NOT EXISTS user_facts (
            tenant_id   TEXT NOT NULL DEFAULT 'default',
            user_id     TEXT NOT NULL,
            fact_key    TEXT NOT NULL,
            fact_value  JSONB NOT NULL,
            confidence  DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            source_id   UUID NULL REFERENCES episodic_events(id) ON DELETE SET NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, user_id, fact_key)
        );
        """,
        "CREATE INDEX IF NOT EXISTS user_facts_user_idx ON user_facts (user_id);",
        "CREATE INDEX IF NOT EXISTS user_facts_tenant_idx ON user_facts (tenant_id);",
        # Agent Traces (Observability)
        """
        CREATE TABLE IF NOT EXISTS agent_traces (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id       TEXT NOT NULL,
            agent_id        TEXT NOT NULL,
            session_id      TEXT NULL,
            user_id         TEXT NULL,
            graph_name      TEXT NULL,
            tool_calls      JSONB NOT NULL DEFAULT '[]'::jsonb,
            token_usage     JSONB NOT NULL DEFAULT '{}'::jsonb,
            timing_ms       INTEGER NULL,
            security_flags  JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            provenance      TEXT[] NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS agent_traces_tenant_idx ON agent_traces (tenant_id);",
        "CREATE INDEX IF NOT EXISTS agent_traces_agent_idx ON agent_traces (agent_id);",
        "CREATE INDEX IF NOT EXISTS agent_traces_session_idx ON agent_traces (session_id);",
        "CREATE INDEX IF NOT EXISTS agent_traces_created_idx ON agent_traces (created_at DESC);",
        "CREATE INDEX IF NOT EXISTS agent_traces_tenant_created_idx ON agent_traces (tenant_id, created_at DESC);",
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
    """NewsEngine tables - for news pipeline news pipeline."""
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
        # Unique constraint for deduplication by fact_url
        "ALTER TABLE news_posts ADD CONSTRAINT IF NOT EXISTS news_posts_tenant_fact_url_uq UNIQUE (tenant_id, fact_url);",
        """
        CREATE INDEX IF NOT EXISTS news_posts_embedding_hnsw
          ON news_posts USING hnsw (embedding vector_cosine_ops);
        """,
    ]


def _column_backfill() -> List[str]:
    """Idempotent column additions for tables that already exist.

    Problem: ``CREATE TABLE IF NOT EXISTS`` skips the entire statement when
    the table exists, so columns added in later code versions never appear.

    Solution: each entry here is an ``ALTER TABLE … ADD COLUMN IF NOT EXISTS``
    that runs on every startup. PostgreSQL executes it as a no-op when the
    column is already present.

    When you add a new column to a CREATE TABLE, also add the matching
    ALTER TABLE here so existing deployments pick it up automatically.
    """
    return [
        # agent_traces — provenance tracking (added after initial trace system)
        "ALTER TABLE agent_traces ADD COLUMN IF NOT EXISTS provenance TEXT[] NULL;",
        # user_facts — tenant isolation (changed PK from (user_id, fact_key)
        # to (tenant_id, user_id, fact_key); the column default handles existing rows)
        "ALTER TABLE user_facts ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';",
    ]


def _constraint_upgrades() -> List[str]:
    """Idempotent constraint updates for evolved schemas.

    CHECK constraints and other DDL that may need updating on existing tables.
    Each statement is wrapped to be safe on re-run.
    """
    return [
        # knowledge_nodes — added 'documentation' to source_type enum
        # DROP + re-ADD is idempotent:  IF EXISTS prevents error on first run
        "ALTER TABLE knowledge_nodes DROP CONSTRAINT IF EXISTS knowledge_nodes_source_type_check;",
        """ALTER TABLE knowledge_nodes ADD CONSTRAINT knowledge_nodes_source_type_check
           CHECK (source_type IN ('video','book','qa','web','knowledge','documentation'));""",
    ]


def _rls_policies() -> List[str]:
    """Row-Level Security policies for tenant isolation.

    Defence-in-depth (Layer 2): even if application-level tenant checks
    are bypassed, PostgreSQL itself blocks cross-tenant data access.

    Architecture:
        - ``brain_app`` role: used by gRPC handlers, RLS enforced
        - ``brain_admin`` role: used by ContextView dashboard, bypasses RLS
        - Every query sets ``SET LOCAL app.current_tenant = '<tenant_id>'``
          before executing — this is done in the Brain gRPC interceptor.

    All statements are idempotent (IF NOT EXISTS / OR REPLACE / DROP IF EXISTS).
    """
    # All tables that have tenant_id column
    tenant_tables = [
        "knowledge_nodes",
        "knowledge_edges",
        "knowledge_aliases",
        "episodic_events",
        "user_facts",
        "agent_traces",
        "catalog_taxonomy",
        "news_raw",
        "news_facts",
        "news_posts",
    ]

    stmts: list[str] = []

    # 1. Create app role (non-superuser, no BYPASSRLS)
    stmts.append("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_app') THEN
                CREATE ROLE brain_app NOLOGIN;
                RAISE NOTICE 'Created role brain_app';
            END IF;
        END
        $$;
    """)

    # 2. Create admin role (bypasses RLS for ContextView dashboard)
    stmts.append("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_admin') THEN
                CREATE ROLE brain_admin NOLOGIN BYPASSRLS;
                RAISE NOTICE 'Created role brain_admin';
            END IF;
        END
        $$;
    """)

    # 3. Enable RLS and create policies for each tenant table
    for table in tenant_tables:
        policy_name = f"{table}_tenant_isolation"

        # Enable RLS (idempotent — no-op if already enabled)
        stmts.append(f"ALTER TABLE IF EXISTS {table} ENABLE ROW LEVEL SECURITY;")

        # Force RLS even for table owner (important!)
        stmts.append(f"ALTER TABLE IF EXISTS {table} FORCE ROW LEVEL SECURITY;")

        # Drop old policy (idempotent) then create
        stmts.append(f"DROP POLICY IF EXISTS {policy_name} ON {table};")

        # Policy: rows visible only when tenant_id matches session variable
        # current_setting('app.current_tenant', true) returns NULL if not set,
        # which means NO rows visible (fail-closed).
        stmts.append(f"""
            CREATE POLICY {policy_name} ON {table}
                USING (
                    tenant_id = current_setting('app.current_tenant', true)
                    OR current_setting('app.current_tenant', true) = '*'
                )
                WITH CHECK (
                    tenant_id = current_setting('app.current_tenant', true)
                );
        """)

        # Grant table access to brain_app role
        stmts.append(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO brain_app;")

        # Grant full access to brain_admin (bypasses RLS via BYPASSRLS flag)
        stmts.append(f"GRANT ALL ON {table} TO brain_admin;")

    return stmts


def build_rls_sql() -> Sequence[str]:
    """Return RLS policy statements for tenant isolation.

    Called as a separate step in ensure_schema AFTER table creation.
    Requires the connection user to be a superuser or table owner.
    """
    return _rls_policies()


def build_extension_sql() -> Sequence[str]:
    """Return CREATE EXTENSION statements (require superuser)."""
    return _extension_statements()


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
        include_news_engine: Include news pipeline news tables

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


def build_column_backfill_sql() -> Sequence[str]:
    """Return ALTER TABLE statements that add columns missing from existing tables.

    Called as a separate step in ensure_schema AFTER the main DDL.
    Idempotent — safe to run on every startup.
    """
    stmts: list[str] = []
    stmts.extend(_column_backfill())
    stmts.extend(_constraint_upgrades())
    return stmts


__all__ = ["build_extension_sql", "build_schema_sql", "build_column_backfill_sql", "build_rls_sql"]
