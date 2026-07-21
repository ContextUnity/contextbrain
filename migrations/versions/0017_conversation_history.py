"""Canonicalize Conversation History and remove legacy Episode storage.

Revision ID: 0017_conversation_history
Revises: 0016_taxonomy_decommission
Create Date: 2026-07-16
"""

from collections.abc import Sequence

from alembic import op

revision = "0017_conversation_history"
down_revision = "0016_taxonomy_decommission"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Backfill fail-closed, reconcile, then remove the legacy table."""
    op.execute(
        r"""
        DO $$
        DECLARE
            source_count BIGINT := 0;
            target_count BIGINT := 0;
            source_digest TEXT := NULL;
            target_digest TEXT := NULL;
        BEGIN
            IF to_regclass('episodic_events') IS NOT NULL
               AND to_regclass('conversation_records') IS NOT NULL THEN
                RAISE EXCEPTION 'both legacy and canonical conversation tables exist'
                    USING ERRCODE = '23514';
            END IF;

            IF to_regclass('episodic_events') IS NOT NULL THEN
                IF EXISTS (
                    SELECT 1 FROM episodic_events
                    WHERE id::text !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
                       OR tenant_id IS NULL OR tenant_id = ''
                       OR user_id IS NULL OR user_id = ''
                       OR content IS NULL
                       OR jsonb_typeof(COALESCE(metadata, '{}'::jsonb)) <> 'object'
                ) THEN
                    RAISE EXCEPTION 'malformed legacy conversation row blocks migration'
                        USING ERRCODE = '23514';
                END IF;

                SELECT count(*), encode(sha256(convert_to(COALESCE(string_agg(
                    tenant_id || ':' || id::text || ':' ||
                    encode(sha256(convert_to(content, 'UTF8')), 'hex'),
                    '|' ORDER BY tenant_id, id::text
                ), ''), 'UTF8')), 'hex')
                INTO source_count, source_digest FROM episodic_events;

                CREATE TABLE conversation_records (
                    record_id UUID PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    session_id TEXT NULL,
                    role TEXT NOT NULL CHECK (role IN ('user','assistant','system','tool','legacy')),
                    kind TEXT NOT NULL CHECK (kind IN ('message','turn_summary','conversation_note','legacy_import')),
                    content TEXT NOT NULL,
                    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
                    source_hash TEXT NOT NULL CHECK (source_hash ~ '^sha256:[0-9a-f]{64}$'),
                    graph_run_id UUID NULL,
                    metadata_version INTEGER NOT NULL CHECK (metadata_version = 1),
                    idempotency_key TEXT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (tenant_id, idempotency_key)
                );

                INSERT INTO conversation_records (
                    record_id, tenant_id, user_id, session_id, role, kind,
                    content, content_hash, source_hash, graph_run_id,
                    metadata_version, idempotency_key, metadata, created_at
                )
                SELECT id::text::uuid, tenant_id, user_id, session_id,
                       COALESCE(
                           NULLIF(metadata #>> '{_conversation_migration,role}', ''),
                           'legacy'
                       ),
                       COALESCE(
                           NULLIF(metadata #>> '{_conversation_migration,kind}', ''),
                           'legacy_import'
                       ),
                       content,
                       'sha256:' || encode(sha256(convert_to(content, 'UTF8')), 'hex'),
                       CASE
                           WHEN metadata #>> '{_conversation_migration,source_hash}'
                               ~ '^sha256:[0-9a-f]{64}$'
                           THEN metadata #>> '{_conversation_migration,source_hash}'
                           WHEN metadata->>'source_hash' ~ '^sha256:[0-9a-f]{64}$'
                           THEN metadata->>'source_hash'
                           ELSE 'sha256:' || encode(
                               sha256(convert_to(content, 'UTF8')), 'hex'
                           )
                       END,
                       CASE
                           WHEN metadata #>> '{_conversation_migration,graph_run_id}'
                               ~ '^[0-9a-fA-F-]{36}$'
                           THEN (
                               metadata #>> '{_conversation_migration,graph_run_id}'
                           )::uuid
                           WHEN metadata->>'graph_run_id' ~ '^[0-9a-fA-F-]{36}$'
                           THEN (metadata->>'graph_run_id')::uuid
                           ELSE NULL
                       END,
                       COALESCE(
                           NULLIF(
                               metadata #>> '{_conversation_migration,metadata_version}',
                               ''
                           )::integer,
                           1
                       ),
                       COALESCE(NULLIF(metadata #>> '{_conversation_migration,idempotency_key}', ''),
                                'legacy:' || id::text),
                       metadata - '_conversation_migration',
                       COALESCE(created_at, now())
                FROM episodic_events;

                SELECT count(*), encode(sha256(convert_to(COALESCE(string_agg(
                    tenant_id || ':' || record_id::text || ':' ||
                    substring(content_hash from 8), '|' ORDER BY tenant_id, record_id::text
                ), ''), 'UTF8')), 'hex')
                INTO target_count, target_digest FROM conversation_records;
                IF source_count <> target_count OR source_digest <> target_digest THEN
                    RAISE EXCEPTION 'conversation migration reconciliation mismatch'
                        USING ERRCODE = '23514';
                END IF;
                CREATE TABLE IF NOT EXISTS conversation_migration_receipts (
                    migration_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    source_count BIGINT NOT NULL,
                    target_count BIGINT NOT NULL,
                    source_digest TEXT NOT NULL,
                    target_digest TEXT NOT NULL,
                    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (migration_id, tenant_id),
                    CHECK (source_count = target_count),
                    CHECK (source_digest = target_digest)
                );
                INSERT INTO conversation_migration_receipts (
                    migration_id, tenant_id, source_count, target_count,
                    source_digest, target_digest
                )
                SELECT 'contextunity.conversation-history/v1', source.tenant_id,
                       source.row_count, target.row_count,
                       source.row_digest, target.row_digest
                FROM (
                    SELECT tenant_id, count(*) AS row_count,
                           encode(sha256(convert_to(COALESCE(string_agg(
                               tenant_id || ':' || id::text || ':' ||
                               encode(sha256(convert_to(content, 'UTF8')), 'hex'),
                               '|' ORDER BY id::text), ''), 'UTF8')), 'hex') AS row_digest
                    FROM episodic_events GROUP BY tenant_id
                ) AS source
                JOIN (
                    SELECT tenant_id, count(*) AS row_count,
                           encode(sha256(convert_to(COALESCE(string_agg(
                               tenant_id || ':' || record_id::text || ':' ||
                               substring(content_hash from 8),
                               '|' ORDER BY record_id::text), ''), 'UTF8')), 'hex') AS row_digest
                    FROM conversation_records GROUP BY tenant_id
                ) AS target USING (tenant_id)
                ON CONFLICT (migration_id, tenant_id) DO UPDATE SET
                    source_count = EXCLUDED.source_count,
                    target_count = EXCLUDED.target_count,
                    source_digest = EXCLUDED.source_digest,
                    target_digest = EXCLUDED.target_digest,
                    completed_at = now();
                DROP TABLE episodic_events;
            ELSIF to_regclass('conversation_records') IS NULL THEN
                CREATE TABLE conversation_records (
                    record_id UUID PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    session_id TEXT NULL,
                    role TEXT NOT NULL CHECK (role IN ('user','assistant','system','tool','legacy')),
                    kind TEXT NOT NULL CHECK (kind IN ('message','turn_summary','conversation_note','legacy_import')),
                    content TEXT NOT NULL,
                    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
                    source_hash TEXT NOT NULL CHECK (source_hash ~ '^sha256:[0-9a-f]{64}$'),
                    graph_run_id UUID NULL,
                    metadata_version INTEGER NOT NULL CHECK (metadata_version = 1),
                    idempotency_key TEXT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (tenant_id, idempotency_key)
                );
            END IF;
        END $$;

        CREATE TABLE IF NOT EXISTS conversation_migration_receipts (
            migration_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            source_count BIGINT NOT NULL,
            target_count BIGINT NOT NULL,
            source_digest TEXT NOT NULL,
            target_digest TEXT NOT NULL,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (migration_id, tenant_id),
            CHECK (source_count = target_count),
            CHECK (source_digest = target_digest)
        );

        DROP INDEX IF EXISTS episodic_events_embedding_hnsw;
        DROP INDEX IF EXISTS episodic_events_user_idx;
        DROP INDEX IF EXISTS episodic_events_session_idx;
        DROP INDEX IF EXISTS episodic_events_tenant_idx;
        CREATE INDEX IF NOT EXISTS conversation_records_tenant_user_time_idx
            ON conversation_records (tenant_id, user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS conversation_records_tenant_session_time_idx
            ON conversation_records (tenant_id, session_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS conversation_records_tenant_run_idx
            ON conversation_records (tenant_id, graph_run_id);
        ALTER TABLE conversation_records ENABLE ROW LEVEL SECURITY;
        ALTER TABLE conversation_records FORCE ROW LEVEL SECURITY;
        DROP POLICY IF EXISTS episodic_events_tenant_isolation ON conversation_records;
        DROP POLICY IF EXISTS conversation_records_tenant_isolation ON conversation_records;
        CREATE POLICY conversation_records_tenant_isolation ON conversation_records
            USING (tenant_id = current_setting('app.current_tenant', true)
                   OR current_setting('app.current_tenant', true) = '*')
            WITH CHECK (tenant_id = current_setting('app.current_tenant', true)
                        OR current_setting('app.current_tenant', true) = '*');
        GRANT SELECT, INSERT, UPDATE, DELETE ON conversation_records TO brain_app;
        GRANT ALL ON conversation_records TO brain_admin;
        ALTER TABLE conversation_migration_receipts ENABLE ROW LEVEL SECURITY;
        ALTER TABLE conversation_migration_receipts FORCE ROW LEVEL SECURITY;
        DROP POLICY IF EXISTS conversation_migration_receipts_tenant_isolation
            ON conversation_migration_receipts;
        CREATE POLICY conversation_migration_receipts_tenant_isolation
            ON conversation_migration_receipts
            USING (tenant_id = current_setting('app.current_tenant', true)
                   OR current_setting('app.current_tenant', true) = '*')
            WITH CHECK (tenant_id = current_setting('app.current_tenant', true)
                        OR current_setting('app.current_tenant', true) = '*');
        GRANT SELECT, INSERT, UPDATE, DELETE ON conversation_migration_receipts TO brain_app;
        GRANT ALL ON conversation_migration_receipts TO brain_admin;
        """
    )


def downgrade() -> None:
    """Restore the legacy shape while retaining canonical replay metadata."""
    op.execute(
        """
        CREATE TABLE episodic_events (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            session_id TEXT NULL,
            content TEXT NOT NULL,
            embedding VECTOR(1536) NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        INSERT INTO episodic_events (
            id, tenant_id, user_id, session_id, content, metadata, created_at
        )
        SELECT record_id, tenant_id, user_id, session_id, content,
               metadata || jsonb_build_object(
                   '_conversation_migration', jsonb_build_object(
                       'source_hash', source_hash,
                       'graph_run_id', graph_run_id,
                       'idempotency_key', idempotency_key,
                       'role', role,
                       'kind', kind,
                       'metadata_version', metadata_version
                   )
               ),
               created_at
        FROM conversation_records;
        DROP TABLE conversation_records;
        DROP TABLE IF EXISTS conversation_migration_receipts;
        CREATE INDEX episodic_events_tenant_idx ON episodic_events (tenant_id);
        CREATE INDEX episodic_events_user_idx ON episodic_events (tenant_id, user_id, created_at DESC);
        CREATE INDEX episodic_events_session_idx ON episodic_events (tenant_id, session_id, created_at DESC);
        ALTER TABLE episodic_events ENABLE ROW LEVEL SECURITY;
        ALTER TABLE episodic_events FORCE ROW LEVEL SECURITY;
        CREATE POLICY episodic_events_tenant_isolation ON episodic_events
            USING (tenant_id = current_setting('app.current_tenant', true)
                   OR current_setting('app.current_tenant', true) = '*')
            WITH CHECK (tenant_id = current_setting('app.current_tenant', true)
                        OR current_setting('app.current_tenant', true) = '*');
        GRANT SELECT, INSERT, UPDATE, DELETE ON episodic_events TO brain_app;
        GRANT ALL ON episodic_events TO brain_admin;
        """
    )
