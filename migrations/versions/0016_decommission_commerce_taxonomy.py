"""Remove Commerce-owned taxonomy and review storage from Brain.

Revision ID: 0016_taxonomy_decommission
Revises: 0015_execution_traces
Create Date: 2026-07-16
"""

from collections.abc import Sequence

from alembic import op

revision = "0016_taxonomy_decommission"
down_revision = "0015_execution_traces"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop legacy domain tables after Commerce reconciliation and cutover."""
    op.execute(
        """
        DO $$
        DECLARE
            has_rows BOOLEAN;
        BEGIN
            IF to_regclass('catalog_taxonomy') IS NOT NULL THEN
                EXECUTE 'SELECT EXISTS (SELECT 1 FROM catalog_taxonomy LIMIT 1)'
                    INTO has_rows;
                IF has_rows THEN
                    RAISE EXCEPTION
                        'legacy catalog_taxonomy contains data; Commerce migration required'
                        USING ERRCODE = '23514';
                END IF;
            END IF;

            IF to_regclass('gardener_pending') IS NOT NULL THEN
                EXECUTE 'SELECT EXISTS (SELECT 1 FROM gardener_pending LIMIT 1)'
                    INTO has_rows;
                IF has_rows THEN
                    RAISE EXCEPTION
                        'legacy gardener_pending contains data; Commerce migration required'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
        END $$;

        DROP TABLE IF EXISTS catalog_taxonomy;
        DROP TABLE IF EXISTS gardener_pending;
        """
    )


def downgrade() -> None:
    """Restore the legacy table shapes without recreating removed data."""
    op.execute(
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
            embedding   VECTOR(1536) NULL,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, domain, path)
        );
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_path_gist
            ON catalog_taxonomy USING GIST (path);
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_domain_idx
            ON catalog_taxonomy (domain);
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_embedding_hnsw
            ON catalog_taxonomy USING hnsw (embedding vector_cosine_ops);
        ALTER TABLE catalog_taxonomy ENABLE ROW LEVEL SECURITY;
        ALTER TABLE catalog_taxonomy FORCE ROW LEVEL SECURITY;
        CREATE POLICY catalog_taxonomy_tenant_isolation ON catalog_taxonomy
            USING (
                tenant_id = current_setting('app.current_tenant', true)
                OR current_setting('app.current_tenant', true) = '*'
            )
            WITH CHECK (
                tenant_id = current_setting('app.current_tenant', true)
                OR current_setting('app.current_tenant', true) = '*'
            );
        GRANT SELECT, INSERT, UPDATE, DELETE ON catalog_taxonomy TO brain_app;
        GRANT ALL ON catalog_taxonomy TO brain_admin;

        CREATE TABLE IF NOT EXISTS gardener_pending (
            id          SERIAL PRIMARY KEY,
            item_type   VARCHAR(50) NOT NULL,
            raw_value   TEXT NOT NULL,
            context     JSONB DEFAULT '{}'::jsonb,
            proposal    TEXT NULL,
            status      VARCHAR(20) DEFAULT 'pending',
            created_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            updated_at  TIMESTAMP WITHOUT TIME ZONE NULL
        );
        """
    )
