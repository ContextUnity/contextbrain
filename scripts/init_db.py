
import asyncio
import os
from psycopg_pool import AsyncConnectionPool
from dotenv import load_dotenv

# Common sql schema
SCHEMA_SQL = """
-- CREATE EXTENSION IF NOT EXISTS vector;
-- CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE IF NOT EXISTS knowledge_nodes (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    node_kind TEXT NOT NULL, -- 'chunk', 'entity', 'image', etc
    source_type TEXT,
    source_id TEXT,
    title TEXT,
    content TEXT NOT NULL,
    struct_data JSONB,
    keywords_text TEXT,
    content_hash TEXT,
    taxonomy_path ltree,
    embedding vector(768),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_kn_tenant ON knowledge_nodes (tenant_id);
CREATE INDEX IF NOT EXISTS idx_kn_path ON knowledge_nodes USING GIST (taxonomy_path);
-- Vector index (hnsw) usually added specifically if needed, omitting for brevity/speed

CREATE TABLE IF NOT EXISTS knowledge_edges (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    weight FLOAT DEFAULT 1.0,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, target_id, relation)
);

CREATE TABLE IF NOT EXISTS episodic_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT,
    content TEXT NOT NULL,
    embedding vector(768),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_facts (
    user_id TEXT NOT NULL,
    fact_key TEXT NOT NULL,
    fact_value JSONB,
    confidence FLOAT DEFAULT 1.0,
    source_id TEXT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, fact_key)
);

-- Missing table that caused the error
CREATE TABLE IF NOT EXISTS catalog_taxonomy (
    tenant_id TEXT NOT NULL,
    domain TEXT NOT NULL, -- 'categories', 'colors', etc
    path ltree NOT NULL,  -- e.g. 'sport.shoes'
    name TEXT NOT NULL,
    keywords TEXT[],
    metadata JSONB,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (tenant_id, domain, path)
);

CREATE INDEX IF NOT EXISTS idx_tax_domain ON catalog_taxonomy (tenant_id, domain);
"""

async def main():
    load_dotenv()
    dsn = os.getenv("BRAIN_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        print("Error: DATABASE_URL not found in environment.")
        return

    print(f"Connecting to {dsn.split('@')[-1]}...")
    async with AsyncConnectionPool(dsn, open=False) as pool:
        await pool.open()
        async with pool.connection() as conn:
            print("Creating tables...")
            await conn.execute(SCHEMA_SQL)
            print("Tables created successfully.")

if __name__ == "__main__":
    asyncio.run(main())
