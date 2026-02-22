# ContextBrain

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![GitHub](https://img.shields.io/badge/GitHub-ContextUnity-black.svg)](https://github.com/ContextUnity/contextbrain)
[![Docs](https://img.shields.io/badge/docs-contextbrain.dev-green.svg)](https://contextbrain.dev)

> ⚠️ **Early Version**: This is an early version of ContextBrain. Documentation is actively being developed, and the API may change.

## What is ContextBrain?

ContextBrain is the **Knowledge Storage and RAG Service** of the [ContextUnity](https://github.com/ContextUnity) ecosystem. It provides:

- **Vector storage** with PostgreSQL + pgvector
- **Semantic search** with hybrid retrieval (vector + full-text)
- **Knowledge Graph** with ltree-based taxonomy
- **Episodic memory** for conversation history
- **gRPC API** for integration with other ContextUnity services

It acts as a **centralized memory backend** that [ContextRouter](https://github.com/ContextUnity/contextrouter) and other services use for retrieval and knowledge management.

## What is it for?

ContextBrain is designed for:

- **RAG backends** — store and retrieve knowledge for LLM applications
- **Product catalogs** — taxonomy, enrichment, and semantic search
- **Memory systems** — episodic and entity-based memory for AI agents
- **News aggregation** — fact storage and deduplication

### Typical use cases:
- Knowledge base backend for chatbots
- Product enrichment and classification
- Semantic search over documents
- Multi-tenant knowledge storage

## Key Features

- **🗄️ Multi-Backend Storage** — PostgreSQL with pgvector (primary), Vertex AI Search, DuckDB for testing
- **🔍 Hybrid Search** — combines vector similarity with full-text search and reranking
- **🌳 Taxonomy & Ontology** — ltree-based hierarchical classification with AI-powered categorization
- **🧠 Memory Types** — semantic (knowledge), episodic (conversations), entity (facts)
- **📡 gRPC Service** — production-ready service with streaming support
- **� Multi-Tenant** — tenant isolation with ContextToken authorization

## Architecture

```
ContextBrain/
├── service/                    # gRPC service (modular)
│   ├── server.py               # Server setup
│   ├── brain_service.py        # Main service class
│   ├── commerce_service.py     # Commerce operations
│   ├── embedders.py            # Embedding providers
│   └── handlers/               # Domain-specific handlers
│       ├── knowledge.py        # Knowledge management
│       ├── memory.py           # Episodic memory
│       ├── taxonomy.py         # Taxonomy operations
│       ├── commerce.py         # Commerce handlers
│       └── news.py             # News engine handlers
├── storage/
│   ├── postgres/               # PostgreSQL + pgvector (primary)
│   │   ├── store/              # Modular store (mixin pattern)
│   │   │   ├── base.py         # Base connection handling
│   │   │   ├── search.py       # Vector search operations
│   │   │   ├── graph.py        # Graph CRUD operations
│   │   │   ├── episodes.py     # Episodic memory
│   │   │   └── taxonomy.py     # Taxonomy operations
│   │   ├── news.py             # News post storage
│   │   └── schema.py           # Database schema
│   └── duckdb_store.py         # Testing backend
├── payloads.py                 # Pydantic validation models
├── ingestion/
│   └── rag/                    # RAG pipeline, processors
└── core/                       # Config, registry, interfaces
```

## gRPC API

ContextBrain exposes its functionality via [gRPC](https://grpc.io/) — a high-performance RPC framework. The protocol definitions (`.proto` files) are defined in [ContextCore](https://github.com/ContextUnity/contextcore), the shared kernel of the ContextUnity ecosystem. This ensures type-safe communication between all services.

BrainService provides these operations:

| Method | Description |
|--------|-------------|
| `QueryMemory` | Hybrid search (vector + text) for knowledge retrieval |
| `Upsert` | Store knowledge with embeddings |
| `AddEpisode` | Add conversation turn to episodic memory |
| `UpsertFact` | Store entity facts (user preferences, etc.) |
| `UpsertTaxonomy` | Sync taxonomy entries |
| `GetTaxonomy` | Export taxonomy for a domain |
| `GetProducts` | Get products for enrichment |
| `UpdateEnrichment` | Update product enrichment data |
| `CreateKGRelation` | Create Knowledge Graph relations |
| `UpsertNewsItem` | Store news facts |
| `GetNewsItems` | Retrieve news by criteria |
| `UpsertNewsPost` | Store generated posts |

## Quick Start

### As Python Library

```python
from contextbrain.storage.postgres import PostgresKnowledgeStore
import asyncio

async def main():
    store = PostgresKnowledgeStore(dsn="postgres://...")
    await store.connect()
    
    # Store knowledge
    await store.upsert_knowledge(
        tenant_id="my_app",
        content="PostgreSQL is a relational database...",
        source_type="document",
        embedding=[0.1, 0.2, ...],  # 1536 dims (OpenAI) or 768 (local)
    )
    
    # Semantic search
    results = await store.search(
        tenant_id="my_app",
        query_embedding=[0.1, 0.2, ...],
        limit=10,
    )

asyncio.run(main())
```

### As gRPC Service

```python
from contextcore import brain_pb2, brain_pb2_grpc, create_channel_sync

channel = create_channel_sync("localhost:50051")
stub = brain_pb2_grpc.BrainServiceStub(channel)

# Query memory
response = stub.QueryMemory(brain_pb2.QueryMemoryRequest(
    tenant_id="my_app",
    query="How does PostgreSQL work?",
    top_k=5,
))
for result in response.results:
    print(result.content)
```

## Installation

```bash
pip install contextbrain

# With PostgreSQL support (recommended):
pip install contextbrain[storage]

# With Vertex AI support:
pip install contextbrain[vertex]
```

## Configuration

```bash
# Required
export BRAIN_DATABASE_URL="postgres://user:pass@localhost:5432/brain"  # or DATABASE_URL

# Server Configuration
export BRAIN_PORT=50051
export BRAIN_SCHEMA="brain"
export BRAIN_TENANTS="tenant1,tenant2"   # Comma-separated list of allowed tenants
export BRAIN_NEWS_ENGINE=true            # Enable news engine tables

# Embeddings (choose one)
export EMBEDDER_TYPE="openai"            # OpenAI text-embedding-3-small (1536 dims)
export EMBEDDER_TYPE="local"             # Local SentenceTransformers (768 dims)
# If not set: auto-selects OpenAI if OPENAI_API_KEY exists, otherwise local

export PGVECTOR_DIM=1536                 # Must match embedder! (1536 for OpenAI, 768 for local)
export OPENAI_API_KEY="sk-..."           # Required for OpenAI embeddings

# Optional: Custom OpenAI model
export OPENAI_EMBEDDING_MODEL="text-embedding-3-large"  # 3072 dims
```

:::note
Database schema must match embedding dimensions (1536 for OpenAI, 768 for local).
Run `uv run alembic upgrade head` after changing embedding provider.
:::

## Development

### Prerequisites
- Python 3.13+
- PostgreSQL 16+ with `vector` and `ltree` extensions
- `uv` package manager

### Database Setup

```bash
# Create database
createdb brain

# Enable extensions
psql brain -c "CREATE EXTENSION IF NOT EXISTS vector;"
psql brain -c "CREATE EXTENSION IF NOT EXISTS ltree;"

# Initialize schema
uv run python scripts/init_db.py
```

### Running the Service

```bash
# Start gRPC server on :50051
uv run python -m contextbrain
```

### Running Tests

```bash
uv run pytest tests/ -v
```

## Documentation

- [Full Documentation](https://contextbrain.dev) — complete guides and API reference
- [Technical Reference](./contextbrain-fulldoc.md) — architecture deep-dive
- [Proto Definitions](../contextcore/protos/brain.proto) — gRPC contract

## Testing & docs

- [Integration tests](../tests/integration/README.md) — cross-service tests (token/trace propagation, etc.)
- Doc site: [contextbrain.dev](https://contextbrain.dev)

## Security

ContextBrain enforces multi-layer tenant isolation.
See [Security Architecture](../../docs/security_architecture.md) for the full model.

### Token Verification

Every gRPC call is verified via `TokenValidationInterceptor`:
- Signature verification (Ed25519 / UnsignedBackend)
- `token.allowed_tenants` must include the requested `tenant_id`
- `BrainPermissionInterceptor` enforces domain-level permissions (`brain:read`, `brain:write`)

### Database-Level Isolation (RLS)

PostgreSQL Row-Level Security ensures that even if application-level checks are bypassed,
data cannot leak between tenants:

```sql
-- Every query is scoped by:
SET LOCAL app.current_tenant = '{tenant_id}';
SET LOCAL app.current_user = '{user_id}';

-- RLS policy on all 10 tenant tables:
USING (tenant_id = current_setting('app.current_tenant', true))

-- Plus, Dual-Dimensional RLS for Personal Data (e.g. episodic_events, knowledge_nodes):
USING (... AND (user_id IS NULL OR user_id = current_setting('app.current_user', true)))
```

- `brain_app` role: RLS enforced (used by service)
- `brain_admin` role: BYPASSRLS (used by ContextView dashboard)
- Wildcard `'*'` for admin access (ContextView) or bypassing user isolation

### Storage Layer

`tenant_connection()` context manager:
- Sets `app.current_tenant` and `app.current_user` on every connection from the pool
- Fails closed — empty `tenant_id` raises `ValueError`
- All store mixins use this for every database operation

## ContextUnity Ecosystem

ContextBrain is the semantic memory layer of the [ContextUnity](https://contextunity.dev) service mesh:

| Service | Role | Documentation |
|---|---|---|
| [ContextCore](https://contextcore.dev) | Shared kernel — types, protocols, contracts | [contextcore.dev](https://contextcore.dev) |
| **ContextBrain** | Semantic memory — knowledge & vector storage | *you are here* |
| [ContextRouter](https://contextrouter.dev) | Agent orchestration — LangGraph + plugins | [contextrouter.dev](https://contextrouter.dev) |
| [ContextWorker](https://contextworker.dev) | Durable workflows — Temporal infrastructure | [contextworker.dev](https://contextworker.dev) |
| ContextZero | Privacy proxy — PII anonymization | — |
| ContextView | Observability dashboard — admin UI, MCP | — |

## License

This project is licensed under the terms specified in [LICENSE.md](LICENSE.md).
