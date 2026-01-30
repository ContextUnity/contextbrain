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
├── service.py          # gRPC service implementation
├── storage/
│   ├── postgres/       # PostgreSQL + pgvector (primary)
│   ├── vertex.py       # Vertex AI Search integration
│   └── duckdb_store.py # Testing backend
├── ingestion/
│   └── rag/            # RAG pipeline, processors, plugins
└── core/               # Config, registry, interfaces
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
import grpc
from contextcore import brain_pb2, brain_pb2_grpc

channel = grpc.insecure_channel("localhost:50051")
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
export BRAIN_DATABASE_URL="postgres://user:pass@localhost:5432/brain"

# Embeddings (choose one)
export EMBEDDER_TYPE="openai"            # OpenAI text-embedding-3-small (1536 dims)
export EMBEDDER_TYPE="local"             # Local SentenceTransformers (768 dims)
# If not set: auto-selects OpenAI if OPENAI_API_KEY exists, otherwise local

export OPENAI_API_KEY="sk-..."           # Required for OpenAI embeddings

# Optional: Custom OpenAI model
export OPENAI_EMBEDDING_MODEL="text-embedding-3-large"  # 3072 dims

# Optional: Vertex AI
export VERTEX_PROJECT_ID="my-project"
export VERTEX_LOCATION="us-central1"
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

## ContextUnity Ecosystem

ContextBrain is part of the [ContextUnity](https://github.com/ContextUnity) platform:

| Service | Role | Documentation |
|---------|------|---------------|
| **ContextCore** | Shared types and gRPC contracts | [contextcore.dev](https://contextcore.dev) |
| **ContextRouter** | AI agent orchestration | [contextrouter.dev](https://contextrouter.dev) |
| **ContextWorker** | Background task execution | [contextworker.dev](https://contextworker.dev) |

## License

This project is licensed under the terms specified in [LICENSE.md](LICENSE.md).
