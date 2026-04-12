# ContextBrain

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)

ContextBrain is the **Knowledge Storage & RAG Service** of the [ContextUnity](https://github.com/ContextUnity) ecosystem. It provides a centralized memory backend using PostgreSQL + pgvector for vector search, knowledge graphs, taxonomy hierarchies, and episodic agent memory.

---

## What is it for?

- **RAG backends** — store and retrieve knowledge for LLM applications
- **Product catalogs** — taxonomy, enrichment, and semantic search
- **Memory systems** — episodic and entity-based memory for AI agents
- **News aggregation** — fact storage and deduplication

---

## Quick Start

### Database Setup

```bash
createdb brain
psql brain -c "CREATE EXTENSION IF NOT EXISTS vector;"
psql brain -c "CREATE EXTENSION IF NOT EXISTS ltree;"
uv run alembic upgrade head
```

### Running the Service

```bash
export BRAIN_DATABASE_URL="postgres://user:pass@localhost:5432/brain"
export EMBEDDER_TYPE="openai"
export OPENAI_API_KEY="sk-..."
uv run python -m contextunity.brain
```

### Running Tests

```bash
uv run --package contextunity-brain pytest
```

---

## Architecture

```
src/contextunity/brain/
├── service/                    # gRPC service handlers & server
│   ├── handlers/               # Domain-specific: knowledge, memory, taxonomy, commerce, news, traces
│   └── interceptors.py         # BrainPermissionInterceptor (RLS enforcement)
├── storage/postgres/           # PostgreSQL + pgvector (primary store)
│   ├── store/                  # Modular mixin pattern (search, graph, episodes, taxonomy)
│   └── schema.py               # DDL definitions
├── ingestion/rag/              # RAG pipeline & NLP processors
├── payloads.py                 # Pydantic validation models
└── core/                       # Config, registry, interfaces
```

All modules follow the 400-Line Code Scale standard. The `BrainService` uses a mixin pattern for composable database operations.

---

## Core Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BRAIN_DATABASE_URL` | — | PostgreSQL connection string (required) |
| `BRAIN_PORT` | `50051` | gRPC server port |
| `BRAIN_TENANTS` | — | Comma-separated allowed tenants |
| `EMBEDDER_TYPE` | auto | `openai` or `local` |
| `PGVECTOR_DIM` | `1536` | Vector dimensions (must match embedder) |
| `OPENAI_API_KEY` | — | Required for OpenAI embeddings |
| `REDIS_URL` | — | Embedding cache (optional, falls back to in-memory LRU) |

---

## Multi-Tenant Security

ContextBrain enforces multi-layer tenant isolation via PostgreSQL Row-Level Security (RLS). Every database operation sets session scope before query execution:
```sql
SET LOCAL app.current_tenant = '{tenant_id}';
```
Only the `brain_admin` role can bypass RLS (used by ContextView dashboard).

---

## Memory Architecture

| Type | Storage | Description |
|------|---------|-------------|
| **Semantic** | `knowledge_nodes` (pgvector) | Long-term knowledge as vector embeddings |
| **Episodic** | `conversation_episodes` (Postgres) | Session summaries, queryable conversation history |
| **Entity** | `user_facts` (Postgres) | Persistent user facts across sessions |
| **Embedding Cache** | Redis (optional) | Avoids redundant API calls; falls back to LRU (2048 entries) |

---

## Further Reading

- **Full Documentation**: [ContextBrain on Astro Site](../../docs/website/src/content/docs/brain/)
- **Agent Boundaries & Golden Paths**: [AGENTS.md](AGENTS.md)
