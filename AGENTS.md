# ContextBrain — Agent Instructions

Knowledge layer: vector storage, knowledge graphs, taxonomy hierarchies, and episodic agent memory.

## Entry & Execution
- **Workspace**: `services/brain/`
- **Run**: `uv run python -m contextunity.brain`
- **Tests**: `uv run --package contextunity-brain pytest`
- **Lint**: `uv run ruff check .`

## Code Standards
You MUST adhere to [Code Standards](../../.agent/skills/code_standards/SKILL.md): 400-line limit, Pydantic strictness, `mise` sync, Ruff compliance.

## Architecture

```
src/contextunity/brain/
├── service/
│   ├── server.py              # gRPC server with interceptor stack
│   ├── brain_service.py       # Main BrainService (mixin composition)
│   ├── interceptors.py        # BrainPermissionInterceptor
│   ├── helpers.py             # Token/tenant validation
│   ├── payloads.py            # Pydantic validation for all gRPC payloads
│   └── handlers/              # Domain-specific handlers
│       ├── knowledge.py       # Search, upsert, graph
│       ├── memory.py          # Episodic/entity memory
│       ├── traces.py          # Agent execution traces
│       ├── taxonomy.py        # Taxonomy CRUD
│       ├── commerce.py        # Commerce/verification
│       └── news.py            # News engine
├── storage/postgres/
│   ├── store/                 # Mixin pattern
│   │   ├── base.py            # Connection management
│   │   ├── search.py          # Vector search (pgvector)
│   │   ├── graph.py           # Knowledge graph CRUD
│   │   ├── episodes.py        # Memory
│   │   └── taxonomy.py        # Categories (ltree)
│   ├── news.py                # News post storage
│   └── schema.py              # DDL definitions
├── storage/duckdb_store.py    # In-memory analytical engine (OLAP, commerce verification)
├── storage/graph/cognee.py    # Graph knowledge via Cognee
├── ingestion/rag/             # RAG pipeline & NLP processors
├── journal.py                 # DuckDB-powered agent journal
└── core/
    ├── config/                # BrainConfig (Pydantic settings)
    ├── exceptions.py          # ContextbrainError → ContextUnityError
    └── registry.py            # Component registry
```

## Strict Boundaries
- **ContextUnit ONLY**: All gRPC calls use `ContextUnit` from `contextunity.core`. No domain-specific proto messages.
- **No Naked SQL**: All PostgreSQL access through `storage/` abstraction layer. Never raw SQL in handlers.
- **Security Non-Negotiable**: `BrainPermissionInterceptor` must be active. All handlers validate tokens via `validate_token_for_read/write`.
- **Tenant Isolation**: `tenant_id` is physically enforced via PostgreSQL RLS. Every query sets `SET LOCAL app.current_tenant`.
- **Exception Hierarchy**: All exceptions extend `ContextUnityError`. Use `@grpc_error_handler` / `@grpc_stream_error_handler`.

## gRPC Interface
Proto definitions: `packages/core/protos/brain.proto`

| Handler | Methods | Location |
|---------|---------|----------|
| Knowledge | Search, Upsert, GraphSearch, CreateKGRelation | `handlers/knowledge.py` |
| Memory | AddEpisode, GetRecentEpisodes, UpsertFact, GetUserFacts | `handlers/memory.py` |
| Taxonomy | GetTaxonomy, SyncTaxonomy, UpsertTaxonomy | `handlers/taxonomy.py` |
| Commerce | GetProducts, UpdateEnrichment | `handlers/commerce.py` |
| News | UpsertNewsItem, GetNewsItems, UpsertNewsPost | `handlers/news.py` |
| Traces | LogTrace, GetTraces | `handlers/traces.py` |

**Client usage**:
```python
from contextunity.core import BrainClient
client = BrainClient(host="localhost:50051")
results = await client.search("winter jacket", limit=10)
```

## Security & Authorization
Two-layer defense:
1. **Interceptor** (`BrainPermissionInterceptor`): RPC → permission mapping (e.g., `Search` → `brain:read`)
2. **Handler** (`validate_token_for_read/write`): Tenant binding + resource-level authorization

```python
from contextunity.brain.service.helpers import validate_token_for_read

async def Search(self, request, context):
    token = validate_token_for_read(context, params.tenant_id)
```

## Configuration

| Variable | Description |
|----------|-------------|
| `BRAIN_DATABASE_URL` | PostgreSQL connection string |
| `BRAIN_PORT` | gRPC server listening port |
| `BRAIN_TENANTS` | Comma-separated allowed tenants |
| `EMBEDDER_TYPE` | `openai` or `local` |
| `OPENAI_API_KEY` | Required if embedder is `openai` |
| `PGVECTOR_DIM` | Must match embedder (1536 OpenAI / 768 Local) |
| `REDIS_URL` | Embedding cache (falls back to in-memory LRU) |

All config accessed via `contextunity.brain.core.get_core_config()`. No `os.getenv()`.

## Golden Paths

### Adding a Database Table
1. Define schema in `storage/postgres/schema.py`
2. Create migration: `uv run alembic revision -m "add_table_name"`
3. Add storage mixin in `storage/postgres/store/`
4. Apply: `uv run alembic upgrade head`

### Adding/Modifying a gRPC Method
1. Edit `protos/brain.proto` in `packages/core/`
2. Regenerate stubs: `uv run python scripts/build_protos.py`
3. `uv sync` to pick up updated types
4. Implement handler in `service/handlers/`, expose via `brain_service.py`

### ContextUnit Protocol
- Always populate `unit.provenance` with transformation context
- Always enforce `unit.payload.tenant_id` — isolation is query-level

## Further Reading
- [Astro Docs: ContextBrain](../../docs/website/src/content/docs/brain/)
- [Brain Operations Skill](../../.agent/skills/brain_ops/SKILL.md)
