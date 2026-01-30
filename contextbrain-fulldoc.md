# ContextBrain — Full Documentation

**The Smart Memory of ContextUnity**

ContextBrain is the semantic knowledge store. It provides vector search, knowledge graph traversal, taxonomy management, and entity resolution. All services rely on Brain for persistent, queryable intelligence.

---

## Overview

ContextBrain acts as the centralized memory for the ContextUnity ecosystem. It stores knowledge as vectors and graphs, enabling semantic search, entity relationships, and taxonomy hierarchies.

### Key Responsibilities

1. **Vector Storage** — Embeddings with pgvector for semantic search
2. **Knowledge Graph** — Entity relationships and traversal
3. **Taxonomy Management** — Hierarchical categories with ltree
4. **Entity Resolution** — Aliases and canonical forms
5. **gRPC Service** — Remote access for Router and Commerce

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              ContextBrain                                   │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  service/                        storage/postgres/                         │
│  ├── server.py        ────────▶  ├── store/           (mixin pattern)     │
│  ├── brain_service.py            │   ├── base.py      (connection)        │
│  ├── commerce_service.py         │   ├── search.py    (vector search)     │
│  ├── embedders.py                │   ├── graph.py     (CRUD)              │
│  └── handlers/                   │   ├── episodes.py  (memory)            │
│      ├── knowledge.py            │   └── taxonomy.py  (categories)        │
│      ├── memory.py               ├── news.py          (news posts)        │
│      ├── taxonomy.py             └── schema.py                            │
│      ├── commerce.py                                                       │
│      └── news.py                 ingestion/                                │
│                                  ├── pipeline.py                          │
│  payloads.py                     ├── transformers/                        │
│  (Pydantic validation)           └── loaders/                             │
│                                                                            │
│  core/                                                                     │
│  ├── config.py                                                            │
│  ├── types.py                                                             │
│  └── exceptions.py                                                        │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Modular Design (400-Line Code Scale)

All modules follow the 400-Line Code Scale standard:
- **service/**: Split from monolithic service.py into focused handlers
- **store/**: Uses mixin pattern for composable database operations
- **payloads.py**: Server-side Pydantic validation for all gRPC requests

---

## Database Schema

Brain uses PostgreSQL with specialized extensions:

### Extensions Required
- `pgvector` — Vector similarity search
- `ltree` — Hierarchical taxonomy paths
- `uuid-ossp` — UUID generation

### Core Tables

#### `knowledge_nodes`
Stores embedded content with vectors.

```sql
CREATE TABLE knowledge_nodes (
    id UUID PRIMARY KEY,
    content TEXT NOT NULL,
    embedding VECTOR(768),          -- Vertex AI embeddings
    domain VARCHAR(100),            -- Taxonomy domain (products, articles, etc.)
    source_type VARCHAR(50),        -- Origin (file, web, api)
    source_uri TEXT,                -- Original location
    metadata JSONB,                 -- Flexible attributes
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE INDEX ON knowledge_nodes USING ivfflat (embedding vector_cosine_ops);
```

#### `knowledge_edges`
Semantic relationships between entities.

```sql
CREATE TABLE knowledge_edges (
    id UUID PRIMARY KEY,
    source_id UUID REFERENCES knowledge_nodes(id),
    target_id UUID REFERENCES knowledge_nodes(id),
    relation_type VARCHAR(100),     -- 'is_a', 'part_of', 'related_to'
    weight FLOAT DEFAULT 1.0,
    metadata JSONB
);
```

#### `knowledge_aliases`
Entity synonyms and canonical mappings.

```sql
CREATE TABLE knowledge_aliases (
    id UUID PRIMARY KEY,
    canonical_id UUID REFERENCES knowledge_nodes(id),
    alias TEXT NOT NULL,
    language VARCHAR(10),
    confidence FLOAT DEFAULT 1.0
);
```

#### `taxonomy_nodes`
Hierarchical category definitions.

```sql
CREATE TABLE taxonomy_nodes (
    id UUID PRIMARY KEY,
    domain VARCHAR(100),            -- 'category', 'color', 'size', 'gender'
    code VARCHAR(100) UNIQUE,
    name_uk TEXT NOT NULL,
    name_en TEXT,
    path LTREE,                     -- Hierarchical path
    parent_id UUID REFERENCES taxonomy_nodes(id),
    metadata JSONB
);

CREATE INDEX ON taxonomy_nodes USING GIST (path);
```

---

## gRPC Interface

Defined in `contextcore/protos/brain.proto`:

### Service Definition

```protobuf
service BrainService {
    // Semantic search
    rpc Search(SearchRequest) returns (stream SearchResult);
    
    // Graph traversal
    rpc GraphSearch(GraphSearchRequest) returns (stream GraphSearchResult);
    
    // Taxonomy operations
    rpc GetTaxonomy(TaxonomyRequest) returns (TaxonomyResponse);
    rpc SyncTaxonomy(stream TaxonomyNode) returns (SyncResult);
    
    // Entity resolution
    rpc ResolveEntity(EntityRequest) returns (EntityResponse);
    
    // Ingestion
    rpc IngestDocument(IngestRequest) returns (IngestResult);
}
```

### Python Client

```python
from contextcore import BrainClient

client = BrainClient(host="localhost:50051")

# Semantic search
async for result in client.search("blue winter jacket", limit=10):
    print(result.content, result.score)

# Graph traversal
async for node in client.graph_search(["entity:jacket"], depth=2):
    print(node.relation, node.target)
```

### Error Handling

Brain uses structured error codes for gRPC responses:

| Error Code | gRPC Status | Description |
|------------|-------------|-------------|
| `SCHEMA_MISMATCH` | `FAILED_PRECONDITION` | Database schema doesn't match expected |
| `DB_QUERY_ERROR` | `UNAVAILABLE` | Query execution failed |
| `DB_CONNECTION_ERROR` | `UNAVAILABLE` | Cannot connect to database |
| `RETRIEVAL_ERROR` | `NOT_FOUND` | No results for query |
| `VALIDATION_ERROR` | `INVALID_ARGUMENT` | Invalid request parameters |

#### Using Error Decorators

```python
from contextbrain.core.exceptions import grpc_error_handler, grpc_stream_error_handler

# For unary methods
@grpc_error_handler
async def Search(self, request, context):
    results = await self.store.search(request.query)
    return SearchResponse(results=results)

# For streaming methods
@grpc_stream_error_handler
async def StreamSearch(self, request, context):
    async for result in self.store.stream_search(request.query):
        yield result
```

---

## Ingestion Pipeline

### Supported Sources

| Source | Transformer | Output |
|--------|-------------|--------|
| PDF files | `pymupdf4llm` | Text chunks with page refs |
| Web pages | `trafilatura` | Clean text extraction |
| Structured data | Custom | Direct node creation |
| RSS feeds | Built-in | Articles with metadata |

### Pipeline Configuration

```python
from contextbrain.ingestion import IngestPipeline

pipeline = IngestPipeline(
    source="file:///path/to/docs",
    domain="knowledge",
    chunk_size=1000,
    overlap=200,
    embedder="vertex-ai",
)

results = await pipeline.run()
```

### ETL Flow

1. **Extract** — Load raw content from source
2. **Transform** — Chunk, clean, extract entities
3. **Embed** — Generate vector embeddings
4. **Load** — Store in PostgreSQL with relationships

---

## Taxonomy Management

Taxonomy definitions are stored in Brain and synced from Commerce:

### Domain Types

| Domain | Description | Example Values |
|--------|-------------|----------------|
| `category` | Product hierarchy | Clothing → Jackets → Winter |
| `color` | Color attributes | Blue, Red, Navy Blue |
| `size` | Size specifications | S, M, L, XL, 42 |
| `gender` | Target demographic | Men, Women, Unisex |
| `brand` | Brand entities | Nike, Adidas |

### Sync Command

```bash
# From Commerce
mise run taxonomy_sync

# Direct Python
from contextbrain.storage.postgres import TaxonomyStore
store = TaxonomyStore(connection)
await store.sync_from_yaml("metadata/taxonomy.yaml")
```

---

## Configuration

### Environment Variables

```bash
# Database
BRAIN_DATABASE_URL="postgres://user:pass@localhost:5432/brain"

# gRPC Server
BRAIN_GRPC_PORT=50051
BRAIN_GRPC_WORKERS=4

# Embeddings
BRAIN_EMBEDDER="vertex-ai"
GOOGLE_CLOUD_PROJECT="my-project"

# Vector search
BRAIN_VECTOR_DIMENSION=768
BRAIN_SEARCH_LIMIT=100
```

### Config Class

```python
from contextbrain.core.config import BrainConfig

config = BrainConfig.from_env()
# Access: config.database, config.grpc, config.embedder
```

---

## Running the Service

### Development

```bash
# Initialize database
uv run python scripts/init_db.py

# Run gRPC server
uv run python -m contextbrain

# Run migrations
uv run alembic upgrade head
```

### Production (Docker)

```dockerfile
FROM python:3.13-slim
WORKDIR /app
COPY . .
RUN pip install -e ".[all]"
CMD ["python", "-m", "contextbrain"]
```

---

## CLI

```bash
# Database shell
mise run db_shell

# Run ingestion
uv run python -m contextbrain.ingest --source file://docs/

# Test search
uv run python -c "from contextbrain import search; print(search('query'))"
```

---

## Testing

```bash
# Run all tests
uv run pytest

# With PostgreSQL (requires running instance)
uv run pytest -m integration

# Coverage
uv run pytest --cov=contextbrain
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `service/server.py` | gRPC server setup |
| `service/brain_service.py` | Main BrainService class |
| `service/handlers/knowledge.py` | Knowledge management handlers |
| `service/handlers/news.py` | News engine handlers |
| `storage/postgres/store/` | Modular store (mixin pattern) |
| `storage/postgres/store/search.py` | Vector search operations |
| `storage/postgres/store/graph.py` | Graph CRUD operations |
| `storage/postgres/news.py` | News post storage |
| `storage/postgres/schema.py` | Database table definitions |
| `payloads.py` | Pydantic validation models |
| `ingestion/pipeline.py` | ETL orchestration |
| `core/config.py` | Configuration management |

---

## Integration Points

### With ContextRouter

```python
# Router uses BrainProvider
from contextrouter.modules.providers.storage import BrainProvider
brain = BrainProvider(config)
results = await brain.search(query)
```

### With ContextCommerce

```python
# Commerce syncs taxonomy
from contextcommerce.management.commands import taxonomy_sync
# Pushes YAML definitions to Brain
```

---

## Links

- **Documentation**: https://contextbrain.dev
- **Repository**: https://github.com/ContextUnity/contextbrain
- **Schema Reference**: https://contextbrain.dev/reference/schema/

---

*Last updated: January 2026*
