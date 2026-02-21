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
5. **NLP Enrichment** — NER, topic extraction, zero-shot classification
6. **Embedding Cache** — Redis-backed with in-memory fallback
7. **gRPC Service** — Remote access for Router and Commerce

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
│  ├── interceptors.py  (security) │   ├── search.py    (vector search)     │
│  ├── embedders.py     (cache)    │   ├── graph.py     (CRUD)              │
│  ├── nlp.py           (NER/NLP)  │   ├── episodes.py  (memory)            │
│  └── handlers/                   │   └── taxonomy.py  (categories)        │
│      ├── knowledge.py            ├── news.py          (news posts)        │
│      ├── memory.py               └── schema.py                            │
│      ├── traces.py                                                         │
│      ├── taxonomy.py             modules/intelligence/                     │
│      ├── commerce.py             ├── hub.py           (enrichment coord)  │
│      └── news.py                 ├── ner.py           (legacy NER)        │
│                                  ├── keyphrases.py    (legacy)            │
│  payloads.py                     ├── keywords.py      (taxonomy match)    │
│  (Pydantic validation)           └── taxonomy_manager.py                  │
│                                                                            │
│  helpers.py                      core/                                     │
│  (token/tenant validation)       ├── config/                              │
│                                  │   ├── main.py      (redis_url, etc.)   │
│                                  │   └── security.py                      │
│                                  ├── tokens.py  (AccessManager)           │
│                                  └── exceptions.py                        │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  External Dependencies                                             │   │
│  │  PostgreSQL+pgvector ── primary store                              │   │
│  │  Redis               ── embedding cache (optional, fallback: mem)  │   │
│  │  OpenAI API           ── embeddings (cached)                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
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
    embedding VECTOR(1536),         -- Dimension configurable via PGVECTOR_DIM (default 1536)
    domain VARCHAR(100),            -- Taxonomy domain (products, articles, etc.)
    source_type VARCHAR(50),        -- Origin (file, web, api)
    source_uri TEXT,                -- Original location
    metadata JSONB,                 -- Flexible attributes
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE INDEX ON knowledge_nodes USING hnsw (embedding vector_cosine_ops);
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

## Security & Access Control

### Permission Interceptor

Brain uses a gRPC server interceptor (`BrainPermissionInterceptor`) that maps every RPC method to a required permission:

```python
RPC_PERMISSION_MAP = {
    # Knowledge
    "Search": Permissions.BRAIN_READ,
    "Upsert": Permissions.BRAIN_WRITE,
    # Memory
    "AddEpisode": Permissions.MEMORY_WRITE,
    "GetRecentEpisodes": Permissions.MEMORY_READ,
    "UpsertFact": Permissions.MEMORY_WRITE,
    "GetUserFacts": Permissions.MEMORY_READ,
    # Traces
    "LogTrace": Permissions.TRACE_WRITE,
    "GetTraces": Permissions.TRACE_READ,
    # ...
}
```

The interceptor validates the `ContextToken` permission against the map. Unmapped RPCs log a warning.

### Tenant Isolation

All handlers enforce tenant isolation via `validate_tenant_access()`:

```python
# In every handler that accesses tenant-scoped data:
validate_tenant_access(token, params.tenant_id, context)
```

This calls `token.can_access_tenant(tenant_id)` and aborts with `PERMISSION_DENIED` if access is denied.

### Domain-Specific Permissions

| Domain | Read | Write |
|--------|------|-------|
| Knowledge | `brain:read` | `brain:write` |
| Memory | `memory:read` | `memory:write` |
| Traces | `trace:read` | `trace:write` |
| Commerce | `brain:read` | `brain:write` |
| News | `brain:read` | `brain:write` |

### Interceptor Stack (server.py)

```python
interceptors = [
    TokenValidationInterceptor(security_config),  # Extract & validate token
    BrainPermissionInterceptor(security_config),   # Check RPC permissions
]
```

---

## Memory Architecture

### Brain Memory (Postgres — persistent)

| Type | Table | Description |
|------|-------|-------------|
| Episodic | `conversation_episodes` | Session summaries, queryable |
| Entity | `user_facts` | Persistent user facts across sessions |

All persistent memory is in Brain's Postgres.

### Brain Cache (Redis — optional)

Brain uses Redis **only** for embedding cache:

| Type | Redis Key Pattern | TTL |
|------|-------------------|-----|
| Embedding Cache | `emb:{sha256}` | 7 days |

If Redis is unavailable, Brain falls back to in-memory LRU cache (max 2048 entries).

### Router Memory (Redis — ephemeral)

Redis stays entirely in Router for compute state:

| Type | Redis Key Pattern | TTL |
|------|-------------------|-----|
| LangGraph Checkpoints | `checkpoint:dispatcher:{thread_id}` | 24hr |
| Working Memory (KV) | `memory:{tenant}:{session}:{key}` | 1hr |
| Query Cache | `query:{tenant}:{hash}` | configurable |
| Rate Limiting | `rl:{identifier}` | window |

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
    embedder="openai",  # or "local"
)

results = await pipeline.run()
```

### ETL Flow

1. **Extract** — Load raw content from source
2. **Transform** — Chunk, clean, extract entities
3. **Enrich** — NLP pipeline (NER, topics, classification)
4. **Embed** — Generate vector embeddings (with Redis cache)
5. **Load** — Store in PostgreSQL with relationships

---

## Embedding Cache

Brain caches all computed embeddings to avoid redundant API calls.

### Architecture

```
Query text → sha256 hash → cache key
      ↓
┌─────────────┐     ┌──────────────┐
│ Redis Cache │ ──▶ │ Return cached │
│ (primary)   │ HIT │ embedding    │
└─────────────┘     └──────────────┘
      │ MISS
      ▼
┌──────────────┐     ┌──────────────┐
│ In-Memory    │ ──▶ │ Return cached │
│ LRU (2048)   │ HIT │ embedding    │
└──────────────┘     └──────────────┘
      │ MISS
      ▼
┌──────────────┐     ┌──────────────┐
│ OpenAI API   │ ──▶ │ Store in both│
│ (or local)   │     │ caches + ret │
└──────────────┘     └──────────────┘
```

### Key Details

- **Redis key format**: `emb:{sha256_of_model:text}` (~20 KB per embedding)
- **TTL**: 7 days (configurable via `EmbeddingCache.TTL_SECONDS`)
- **Fallback**: In-memory dict (max 2048 entries) if Redis is down
- **Stats**: Logged every 50 requests with hit rate %
- **Config**: `REDIS_URL` environment variable

### Usage

```python
from contextbrain.service.embedders import get_embedding_cache

cache = get_embedding_cache()
print(cache.stats)  # {'hits': 45, 'misses': 5, 'hit_rate': '90%', ...}
```

---

## NLP Enrichment

Brain enriches documents during ingestion with three NLP pipelines.
All are **local** (no LLM calls) and **optional** (degrade gracefully).

### Capabilities

| Pipeline | Library | Purpose | Model |
|----------|---------|---------|-------|
| **NER** | spaCy | Entity extraction (ORG, GPE, PERSON, etc.) | `en_core_web_sm` |
| **Topics** | KeyBERT | Keyword/topic extraction | `all-MiniLM-L6-v2` |
| **Classification** | sentence-transformers | Zero-shot categorization | `all-MiniLM-L6-v2` |

### Zero-Shot Categories

Documents are automatically classified against 12 predefined labels:
`commerce`, `infrastructure`, `security`, `documentation`, `data engineering`,
`machine learning`, `web development`, `devops`, `api design`, `database`,
`observability`, `testing`.

Custom labels can be provided:

```python
from contextbrain.service.nlp import NLPEnricher

enricher = NLPEnricher(category_labels=["finance", "healthcare", "legal"])
result = enricher.enrich("Patient records must comply with HIPAA regulations.")
print(result.top_category)  # "healthcare"
```

### Integration with IntelligenceHub

`IntelligenceHub` (the ingestion enrichment coordinator) uses `NLPEnricher` as
its primary pipeline, falling back to regex-based extractors if NLP dependencies
are not installed:

```python
# Automatic during ingestion (via IngestionService._enrich_metadata)
hub = IntelligenceHub()
result = await hub.enrich_content(text)
# result keys: entities, keyphrases, keywords, topics, language
```

### Dependencies

NLP libraries are optional:

```bash
# Install NLP support
uv pip install contextbrain[nlp]

# Download spaCy model
uv run python -m spacy download en_core_web_sm
```

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
BRAIN_DATABASE_URL="postgres://user:pass@localhost:5432/brain"  # or DATABASE_URL
BRAIN_SCHEMA="brain"

# Server
BRAIN_PORT=50051
BRAIN_INSTANCE_NAME="shared"
BRAIN_TENANTS="tenant1,tenant2"  # Comma-separated allowed tenants
BRAIN_NEWS_ENGINE=true           # Enable news engine tables

# Embeddings & Vector Search
EMBEDDER_TYPE="openai"           # "openai", "local" (default: auto-detect)
OPENAI_API_KEY="sk-..."          # Required for OpenAI
PGVECTOR_DIM=1536                # 1536 (OpenAI standard) or 768 (local)

# Redis (Embedding Cache)
REDIS_URL="redis://localhost:6379/0"  # Optional, fallback: in-memory cache
```

### Config Class

```python
from contextbrain.core import get_core_config

config = get_core_config()
# Access: config.database_url, config.port, config.postgres.vector_dim
# Access: config.redis_url, config.embedder_type
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
| `service/server.py` | gRPC server setup with interceptor stack |
| `service/brain_service.py` | Main BrainService (mixin composition) |
| `service/interceptors.py` | `BrainPermissionInterceptor` — RPC→permission mapping |
| `service/helpers.py` | Token validation, tenant access helpers |
| `service/embedders.py` | `OpenAIEmbedder`, `LocalEmbedder`, `EmbeddingCache` |
| `service/nlp.py` | `NLPEnricher`, `EntityExtractor`, `TopicExtractor`, `ZeroShotClassifier` |
| `service/handlers/knowledge.py` | Search, upsert, graph handlers |
| `service/handlers/memory.py` | Episodic/entity memory handlers |
| `service/handlers/traces.py` | Agent execution trace handlers |
| `service/handlers/taxonomy.py` | Taxonomy CRUD handlers |
| `service/handlers/news.py` | News engine handlers |
| `service/handlers/commerce.py` | Commerce/verification handlers |
| `modules/intelligence/hub.py` | `IntelligenceHub` — enrichment coordinator (NLP + legacy) |
| `storage/postgres/store/` | Modular store (mixin pattern) |
| `storage/postgres/store/search.py` | Vector search operations |
| `storage/postgres/store/graph.py` | Graph CRUD operations |
| `storage/postgres/news.py` | News post storage |
| `storage/postgres/schema.py` | Database table definitions |
| `ingest.py` | `IngestionService` — ETL pipeline with NLP enrichment |
| `payloads.py` | Pydantic validation models |
| `core/config/main.py` | Configuration management (`redis_url`, embeddings, etc.) |
| `core/config/security.py` | Security policies (permissions) |
| `core/tokens.py` | AccessManager for permission verification |

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

## Optional Dependencies

| Group | Packages | Purpose |
|-------|----------|---------|
| `vertex` | `google-cloud-aiplatform`, `langchain-google-vertexai` | Vertex AI models |
| `storage` | `google-cloud-storage` | GCS integration |
| `ingestion` | `trafilatura`, `pymupdf4llm`, `beautifulsoup4` | Document extraction |
| `nlp` | `spacy>=3.7.0`, `keybert>=0.8.0` | NER + topic extraction |
| `observability` | `langfuse`, `opentelemetry` | Tracing/monitoring |
| `models` | OpenAI, Anthropic, HF Hub | LLM providers |
| `all` | Everything above | Development/testing |

---

## Migrated Service Notes (from retired `services/contextbrain/docs/`)

- **Multi-service operation**: Brain is intended to serve multiple clients concurrently with tenant isolation.
- **gRPC migration status tracking**: endpoint parity, security boundary checks, and client compatibility should be part of release readiness.
- **Database configuration contract**: keep connection/schema variables explicit, with migration safety checks before rollout.
- **NLP enrichment**: NER (spaCy), topic extraction (KeyBERT), and zero-shot classification are now implemented — see NLP Enrichment section.

---

## Links

- **Documentation**: https://contextbrain.dev
- **Repository**: https://github.com/ContextUnity/contextbrain
- **Schema Reference**: https://contextbrain.dev/reference/schema/

---

*Last updated: February 2026*


---

## Component Docs: services/contextbrain/src/contextbrain/ingestion/rag/README.md

# RAG Ingestion Pipeline

Single canonical flow (persisted stage artifacts under `assets/ingestion/`):

```mermaid
flowchart TD
  RawFiles[RawFiles_assetsIngestionSource] --> Preprocess[Stage1_PreprocessToCleanText]
  Preprocess --> CleanTextStore[CleanTextStore_perTypeFiles]

  CleanTextStore --> PersonaStage[Stage1b_BuildPersonaOptional]
  PersonaStage --> PersonaFile[persona_txt]

  CleanTextStore --> TaxonomyStage[Stage2_BuildTaxonomy]
  TaxonomyStage --> TaxonomyFile[taxonomy_json]

  TaxonomyFile --> EnrichStage[Stage2b_EnrichNER_Keyphrases]
  CleanTextStore --> EnrichStage
  EnrichStage --> CleanTextStore

  TaxonomyFile --> OntologyStage[Stage2c_BuildOntology]
  OntologyStage --> OntologyFile[ontology_json]

  CleanTextStore --> GraphStage[Stage3_BuildGraph]
  TaxonomyFile --> GraphStage
  OntologyFile --> GraphStage
  GraphStage --> GraphFile[knowledge_graph_pickle]

  CleanTextStore --> ShadowStage[Stage4_BuildShadowRecords]
  TaxonomyFile --> ShadowStage
  GraphFile --> ShadowStage
  ShadowStage --> ShadowStore[ShadowRecords_perTypeFiles]

  ShadowStore --> ExportStage[Stage5_ExportJSONL]
  ExportStage --> JsonlStore[JSONL_perType]

  JsonlStore --> DeployStage[Stage6_DeployToSearchIndex]
  DeployStage --> VertexIndex[VertexAISearchIndex]
```

## CLI

All commands are under `contextbrain ingest ...`.

- `preprocess`: Raw → CleanText
- `persona`: CleanText → `persona.txt` (optional; controlled by `[persona].enabled`)
- `taxonomy`: CleanText → `taxonomy.json` (hybrid Pass A/B/C pipeline)
- `ontology`: Taxonomy → `ontology.json` (schema + relation constraints)
- `enrich`: CleanText → NER + keyphrases (optional; does not depend on taxonomy)
  - Output is merged into `keywords` and indexed for keyword search in Postgres
- `graph`: CleanText + `taxonomy.json` + `ontology.json` → `knowledge_graph.pickle`
- `graph-peek`: Quick inspection of graph (nodes/edges + top labels + sample edges)
- `graph-show`: Comprehensive graph statistics and structure
- `graph-audit`: Inspect graph quality (label distribution, directional labels, sample facts)
- `shadow`: CleanText + taxonomy + graph → `shadow/<type>.jsonl`
- `export`: Shadow → Vertex import JSONL (per type)
- `deploy`: JSONL → upload + import into Vertex AI Search
- `report`: Generate ingestion report with stats (taxonomy/graph/ontology coverage)
- `run`: runs preprocess → persona (optional) → taxonomy → ontology → enrich → graph → shadow → export (deploy is separate)
  - `enrich` can be run independently after `preprocess`

**Preflight policy**: every stage checks prerequisites and prints an actionable hint when something is missing.

### Common flags

- `--overwrite/--no-overwrite`:
  - Default is **overwrite** (rebuild stage artifacts)
  - Use `--no-overwrite` to append/keep existing outputs where supported (`preprocess`, `shadow`, `export`)
- `--workers N`:
  - Enables per-type parallelism (where supported)
  - Default is `1` (serial)

## Directory layout

Under `assets/ingestion/`:

- `source/<type>/...`: raw inputs
  - `source/video/`: Video transcript files (`.txt`, `.srt`, or `.vtt`)
  - `source/book/`: Book chapter files (`.txt` or `.md`)
  - `source/qa/`: Q&A session transcripts (`.txt`)
  - `source/web/`: Web content (fetched via `url.toml` or static `.html`/`.txt`)
  - `source/knowledge/`: Knowledge base articles (`.txt` or `.md`)
  - `source/audio/`: Audio transcript files (`.txt`, `.srt`, or `.vtt`) - **Note**: Audio ingestion requires transcript files. Audio files (`.mp3`, `.wav`, etc.) are not directly supported. Use a transcription service to generate `.txt`, `.srt`, or `.vtt` files first, then place them in `source/audio/`.
- `clean_text/<type>.jsonl`: CleanText artifacts (per type)
- `persona.txt`: optional generated persona instruction
- `taxonomy.json`: taxonomy artifact (concepts, categories, canonical_map)
- `ontology.json`: ontology schema (entity types, relation constraints, runtime fact labels)
- `knowledge_graph.pickle`: graph artifact (NetworkX graph with typed relations)
- `shadow/<type>.jsonl`: ShadowRecords (per type)
- `output/jsonl/<type>/*.jsonl`: Vertex import JSONL exports (per type)
- `output/_processing/`: optional debug artifacts (topic_profiles.jsonl, graph_debug_*.json, report/)

## Core Modules

The ingestion pipeline is built on reusable core modules in `modules/ingestion/rag/core/`:

### `batch.py` - LLM Batch Processing

Unified utilities for batch LLM operations:

- `batch_validate()` - Validate items in batches, returning valid indices
- `batch_transform()` - Transform items in batches, returning index→result dict
- `filter_by_indices()` - Filter sequence by valid indices
- `chunked()` - Yield successive chunks from iterable

Used by: video segment validation, QA validation, summary generation.

### `loaders.py` - File Loading Utilities

Utilities for loading and iterating over source files:

- `iter_files()` - Iterate over files matching glob patterns
- `load_text_files()` - Load text files into `LoadedFile` objects
- `read_text_file()` - Read single text file with encoding detection
- `FileLoaderMixin` - Mixin for plugins to load files

### `types.py` - Core Data Types

- `RawData` - Intermediate object returned by loaders (content + metadata)
- `ShadowRecord` - Final output object matching Vertex AI schema
- `IngestionMetadata` - Flexible metadata container (varies by source type)

### `plugins.py` - Source Type Plugins

Plugins implement per-type processing rules:

- `BookPlugin` - Book chapter processing (chunking, topic extraction)
- `VideoPlugin` - Video transcript processing (segment validation, timestamp formatting)
- `QAPlugin` - Q&A session processing (question/answer validation)
- `WebPlugin` - Web content processing (HTML parsing, metadata extraction)
- `TextPlugin` - Generic text processing

Each plugin implements:
- `load()` - Load raw files into `RawData` objects
- `preprocess()` - Transform `RawData` → `CleanText` records
- `validate()` - Validate records (optional)

### `utils.py` - Common Utilities

- `resolve_workers()` - Resolve worker count from config/env
- `normalize_clean_text()` - Normalize text (unicode, whitespace)
- `parallel_map()` - Parallel processing utilities

## Configuration

Configuration is loaded from `settings.toml` (see `settings.toml.example`).

Environment variables:
- `CONTEXTBRAIN_CONFIG_PATH`: Direct path to config file
- `CONTEXTBRAIN_ASSETS_PATH`: Path to assets folder (config = `{assets}/settings.toml`)

Priority: `CONTEXTBRAIN_CONFIG_PATH` > `CONTEXTBRAIN_ASSETS_PATH` > package default

## Usage

### Short imports

```python
from contextbrain.modules.ingestion.rag import (
    load_config,
    get_assets_paths,
    preprocess_to_clean_text,
    build_taxonomy_from_clean_text,
    build_graph_from_clean_text,
    build_shadow_records,
    export_jsonl_per_type,
    deploy_jsonl_files,
    RawData,
    ShadowRecord,
)
```

### Programmatic usage

```python
from contextrouter.modules.ingestion.rag import load_config, preprocess_to_clean_text

cfg = load_config()
preprocess_to_clean_text(config=cfg, only_types=["book", "video"], overwrite=True)
```

### Flow-based usage (via FlowManager)

The ingestion stages can also be used as transformers in a Flow:

```toml
[flows.ingestion_rag]
source = "file"
logic = ["ingestion.preprocess", "ingestion.taxonomy", "ingestion.graph", "ingestion.shadow"]
sink = "vertex"
```

## Architecture

This module follows the **modular ingestion pattern**:

- **Connectors** (`modules/connectors/`): Raw data sources (file, API, RSS, web)
- **Transformers** (`modules/ingestion/logic/`): Processing stages (taxonomy, ontology, graph, shadow)
- **Providers** (`modules/providers/`): Storage sinks (Vertex, Postgres, GCS)

The RAG ingestion engine (`modules/ingestion/rag/`) contains:
- Deep business logic for RAG-specific processing
- Type-specific plugins (book, video, qa, web)
- Knowledge graph construction
- Shadow record enrichment

All data flows through `ContextUnit` (from `contextcore`) for provenance tracking and security.



---

