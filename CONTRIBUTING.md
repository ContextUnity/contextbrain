# Contributing to ContextBrain

Thanks for contributing to **ContextBrain** — the Knowledge Storage and RAG Service of the [ContextUnity](https://github.com/ContextUnity) ecosystem.

## Development Setup

```bash
cd contextbrain
uv sync --dev

# If you work on ingestion code:
uv sync --dev --extra ingestion
```

## Pre-commit

Install the git hooks once:

```bash
pre-commit install
```

Run on-demand:

```bash
pre-commit run --all-files
```

## Linting & Tests

```bash
uv run ruff check . --fix
uv run ruff format .
uv run pytest -q
```

## Architecture Overview

ContextBrain is a **gRPC service** for knowledge storage and retrieval. Key components:

```
src/contextbrain/
├── service.py         # gRPC service implementation (BrainService)
├── storage/           # Storage backends
│   ├── postgres/      # PostgreSQL + pgvector (primary)
│   └── vertex.py      # Vertex AI Search integration
├── ingestion/         # Data ingestion pipelines
│   └── rag/           # RAG-specific processing
└── core/              # Config, registry, interfaces
```

## Golden Path: Adding New Functionality

### 1. Adding a New Database Table

1. **Define the model** in `storage/postgres/models.py` (if using SQLAlchemy) or as raw SQL
2. **Create a migration** in `migrations/`:
   ```bash
   uv run alembic revision -m "add_new_table"
   ```
3. **Add storage methods** in `storage/postgres/store.py`
4. **Test locally**:
   ```bash
   uv run alembic upgrade head
   uv run pytest tests/storage/
   ```

### 2. Adding a New gRPC Method

⚠️ **gRPC methods are defined in ContextCore**. Follow this order:

1. **Update proto** in [ContextCore](https://github.com/ContextUnity/contextcore):
   - Edit `protos/brain.proto`
   - Add request/response messages
   - Add method to `BrainService`
   
2. **Regenerate stubs**:
   ```bash
   cd contextcore
   uv run python scripts/gen_protos.py
   ```

3. **Bump ContextCore version** and commit

4. **Update ContextBrain dependency**:
   ```bash
   cd contextbrain
   uv sync  # pulls new contextcore
   ```

5. **Implement the method** in `service.py`:
   ```python
   class BrainService(brain_pb2_grpc.BrainServiceServicer):
       def NewMethod(self, request, context):
           # Implementation here
           return brain_pb2.NewMethodResponse(...)
   ```

6. **Add tests** in `tests/test_service.py`

### 3. Adding a New Storage Backend

1. **Create module** in `storage/new_backend.py`
2. **Implement interfaces** from `core/interfaces.py`:
   - `IKnowledgeStore` for knowledge operations
   - `ITaxonomyStore` for taxonomy operations
3. **Register** in `storage/__init__.py`
4. **Add tests** in `tests/storage/test_new_backend.py`

### 4. Adding a New Ingestion Processor

1. **Create processor** in `ingestion/rag/processors/`
2. **Register** in processor registry
3. **Configure** in `ingestion/rag/config.py`

## ContextUnit Protocol

All data passing through ContextBrain uses `ContextUnit` from [ContextCore](https://github.com/ContextUnity/contextcore):

- **Provenance tracking**: Always append to `unit.provenance` when transforming data
- **Security scopes**: Use `ContextToken` for authorization
- **Tenant isolation**: Every operation requires `tenant_id`

```python
from contextcore import ContextUnit, SecurityScopes

# Creating a unit with security
unit = ContextUnit(
    payload={"content": "..."},
    security=SecurityScopes(read=["brain:read"], write=["brain:write"]),
)
```

## Engineering Principles

1. **Type Safety**: Use Pydantic for runtime entities, TypedDict for JSON contracts
2. **No direct `os.environ`**: Use config from environment via `SharedConfig`
3. **Immutability**: Storage methods should not mutate input objects
4. **Logging**: Use `get_context_unit_logger(__name__)` from contextcore

## Error Conventions

- All exceptions that cross module boundaries inherit from `ContextbrainError`
- Every `ContextbrainError` has a stable, non-empty `code` string
- gRPC methods map errors to appropriate status codes

## Branching & PR Flow

### Branch naming
- `feat/<short-topic>` — new features
- `fix/<short-topic>` — bug fixes
- `docs/<short-topic>` — documentation
- `refactor/<short-topic>` — code refactoring

### PR flow
1. Branch off `main`
2. Keep PRs small and focused
3. Run `pre-commit run --all-files` before pushing
4. Ensure CI passes (lint + tests)

### Merge strategy
- **Squash & merge** into `main`
- Use **Conventional Commits**: `feat:`, `fix:`, `docs:`, `refactor:`

## Releases

1. Bump version in `pyproject.toml` (SemVer)
2. Tag as `vX.Y.Z`
3. CI publishes to PyPI

## Questions?

- Check [contextbrain.dev](https://contextbrain.dev) for documentation
- Open an issue on [GitHub](https://github.com/ContextUnity/contextbrain)
