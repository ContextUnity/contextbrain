# ContextBrain — Agent Instructions

Knowledge layer: vector storage, knowledge graphs, BrainCells, and typed Conversation History.

**Types & payloads:** [docs/architecture/type-boundaries.md](../../docs/architecture/type-boundaries.md)
**Code quality:** [docs/architecture/code-quality.md](../../docs/architecture/code-quality.md)

## Entry & Execution

Run from monorepo root (`contextunity/`) unless noted.

| Task | Command |
|------|---------|
| Workspace | `services/brain/` |
| Run | `uv run python -m contextunity.brain` |
| Tests | `uv run --package contextunity-brain pytest services/brain/tests` |
| Lint | `cd services/brain && uv run ruff check src tests` |
| Types (brain scope) | `uv run basedpyright services/brain/src/contextunity/brain --warnings` |
| Monorepo gate | `uv run basedpyright --project pyrightconfig.json --warnings` |
| Core guards (shared types / interceptors) | [type-boundaries.md §8.1](../../docs/architecture/type-boundaries.md) |

## Type hardening & skills

Import types from `contextunity.core.types` / `contextunity.core.sdk.types` — never fork `JsonValue` or `ContextUnitPayload` locally.

**Narrowing:** graph state, storage rows, ingestion — `from contextunity.core.narrowing import as_*`; gRPC/SDK payload fields — `sdk.payload.get_*`. See [type-boundaries.md §4.5](../../docs/architecture/type-boundaries.md). No service-local `narrow.py` re-exports.

| Trigger | Skill |
|---------|-------|
| New feature / flow (storage RPC, enqueue, admin surface, product path) | **`acdd-feature-development`** → then `tdd` |
| Typing, JSON/gRPC, ContextUnit payloads, `dict[str, object]`, basedpyright | **`contract-boundaries`** (primary) → **`type-validation`** |
| Core types / parsing / SDK | **`core-contract-change`** + **`contract-boundaries`** |
| Bug / regression | `diagnose` |
| Implementation loop (Red-Green after ACDD or small fix) | `tdd` |
| Full suite / pre-push / Postgres lanes | `test-suite` |

Workflows: [/acdd](../../.agents/workflows/acdd-feature-development.md), [/contract-boundaries](../../.agents/workflows/contract-boundaries.md). Monorepo: [AGENTS.md](../../AGENTS.md).

## Platform Invariants
Follow `packages/core/AGENTS.md` for proto, config, exception, and token rules. In this service:
- **Config**: use `SharedConfig` / Brain config models — no bare `os.getenv()` or `os.environ`.
- **Exceptions**: inherit `contextunity.core.exceptions.ContextUnityError`.
- **Crypto/tokens**: use `contextunity.core.token_utils` — no inline HMAC or encryption.

## Strict Boundaries & Tenancy
1. **ContextUnit Only**: All gRPC requests and responses MUST use the `ContextUnit` envelope from `contextunity.core`. No custom payload schemas.
2. **PostgreSQL RLS Tenancy**: All postgres access MUST enforce tenant isolation via Row Level Security (RLS). Every database connection must set the current tenant using:
   ```sql
   SET LOCAL app.current_tenant = '{tenant_id}';
   ```
3. **No Raw SQL in Handlers**: All SQL queries must live in the `storage/` abstraction layer, never inside gRPC handler classes.
4. **Strict Payload Configuration**: Brain payload models use Pydantic `extra="forbid"`. Any client payload MUST provide `tenant_id` for validation.
5. **No Model Orchestration**: Brain does not execute agent loops or make routing calls. It only performs data processing (RAG parsing, embedding generation, graph traversal, and storage).

## Database & Migration Invariants
- **Schema Authority**: `storage/postgres/schema.py` is the single source of truth for DDL.
- **Alembic Migrations**: All schema modifications MUST be accompanied by an Alembic migration script generated under `migrations/`.
- **Embeddings Dimension**: Vector dimension is fixed per deployment via `PGVECTOR_DIM` (e.g., `1536` for OpenAI, `768` for local models). Any mismatch in dimensions between client-side embeddings and the database schema will raise a validation exception.

---

## Workflow Routing Table (Slash Commands)

- **gRPC Brain Client SDK** → [/brain-sdk](../../.agents/workflows/brain-sdk.md)
