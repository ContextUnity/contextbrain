# ContextBrain — Agent instructions

Knowledge layer: vector databases, knowledge graphs, taxonomy hierarchies, data ingestion pipelines, and episodic agent memory storage.

**License Context**: This service operates under the **Apache 2.0 Open-Source License**.

## Navigation & Entry
- **Workspace**: `services/contextbrain/`
- **Application Execution**: Run the gRPC server via `python -m contextbrain`.
- **Tests**: run `uv run --package contextbrain pytest` from the monorepo root.

## Architecture Context (Current Code State)
- **Data Stores (`storage/`)**: Integrates natively with `pgvector` inside PostgreSQL. It manages:
  - `knowledge_nodes`: Raw embeddings and text.
  - `taxonomy_nodes`: Hierarchical categories using the `ltree` extension.
  - `conversation_episodes`: Agent memory histories.
- **Data Ingestion (`ingestion/`)**: Built-in ETL pipelines executing chunking and embeddings automatically before inserting into the storage backends.
- **Intelligence Modules (`modules/`)**: High-level semantic actions for routing (e.g., `taxonomy.py` for semantic category classification).
- **gRPC Services (`service/`)**: Exposes Brain endpoints (`Search`, `Upsert`, `AddEpisode`, `GetRecentEpisodes`) governed by `BrainPermissionInterceptor`, ensuring `ContextToken` claims are strictly validated prior to DB access.

## Documentation Strategy
When modifying or extending this service, update documentation strictly across these boundaries:
1. **Technical Specifications**: `services/contextbrain/contextbrain-fulldoc.md`. Update this when changing database schemas (`storage/`), adding new ingestion sources, or changing gRPC signatures.
2. **Public Website**: `docs/website/src/content/docs/brain/`. For conceptual discussions on Vector similarity or RAG architecture.
3. **Plans & Architecture**: `plans/brain/`.

## Rules specific to ContextBrain
- Any direct interaction with PostgreSQL MUST be conducted through the abstracted layers in `storage/`, never through naked SQL execution outside of dedicated repositories.
- `ContextToken` validation is non-negotiable. Do not expose `storage/` methods via gRPC without ensuring the `TokenValidationInterceptor` and `BrainPermissionInterceptor` are active.
- Rely on `contextcore` for `ContextUnit` protobuf manipulation.


## AI Agent Rules (`rules/`)
ContextUnity uses strict AI assistant rules. You **MUST** review and adhere to the following rule files before modifying this service:
- `rules/global-rules.md` (General ContextUnity architecture and boundaries)
- `rules/contextbrain-rules.md` (Specific constraints for the **contextbrain** domain)
