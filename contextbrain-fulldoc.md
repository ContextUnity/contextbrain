# ContextBrain: Knowledge Store

## Overview
ContextBrain is the sovereign **Semantic Knowledge Store** for the ecosystem. It provides Vector Search, Graph Traversal, and Taxonomy management.

## Architecture
- **Database**: PostgreSQL (with `pgvector`, `ltree`).
- **Data Model**:
    - `knowledge_nodes`: Vectors + Text Chunks.
    - `knowledge_edges`: Semantic Graph.
    - `knowledge_aliases`: Entity Synonyms.
- **Ingestion**:
    - Cognee Integration for Knowledge Graph extraction.
    - ETL pipeline for Files/Web/DB.

## Interfaces
- **gRPC**: `contextcore/protos/brain.proto`
    - `Search(query)`: Hybrid retrieval.
    - `GraphSearch(entrypoints)`: Graph expansion.

## Key Files
- `src/contextbrain/storage/postgres/schema.py`: Database tables.
- `src/contextbrain/ingestion/`: ETL logic.
