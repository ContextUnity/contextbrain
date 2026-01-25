# ContextBrain Full Documentation

## Overview

ContextBrain is the SmartMemory and Intelligence layer of the ContextUnity ecosystem. It manages vector storage, RAG retrieval pipelines, and the Knowledge Graph.

---

## Core Components

### 1. RAG Engine
**Purpose**: High-performance retrieval of relevant context and knowledge facts.
**Key Functions**:
- Hybrid search (Vector + Full-Text)
- Mandatory re-ranking
- Context assembly for LLM agents
- Streaming retrieval results

### 2. Knowledge Store (Persistence)
**Purpose**: Durable storage for structure and semantic data.
**Stack**: PostgreSQL + pgvector / LanceDB.
**Interfaces**:
- `IRead` / `IWrite` (via ContextUnit)

### 3. Documentation Mandate
Any functional change to the gRPC contract or logic MUST be documented here first.

---

## Performance Targets
- **Vector Retrieval**: <1s
- **Reranking**: <500ms
- **Multi-tenant Isolation**: Mandatory
