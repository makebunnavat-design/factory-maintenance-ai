# Elin Architecture

## Overview

Elin uses a custom orchestration layer rather than a framework-first agent architecture.

The system is centered around a FastAPI application that receives chat requests, resolves domain context, selects a pipeline, and returns a formatted result to the frontend.

## Architecture Diagram

```text
User / Browser
  -> Frontend Chat UI
  -> FastAPI Backend
     -> Data Readiness
     -> Guardrails
     -> Entity Resolution
     -> Unified Router
        -> CHAT
        -> SQL
        -> VECTOR
        -> HYBRID
        -> META
     -> Response Formatting
     -> Frontend Render
```

## Main Runtime Components

### 1. Frontend

The frontend is a static chat interface with:

- async request flow
- loading stages
- chart and table rendering
- mode switching
- TTS integration

Primary file:

- `frontend/static/js/app.js`

### 2. FastAPI Orchestrator

The backend entry point is `backend/main.py`.

Responsibilities:

- receive synchronous and asynchronous chat requests
- ensure local data sources are available
- apply request guardrails
- run routing
- call the selected pipeline
- log results and return a structured response

### 3. Router Layer

The unified router lives in `backend/pipelines/llm_router.py`.

Responsibilities:

- detect likely pipeline
- determine target database
- rewrite query for downstream execution
- apply override rules when LLM routing is weak

This makes the system a router-based multi-pipeline assistant rather than a single-agent chatbot.

### 4. SQL Pipeline

Key files:

- `backend/pipelines/query_preprocessing.py`
- `backend/pipelines/sql_generator.py`
- `backend/main.py`

Responsibilities:

- normalize user wording
- map terms to repair or PM context
- build SQL prompts
- generate SQL with a local LLM
- execute SQL through safety checks

### 5. Vector Pipeline

Key files:

- `backend/pipelines/vector_pipeline.py`
- `backend/services/vector_search.py`
- `backend/services/reranker.py`
- `backend/services/context_compressor.py`

Responsibilities:

- retrieve similar repair logs or knowledge entries
- rerank retrieved evidence
- compress context
- synthesize a readable answer with the local chat model

### 6. Hybrid Pipeline

Key file:

- `backend/pipelines/hybrid_pipeline.py`

Responsibilities:

- run structured analytics first
- convert the analytics result into a retrieval-oriented follow-up query
- retrieve evidence
- combine facts and explanations into one answer

### 7. Meta Mode

Key files:

- `backend/services/meta_database.py`
- `backend/services/meta_vector.py`

Responsibilities:

- support curated knowledge mode
- search internal knowledge entries semantically
- synthesize answers from curated content

## Data and Model Layer

Elin uses local-only dependencies for industrial deployment:

- SQLite databases for repair and PM data
- local Ollama-hosted models for chat and SQL generation
- FAISS indexes for semantic retrieval
- local embedding and reranking models

This keeps the stack compatible with air-gapped environments.

## Why This Architecture Matters

Elin is designed around practical factory constraints:

- no external API usage
- low operational cost
- predictable behavior
- support for domain-specific language
- explainable pipeline selection

The result is a system that behaves more like a custom industrial AI workflow engine than a generic chat application.
