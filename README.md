# Factory Maintenance AI System

> Offline AI assistant for factory maintenance, repair analytics, and technical support.

<p align="left">
  <img src="https://img.shields.io/badge/Deployment-Offline%20LAN-0b7285?style=for-the-badge" alt="Offline LAN">
  <img src="https://img.shields.io/badge/Backend-FastAPI-0f766e?style=for-the-badge" alt="FastAPI">
  <img src="https://img.shields.io/badge/LLM-Ollama%20(Local)-1d4ed8?style=for-the-badge" alt="Ollama">
  <img src="https://img.shields.io/badge/Retrieval-FAISS%20%2B%20BGE-7c3aed?style=for-the-badge" alt="FAISS and BGE">
  <img src="https://img.shields.io/badge/Data-SQLite-92400e?style=for-the-badge" alt="SQLite">
  <img src="https://img.shields.io/badge/UI-Thai%20%2F%20English-475569?style=for-the-badge" alt="Thai and English">
</p>

## Project Summary

This project is a custom-built AI assistant designed for industrial maintenance teams working in air-gapped environments.  
It combines local LLM inference, SQL analytics, vector retrieval, and hybrid reasoning to answer operational questions in Thai and English without relying on external APIs.

This project is positioned as:

- an offline-first industrial AI system
- a multi-pipeline maintenance assistant
- a practical example of custom orchestration over local models
- a system-level case study in AI systems design

## Why This Project Stands Out

Most chatbot projects stop at conversation. This system goes further by deciding how a question should be answered:

- `CHAT` for conversational assistance
- `SQL` for factual repair / PM analytics
- `VECTOR` for similar repair-case retrieval
- `HYBRID` for analytics plus explanation
- `META` for curated internal knowledge mode

That makes the project closer to a small AI operating layer for maintenance teams than a generic assistant.

## Key Highlights

| Area | What This System Demonstrates |
| --- | --- |
| AI architecture | Custom orchestration with router-driven pipeline selection |
| Practical deployment | Works in offline factory LAN environments |
| Data reasoning | Uses generated SQL with guardrails for structured answers |
| Retrieval | Uses FAISS, embeddings, reranking, and context compression |
| UX thinking | Async request flow, loading stages, chart/table rendering, TTS |
| Domain fit | Designed around maintenance, PM, line names, process names, and repair logs |

## Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| API backend | Python, FastAPI | Request handling and orchestration |
| LLM runtime | Ollama | Local inference for chat, router, and SQL generation |
| SQL generation | Qwen Coder | Natural language to SQL |
| Chat / router models | Local Llama / Typhoon family | Chat behavior and routing decisions |
| Databases | SQLite | Repair and PM data storage |
| Retrieval | FAISS | Local semantic search |
| Embeddings | BGE-M3 | Retrieval and entity matching |
| Reranking | BGE Reranker Large | Search quality improvement |
| Frontend | HTML, CSS, vanilla JavaScript | Chat interface and presentation |
| Deployment | Docker Compose | Local service orchestration |

## Architecture Snapshot

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

For the full breakdown, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## System Showcase / Demo Layer

This repository includes a static system showcase page in [`portfolio/`](portfolio/README.md).

- Open [`portfolio/index.html`](portfolio/index.html) locally for the mockup/showcase
- Use `portfolio/` as a lightweight presentation layer for GitHub Pages or project demos

## What The User Experience Looks Like

- user asks a question in Thai or English
- backend resolves line / process / technician entities
- router selects the correct pipeline
- system queries SQL, retrieves similar cases, or combines both
- frontend renders the answer as chat, table, or chart

This is especially useful for questions like:

- "วันนี้มีอะไรเสียบ้าง"
- "เดือนนี้อันไหนเสียบ่อยที่สุด"
- "ปัญหานี้มีสาเหตุจากอะไร"
- "มีเคสคล้ายกันที่เคยซ่อมไว้ไหม"

## Important Source Areas

- [`backend/main.py`](backend/main.py): main FastAPI app and orchestration flow
- [`backend/pipelines/llm_router.py`](backend/pipelines/llm_router.py): routing and override rules
- [`backend/pipelines/sql_generator.py`](backend/pipelines/sql_generator.py): SQL prompt building and generation
- [`backend/pipelines/vector_pipeline.py`](backend/pipelines/vector_pipeline.py): vector retrieval pipeline
- [`backend/pipelines/hybrid_pipeline.py`](backend/pipelines/hybrid_pipeline.py): hybrid analytics + retrieval path
- [`backend/services/entity_matching.py`](backend/services/entity_matching.py): entity resolution for factory terms
- [`frontend/static/js/app.js`](frontend/static/js/app.js): frontend chat flow, async loading, rendering, and interaction

## Running The Project

Typical local flow:

1. prepare local models for Ollama
2. ensure required factory databases are available in `backend/data/`
3. start services with Docker Compose
4. open the frontend in a browser on the factory LAN

Example:

```bash
docker-compose up --build
```

The exact setup depends on local models, internal data availability, and deployment environment.

## Repo Hygiene

This repository intentionally ignores heavy or environment-specific artifacts such as:

- local databases
- logs and QA history
- downloaded model weights
- generated vector indexes
- cached audio output

That keeps Git history focused on source code, reusable assets, and documentation.

## Current Limitations

- orchestration is still concentrated in `backend/main.py`
- routing accuracy still depends on domain rules plus LLM behavior
- some hybrid reasoning remains heuristic-based
- automated test coverage is still limited

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): system architecture overview
- [`docs/GITHUB_PROFILE_COPY.md`](docs/GITHUB_PROFILE_COPY.md): ready-to-use GitHub profile text
- [`portfolio/README.md`](portfolio/README.md): notes for the system showcase page

## Why This Repo Is Useful In A Hiring Context

This project shows more than UI polish or single-model prompting. It demonstrates:

- backend architecture thinking
- AI workflow orchestration
- retrieval and analytics integration
- domain adaptation for industrial operations
- product-minded UX decisions for operational tools

If you are reviewing this project as a hiring manager or technical lead, the best place to start is:

1. [`README.md`](README.md)
2. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
3. [`portfolio/index.html`](portfolio/index.html)
4. [`backend/main.py`](backend/main.py)
