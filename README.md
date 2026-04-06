# Elin

Offline AI assistant for factory maintenance and technical support.

Elin is a FastAPI-based assistant designed for air-gapped industrial environments. It combines local LLM inference, SQL analytics, vector retrieval, and hybrid reasoning to answer maintenance questions in Thai and English without relying on external APIs.

## Why This Project Exists

Manufacturing teams often have large amounts of repair history, PM data, and operational knowledge, but the data is fragmented across databases, logs, and human know-how.

Elin was built to make that information usable through a single interface that can:

- answer conversational questions
- query maintenance and PM data from local databases
- retrieve similar repair cases and knowledge snippets
- combine structured facts with semantic evidence

## Portfolio Showcase

This repository includes a portfolio-friendly static showcase page in [`portfolio/`](portfolio/README.md).

- Open [`portfolio/index.html`](portfolio/index.html) locally to view the mockup
- Use the `portfolio/` folder as a lightweight landing page for GitHub Pages or demos

## Core Capabilities

- Offline-first deployment inside a factory LAN
- Local LLM inference via Ollama
- SQL generation and execution for repair and PM analytics
- Vector search over repair logs and knowledge content
- Hybrid pipeline that combines analytics with evidence retrieval
- Async chat flow for better UI responsiveness
- Guardrails for domain filtering and safer SQL execution

## Tech Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| API backend | Python, FastAPI | Main application and orchestration |
| LLM runtime | Ollama | Local inference for chat, routing, and SQL generation |
| Chat / router models | Local Llama / Typhoon family | Conversation and routing decisions |
| SQL generation | Qwen Coder via Ollama | Natural language to SQL |
| Databases | SQLite | Repair work DB and PM DB |
| Retrieval | FAISS | Local vector search |
| Embeddings | BGE-M3 | Semantic retrieval and entity matching |
| Reranking | BGE Reranker Large | Improves retrieval quality |
| Frontend | HTML, CSS, vanilla JavaScript | Operator-facing chat UI |
| Deployment | Docker Compose | Local service orchestration |

## How Elin Works

At a high level, each request goes through a custom orchestrator:

1. Validate the request and ensure local data is ready
2. Resolve entities such as line, process, or technician names
3. Apply guardrails for out-of-domain or sensitive requests
4. Route the message into the best pipeline
5. Execute the selected pipeline
6. Format the response for chat, table, or chart output

Supported pipelines:

- `CHAT`: conversational answers
- `SQL`: structured answers from repair / PM databases
- `VECTOR`: semantic retrieval from repair logs or knowledge text
- `HYBRID`: analytics first, retrieval second
- `META`: knowledge mode for curated internal content

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the detailed flow.

## Project Structure

```text
repair-chatbot/
|-- backend/
|   |-- main.py
|   |-- core/
|   |-- pipelines/
|   |-- services/
|   |-- utils/
|   |-- scripts/
|   `-- config/
|-- frontend/
|   `-- static/
|-- portfolio/
|-- docker-compose.yml
|-- .gitignore
`-- README.md
```

## Important Source Areas

- [`backend/main.py`](backend/main.py): main FastAPI app and request orchestration
- [`backend/pipelines/llm_router.py`](backend/pipelines/llm_router.py): unified router for pipeline and database selection
- [`backend/pipelines/sql_generator.py`](backend/pipelines/sql_generator.py): SQL prompt building and SQL generation flow
- [`backend/pipelines/vector_pipeline.py`](backend/pipelines/vector_pipeline.py): vector retrieval and synthesis
- [`backend/pipelines/hybrid_pipeline.py`](backend/pipelines/hybrid_pipeline.py): hybrid analytics + retrieval reasoning
- [`backend/services/entity_matching.py`](backend/services/entity_matching.py): entity matching for line/process resolution
- [`frontend/static/js/app.js`](frontend/static/js/app.js): chat UI behavior, async flow, loading state, charts, and TTS

## Running the Project

Typical local flow:

1. Prepare local models for Ollama
2. Ensure the required factory databases are available in `backend/data/`
3. Start services with Docker Compose
4. Open the frontend in a browser on the factory LAN

Example:

```bash
docker-compose up --build
```

The exact runtime setup depends on your local environment, model availability, and factory data sources.

## Repo Hygiene

This repository intentionally ignores heavy or environment-specific runtime artifacts such as:

- local databases
- logs and QA history
- downloaded model weights
- generated vector indexes
- cached audio output

That keeps the Git history focused on source code, documentation, and reusable project assets.

## Current Positioning

Elin is best described as:

- a custom-orchestrated AI assistant
- a multi-pipeline maintenance support system
- an offline industrial AI stack
- a hybrid structured + retrieval application

It is not a generic chatbot and not a framework-first agent project.

## Limitations

- orchestration is still concentrated in `backend/main.py`
- routing accuracy depends on domain rules plus LLM behavior
- some hybrid reasoning is still heuristic-based
- test coverage is limited

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): architecture overview and pipeline breakdown
- [`portfolio/README.md`](portfolio/README.md): portfolio page notes

## Suggested Next Step

If you want to present this project professionally on GitHub:

- keep this repo as the engineering source
- use the `portfolio/` folder as the public-facing showcase layer
- add screenshots or a short demo GIF later for even stronger presentation
