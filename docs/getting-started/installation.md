---
title: Installation
description: Install ALMa from source, build the frontend, and run the backend on http://localhost:8000.
---

# Installation

This page is **bare metal only**. Docker users should use
[Docker](docker.md); they do not need to install Python packages, Node
packages, or a virtual environment on the host.

## 1. Clone

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
```

## 2. Python environment

ALMa is one Python package (`alma`) installable in editable mode.

=== "venv"

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -e .
    ```

=== "conda / mamba"

    ```bash
    conda create -n alma python=3.11 -y
    conda activate alma
    pip install -e .
    ```

=== "uv"

    ```bash
    uv venv
    source .venv/bin/activate
    uv pip install -e .
    ```

### Optional: AI extras

The AI stack (embeddings, LLMs, SPECTER2) is opt-in:

```bash
pip install -e ".[ai]"
```

This pulls `transformers`, `adapters`, `torch`, `sentence-transformers`,
`scikit-learn`, `umap-learn`, and `hdbscan`. On Apple Silicon and
Linux x86_64 the wheels install cleanly; on Windows expect to wait
longer for `torch`.

You can install AI extras later — ALMa will detect them and light up
the matching settings.

## 3. Frontend build

The React SPA is committed as source; it has to be built once before
the backend can serve it.

```bash
cd frontend
npm install
npm run build      # writes to frontend/dist
cd ..
```

For active frontend development, use `npm run dev` instead and let
Vite serve on `http://localhost:5173` while the backend serves API on
`http://localhost:8000`. The provided
`scripts/start-dev.sh` orchestrates both processes.

## 4. Configuration

ALMa reads configuration from two places:

* `settings.json` at the repo root — runtime preferences (auto-created
  on first run with sensible defaults).
* `.env` at the repo root — secrets (API keys). Copy from `.env.example`:

```bash
cp .env.example .env
chmod 600 .env
$EDITOR .env
```

The most useful keys to set:

| Variable | Purpose |
|---|---|
| `OPENALEX_EMAIL` | Joins the [polite pool](https://docs.openalex.org/how-to-use-the-api/api-overview#the-polite-pool). Set this. |
| `OPENALEX_API_KEY` | Optional, for higher quotas. |
| `SEMANTIC_SCHOLAR_API_KEY` | Improves S2 batch/related rate limits. |
| `OPENAI_API_KEY` | For OpenAI embeddings or LLM. |
| `ANTHROPIC_API_KEY` | For Claude-backed LLM features. |
| `SLACK_TOKEN` / `SLACK_CHANNEL` | Slack digest alerts. |

See the [configuration reference](../reference/configuration.md) for
the complete list.

## 5. Run

```bash
python -m uvicorn alma.api.app:app --reload --port 8000
```

Open <http://localhost:8000>. The frontend SPA is served from the
same port as the API.

For production, drop `--reload` and consider running behind a reverse
proxy. See [Deployment](../operations/deployment.md).

## 6. CLI

A small CLI is also installed:

```bash
alma --help
```

It exposes the same operations the API does — useful for cron
scripts, backups, and one-off debugging.

## Next steps

* [First-run checklist](first-run.md) — point ALMa at OpenAlex,
  follow your first author, sanity-check the Activity panel.
* [Vision & philosophy](../vision.md) — read this once; it makes the
  rest of the UI map cleanly.
