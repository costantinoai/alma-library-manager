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
Pick `[import]` (BibTeX/Zotero support) for everyday installs;
add `[ai]` if you want the local SPECTER2 encoder for embeddings.

=== "venv"

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -e ".[import]"          # lite-equivalent
    # or
    pip install -e ".[ai,import]"       # normal-equivalent
    ```

=== "conda / mamba"

    ```bash
    conda create -n alma python=3.11 -y
    conda activate alma
    pip install -e ".[import]"
    ```

=== "uv"

    ```bash
    uv venv
    source .venv/bin/activate
    uv pip install -e ".[import]"
    ```

### What `[ai]` adds

`[ai]` pulls `transformers`, `adapters`, `torch`, `scikit-learn`,
`umap-learn`, and `hdbscan` (~1.5 GB on disk). On Apple Silicon and
Linux x86_64 the wheels install cleanly; on Windows expect to wait
longer for `torch`. Without `[ai]`, embeddings still work — ALMa
fetches Semantic Scholar's pre-computed SPECTER2 vectors, and you
can configure OpenAI as an embedding provider from the Settings
page. What you lose is the *local* encoder for papers Semantic
Scholar doesn't have a vector for.

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

For active frontend development, run Vite and the backend in
separate terminals:

```bash
# Terminal 1 — backend with auto-reload
python -m uvicorn alma.api.app:app --reload

# Terminal 2 — Vite dev server (proxies /api/* to :8000)
cd frontend && npm run dev
```

Vite serves on `http://localhost:5173`; the backend serves API on
`http://localhost:8000`.

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
| `OPENAI_API_KEY` | Optional OpenAI embedding provider. |
| `SLACK_TOKEN` / `SLACK_CHANNEL` | Slack digest alerts. |
| `API_KEY` | Optional shared key — if set, requires `X-API-Key` on every request. |

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
