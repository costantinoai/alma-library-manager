# ALMa — Another Library Manager

> 🚧 **Early preview (`0.9.0`).** Public testing release. The three core
> jobs (Library / Discovery / Feed) work end-to-end. Pre-built Docker
> images are available on GHCR as `:0.9.0` for both variants — see
> [Option A](#option-a--pull-a-pre-built-image-from-ghcr-suggested)
> below. The first-run experience is bare; polished in-app
> onboarding ships with `1.0.0`. Read [Getting started after install](#getting-started-after-install)
> before launching for the first time.

Your personal academic research feed. ALMa tracks authors and publications
via [OpenAlex](https://openalex.org/) (primary) + [Semantic Scholar](https://www.semanticscholar.org/)
(metadata + SPECTER2 vectors), with Google Scholar (`scholarly`) as an
opt-in best-effort author-resolution fallback. Discovery candidates are
ranked with SPECTER2 embeddings; saved papers live in your local
SQLite library — all from a single, self-hosted web UI.

---

## Documentation

Full documentation lives at **<https://costantinoai.github.io/alma-library-manager/>**
(or build it locally with `pip install -e ".[docs]"` then `mkdocs serve`).

* [Vision & philosophy](https://costantinoai.github.io/alma-library-manager/vision/) — read this once
* [Getting started](https://costantinoai.github.io/alma-library-manager/getting-started/) — install + first run
* [Concepts](https://costantinoai.github.io/alma-library-manager/concepts/) — Feed, Library, Discovery, Authors, Insights, Alerts
* [REST API reference](https://costantinoai.github.io/alma-library-manager/reference/api/) — every endpoint

---

## Features

| Area | What it does |
|------|-------------|
| **Feed** | Chronological inbox of new publications from monitored authors, topics, and queries (60-day window, dedup across sources) |
| **Discovery** | Lens-based recommendations using a multi-source retrieval lane (OpenAlex + Semantic Scholar + citation chain + co-author graph + SPECTER2 similarity), with branch studio for sub-lens exploration |
| **Authors** | Multi-source author suggestions (D12: library_core + library_reference + semantic_similar + openalex_related + s2_related + cited_by_high_signal) with hierarchical identity resolver and preprint↔journal twin engine |
| **Library** | Save, rate (like / love / dislike), reading-list workflow, tag, organize into collections; BibTeX, Zotero JSON, and Zotero RDF import — imports land directly in Saved (D4) |
| **Insights** | Publication timeline, geography, topics, journals, author rails, recommendation engagement, and a clustered SPECTER2 knowledge graph with auto-k clusters + word-cloud overlay |
| **Alerts** | Composable rule sets (`author`, `keyword`, `topic`, `similarity`, `discovery_lens`) delivered as Slack digests on manual / daily / weekly schedules |
| **Settings** | OpenAlex / S2 / Slack credentials, discovery weights, AI provider environment, corpus maintenance (refresh authors with scope, dedup preprint↔journal twins), Activity log |

### AI capabilities (opt-in)

- **Paper embeddings**: SPECTER2 is canonical (`allenai/specter2_base`).
  ALMa fetches Semantic Scholar's pre-computed `specter_v2` vectors when
  available; missing papers can be computed locally on demand. OpenAI
  embeddings remain available as an optional provider.
- **Clustering**: HDBSCAN over SPECTER2 vectors with auto-k via
  silhouette sweep, projected to 2D for the Insights graph.

AI dependencies run in an isolated Python environment (venv / uv /
conda) selected from Settings — they never touch your system Python.

### Performance

| Operation | Budget |
|---|---|
| Discovery lens refresh (canonical lens, ~330 saved papers) | **~76 s** end-to-end (down from 298 s) |
| Backend page-mount reads (`/library/saved`, `/feed`, `/authors`) | < 1 s P95 under concurrent refresh |
| Frontend tsc check | < 30 s |

---

## Quick start

**Docker is the recommended way to run ALMa.** It pins the Python
runtime, the AI stack, and all native dependencies into a single
reproducible image, and bind-mounts your data from the host so
nothing personal is baked in. Bare metal installs are supported but
fragile (torch / transformers / hdbscan have heavyweight native
deps); use Docker unless you have a specific reason not to.

### Choosing a variant — `normal` vs `lite`

ALMa ships in two image flavours:

| | `normal` (default) | `lite` |
|---|---|---|
| Library / Discovery / Feed (the three core jobs) | ✅ | ✅ |
| BibTeX + Zotero imports | ✅ | ✅ |
| Authors monitoring + ORCID dedup | ✅ | ✅ |
| OpenAlex enrichment + Semantic Scholar vectors | ✅ | ✅ |
| HDBSCAN / KMeans clustering (Discovery branches) | ✅ | ✅ |
| UMAP / t-SNE projections (Insights Graph tab) | ✅ | ✅ |
| TF-IDF text similarity (similarity fallback) | ✅ | ✅ |
| Cloud OpenAI embeddings (set provider in Settings) | ✅ | ✅ |
| **Local SPECTER2 embeddings** (no API needed) | ✅ | ❌ (no torch) |
| Image size | ~2 GB | ~450 MB |
| Memory at runtime | ~2 GB peak (model loaded) | ~512 MB-1 GB |
| Recommended host | desktop / server, ≥4 GB RAM | Raspberry Pi / NAS / VPS |

The only thing `lite` actually loses is the **local** embedding
encoder — `transformers` + `adapters` + `torch` is ~1.5 GB and
mostly impractical on a Pi. Discovery branches, the Insights Graph,
and TF-IDF fallback all stay because their deps (`scikit-learn`,
`hdbscan`, `umap-learn`) are foundational and ship ARM wheels.

In `lite` you can still get embeddings by either (a) configuring
the OpenAI cloud provider in Settings, or (b) relying on the
Semantic Scholar vectors that ALMa fetches automatically for any
paper with a known DOI — most of your library will land with
vectors via this path. Local SPECTER2 is the unique normal-only
feature.

Pick `lite` only if you intend to run on a memory-constrained host
(or you're happy delegating embeddings to a cloud provider).

---

### Option A — pull a pre-built image from GHCR (suggested)

Pre-built images are published to GitHub Container Registry on every
release tag. The current public-testing tag is `0.9.0`.

```bash
# normal variant (full AI stack)
docker pull ghcr.io/costantinoai/alma-library-manager:0.9.0

# lite variant (no torch; for Raspberry Pi / low-memory hosts)
docker pull ghcr.io/costantinoai/alma-library-manager:0.9.0-lite

# Once 1.0.0 ships, `:latest` and `:latest-lite` will track the
# newest stable release.
```

Then drop a minimal `docker-compose.yml` next to your data:

```bash
mkdir alma && cd alma
mkdir -p data config
touch .env settings.json
chmod 600 .env
# edit .env — at minimum set OPENALEX_EMAIL (free) and any optional
# API keys
```

Use the [docker-compose.yml from this repo](./docker-compose.yml) as
a template, replacing the `build:` block with `image:`:

```yaml
services:
  alma:
    image: ghcr.io/costantinoai/alma-library-manager:0.9.0          # or :0.9.0-lite
    container_name: alma
    restart: unless-stopped
    ports: ["127.0.0.1:8000:8000"]
    env_file: [.env]
    volumes:
      - type: bind
        source: ./data
        target: /app/data
      - type: bind
        source: ./config
        target: /app/config
      - type: bind
        source: ./settings.json
        target: /app/settings.json
      - type: bind
        source: ./.env
        target: /app/.env
```

Then `docker compose up -d` and open [http://localhost:8000](http://localhost:8000).

First steps in the app:

1. Confirm OpenAlex email in **Settings → External APIs**.
2. Follow 3-5 authors from **Discovery → Find & Add**.
3. Wait for backfills in **Activity**.
4. Save / like a few papers in **Feed**.
5. Optionally import BibTeX or Zotero data from **Library → Imports**.
6. Refresh the default Discovery lens.

### Option B — build locally with Docker

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
cp .env.example .env          # then edit and add your API keys
chmod 600 .env

mkdir -p data config
[ -f settings.json ] || cp settings.example.json settings.json

# normal variant (default)
docker compose up -d

# lite variant
ALMA_VARIANT=lite docker compose up -d

# constrained host (e.g. Raspberry Pi 4 with 4 GB RAM)
ALMA_VARIANT=lite ALMA_CPUS=2.0 ALMA_MEMORY=1G docker compose up -d
```

Open [http://localhost:8000](http://localhost:8000).

Lifecycle:

```bash
docker compose logs -f alma                       # follow logs
docker compose ps                                 # status + healthcheck
docker compose down                               # stop + remove (host data persists)
docker compose build --no-cache                   # rebuild normal
ALMA_VARIANT=lite docker compose build --no-cache # rebuild lite
```

The container binds to `127.0.0.1:8000` only — put a reverse proxy
in front for remote access. Personal state (`.env`, `settings.json`,
`data/`, `config/`) is bind-mounted from the host and **never baked
into the image**, so you can rebuild or pull a fresh image without
touching your library.

---

### Option C — bare metal (advanced, **not recommended**)

Skip Docker only if you have a specific reason. The AI stack
(`torch`, `transformers`, `hdbscan`, `umap-learn`) has heavyweight
native deps that are easy to mismatch in unmanaged Python
environments — Docker pins everything for you.

If you must:

**Prerequisites**
- Python 3.11+
- Node.js 20+ (for the frontend build)
- A clean virtual environment (`python -m venv` / `conda` / `uv`)

**Install**

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
python -m venv .venv
source .venv/bin/activate

# Lite-equivalent: core deps only
pip install -e ".[import]"

# OR Normal-equivalent: core + AI stack
pip install -e ".[ai,import]"
```

**Build the frontend**

```bash
(cd frontend && npm ci && npm run build)
```

**Run**

```bash
cp .env.example .env          # edit + chmod 600
mkdir -p data config
[ -f settings.json ] || cp settings.example.json settings.json

uvicorn alma.api.app:app --port 8000
```

Open [http://localhost:8000](http://localhost:8000).

---

## Getting started after install

ALMa is empty on first launch — no library, no followed authors, no
recommendations. The three things to do (in order) before the app
becomes useful:

1. **Set your OpenAlex contact email.** Edit `.env` and add
   `OPENALEX_EMAIL=you@example.com`. OpenAlex is free, no key needed,
   but the email enrolls you in the [polite pool](https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication)
   so your requests don't get rate-limited under load.
2. **Follow 3-5 authors.** Go to **Discovery → Find & Add**, switch
   the scope toggle to **Author**, and search by name (or paste an
   ORCID / OpenAlex ID). Click follow on the matches you want. Each
   follow kicks off a background backfill that pulls their recent
   papers into your corpus.
3. **Import your own work (optional but recommended).** Go to
   **Library → Imports**, upload a BibTeX file or a Zotero export, or
   paste a list of DOIs. These become the seed for the Discovery
   ranker.
4. **Wait one refresh cycle.** The Feed inbox and Discovery
   recommendations both populate from the backfill. You can force a
   refresh from the Feed page (top-right) and from any lens in
   Discovery.
5. **Save / like / dismiss to teach the ranker.** Every action writes
   a feedback signal. The more you triage, the better the next round
   of recommendations.

Polished onboarding (Apple-style first-run flow) lands with v1.0.0.

---

## Configuration

All settings live in `settings.json` (auto-created on first run).
Environment variables override file settings:

| Variable | Purpose |
|----------|---------|
| `DB_PATH` | Path to the SQLite database (default: `./data/scholar.db`) |
| `OPENALEX_EMAIL` | Contact email for OpenAlex polite pool |
| `OPENALEX_API_KEY` | OpenAlex API key (required for production use) |
| `SLACK_TOKEN` | Slack Bot User OAuth Token |
| `SLACK_CHANNEL` | Default Slack notification channel |
| `API_KEY` | Optional API key to protect the REST API |

---

## Project structure

```
alma/
├── src/alma/       # Python backend
│   ├── api/               #   FastAPI app, routes, models, deps
│   ├── ai/                #   Embedding providers, clustering, tagging
│   ├── discovery/         #   Recommendation engine, similarity
│   ├── library/           #   Import, enrichment, deduplication
│   ├── openalex/          #   OpenAlex API client
│   ├── core/              #   Shared utilities
│   └── config.py          #   Centralized configuration
├── frontend/              # React 19 + Vite + TypeScript + Tailwind
│   └── src/
│       ├── pages/         #   Feed, Discovery, Authors, Library, ...
│       ├── components/    #   Shared UI components
│       └── api/           #   API client and types
├── tests/                 # pytest test suite
├── docs/                  # Architecture and API documentation
├── settings.json          # Runtime configuration
└── pyproject.toml         # Python project metadata
```

---

## Tech stack

- **Backend**: Python 3.11+, FastAPI, SQLite (WAL mode), APScheduler
- **Frontend**: React 19, Vite 6, TypeScript, Tailwind CSS 4, shadcn/ui
- **Data**: OpenAlex (primary), Semantic Scholar (`/paper/batch`,
  related-works, related-authors), Crossref + arXiv + bioRxiv
- **AI** (opt-in): SPECTER2 via Semantic Scholar / local
  `transformers` + `adapters`, optional OpenAI embeddings, HDBSCAN, UMAP

---

## License

This project is licensed under
[CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).

- **Personal use**: free
- **Academic use**: free, but you **must cite** this software in any
  resulting publications
- **Commercial use**: **not permitted**

See [LICENSE](LICENSE) for the full text and citation format.

---

## Citation

If you use ALMa in academic work, please cite:

```bibtex
@software{costantino2026alma,
  author    = {Costantino, Andrea Ivan},
  title     = {{ALMa} --- {A}nother {L}ibrary {M}anager},
  year      = {2026},
  url       = {https://github.com/costantinoai/alma-library-manager},
  license   = {CC-BY-NC-4.0}
}
```

---

## Author

**Andrea Ivan Costantino**
KU Leuven — andreaivan.costantino@kuleuven.be
