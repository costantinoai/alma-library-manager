# ALMa — Another Library Manager

> **Early preview (`v0.9.1`).** The three core jobs — Library, Discovery,
> and Feed — work end-to-end. The first-run experience is bare; a polished
> onboarding ships with `v1.0.0`. Public testing welcome.

ALMa is a personal academic research feed. You follow a handful of
authors and topics; ALMa watches OpenAlex (the open citation graph)
and Semantic Scholar for new work, builds a local SQLite library of
the things you save, and uses SPECTER2 embeddings to surface papers
related to what you already care about. It runs on your own machine —
nothing about your reading list leaves the box you put it on.

The app has five views: a chronological **Feed** of new publications
from the people you follow, a **Library** for the things you've saved
and the notes you've taken on them, **Authors** for the people whose
work you track, **Discovery** for "things related to your library that
aren't in your feed yet", and **Insights** for a 30,000-foot view of
your collection (timelines, topics, a clustered map of what you read).

Full docs: **<https://costantinoai.github.io/alma-library-manager/>**

---

## Quick start (Docker)

The fastest way to run ALMa, and what you should pick unless you have
a strong reason not to. Docker pins Python, the AI stack, and every
native dependency into one image so you don't have to.

```bash
mkdir alma && cd alma
mkdir -p data config
touch .env settings.json
chmod 600 .env
```

Drop a `docker-compose.yml` next to those files:

```yaml
services:
  alma:
    image: ghcr.io/costantinoai/alma-library-manager:0.9.1
    container_name: alma
    restart: unless-stopped
    ports: ["127.0.0.1:8000:8000"]
    env_file: [.env]
    volumes:
      - ./data:/app/data
      - ./config:/app/config
      - ./settings.json:/app/settings.json
      - ./.env:/app/.env
```

Then bring it up and open `http://localhost:8000`:

```bash
docker compose up -d
```

That's it. The container binds to `127.0.0.1` only; if you want to
expose ALMa beyond your own machine, put a reverse proxy in front and
set `API_KEY` in `.env`.

Your data lives in the host folder you just created (`./data`,
`./config`, `./settings.json`, `./.env`). Nothing personal goes into
the image, so you can pull a newer version any time without losing
your library.

---

## After it starts

ALMa is empty on first launch — no library, no followed authors, no
recommendations. Three things to do, in order, before it becomes
useful:

1. **Add your email.** Edit `.env` and set `OPENALEX_EMAIL=you@example.com`.
   OpenAlex is free, no API key needed, but the email enrolls you in
   their polite pool so requests don't hit anonymous rate limits. (You
   can also set this from **Settings → External APIs** in the UI.)

2. **Follow a few authors.** Open **Discovery → Find & Add**, switch
   the scope toggle to **Author**, and search by name (an ORCID or
   OpenAlex ID also works if you have one). Pick three to five people
   whose work you actually want to track. Each follow kicks off a
   background backfill that pulls their recent papers into your
   corpus — you'll see it run under **Activity**.

3. **Wait one refresh.** Once the backfills finish, the **Feed**
   surfaces new papers from those authors and **Discovery**
   recommends related work. Save, like, or dismiss as you go — every
   action teaches the ranker what you actually care about.

Optional: if you have a BibTeX file or a Zotero library, import it
from **Library → Imports**. Existing references give Discovery much
better seed material from day one.

---

## Two image variants

ALMa publishes two flavours of the Docker image. Both run the full
app — Library, Discovery, Feed, Authors, Insights, the Insights graph,
clustering, BibTeX/Zotero imports. They differ only in whether the
local SPECTER2 encoder is bundled.

**`normal`** (the default, `:0.9.1`) includes `torch` + `transformers`,
so SPECTER2 embeddings can be computed locally on demand. Image size
is around 1.4 GB, peak runtime memory ~2 GB. Pick this on a desktop
or server with at least 4 GB RAM.

**`lite`** (`:0.9.1-lite`) drops `torch`. Image size is around
1.2 GB, runtime memory ~1 GB. You still get full embeddings via
Semantic Scholar's pre-computed SPECTER2 vectors (most papers with a
DOI have one) and you can configure OpenAI as the embedding provider
from Settings if you want. Pick this on a Raspberry Pi or a smaller
host where 1.5 GB of torch on disk is precious.

Both variants are published for `linux/amd64` and `linux/arm64`, so
Apple Silicon Macs and ARM servers (Pi, Graviton, Ampere) get native
images.

---

## Bare metal install (advanced)

Skip Docker only if you have a specific reason — the AI stack
(`torch`, `transformers`, `hdbscan`, `umap-learn`) has heavy native
dependencies that are easy to mismatch in unmanaged Python
environments.

You'll need Python 3.11+ and Node 20+. From a clean virtualenv:

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
python -m venv .venv && source .venv/bin/activate

# Lite-equivalent
pip install -e ".[import]"
# Or normal-equivalent (with the AI stack)
pip install -e ".[ai,import]"

(cd frontend && npm ci && npm run build)

cp .env.example .env  # add your OpenAlex email
mkdir -p data config

uvicorn alma.api.app:app --port 8000
```

Open `http://localhost:8000`.

---

## Configuration in one paragraph

Most settings — Discovery weights, AI provider, clustering knobs —
live in the SQLite database and are tuned from the **Settings** page.
The `.env` file holds secrets (API keys, Slack tokens) and a few
deployment knobs (`DB_PATH`, `API_KEY`). The committed
`.env.example` is a fully-commented template; copy it to `.env` and
fill in what you have. The full reference is in
[docs/reference/configuration.md](docs/reference/configuration.md).

---

## Tech stack

Python 3.11 + FastAPI + SQLite (WAL) + APScheduler on the backend.
React 19 + Vite + TypeScript + Tailwind + shadcn/ui on the frontend.
Data comes from OpenAlex (primary), Semantic Scholar, Crossref, arXiv,
and bioRxiv. Embeddings are SPECTER2 (local via `transformers` +
`adapters`, or remote via Semantic Scholar / OpenAI). Clustering is
HDBSCAN over those vectors with UMAP for the 2D Insights graph.

---

## License

Licensed under [PolyForm Noncommercial 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/) —
a software-specific source-available license. Personal use, academic
research, hobby projects, and use by nonprofits or educational
institutions are all permitted. Commercial use is not. Attribution
(this `LICENSE` file and the copyright notice) must be preserved in
copies and derivative works. See [LICENSE](LICENSE) for the full text.

---

## Author

**Andrea Ivan Costantino** · [github.com/costantinoai](https://github.com/costantinoai)
