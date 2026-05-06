# ALMa — **A**nother **L**ibrary **Ma**nager

> **Early preview (`v0.10.3`).** The three core jobs — Library, Discovery,
> and Feed — work end-to-end. The first-run experience is bare; a polished
> onboarding ships with `v1.0.0`. Public testing welcome.

ALMa watches [OpenAlex](https://openalex.org/) (the open citation
graph) and Semantic Scholar for new work from authors and topics you
follow, builds a local SQLite library of the things you save, and uses
SPECTER2 embeddings to surface papers related to what you already care
about. It runs on your own machine — nothing about your reading list
leaves the box you put it on.

**Documentation:** <https://costantinoai.github.io/alma-library-manager/>

The app has five views:

- **Feed** — a chronological inbox of new publications from the
  authors and topics you follow.
- **Library** — every paper you've saved, with notes, ratings, tags,
  collections, and a reading list.
- **Authors** — the researchers you track, plus suggested authors
  whose work overlaps with what you read.
- **Discovery** — papers related to your library that haven't shown
  up in the Feed yet, ranked by topical and citation similarity.
- **Insights** — charts and a clustered map of your library: how it's
  spread across years, topics, journals, and which papers cluster
  together by content.

<p align="center">
  <img src="docs/screenshots/desktop-library.png" alt="Library" width="49%">
  <img src="docs/screenshots/desktop-discovery.png" alt="Discovery" width="49%">
</p>
<p align="center">
  <img src="docs/screenshots/desktop-insights.png" alt="Insights" width="49%">
  <img src="docs/screenshots/desktop-feed.png" alt="Feed" width="49%">
</p>
<p align="center">
  <img src="docs/screenshots/desktop-authors.png" alt="Authors" width="49%">
  <img src="docs/screenshots/desktop-settings.png" alt="Settings" width="49%">
</p>

---

## Quick start

The suggested install is one `docker run` line that pulls the
prebuilt image from GitHub Container Registry — no clone, no build,
no compose file. Three paths, pick the one that matches your
hardware. Replace `you@example.com` with the email you want to
identify yourself to OpenAlex (free, no signup required).

### Most laptops / desktops / servers (CPU)

```bash
docker run -d --name alma --restart unless-stopped \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest
```

Open <http://localhost:8000>. That's the whole install. Local
embedding compute (the SPECTER2 encoder) runs on CPU — fine for
ongoing use; the only time you'll notice it is when you mass-recompute
embeddings on a large library (thousands of papers without S2
vectors). Most papers come with pre-computed Semantic Scholar vectors
already, so day-to-day you rarely hit the local encoder.

### NVIDIA GPU host (faster embedding compute)

One-time host setup — install the
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/)
so Docker can pass the GPU through. On Ubuntu / Debian:

```bash
sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
```

On Fedora / RHEL replace `apt-get` with `dnf`. Then run the GPU image:

```bash
docker run -d --name alma --restart unless-stopped --gpus all \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest-gpu
```

The `-gpu` tag ships the CUDA torch wheel (~3.2 GB image) so SPECTER2
inference uses the GPU. Confirm with
`docker exec alma python -c "import torch; print(torch.cuda.is_available())"`
— it should print `True`.

### Raspberry Pi or other low-resource host

```bash
docker run -d --name alma --restart unless-stopped \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest-lite
```

Drops `torch` entirely (~1.2 GB image, ~1 GB runtime memory). You
still get full embeddings via Semantic Scholar's pre-computed
SPECTER2 vectors, and you can configure OpenAI as the embedding
provider from Settings if you want.

### Auto-restart and updates

`--restart unless-stopped` already auto-restarts the container on
crashes, Docker daemon restarts, and host reboots — you don't have to
do anything for that.

To pull the latest image and restart:

```bash
docker pull ghcr.io/costantinoai/alma-library-manager:latest   # or :latest-gpu / :latest-lite
docker rm -f alma
# rerun the same `docker run` command — your data lives in the named volumes
```

To automate updates daily, drop in [Watchtower](https://containrrr.dev/watchtower/):

```bash
docker run -d --name watchtower --restart unless-stopped \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower alma --cleanup --interval 86400
```

That polls once a day, pulls a new image when `:latest` changes,
restarts the container, and removes the old image layer.

> **More to configure?** Add `-e API_KEY=your-key` to require an
> `X-API-Key` header on every request, `-e SLACK_TOKEN=…` for Slack
> digests, etc. Full env-var reference:
> [docs/reference/configuration.md](docs/reference/configuration.md).

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

## Backups

ALMa's source of truth is the local SQLite database, so keep backups
boring and regular. Use **Settings → Data & system → Library
management → Create backup** before risky maintenance, and schedule a
weekly online backup with `POST /api/v1/library-mgmt/backup` for
always-on installs. For off-machine safety, copy the full `data/`
directory, `settings.json`, and `.env` monthly. See
[docs/operations/backups.md](docs/operations/backups.md) for restore
steps and the verification checklist.

---

## Three image flavors

ALMa publishes three flavors of the Docker image. All of them run the
full app — Library, Discovery, Feed, Authors, Insights, the Insights
graph, clustering, BibTeX/Zotero imports. They differ only in the
bundled embedding stack and image size.

**`:latest`** (the default `normal` CPU build) includes `torch` +
`transformers` so SPECTER2 embeddings can be computed locally on
demand. Image size ~1.4 GB, peak runtime memory ~2 GB. The default
because it's small and works everywhere — pick this on a desktop or
server without an NVIDIA GPU.

**`:latest-gpu`** is the `normal` flavor built against the CUDA torch
wheel (~3.2 GB image). Use this when the host has an NVIDIA GPU
exposed to Docker via the
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/);
SPECTER2 inference then runs on the GPU instead of the CPU. The same
image still works without a GPU — `torch.cuda.is_available()` simply
returns `False` and the encoder falls back to CPU. Available for
`linux/amd64` only.

**`:latest-lite`** drops `torch` entirely (~1.2 GB image, ~1 GB
runtime). You still get full embeddings via Semantic Scholar's
pre-computed SPECTER2 vectors (most papers with a DOI have one) and
you can configure OpenAI as the embedding provider from Settings.
Pick this on a Raspberry Pi or any host where 1.5 GB of torch on
disk is precious.

The CPU + lite flavors are published for `linux/amd64` and
`linux/arm64`. The GPU flavor is `amd64`-only.

---

## Build from source with Docker Compose (alternative)

If you'd rather build the image from this checkout, run ALMa
alongside other compose-managed services, or have a host folder you
can browse directly instead of named volumes:

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
cp .env.example .env             # add OPENALEX_EMAIL=you@example.com
docker compose up -d             # CPU build (default)
```

GPU build with passthrough — needs the
[NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/)
installed on the host (see the GPU box in the quick start above):

```bash
ALMA_TORCH_VARIANT=cuda \
  docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --build
```

Lite build:

```bash
ALMA_VARIANT=lite docker compose up -d --build
```

Update with `docker compose pull && docker compose up -d`. Your data
lives in `./data` next to the compose file; nothing personal is
baked into the image.

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
