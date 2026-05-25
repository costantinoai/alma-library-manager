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

**One line, any OS.** The installer checks Docker, picks the right
image variant for your hardware (auto-detects NVIDIA GPU and
Raspberry Pi), prompts for your OpenAlex email, and starts ALMa with
named Docker volumes so your library survives upgrades. **Prerequisite:**
[Docker](https://docs.docker.com/get-docker/) installed and running.

### Linux / macOS

```bash
curl -sSL https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.sh | bash
```

### Windows (PowerShell)

```powershell
irm https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.ps1 | iex
```

Open <http://localhost:8000>. To update later, re-run the same
command — the installer detects an existing install and pulls the
latest image.

> Power users — three things you can do instead of the one-liner:
> [run the `docker run` command by hand](#manual-docker-run-pick-your-image-flavor)
> (no script), use [Docker Compose](#run-with-docker-compose-alternative)
> (build locally or pull from GHCR with extra hardening + bind
> mounts), or fall back to a [bare-metal Python install](#bare-metal-install-advanced--not-recommended)
> (not recommended unless you're developing on ALMa).

### What the installer does

1. Confirms Docker is installed and the daemon is running.
2. Picks an image variant:
   - **NVIDIA GPU + Container Toolkit** detected → `:latest-gpu` (CUDA torch, ~3.2 GB image, fastest local SPECTER2 inference).
   - **Raspberry Pi / armv7** detected → `:latest-lite` (no torch, ~1.2 GB image, ~1 GB runtime).
   - **Anything else** → `:latest` (CPU torch, ~1.4 GB image, ~2 GB runtime). Fine for ongoing use — most papers ship pre-computed Semantic Scholar vectors so the local CPU encoder is rarely hit.
3. Prompts for your OpenAlex polite-pool email (free, no signup — just identifies you so you don't share anonymous rate limits).
4. Pulls the image and runs the container with `--restart unless-stopped` (auto-restarts on crashes + at boot) and named volumes for your data.

Override the image tag with the `ALMA_IMAGE_TAG` env var if you want
to force a specific variant (e.g. `ALMA_IMAGE_TAG=0.12.1`).

### Manual `docker run` (pick your image flavor)

If you'd rather skip the script and copy-paste a one-liner:

**CPU (default):**
```bash
docker run -d --name alma --restart unless-stopped \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest
```

**NVIDIA GPU** (host needs the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/)
installed):
```bash
docker run -d --name alma --restart unless-stopped --gpus all \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest-gpu
```

**Raspberry Pi / lite:**
```bash
docker run -d --name alma --restart unless-stopped \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest-lite
```

On Windows PowerShell, replace the trailing `\` line continuations with backticks (`` ` ``).

### Auto-restart and updates

`--restart unless-stopped` already auto-restarts the container on
crashes, Docker daemon restarts, and host reboots — you don't have to
do anything for that.

To update, the easiest path is to **re-run the installer** — it
detects an existing install, pulls the latest image, and recreates
the container. Your data is in named volumes, so nothing is lost:

```bash
# Linux / macOS
curl -sSL https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.sh | bash

# Windows
irm https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.ps1 | iex
```

Manual equivalent if you prefer:

```bash
docker pull ghcr.io/costantinoai/alma-library-manager:latest   # or :latest-gpu / :latest-lite
docker rm -f alma
# rerun your original `docker run` command — your data lives in the named volumes
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

## Exposing on your network

By default ALMa binds to `127.0.0.1` — it's reachable only from
`http://localhost:8000` on the machine running it. In single-user
mode ALMa has **no authentication**, so this is the deliberate, secure
default: nothing is offered to your network or the internet.

For a **headless always-on instance — e.g. a Raspberry Pi you run 24/7
and open from your laptop** — you'll want it reachable on your LAN.
Both the installer and the compose file honour a `BIND_ADDR` variable:

```bash
# Installer (the script passes BIND_ADDR through to the container)
BIND_ADDR=0.0.0.0 bash setup.sh

# Docker Compose
BIND_ADDR=0.0.0.0 docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d

# Plain docker run — put the host IP in the -p flag
docker run -d --name alma --restart unless-stopped \
  -p 0.0.0.0:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -e ALMA_SETTINGS_PATH=/app/data/settings.json \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest-lite
```

`0.0.0.0` binds all interfaces, so from your laptop you reach the Pi at
`http://<pi-lan-ip>:8000` (find the Pi's IP with `hostname -I`). The
`:latest-lite` tag above is the right ALMa image for a Pi.

> **Because ALMa has no auth by default, only expose it on a trusted
> private network.** Harden further by:
> - binding to the Pi's LAN IP only (`BIND_ADDR=192.168.1.50`) rather
>   than `0.0.0.0`, so it isn't offered on other interfaces (e.g. a VPN);
> - setting an `API_KEY` (`-e API_KEY=…`) so every request needs an
>   `X-API-Key` header;
> - restricting the port with the host firewall (`ufw allow from
>   192.168.1.0/24 to any port 8000`);
> - or, for any access beyond your LAN, a reverse proxy with HTTPS +
>   auth while keeping the container bound to `127.0.0.1`.

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

## Run with Docker Compose (alternative)

> **Most users should use the [Quick start](#quick-start) above.** This
> section is for users who already manage other services with Docker
> Compose, want a host folder they can browse directly instead of named
> volumes, or want the extra security hardening shipped in
> `docker-compose.yml` (read-only rootfs, `cap_drop ALL`, localhost-only
> port binding).

Two paths — both use the same `docker-compose.yml`, and an overlay
chooses whether the image is **pulled from GHCR** (recommended) or
**built locally** from this checkout.

Shared first step:

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
cp .env.example .env             # add OPENALEX_EMAIL=you@example.com
```

The compose files use **named volumes** (`alma-data`, `alma-config`),
created and owned by the container's app user — no host directories to
make and no permission tinkering.

### A. Pull the prebuilt image from GHCR (fast, no build)

Adds `docker-compose.ghcr.yml`, which clears the `build:` block and points
`image:` at GHCR. Pick the image tag via `ALMA_IMAGE_TAG`:

```bash
# CPU (default — :latest)
docker compose -f docker-compose.yml -f docker-compose.ghcr.yml pull
docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d

# GPU (needs NVIDIA Container Toolkit on the host)
ALMA_IMAGE_TAG=latest-gpu \
  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml -f docker-compose.gpu.yml up -d

# Lite
ALMA_IMAGE_TAG=latest-lite \
  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d
```

Update later with `docker compose -f docker-compose.yml -f docker-compose.ghcr.yml pull && ... up -d`.

### B. Build the image locally from this checkout

Useful if you've made local code changes or want to pin every layer to
your own build. Skips GHCR entirely.

```bash
# CPU build (default)
docker compose up -d --build

# GPU build with passthrough — also needs the NVIDIA Container Toolkit
ALMA_TORCH_VARIANT=cuda \
  docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --build

# Lite build
ALMA_VARIANT=lite docker compose up -d --build
```

### Resource limits and other knobs

`docker-compose.yml` defaults to **8 vCPUs / 4 GB RAM** for the alma
container — sized for a typical desktop host. Override per-host with
`ALMA_CPUS` / `ALMA_MEMORY` env vars (e.g. `ALMA_CPUS=2.0 ALMA_MEMORY=1G`
on a Raspberry Pi). All other deployment knobs live in `.env`; nothing
personal is baked into the image — your library lives in the `alma-data`
and `alma-config` Docker volumes, which survive upgrades. See
[docs → Docker](docs/getting-started/docker.md#where-everything-lives-on-disk)
for backup commands and advanced options (e.g. host-visible folders).

---

## Bare metal install (advanced — not recommended)

> **Not recommended unless you're doing local development on ALMa
> itself or are comfortable managing a heavy native Python AI stack
> yourself.** Almost every "ALMa won't start" report we've seen on this
> path is a `torch` / `transformers` / `hdbscan` build mismatch. Use
> the [Quick start](#quick-start) Docker image instead — it bundles a
> verified version of every dependency.

If you still want to skip Docker, the AI stack (`torch`,
`transformers`, `hdbscan`, `umap-learn`) has heavy native
dependencies that are easy to mismatch in unmanaged Python
environments. Be ready to debug them.

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

# scholar.db is created automatically in your OS data dir
# (~/.local/share/alma on Linux; set DATA_DIR=/abs/path to override).
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

## Browser connector (Firefox)

Save the paper open in your browser straight into ALMa, like the Zotero
connector. Download **`alma-connector-<version>.xpi`** from the
[release](https://github.com/costantinoai/alma-library-manager/releases)
matching your ALMa version and open it in Firefox (`about:addons` → ⚙ →
**Install Add-on From File**). Full guide:
[docs/user-guide/browser-connector.md](docs/user-guide/browser-connector.md).

**Releasing it (maintainer):** keep an AMO API key in
`~/.config/alma/amo.env` and run `gh auth login` once, then from a clean
tree run `extension/release.sh` — it switches to an up-to-date `main`,
signs the add-on locally (unlisted AMO), and (after confirmation) uploads
the `.xpi` to the `v<version>` release and pushes the auto-update manifest.
`extension/release.sh --local` just builds the signed `.xpi`. See
[`extension/README.md`](extension/README.md).

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
