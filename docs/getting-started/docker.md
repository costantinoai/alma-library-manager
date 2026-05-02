---
title: Docker
description: Run ALMa as a container — the suggested install path, with named volumes that persist across upgrades.
---

# Docker

Docker is how almost everyone should run ALMa. The published image
includes the FastAPI backend, the built React frontend, the SPECTER2
encoder (in the `normal` variant), and every native dependency
already pinned and tested. You provide three things: a port, an
OpenAlex email, and a place for ALMa to store your library.

There are two install paths:

1. **One-command `docker run`** with named volumes. Suggested for
   single-user workstations, NAS boxes, and quick evaluation. No
   compose file, no host-side bind-mounts, no permission tinkering.
2. **`docker compose`** with host bind-mounts. For users who want to
   build the image from source, manage ALMa alongside other
   compose-managed services, or directly inspect `data/` on the host.

Both paths use the same image; pick by ergonomics. Three image tags
are published — `:latest` (CPU, default), `:latest-gpu` (CUDA torch
for NVIDIA hosts), and `:latest-lite` (no torch, for Pis). The
README quick start has copy-paste `docker run` commands for each one.

## Path 1 — single `docker run` (suggested)

```bash
docker run -d --name alma --restart unless-stopped \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data \
  -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest
```

Open <http://localhost:8000>.

What this does:

* Pulls the `:latest` image from GHCR (multi-arch — works on
  `linux/amd64` and `linux/arm64`).
* Creates two **Docker named volumes** (`alma-data`, `alma-config`)
  that survive container removal and image upgrades. They live under
  `/var/lib/docker/volumes/<name>/_data` on Linux, or under the
  Docker Desktop VM elsewhere.
* Binds the API to `127.0.0.1` only. Nothing is exposed to your
  network until you put a reverse proxy in front and set `API_KEY`.
* Sets the OpenAlex polite-pool email. ALMa runs without it, but
  you'll be sharing rate limits with anonymous users.

### What "persistent" actually means here

The `alma-data` volume is the only thing that holds your library —
`scholar.db` (SQLite) plus its WAL/SHM files, embedding caches, the
backups directory, and any imported BibTeX/Zotero state. The
`alma-config` volume holds plugin configs (e.g. Slack channel
mappings). These survive:

* `docker stop alma` / `docker rm alma`
* `docker pull` of a newer image
* Restarts and reboots

They do **not** survive `docker volume rm alma-data` — that's the
only command that wipes your library.

If you ever want a copy on the host filesystem (for backup or
inspection), the simplest way is the **Settings → Library
Management → Backup** button, which writes a gzipped SQLite file into
the volume's `backups/` subdirectory. You can also `docker run --rm
-v alma-data:/d alpine cat /d/scholar.db > /tmp/scholar.db` to copy
the live DB out.

### Upgrade

```bash
docker pull ghcr.io/costantinoai/alma-library-manager:latest
docker rm -f alma
# rerun the original `docker run` command — your data lives in the volumes
```

`:latest` tracks the newest stable release on `main`. For shared
servers or production-ish setups, pin a specific version
(`:0.9.2`, `:0.9`, or `:0`) so an automatic refresh of `main` doesn't
upgrade the running stack out from under you.

## Path 2 — Docker Compose with host bind-mounts

Use this when you want to build the image from source (`build: .`),
manage ALMa alongside other compose-managed services, or have the
SQLite file directly on your host filesystem.

```bash
mkdir alma && cd alma
mkdir -p data config
touch .env settings.json
chmod 666 .env settings.json   # see "File ownership" below
```

Edit `.env` and set at least:

```dotenv
OPENALEX_EMAIL=you@example.com
```

Save as `docker-compose.yml`:

```yaml
services:
  alma:
    image: ghcr.io/costantinoai/alma-library-manager:latest
    # build: .   # uncomment to build locally instead of pulling
    container_name: alma
    restart: unless-stopped
    ports:
      - "127.0.0.1:8000:8000"
    env_file:
      - .env
    volumes:
      - ./data:/app/data
      - ./config:/app/config
      - ./settings.json:/app/settings.json
      - ./.env:/app/.env
```

Start:

```bash
docker compose up -d
docker compose logs -f alma
```

Open <http://localhost:8000>.

Updating:

```bash
docker compose pull && docker compose up -d
```

### File ownership

The image runs as a non-root `appuser` (UID `10001`, GID `10001`).
With **named volumes** (Path 1) Docker manages the perms — nothing to
configure. With **host bind-mounts** (Path 2) the host file's UID
needs to be readable/writable by the container's `appuser`. The
simplest fix is `chmod 666 .env settings.json` before starting; the
broader rules are in the [configuration reference](../reference/configuration.md).

If you see boot-time warnings like
`Could not load .env … using environment vars only`, that's the
graceful fallback in action — env vars from the `env_file:` directive
have already been passed into the container, so the app keeps
working; it just can't *also* load the file from inside.

## Three image flavors

ALMa publishes three flavors; all of them run every feature in the
app — only the bundled embedding stack differs.

| | `normal` CPU (default) | `normal` GPU | `lite` |
|---|---|---|---|
| Tag | `:latest`, `:0.13.0` | `:latest-gpu`, `:0.13.0-gpu` | `:latest-lite`, `:0.13.0-lite` |
| Compressed image | ~1.4 GB | ~3.2 GB | ~1.2 GB |
| Peak runtime memory | ~2 GB | ~3 GB (more on GPU init) | ~1 GB |
| Local SPECTER2 encoder | yes (CPU) | yes (CUDA when host GPU is exposed; CPU otherwise) | no |
| Semantic Scholar pre-computed vectors | yes | yes | yes |
| OpenAI / cloud embedding provider | configurable | configurable | configurable |
| Discovery / Insights graph / clustering | yes | yes | yes |
| BibTeX / Zotero import | yes | yes | yes |
| Architectures | amd64 + arm64 | amd64 only | amd64 + arm64 |

The CPU flavor is the default `:latest` so users without a GPU only
download the small image. GPU users explicitly opt in via the `-gpu`
suffix (or let `setup.sh` autodetect — see below). Pick `lite` on a
Raspberry Pi or any host where 1.5 GB of `torch` on disk is precious.

### GPU acceleration (`docker run` or compose)

Two pieces are required to reach a host GPU from inside the container:

1. **Host-side**: an NVIDIA GPU plus the
   [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/)
   registered as a Docker runtime.
2. **Image-side**: the `-gpu` tag (it ships the CUDA torch wheel; the
   default `:latest` doesn't, so passthrough won't help even if you
   pass `--gpus all`).

`docker run`:

```bash
docker run -d --name alma --restart unless-stopped --gpus all \
  -p 127.0.0.1:8000:8000 \
  -e OPENALEX_EMAIL=you@example.com \
  -v alma-data:/app/data -v alma-config:/app/config \
  ghcr.io/costantinoai/alma-library-manager:latest-gpu
```

Compose: bring the stack up with the GPU overlay so the device
reservation lands on the alma service.

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

Verify the container saw the GPU:

```bash
docker exec alma python -c "import torch; print(torch.cuda.is_available())"
# Expect: True
```

If `False`, the runtime device-resolver in
`discovery.similarity.SpecterEmbedder` silently falls back to CPU and
the rest of the app still works — GPU passthrough is purely an
acceleration. Common reasons for `False`: you pulled `:latest`
instead of `:latest-gpu` (no CUDA wheel in the image), or the
NVIDIA Container Toolkit isn't installed on the host.

### Building locally with a chosen torch wheel

`docker compose build` defaults to the CPU torch wheel (matching
the published `:latest`). To build the CUDA flavor from this
checkout:

```bash
ALMA_TORCH_VARIANT=cuda docker compose build alma
```

`TORCH_VARIANT` accepts `cpu` or `cuda` and is ignored when
`ALMA_VARIANT=lite` (lite installs no torch at all).

## Where everything lives on disk

There are four pieces of state to track across an upgrade. Where each
of them lives depends on which install path you took and which OS
your Docker daemon is on.

### 1. The library (`scholar.db`)

The single SQLite file with every paper, author, lens, feedback
event, and the on-disk citation graph. **The most important thing to
back up** — losing this means losing your library.

| Path 1 — `docker run` with named volumes | Path 2 — Compose with bind-mounts |
|---|---|
| In the **`alma-data`** volume at `/app/data/scholar.db` (inside the container). On Linux Docker the volume's bytes live at `/var/lib/docker/volumes/alma-data/_data/scholar.db` (root-only, no need to touch directly). On Docker Desktop (macOS / Windows), inside the LinuxKit / WSL VM — accessible via `docker run --rm -v alma-data:/d alpine ls /d`. | At `./data/scholar.db` next to your `docker-compose.yml`. Owned by the container's `appuser` (UID 10001) on Linux; on macOS/Windows the host filesystem is shared via virtiofs/9P and ownership is mapped automatically. |

### 2. Secrets and runtime knobs (`.env` values)

API keys, polite-pool email, Slack tokens, optional `API_KEY` for
the REST endpoint, etc.

| Path 1 — `docker run` with named volumes | Path 2 — Compose with bind-mounts |
|---|---|
| **Not on disk inside the container.** Each `-e KEY=value` flag is passed to the container's process environment by the Docker daemon at start. To add or remove a key you `docker rm -f alma` and rerun the `docker run` command. No file is written; no chmod games. | At `./.env` next to your `docker-compose.yml`. Read by Docker Compose (`env_file:`) and re-read by the app at startup. Strict perms (`chmod 600`) work on Linux only when the host UID matches `appuser`; otherwise use `chmod 644` and trust your filesystem ACLs. |

### 3. Bootstrap settings (`settings.json`)

A small JSON file with defaults like the OpenAlex polite-pool email
and the SQLite path. Almost everything user-tunable from the UI
(Discovery weights, AI provider selection, scheduler intervals) is
written into the `discovery_settings` table inside `scholar.db`, not
this file. So `settings.json` is mostly cosmetic.

| Path 1 — `docker run` with named volumes | Path 2 — Compose with bind-mounts |
|---|---|
| Auto-created at `/app/settings.json` on first boot, populated with built-in defaults. Lives **on the container's writable layer**, so it does *not* survive `docker rm -f alma` (data and config volumes do). To persist it across rebuilds, set `-e ALMA_SETTINGS_PATH=/app/data/settings.json` so the file lives inside the `alma-data` volume. | At `./settings.json` next to your `docker-compose.yml`. Persists with the rest of the bind-mount. Boot-time write-failures (mode-600 with mismatched UID) degrade to a warning — see *File ownership* above. |

### 4. Plugin configs

Slack channel mappings, etc. Used by the Slack notifier plugin.

| Path 1 — `docker run` with named volumes | Path 2 — Compose with bind-mounts |
|---|---|
| In the **`alma-config`** volume at `/app/config/`. Same Linux-vs-VM rules as the data volume. | At `./config/` next to your `docker-compose.yml`. |

### Quick reference

```
container               Path 1 (named volumes)            Path 2 (bind-mounts)
─────────────────────   ─────────────────────────────     ─────────────────────
/app/data/scholar.db    alma-data volume                  ./data/scholar.db
/app/data/backups/      alma-data volume                  ./data/backups/
/app/config/            alma-config volume                ./config/
/app/.env               not on disk (env vars only)       ./.env
/app/settings.json      writable layer (ephemeral)        ./settings.json
                        unless ALMA_SETTINGS_PATH set
```

What's **never** on the host (lives only inside the image, replaced on every `docker pull`):

* Python dependencies — `/opt/venv/` (~1 GB on `normal`)
* The frontend bundle — `/app/frontend/dist/`
* The `alma` Python package source — `/app/src/`

This is why `docker pull` of a newer image is safe: you're replacing
the code, not your data.

### Inspecting volumes from the host

Even on Docker Desktop where the volume isn't directly mountable, you
can always shell in:

```bash
# list everything in the data volume
docker run --rm -v alma-data:/d alpine ls -la /d

# read a file out of the volume (e.g., latest backup)
docker run --rm -v alma-data:/d -v "$PWD":/out alpine \
  cp /d/backups/scholar_<timestamp>.db.gz /out/

# disk usage
docker run --rm -v alma-data:/d alpine du -sh /d
```

## After it starts

Open <http://localhost:8000>. The first run lands you on the Library
page with no papers, no followed authors, nothing in the Feed. Three
things, in order, before the app becomes useful — see the
[first-run checklist](first-run.md).

![Library on first run, before any saves](../screenshots/desktop-library.png)

## Lifecycle commands

```bash
docker logs -f alma                  # tail logs (Path 1)
docker compose logs -f alma          # tail logs (Path 2)
docker exec alma /opt/venv/bin/alma --help    # invoke CLI inside container
docker stop alma                     # graceful stop
docker rm -f alma                    # stop + remove (data persists in volumes)
docker volume ls | grep alma         # check what's stored
```

## Backups

The `data/` directory is the source of truth. The fastest backup
path is the **Settings → Library Management → Backup** button, which
writes a gzipped, transactionally-consistent SQLite snapshot into
`data/backups/scholar_<timestamp>.db.gz`. By default, only the last
five snapshots are kept (override with `-e ALMA_BACKUP_RETAIN=20`).

To pull a snapshot out of the volume to the host:

```bash
docker run --rm -v alma-data:/d -v "$PWD":/out alpine \
  cp /d/backups/scholar_<timestamp>.db.gz /out/
```

See [Backups](../operations/backups.md) for the full strategy
(retention, cron, restore round-trip).

## Troubleshooting

**Container exits immediately on first run.**
Check logs for the actual error: `docker logs alma`. Common causes:

* OpenAlex 503s during startup probe — usually clears within a
  minute; the container has a healthcheck that retries.
* Bind-mount perms (Path 2 only) — see *File ownership* above.

**Browser shows "broken image" icons in the sidebar.**
Pull `:latest` again — versions before v0.9.2 had a static-asset
routing bug where `/brand/*.svg` returned the SPA index.

**`docker pull` hangs or times out on arm64.**
Multi-arch manifests can be slow to fetch over poor networks. Pin
the architecture explicitly:

```bash
docker pull --platform linux/arm64 ghcr.io/costantinoai/alma-library-manager:latest
```

**`docker run` says "image not found" or "401 Unauthorized".**
The package is public; if you see a 401 the most likely cause is a
stale stored credential. Run `docker logout ghcr.io` and retry.
