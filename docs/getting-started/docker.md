---
title: Docker
description: Run ALMa as a container with all personal data bind-mounted from the host.
---

# Docker

ALMa ships with a hardened multi-stage `Dockerfile` and a
`docker-compose.yml` that bind-mount **all** user-sensitive state
from the host. Nothing personal is baked into the image — the image
is reproducible and shareable; the host is where your data stays.

What lives on the host (mounted into the container):

| Path on host | Path in container | Contents |
|---|---|---|
| `.env` | `/app/.env` | API keys and secrets (OpenAlex, Semantic Scholar, OpenAI, Slack…) |
| `settings.json` | `/app/settings.json` | Small bootstrap/runtime settings file |
| `data/` | `/app/data/` | `scholar.db`, embedding caches, `secrets.json` |
| `config/` | `/app/config/` | Plugin configs (Slack channel mappings, etc.) |

What lives only in the image:

* The Python `alma` package and its dependencies
* The built frontend (`frontend/dist/`)
* The `uvicorn` entry point

## Quick start from the published image

Create a working directory for local state:

```bash
mkdir alma && cd alma
mkdir -p data config
touch .env settings.json
chmod 600 .env
```

Edit `.env` and set at least:

```dotenv
OPENALEX_EMAIL=you@example.com
```

Then create `docker-compose.yml`:

```yaml
services:
  alma:
    image: ghcr.io/costantinoai/alma-library-manager:0.9.0
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

Start it:

```bash
docker compose up -d
docker compose logs -f alma
```

Open <http://localhost:8000>.

## First steps in the app

1. Open **Settings → External APIs** and confirm OpenAlex is using your
   email.
2. Go to **Discovery → Find & Add**, switch to author search, and follow
   3-5 authors.
3. Open **Activity** and wait for the author backfills to finish.
4. Open **Feed** and save / like a few relevant papers.
5. Optional: go to **Library → Imports** and import BibTeX or Zotero
   data to seed Discovery faster.
6. Open **Discovery** and refresh the default lens.

## Build locally with Docker

Use this path only if you cloned the repository and want to build the
image yourself:

```bash
cp .env.example .env
chmod 600 .env
$EDITOR .env

mkdir -p data config
[ -f settings.json ] || cp settings.example.json settings.json

docker compose up -d
```

Open <http://localhost:8000>. The container binds to `127.0.0.1:8000`
only; put a reverse proxy in front for remote access.

## Lifecycle

```bash
docker compose logs -f alma     # tail logs
docker compose ps               # status + healthcheck
docker compose down             # stop + remove (host data persists)
docker compose build --no-cache # rebuild from scratch
```

## File ownership

By default the container runs as your host UID/GID (`${UID}:${GID}`)
so that anything written into `data/` stays owned by you on the host.
On rootless Docker this works without further setup; on rootful
Docker make sure the IDs match.

If you see permission errors writing to `data/scholar.db`, check that
the host directory is owned by the UID your shell uses (`id -u`).

## Updating

```bash
git pull
docker compose build
docker compose up -d
```

The migration step on container start brings any schema additions in
without manual intervention. If a migration fails, the container will
exit with a non-zero status — check `docker compose logs alma` for
the specific error.

## Backups

The `data/` directory is the source of truth. Back it up while ALMa
is stopped (or use SQLite's online backup API via the **Settings →
Library management → Backup** card while it runs). See
[Backups](../operations/backups.md) for the full strategy.
