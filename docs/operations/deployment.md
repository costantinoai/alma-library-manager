---
title: Deployment
description: Reverse proxy, secrets, hardening, and what to think about when exposing ALMa beyond localhost.
---

# Deployment

ALMa was built to run on `127.0.0.1`. If you want to access it from
another machine — your laptop reaching a desktop, a tablet via a
home VPN — you'll need a reverse proxy and an API key.

## Reverse proxy

Pick one of:

* **Caddy** — the easiest. Auto-TLS via Let's Encrypt.
* **Nginx** — proven and ubiquitous.
* **Traefik** — good fit for Docker compose stacks.
* **Tailscale Funnel** — if your machine is on Tailscale and you
  want to expose to the internet.

Minimal Caddyfile:

```caddyfile
alma.example.com {
    reverse_proxy 127.0.0.1:8000
}
```

The same with Nginx:

```nginx
server {
    listen 443 ssl http2;
    server_name alma.example.com;

    ssl_certificate     /etc/letsencrypt/live/alma.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/alma.example.com/privkey.pem;

    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
    }
}
```

## Browser origins

ALMa refuses any `POST`, `PUT`, `PATCH` or `DELETE` that a browser sends
from another website, so that a page you happen to have open cannot
wipe your data. It works without configuration on `localhost`,
`127.0.0.1` and any IP address.

**If you open ALMa at a hostname, list it.** That covers a reverse proxy
domain, a Tailscale name and a `.local` name:

```bash
# .env
ALMA_ALLOWED_ORIGINS=https://alma.example.com
```

Several origins are comma-separated. Each is a scheme and a host, with a
port if it is not the default, and no path. A malformed entry or a `*`
stops ALMa at startup.

Without this, pages load but every save, like and setting fails with
`403`, and the log shows `Refused cross-site POST` with the origin to
add. Hostnames must be listed because a hostile domain can be re-pointed at
your machine (DNS rebinding) and then look like ALMa's own address.
Only IP addresses and `localhost` are safe from that.

**Containers.** `docker-compose.yml` reads the value from `.env`.
`scripts/deploy-prod.sh` keeps it from the running container, and takes
it from the calling shell on a first deploy:

```bash
ALMA_ALLOWED_ORIGINS=https://alma.example.com scripts/deploy-prod.sh 0.25.0
```

Older images ignore the variable, so you can set it before upgrading.
Do that first if Watchtower updates ALMa for you: the new image will
otherwise arrive without it, and your hostname will be refused until
you add it.

The browser connector, curl and scripts need nothing here. Extensions
are recognised by their `moz-extension://` or `chrome-extension://`
origin, and clients with no `Origin` header pass. That second case is
why this is not authentication: it stops other websites, not other
programs that can reach the port. Use an `API_KEY` for those.

## API key

When ALMa is reachable from outside `127.0.0.1`, set an API key:

```bash
# .env
API_KEY=$(openssl rand -hex 32)
```

Restart the backend. Every request now requires:

```
X-API-Key: <your key>
```

Without the header, the API returns `401`.

The frontend SPA reads the key from `window.localStorage.api_key`.
Set it once via the browser console:

```javascript
localStorage.setItem('api_key', 'your-key-here')
```

You only do this once per browser; subsequent requests carry the
header automatically.

## Docker production

The shipped `docker-compose.yml` already encodes the production
defaults you want: `restart: unless-stopped`, localhost-only port
binding (`127.0.0.1:8000:8000`), read-only rootfs, `cap_drop ALL`,
`no-new-privileges`, healthcheck on `/api/v1/health`, log rotation,
and per-host resource limits (`ALMA_CPUS` / `ALMA_MEMORY`). For most
deployments, the right move is to pull the prebuilt GHCR image
through the shipped overlay rather than rebuilding locally:

```bash
git clone https://github.com/costantinoai/alma-library-manager.git
cd alma-library-manager
cp .env.example .env             # API_KEY, OPENALEX_EMAIL, etc.

# Pin a specific version in production (avoid surprise upgrades)
ALMA_IMAGE_TAG=0.20.1 \
  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d
```

State lives in the `alma-data` / `alma-config` named volumes (owned by
the container app user — no host permission setup). See
[Getting started → Docker](../getting-started/docker.md#path-2-docker-compose-named-volumes)
for the full list of compose flags (GPU overlay, lite image, build
locally, etc.).

The image is multi-stage: a builder layer compiles the frontend,
the runtime layer carries only the Python app + the built
frontend. The full image (with torch + CUDA) is large (~2 GB
on-disk); the `lite` flavour, which drops the AI stack, is ~300 MB.

## Secrets

* `.env` — `chmod 600`, owned by the user that runs the container.
* `data/secrets.json` — auto-managed; same permissions.
* Never commit either to git. Both are in `.gitignore`.
* For team / shared deployments, use a secrets manager
  (Vault, 1Password CLI, etc.) and template into `.env` at
  start-up.

## Process supervision

When running outside Docker, use a supervisor:

* **systemd** (Linux) — example unit:

```ini
[Unit]
Description=ALMa
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/alma
EnvironmentFile=/opt/alma/.env
ExecStart=/opt/alma/.venv/bin/uvicorn alma.api.app:app --host 127.0.0.1 --port 8000
Restart=on-failure
User=alma

[Install]
WantedBy=multi-user.target
```

* **launchd** (macOS) — wrap in a `.plist` with `KeepAlive=true`.

Don't use `nohup uvicorn …` long-term; it has no restart-on-crash.

## Updating

**Docker (the normal path)** — deploy a released version with the
versioned script; it pulls `ghcr.io/costantinoai/alma-library-manager:
X.Y.Z[-gpu|-lite]` (flavor autodetected), recreates the `alma`
container preserving your env and the `alma-data` / `alma-config`
volumes, health-polls, and exits non-zero unless the container is
healthy and reports the requested release version:

```bash
scripts/deploy-prod.sh 0.21.0
```

**Bare metal**:

```bash
git pull
pip install -e ".[ai]"           # if AI extras already installed
cd frontend && npm install && npm run build && cd ..
# restart the service
systemctl restart alma           # or docker compose up -d
```

Schema migrations run on backend start-up. If a migration fails,
the backend exits non-zero — check the systemd / docker logs.

**Cutting a release** (maintainer): write `docs/releases/vX.Y.Z.md`,
then run `scripts/release.sh X.Y.Z` — preflight, tests, bump, tag,
locally-signed connector built from the tag, push, GitHub release.
Fully local; no CI secrets. Details in `extension/README.md` →
"How releases work".

## Health

`GET /api/v1/health` answers one question: can this process do its job?
The Docker healthcheck and `scripts/deploy-prod.sh` act on its status code.

| Status | Means | `status` | `database_ok` | `scheduler` |
|---|---|---|---|---|
| `200` | Ready | `healthy` | `true` | `running` or `disabled` |
| `503` | The database failed | `unhealthy` | `false` | any |
| `503` | The scheduler never started | `unhealthy` | `true` | `failed` |

The database check fails when the file is missing or unreadable, or holds
no `papers` table. The scheduler is required because Feed refreshes,
alerts and repairs all run on it, so a process without it would serve a
library that quietly stops updating. `SCHEDULER_ENABLED=false` is a
deliberate choice and stays healthy.

A scheduler failure does not stop ALMa: the Library and the logs stay
reachable, Docker marks the container `unhealthy`, and
`docker logs alma` holds the cause. Restart it after fixing that.

Providers, the internet and DNS are never checked here. An outage out
there must not mark a working ALMa unhealthy. Those failures appear on
Home's Connections rail and in Activity.

## Backups

See [Backups](backups.md). The single most important habit on a
deployed install: a weekly cron that calls
`POST /api/v1/library-mgmt/backup` and keeps the last N snapshots.

```bash
# /etc/cron.weekly/alma-backup
#!/usr/bin/env bash
# Backups are auto-named with a server timestamp; there is no name param.
curl -fsS -X POST \
  -H "X-API-Key: ${ALMA_API_KEY}" \
  http://127.0.0.1:8000/api/v1/library-mgmt/backup
```

## What not to do

* **Don't expose `8000` directly to the internet.** Even with an
  API key, run behind TLS at the proxy.
* **Don't share `data/scholar.db` across two ALMa instances.**
  SQLite WAL doesn't survive concurrent writers from two processes.
* **Don't run as `root` in Docker.** The compose file already
  pins `user: "10001:10001"` (the fixed image UID/GID). See
  [Docker volume ownership](docker-volume-ownership.md).
* **Don't disable the migration check on start-up.** Old DBs
  occasionally need a column added; the check is what makes that
  safe.
