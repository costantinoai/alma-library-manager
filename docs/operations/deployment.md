---
title: Deployment
description: Running ALMa safely on a private network, the one setting for other devices, what API_KEY does, Docker hardening and updates.
---

# Deployment

**ALMa has no login.** Anyone who can reach its port can read and
change your library. Keep it on `127.0.0.1`, which is the default, or
on a private network you control. **Never expose it to the open
internet.** That rules out port forwarding, Tailscale Funnel and public
reverse proxies. HTTPS encrypts the connection; it does not say who is
connecting, and nothing behind it would ask.

## Open ALMa from your phone or another device

Use a private network: your home LAN, or a tailnet through
[Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve), which only your
own devices can reach. Do not use Tailscale Funnel: it is public.

**1. Give ALMa a private address.** With Tailscale, run this on the
computer that runs ALMa:

```bash
tailscale serve --bg 8000
```

It prints your address, for example
`https://alma.your-tailnet.ts.net`. On a LAN, use the computer's IP
address as described in
[Exposing on your network](../getting-started/docker.md#exposing-on-your-network).
An IP address works as it is, so you can stop here.

**2. Tell ALMa that address, then restart it.** With Compose, in `.env`:

```bash
ALMA_ALLOWED_ORIGINS=https://alma.your-tailnet.ts.net
```

With `docker run`, add `-e ALMA_ALLOWED_ORIGINS=https://alma.your-tailnet.ts.net`.
The one-line installer does not carry this setting, so re-create the
container with the
[`docker run` command](../getting-started/docker.md#or-run-docker-run-directly)
and that flag.

**If you skip step 2**, the address does not work, in one of two ways:

* The page shows one line of JSON containing `UnknownHostRefused` and
  your address.
* Or, when a proxy replaces the address on the way in, pages load but
  every save, like and setting fails with `403`, and the log shows
  `Refused cross-site POST`.

Everyone on that private network can use your ALMa, so limit who is on
it.

## Browser origins

`ALMA_ALLOWED_ORIGINS` is the one setting that tells ALMa its own
addresses. It decides two things.

**Which addresses ALMa answers to, on every request.** The `Host`
header must be `localhost`, an IP address, `alma:8000` (the container's
name on a Docker network) or a listed address. Anything else gets
`403`, reads included. This stops DNS rebinding: a hostile domain can
be re-pointed at your machine and then look to your browser like
ALMa's own address, so only the name it arrived under gives it away.

**Which pages may change data.** `POST`, `PUT`, `PATCH` and `DELETE`
from a browser must come from ALMa's own page on `localhost` or an IP
address, from a listed origin, or from a browser extension. A page on
any other website gets `403`.

Several origins are comma-separated. Each is a scheme and a host, with
a port if it is not the default, and nothing else. A malformed entry,
a `*`, a username, a path, a query or a fragment stops ALMa at startup
with an error naming the entry. `FRONTEND_URL` counts as one more entry.

**Behind your own proxy on a private network**, one entry is enough if
the proxy keeps the `Host` the browser sent, or sends `alma:8000` or an
IP address. If it sends another upstream name, list that too, for
example `http://my-upstream:8000`. ALMa ignores `X-Forwarded-Host`
and the like, because any client can write them.

**Containers.** `docker-compose.yml` reads the value from `.env`.
`scripts/deploy-prod.sh` keeps it from the running container, and takes
it from the calling shell on a first deploy:

```bash
ALMA_ALLOWED_ORIGINS=https://alma.your-tailnet.ts.net scripts/deploy-prod.sh 0.25.0
```

Older images ignore the variable, so you can set it before upgrading.
Do that first if Watchtower updates ALMa for you: the new image will
otherwise arrive without it, and your hostname will be refused until
you add it.

The browser connector, curl and scripts need nothing here when they use
`localhost` or an IP address. Clients with no `Origin` header pass once
their `Host` is known, which is why this is not authentication: it
stops other websites, not other programs that can reach the port.

## API key

`API_KEY` is for API and connector use. **It does not protect the web
app: the app sends no key, so setting `API_KEY` breaks the browser
UI.** The page still loads, but its sections say they could not be
loaded and saves fail.

```bash
# .env
API_KEY=$(openssl rand -hex 32)
```

With a key set, all write routes and most read routes answer `401`
unless the request carries `X-API-Key: <your key>` or
`Authorization: Bearer <your key>`. `/api/v1/health` stays open. The
[browser connector](../user-guide/browser-connector.md) can send the
key.

It is one shared secret, not a login, and it does not make internet
exposure safe.

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
cp .env.example .env             # OPENALEX_API_KEY, OPENALEX_EMAIL, etc.

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
* `data/secrets.json` — auto-managed; same permissions. It is **plain
  JSON, not encrypted**. Mode `600` is its only protection, so treat
  any backup of it, or of the `alma-data` volume, as a credential.
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
volumes, health-polls, and verifies the container reports the
requested release version:

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

## Backups

See [Backups](backups.md). The single most important habit on a
deployed install: a weekly cron that calls
`POST /api/v1/library-mgmt/backup` and keeps the last N snapshots.

```bash
# /etc/cron.weekly/alma-backup
#!/usr/bin/env bash
# Backups are auto-named with a server timestamp; there is no name param.
curl -fsS -X POST http://127.0.0.1:8000/api/v1/library-mgmt/backup
```

## What not to do

* **Don't expose ALMa to the internet**, directly or through a proxy.
  It has no login. See the top of this page.
* **Don't share `data/scholar.db` across two ALMa instances.**
  SQLite WAL doesn't survive concurrent writers from two processes.
* **Don't run as `root` in Docker.** The compose file already
  pins `user: "10001:10001"` (the fixed image UID/GID). See
  [Docker volume ownership](docker-volume-ownership.md).
* **Don't disable the migration check on start-up.** Old DBs
  occasionally need a column added; the check is what makes that
  safe.
