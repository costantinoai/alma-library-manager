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

**2. Tell ALMa that address.** Add it to `.env` in `~/alma-docker`, then
apply it:

```bash
ALMA_ALLOWED_ORIGINS=https://alma.your-tailnet.ts.net
```

```bash
cd ~/alma-docker
docker compose up -d --wait
```

Or pass it to the one-line installer, which saves it in `.env`:

```bash
curl -sSL https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.sh \
  | ALMA_ALLOWED_ORIGINS=https://alma.your-tailnet.ts.net bash
```

**If you skip step 2**, the address does not work: the page shows one
line of JSON containing `UnknownHostRefused` and your address.

Everyone on that private network can use your ALMa, so limit who is on
it.

## Browser origins

`ALMA_ALLOWED_ORIGINS` is the one setting that tells ALMa its own
addresses. It decides two things.

**Which addresses ALMa answers to, on every request.** The `Host`
header must be `localhost`, an IP address or a listed address. Anything
else gets `403`, reads included. This stops DNS rebinding: a hostile
domain can be re-pointed at your machine and then look to your browser
like ALMa's own address, so only the name it arrived under gives it
away. One exception: `/api/v1/health` also answers to `alma:8000`, so a
sidecar on the Docker network can check on the container.

**Which pages may change data.** `POST`, `PUT`, `PATCH` and `DELETE`
from a browser must come from ALMa's own page on `localhost` or an IP
address, from a listed origin, or from a browser extension. A page on
any other website gets `403`.

Several origins are comma-separated. Each is a scheme and a host, with
a port if it is not the default, and nothing else. A malformed entry,
a `*`, a username, a path, a query or a fragment stops ALMa at startup
with an error naming the entry. `FRONTEND_URL` counts as one more entry.

**Behind your own proxy on a private network**, list the address you
open and make the proxy keep the `Host` the browser sent. Tailscale
Serve does this already. nginx does not by default, so add
`proxy_set_header Host $host;`, and accept only your own name with
`server_name`. Never list the proxy's upstream name, and never point a
proxy at ALMa by IP address without keeping `Host`: either way ALMa can
no longer tell a hostile page from its own. ALMa ignores
`X-Forwarded-Host` and the like, because any client can write them.

**Containers.** `docker-compose.yml` reads the value from `.env`. The
installer saves it there, and keeps it from an older container the
first time it runs.

Older images ignore the variable, so you can set it before upgrading.
Do that first if Watchtower updates ALMa for you: Watchtower keeps the
settings the container already has, so a new image arrives with the
value only if you applied it beforehand. Without it, your hostname is
refused until you add it.

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

The shipped `docker-compose.yml` is the one production definition. It
encodes `restart: unless-stopped`, localhost-only port binding
(`127.0.0.1:8000:8000`), read-only rootfs, `cap_drop ALL`,
`no-new-privileges`, healthcheck on `/api/v1/health`, log rotation,
and per-host resource limits (`ALMA_CPUS` / `ALMA_MEMORY`). It pulls the
`:stable` image. The one-line installer and `scripts/deploy-prod.sh`
both go through it, so install and update can't drift apart.

State lives in the `alma-data` / `alma-config` named volumes (owned by
the container app user — no host permission setup). The deployment lives
in `~/alma-docker`: the Compose files, and `.env` with your settings. See
[Getting started → Docker](../getting-started/docker.md) for the flavors
and for running the same file by hand.

To add a proxy or a second network, keep them in
`docker-compose.override.yml` in that folder, so every update declares
them. A proxy and ALMa that share a declared network reach each other at
`http://alma:8000`:

```yaml
services:
  alma:
    networks: [default, proxy]
networks:
  proxy:
    external: true
```

The installer keeps this file in the stack. Watchtower keeps both
networks when it recreates the container.

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

**Docker (the normal path)** — re-run the one-line installer, or run
this in `~/alma-docker`:

```bash
docker compose pull          # the old container keeps serving
docker compose up -d --wait  # replaces it and waits for health
```

If the pull fails, nothing has stopped. With a checkout,
`scripts/deploy-prod.sh` does the same for a named release: it pulls
`ghcr.io/costantinoai/alma-library-manager:X.Y.Z[-gpu|-lite]` (flavor
autodetected) through the installer, and exits non-zero unless the
container is healthy and reports that version:

```bash
scripts/deploy-prod.sh 0.21.0
```

[Watchtower](../getting-started/docker.md#automatic-updates-with-watchtower)
also works on this container.

**If the new version does not start**, the installer stops with an
error. By then the old container has been replaced, so ALMa is down, but
your data is untouched in the volumes and `.env` names the new image.
The installer prints the commands to go back to the exact image that was
running, even when a tag such as `stable` has moved. They are:

```bash
cd ~/alma-docker
sed -i.bak 's|^ALMA_IMAGE_TAG=.*|ALMA_IMAGE_TAG="stable@sha256:<digest>"|' .env
docker compose up -d --wait
```

That pins one image, so nothing updates it. To follow the tag again,
re-run the installer with `ALMA_IMAGE_TAG=stable`, or your own tag,
in front of `bash`. If the new version had already migrated the
database, the old image cannot use it: restore a backup taken before the
update, as described in [Backups](backups.md#restore).

**Bare metal**:

```bash
git pull
pip install -e ".[ai]"           # if AI extras already installed
cd frontend && npm install && npm run build && cd ..
# restart the service
systemctl restart alma
```

Schema migrations run on backend start-up. If a migration fails,
the backend exits non-zero — check the systemd / docker logs.

A migration cannot be undone by going back to an older image, so take a
backup first if the release notes mention one:
`curl -fsS -X POST http://127.0.0.1:8000/api/v1/library-mgmt/backup`.

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
