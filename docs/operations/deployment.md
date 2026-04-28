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

Start from the supplied `docker-compose.yml`. Production tweaks:

```yaml
services:
  alma:
    build: .
    image: alma:latest
    container_name: alma
    restart: unless-stopped
    user: "${UID}:${GID}"
    env_file: .env
    ports:
      - "127.0.0.1:8000:8000"   # bind to localhost only
    volumes:
      - .env:/app/.env:ro
      - ./settings.json:/app/settings.json
      - ./data:/app/data
      - ./config:/app/config
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:8000/api"]
      interval: 30s
      timeout: 5s
      retries: 3
```

The image is multi-stage: a builder layer compiles the frontend,
the runtime layer carries only the Python app + the built
frontend. Final image is around 400 MB.

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

```bash
git pull
pip install -e ".[ai]"           # if AI extras already installed
cd frontend && npm install && npm run build && cd ..
# restart the service
systemctl restart alma           # or docker compose up -d
```

Schema migrations run on backend start-up. If a migration fails,
the backend exits non-zero — check the systemd / docker logs.

## Backups

See [Backups](backups.md). The single most important habit on a
deployed install: a weekly cron that calls
`POST /api/v1/library-mgmt/backup` and keeps the last N snapshots.

```bash
# /etc/cron.weekly/alma-backup
#!/usr/bin/env bash
curl -fsS -X POST \
  -H "X-API-Key: ${ALMA_API_KEY}" \
  -d '{"name":"weekly-'$(date +%F)'"}' \
  http://127.0.0.1:8000/api/v1/library-mgmt/backup
```

## What not to do

* **Don't expose `8000` directly to the internet.** Even with an
  API key, run behind TLS at the proxy.
* **Don't share `data/scholar.db` across two ALMa instances.**
  SQLite WAL doesn't survive concurrent writers from two processes.
* **Don't run as `root` in Docker.** The compose file already
  sets `user: "${UID}:${GID}"`.
* **Don't disable the migration check on start-up.** Old DBs
  occasionally need a column added; the check is what makes that
  safe.
