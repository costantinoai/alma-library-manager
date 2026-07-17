---
title: Docker volume ownership
description: Fixing named-volume permissions when upgrading a deployment that previously ran as root.
---

# Docker volume ownership (upgrading an existing deployment)

The container runs as the non-root app user **`10001:10001`** (`user:` in
`docker-compose.yml`). This is the native UID/GID the image builds its
`/app/data` and `/app/config` under, so a fresh deployment is writable out of
the box.

## The one-time migration

If you ran an **older image as root** (`0:0`), the named volumes were populated
with root-owned files. After pinning the service to `10001:10001`, the non-root
process can no longer open those files and you'll see:

```
unable to open database file
```

or, from ALMa's own preflight, a `RuntimeError` naming the offending sidecar and
this exact fix. Repair the ownership **once**, then start normally:

```bash
docker compose run --user 0 --rm alma chown -R 10001:10001 /data /config
docker compose up -d
```

`--user 0` runs that single throwaway command as root purely to `chown` the
mounted volumes; the long-running service stays non-root.

## Why not auto-chown on startup?

An entrypoint that silently `chown`s mounted volumes is a privilege dance that
hides what changed and needs the container to start as root. ALMa's style is a
**loud, documented migration**: the preflight fails fast with the exact command
above instead of quietly rewriting ownership. See
`src/alma/api/deps.py::_raise_unwritable_volume_error`.
