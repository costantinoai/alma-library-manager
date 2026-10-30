---
title: Backups
description: Online backups, offline file copies, and the offline restore for scholar.db and the surrounding data directory.
---

# Backups

Your `data/` directory is the source of truth. Back it up.

The most important file is `data/scholar.db`. Everything else
(caches, secrets, embedding fetch state) is regenerable from it
plus your `settings.json`.

## What's in `data/`

```
data/
├── scholar.db                # Primary SQLite — papers, authors, settings, etc.
├── scholar.db-wal            # WAL — write-ahead log
├── scholar.db-shm            # Shared memory — WAL coordination
├── secrets.json              # Optional — secrets ALMa wrote
├── backups/                  # UI-driven backup snapshots land here
└── caches/                   # Embedding / network caches (regenerable)
```

WAL files (`-wal`, `-shm`) are part of the live database. Never copy
`scholar.db` while ALMa is running: committed rows can sit only in the
WAL, so the copy would miss them. While ALMa runs, use the online
paths below.

## Online backup (UI)

**Settings → Data & system → Library maintenance → Backup**:

* **Create backup** — uses SQLite's online backup API. Safe while
  ALMa is running. Writes a gzip-compressed, timestamped snapshot to
  `data/backups/scholar_<timestamp>.db.gz`.
* **List backups** — shows existing snapshots with size and
  timestamp.

Online backups read through SQLite, so a snapshot holds every
committed transaction, including those still in the WAL, and writes
can continue while it is taken.

There is no restore button. A running ALMa cannot replace its own
database, so a restore is an [offline step](#restore).

## Online backup (API)

Same operations via REST:

The backup name is a server-generated timestamp (returned as
`backup_name`); there is no custom-name parameter.

```bash
# create — returns {"success": true, "backup_name": "20260425_143000", ...}
curl -X POST http://localhost:8000/api/v1/library-mgmt/backup

# list — the `backups` array in the info payload
curl http://localhost:8000/api/v1/library-mgmt/info

# download a full copy of the database, from any machine that can reach ALMa
curl -o alma-backup.db http://localhost:8000/api/v1/backup/export
```

`POST /library-mgmt/restore/{name}` always answers `409` and changes
nothing. See [Restore](#restore).

## Offline backup (file copy)

If ALMa is **stopped**:

```bash
# stop ALMa first
docker compose down               # in ~/alma-docker, or kill the uvicorn process

# tar everything
tar -czf alma-backup-$(date +%F).tar.gz data/ settings.json .env

# done; restart
docker compose up -d --wait
```

Under Docker, the data sits in the volumes, not in a `data/` folder.
With ALMa stopped, archive both of them:

```bash
docker run --rm -v alma-data:/data:ro -v alma-config:/config:ro \
  -v "$PWD":/out alpine tar -czf /out/alma-backup-$(date +%F).tar.gz /data /config
```

The archive holds your API keys in plain text (`secrets.json`, `.env`),
so store it like a credential.

If ALMa is **running**, never copy `scholar.db`: not with `cp`, not with
`docker run … cat`, and not after a `wal_checkpoint`, because a write
can land during the copy. Use the [online paths](#online-backup-ui)
instead. They go through SQLite's backup API, which is safe while ALMa
writes.

## Restore

Restoring always happens with ALMa stopped. Open connections,
background jobs and the WAL all outlive a file swap, so overwriting the
database of a running ALMa can corrupt it. ALMa refuses to try.

Restore the database and the ALMa release as a pair. ALMa refuses to
start on a database written by a newer release, and it migrates an
older one forward on start-up, so take a backup first if you may want
to go back. Releases up to 0.24.2 do not have this check: they log a
warning, report healthy and run on a schema they do not know, so never
roll back to one of them past a migration without restoring a backup
taken on it.

**Docker** (the volume is `alma-data`). List the snapshots, then restore
one:

```bash
docker run --rm -v alma-data:/d:ro alpine ls /d/backups
```

```bash
docker stop alma
docker run --rm --user 10001:10001 -v alma-data:/d alpine sh -c '
  cd /d &&
  gunzip -c backups/scholar_<timestamp>.db.gz > scholar.db.restoring &&
  mv scholar.db scholar.db.before-restore &&
  rm -f scholar.db-wal scholar.db-shm &&
  mv scholar.db.restoring scholar.db'
docker start alma
```

**Bare metal** (in the data directory, with the ALMa process stopped):

```bash
gunzip -c backups/scholar_<timestamp>.db.gz > scholar.db.restoring
mv scholar.db scholar.db.before-restore
rm -f scholar.db-wal scholar.db-shm
mv scholar.db.restoring scholar.db
```

Remove the old `-wal` and `-shm` files in the same step. They belong to
the database you just replaced, and SQLite would try to apply them to
the restored file.

For a `.db` file from `GET /backup/export`, skip `gunzip` and copy the
file in place of `scholar.db.restoring`. Under Docker the file has to
reach the volume as the container's user, so put it in a folder of its
own that user can read:

```bash
mkdir restore && mv alma-backup.db restore/
chmod a+rx restore && chmod a+r restore/alma-backup.db
docker stop alma
docker run --rm --user 10001:10001 -v alma-data:/d -v "$PWD/restore":/in:ro alpine sh -c '
  cd /d &&
  cp /in/alma-backup.db scholar.db.restoring &&
  mv scholar.db scholar.db.before-restore &&
  rm -f scholar.db-wal scholar.db-shm &&
  mv scholar.db.restoring scholar.db'
docker start alma
```

Then start ALMa and watch the Activity panel. Fresh backups need no
migration; older ones might. If the backup was written by a newer
release, ALMa exits with a message naming both schema versions and
changes nothing. When you are satisfied, delete
`scholar.db.before-restore`.

## Verifying a backup

A quick sanity check after restore:

```bash
sqlite3 data/scholar.db "SELECT COUNT(*) AS papers FROM papers;"
sqlite3 data/scholar.db "SELECT COUNT(*) AS saved FROM papers WHERE status='library';"
sqlite3 data/scholar.db "SELECT COUNT(*) AS authors FROM authors;"
```

Under Docker the image has no `sqlite3`, so ask Python in the container:

```bash
docker exec alma python -c "import sqlite3; c = sqlite3.connect('/app/data/scholar.db'); print(c.execute('SELECT COUNT(*) FROM papers').fetchone()[0])"
```

Compare against the numbers you had before. The Insights overview
also shows totals — open it after restore and confirm.

## Export

Different from backup — exports are **lossy** but **portable**:

* **JSON export** — `GET /api/v1/backup/export/json` dumps the
  saved Library as JSON. Useful for moving to another tool.
* **BibTeX export** — `GET /api/v1/backup/export/bibtex` dumps
  saved papers as `.bib`. Good for citation managers.

These don't preserve internal state (lens configs, signal events,
schedules). For a lossless backup, use the file or online paths
above.

## Cadence

A reasonable default for a personal install:

* **Online backup**: weekly (cron the API call).
* **File-copy backup**: monthly to off-machine storage (cloud
  bucket, second drive).
* **Before any risky operation**: manual backup right before an
  upgrade, or Settings → Library maintenance → Reset / dedup / large
  bulk import.

## What's not backed up by default

* `frontend/dist/` — regenerable with `npm run build`.
* `__pycache__/`, `.venv/` — regenerable with `pip install -e .`.
* `data/caches/` — regenerable on next refresh.

The full backup target is `data/` + `settings.json` + `.env`. Keep
them on a backup that includes file timestamps; ALMa uses
`added_at` / `updated_at` heavily, but those are SQLite columns,
not filesystem timestamps.
