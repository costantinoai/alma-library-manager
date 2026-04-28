---
title: Backups
description: Online and offline backup strategies for scholar.db and the surrounding data directory.
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

WAL files (`-wal`, `-shm`) are part of the live database. Don't
copy `scholar.db` while ALMa is running without using the online
backup paths below — you'll get an inconsistent snapshot otherwise.

## Online backup (UI)

**Settings → Data & system → Library management → Backup**:

* **Create backup** — uses SQLite's online backup API. Safe while
  ALMa is running. Writes a timestamped snapshot to
  `data/backups/scholar-YYYY-MM-DD-HHMMSS.db`.
* **List backups** — shows existing snapshots with size and
  timestamp.
* **Restore from backup** — replaces the live `scholar.db` with
  the chosen snapshot. **Backend will restart**; in-flight jobs
  abort.

Online backups are atomic at the SQLite page level — no risk of a
torn read.

## Online backup (API)

Same operations via REST:

```bash
# create
curl -X POST http://localhost:8000/api/v1/library-mgmt/backup \
  -d '{"name":"pre-migration-2026-04-25"}'

# list
curl http://localhost:8000/api/v1/library-mgmt/backup

# restore
curl -X POST http://localhost:8000/api/v1/library-mgmt/restore/pre-migration-2026-04-25
```

## Offline backup (file copy)

If ALMa is **stopped**:

```bash
# stop ALMa first
docker compose down               # or kill the uvicorn process

# tar everything
tar -czf alma-backup-$(date +%F).tar.gz data/ settings.json .env

# done; restart
docker compose up -d
```

If ALMa is **running**, you can still make a consistent file copy by
forcing a SQLite checkpoint first:

```bash
sqlite3 data/scholar.db "PRAGMA wal_checkpoint(TRUNCATE);"
cp data/scholar.db data/scholar-snapshot.db
```

The checkpoint folds the WAL back into the main file, then the copy
is consistent. Repeat as a cron job for periodic offline backups.

## Restore

For a UI-driven restore, see above.

For a file-copy restore:

1. Stop ALMa.
2. Replace `data/scholar.db` (and remove any `-wal` / `-shm`).
3. Start ALMa.
4. Watch the Activity panel — the schema migration check runs on
   start-up. Fresh backups don't need migration; older ones might.

## Verifying a backup

A quick sanity check after restore:

```bash
sqlite3 data/scholar.db "SELECT COUNT(*) AS papers FROM papers;"
sqlite3 data/scholar.db "SELECT COUNT(*) AS saved FROM papers WHERE status='library';"
sqlite3 data/scholar.db "SELECT COUNT(*) AS authors FROM authors;"
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
* **Before any risky operation**: manual backup right before
  Settings → Library management → Reset / dedup / large bulk import.

## What's not backed up by default

* `frontend/dist/` — regenerable with `npm run build`.
* `__pycache__/`, `.venv/` — regenerable with `pip install -e .`.
* `data/caches/` — regenerable on next refresh.

The full backup target is `data/` + `settings.json` + `.env`. Keep
them on a backup that includes file timestamps; ALMa uses
`added_at` / `updated_at` heavily, but those are SQLite columns,
not filesystem timestamps.
