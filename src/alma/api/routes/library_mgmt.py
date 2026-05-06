"""Library database management API endpoints.

Provides endpoints for inspecting, backing up, restoring, and resetting
the authors and publications databases.

Backups are written gzip-compressed (``scholar_<ts>.db.gz``) — typically
2.5–3× smaller than the raw SQLite file. Retention is capped at the
``ALMA_BACKUP_RETAIN`` count (default 5) so the backups directory can't
grow unbounded.
"""

import gzip
import logging
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status

from alma.api.deps import get_db, get_current_user, open_db_connection
from alma.api.helpers import raise_internal
from alma.config import get_db_path

logger = logging.getLogger(__name__)

router = APIRouter(
    responses={
        401: {"description": "Unauthorized"},
        500: {"description": "Internal Server Error"},
    },
)

# Relative path used for display purposes in the info response.
_BACKUPS_DIR_NAME = "data/backups"

# How many backups to keep on disk before pruning the oldest. Override
# with the ``ALMA_BACKUP_RETAIN`` env var.
_DEFAULT_BACKUP_RETAIN = 5


def _backup_retain_count() -> int:
    raw = os.getenv("ALMA_BACKUP_RETAIN", "").strip()
    if not raw:
        return _DEFAULT_BACKUP_RETAIN
    try:
        n = int(raw)
    except ValueError:
        return _DEFAULT_BACKUP_RETAIN
    return max(1, n)


def _backups_dir() -> Path:
    """Return the absolute path to the backups directory."""
    # Backups are stored alongside the data directory.
    db_path = get_db_path()
    return db_path.parent / "backups"


def _is_backup_file(path: Path) -> bool:
    name = path.name
    return name.endswith(".db") or name.endswith(".db.gz")


def _list_backups() -> list[dict]:
    """List all backup files (both compressed and legacy uncompressed)."""
    backups_path = _backups_dir()
    if not backups_path.exists():
        return []

    backups = []
    for f in sorted(backups_path.iterdir()):
        if not _is_backup_file(f):
            continue
        try:
            backups.append({
                "name": f.name,
                "created_at": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                "size_bytes": f.stat().st_size,
                "compressed": f.name.endswith(".db.gz"),
            })
        except OSError:
            continue
    return backups


def _online_sqlite_backup(src_path: Path, dst_path: Path) -> None:
    """Copy ``src_path`` to ``dst_path`` via SQLite's online backup API.

    Unlike ``shutil.copy2``, this captures a transactionally-consistent
    snapshot even when the live DB is mid-write or has uncheckpointed
    WAL frames.
    """
    src_conn = sqlite3.connect(str(src_path))
    try:
        dst_conn = sqlite3.connect(str(dst_path))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()


def _gzip_file_in_place(src: Path) -> Path:
    """Replace ``src`` with ``src.with_suffix('.gz')`` and return the new path.

    Streams through gzip with the default compression level (6), which
    is the sweet spot for SQLite files (level 9 saves <2% at 4× CPU).
    """
    dst = src.with_name(src.name + ".gz")
    with src.open("rb") as fin, gzip.open(str(dst), "wb") as fout:
        shutil.copyfileobj(fin, fout, length=1 << 20)
    src.unlink()
    return dst


def _prune_backups(retain: int | None = None) -> list[str]:
    """Delete the oldest backups beyond the retention limit.

    Returns the names of files that were pruned.
    """
    keep = retain if retain is not None else _backup_retain_count()
    backups_path = _backups_dir()
    if not backups_path.exists():
        return []
    files = sorted(
        (f for f in backups_path.iterdir() if _is_backup_file(f)),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    pruned: list[str] = []
    for old in files[keep:]:
        try:
            old.unlink()
            pruned.append(old.name)
        except OSError as exc:
            logger.warning("Could not prune backup %s: %s", old.name, exc)
    if pruned:
        logger.info("Pruned %d old backup(s): %s", len(pruned), pruned)
    return pruned


def _create_backup() -> dict:
    """Create a transactionally-consistent gzip-compressed backup.

    Uses SQLite's online backup API to write a tmp ``.db`` file, gzips
    the result to ``scholar_<ts>.db.gz``, then prunes old backups beyond
    the retention limit.

    Returns a dict with ``backup_name`` (timestamp), ``db_size`` (final
    on-disk size in bytes), ``raw_size`` (uncompressed source size), and
    ``compressed=True``.
    """
    backups_path = _backups_dir()
    backups_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_src = get_db_path()

    raw_size = db_src.stat().st_size if db_src.exists() else 0

    # Write the consistent snapshot to a tmp .db, then gzip atomically
    # next to it (fsync of the gz file before rename).
    with tempfile.NamedTemporaryFile(
        prefix=f"scholar_{timestamp}_",
        suffix=".db",
        dir=str(backups_path),
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        _online_sqlite_backup(db_src, tmp_path)
        gz_path = _gzip_file_in_place(tmp_path)
        final = backups_path / f"scholar_{timestamp}.db.gz"
        gz_path.rename(final)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    _prune_backups()

    return {
        "backup_name": timestamp,
        "db_size": final.stat().st_size,
        "raw_size": raw_size,
        "compressed": True,
    }


def _resolve_backup_file(backup_name: str) -> Path:
    """Resolve a backup identifier to an existing file in the backups directory.

    Accepts (case-sensitive on ``.db``/``.db.gz`` suffixes):

    - timestamp only: ``20260214_091500``
    - filename without suffix: ``scholar_20260214_091500``
    - full filename: ``scholar_20260214_091500.db`` or ``...db.gz``
    """
    raw = (backup_name or "").strip()
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Backup name is required")
    if "/" in raw or "\\" in raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid backup name")

    backups_path = _backups_dir()
    candidates: list[Path] = []
    if raw.endswith(".db.gz") or raw.endswith(".db"):
        candidates.append(backups_path / raw)
    else:
        # Try compressed first (current default), then legacy uncompressed.
        candidates.extend([
            backups_path / f"{raw}.db.gz",
            backups_path / f"scholar_{raw}.db.gz",
            backups_path / f"{raw}.db",
            backups_path / f"scholar_{raw}.db",
            backups_path / raw,
        ])

    for cand in candidates:
        try:
            if cand.exists() and cand.is_file():
                return cand
        except OSError:
            continue

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Backup '{backup_name}' not found.",
    )


def _decompress_to_tmp(gz_path: Path) -> Path:
    """Decompress a ``.db.gz`` backup into a tmp ``.db`` file in the same dir.

    Caller is responsible for unlinking the returned path.
    """
    with tempfile.NamedTemporaryFile(
        prefix="restore_",
        suffix=".db",
        dir=str(gz_path.parent),
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)
    with gzip.open(str(gz_path), "rb") as fin, tmp_path.open("wb") as fout:
        shutil.copyfileobj(fin, fout, length=1 << 20)
    return tmp_path


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------

@router.get(
    "/info",
    summary="Database information",
    description="Return metadata about the unified database and available backups.",
)
def db_info(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Return size, record counts, and backup listing for the unified database."""
    try:
        db_path = get_db_path()
        db_size = db_path.stat().st_size if db_path.exists() else 0

        # Authors count
        authors_count = db.execute("SELECT COUNT(*) AS c FROM authors").fetchone()["c"]

        # Publications count
        publications_count = db.execute("SELECT COUNT(*) AS c FROM papers").fetchone()["c"]

        # Optional related table counts (may not exist yet)
        topics_count = 0
        institutions_count = 0
        try:
            topics_count = db.execute("SELECT COUNT(*) AS c FROM publication_topics").fetchone()["c"]
        except sqlite3.OperationalError:
            pass
        try:
            institutions_count = db.execute("SELECT COUNT(*) AS c FROM publication_institutions").fetchone()["c"]
        except sqlite3.OperationalError:
            pass

        backups = _list_backups()

        return {
            "database": {
                "path": str(db_path),
                "size_bytes": db_size,
                "authors_count": authors_count,
                "publications_count": publications_count,
                "topics_count": topics_count,
                "institutions_count": institutions_count,
            },
            "backups": backups,
        }
    except Exception as e:
        raise_internal("Failed to retrieve database info", e)


@router.post(
    "/backup",
    summary="Create database backup",
    description="Create a timestamped backup of the unified database.",
)
def create_backup(
    user: dict = Depends(get_current_user),
):
    """Copy the unified DB file into data/backups/ with a timestamp suffix."""
    try:
        result = _create_backup()
        logger.info("Backup created: %s", result["backup_name"])
        return {
            "success": True,
            "backup_name": result["backup_name"],
            "db_size": result["db_size"],
        }
    except Exception as e:
        raise_internal("Failed to create backup", e)


@router.post(
    "/restore/{backup_name}",
    summary="Restore from backup",
    description="Restore the unified database from a previously created backup.",
)
def restore_backup(
    backup_name: str,
    user: dict = Depends(get_current_user),
):
    """Restore the unified database from a named backup.

    Transparently decompresses ``.db.gz`` backups into a tmp file before
    overwriting the live DB.
    """
    tmp_decompressed: Path | None = None
    try:
        db_backup = _resolve_backup_file(backup_name)
        db_dst = get_db_path()

        if db_backup.name.endswith(".db.gz"):
            tmp_decompressed = _decompress_to_tmp(db_backup)
            shutil.copy2(str(tmp_decompressed), str(db_dst))
        else:
            shutil.copy2(str(db_backup), str(db_dst))

        logger.info("Restored from backup file: %s", db_backup.name)
        return {"success": True, "restored_from": db_backup.name}

    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to restore backup '{backup_name}'", e)
    finally:
        if tmp_decompressed and tmp_decompressed.exists():
            try:
                tmp_decompressed.unlink()
            except OSError:
                pass


@router.delete(
    "/backup/{backup_name}",
    summary="Delete a backup",
    description="Delete a backup file from the backups directory.",
)
def delete_backup(
    backup_name: str,
    user: dict = Depends(get_current_user),
):
    """Delete a backup by timestamp or filename."""
    try:
        db_backup = _resolve_backup_file(backup_name)
        deleted_name = db_backup.name
        db_backup.unlink()
        logger.info("Deleted backup: %s", deleted_name)
        return {"success": True, "deleted": deleted_name}
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to delete backup '{backup_name}'", e)


_RESET_TABLES = (
    "papers",
    "feed_items",
    "authors",
    "followed_authors",
    "publication_topics",
    "publication_institutions",
    "publication_embeddings",
    "publication_clusters",
    "publication_references",
    "publication_tags",
    "tag_suggestions",
    "tags",
    "collection_items",
    "collections",
    "recommendations",
    "similarity_cache",
    "graph_cache",
    "alerted_publications",
    "alert_history",
    "alert_rule_assignments",
    "alert_rules",
    "alerts",
)


def _run_reset(job_id: str) -> dict:
    """Execute the destructive reset as an Activity-tracked job.

    Phases commit the SQLite writer lock between steps so the operation
    never blocks concurrent reads for longer than one phase, and VACUUM
    runs only after the delete transaction has fully committed.
    """
    from alma.api.scheduler import add_job_log, set_job_status

    total_phases = 3  # backup, delete, vacuum
    phase = 0

    def _progress(step: str, message: str, *, data: dict | None = None) -> None:
        nonlocal phase
        phase += 1
        set_job_status(
            job_id,
            status="running",
            processed=phase,
            total=total_phases,
            message=message,
        )
        add_job_log(job_id, message, step=step, data=data)

    # Phase 1: backup (file copy, no DB lock held)
    backup_result = _create_backup()
    backup_name = backup_result["backup_name"]
    logger.info("Pre-reset backup created: %s", backup_name)
    _progress(
        "backup",
        f"Backup created: {backup_name}",
        data={"backup_name": backup_name, "db_size": backup_result["db_size"]},
    )

    # Phase 2: clear library tables in a short, committed transaction
    conn = open_db_connection()
    tables_cleared: list[str] = []
    try:
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table in _RESET_TABLES:
            if table in existing_tables:
                try:
                    conn.execute(f"DELETE FROM {table}")  # noqa: S608 -- hardcoded allowlist
                    tables_cleared.append(table)
                except sqlite3.OperationalError as exc:
                    logger.warning("Could not clear table %s: %s", table, exc)
        conn.commit()
    finally:
        conn.close()
    _progress(
        "delete",
        f"Cleared {len(tables_cleared)} tables",
        data={"tables_cleared": tables_cleared},
    )

    # Phase 3: VACUUM on a fresh connection (requires no active transaction).
    vacuum_ok = True
    vacuum_conn = open_db_connection()
    try:
        try:
            vacuum_conn.isolation_level = None  # autocommit for VACUUM
            vacuum_conn.execute("VACUUM")
        except Exception as exc:
            vacuum_ok = False
            logger.warning("VACUUM after reset failed: %s", exc)
    finally:
        vacuum_conn.close()
    _progress(
        "vacuum",
        "VACUUM complete" if vacuum_ok else "VACUUM skipped",
        data={"vacuum_ok": vacuum_ok},
    )

    return {
        "success": True,
        "backup_name": backup_name,
        "tables_cleared": tables_cleared,
        "vacuum_ok": vacuum_ok,
    }


_EMBEDDINGS_RESET_TABLES = (
    "publication_embeddings",
    "author_centroids",
    "publication_embedding_fetch_status",
)


@router.post(
    "/embeddings/reset",
    summary="Delete every cached embedding so they can be re-fetched",
    description=(
        "Wipes `publication_embeddings`, `author_centroids`, and "
        "`publication_embedding_fetch_status` so the next AI run / S2 "
        "backfill repopulates them from scratch. Synchronous (small "
        "tables, < 1 s). Library papers, followed authors, lenses, "
        "feedback events, collections, tags, and the corpus are "
        "preserved — only the vector caches and their per-paper "
        "fetch markers are removed."
    ),
)
def reset_embeddings(
    conn: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    """Clear every embedding-storing table; counts what was removed."""
    cleared: dict[str, int] = {}
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }

    for table in _EMBEDDINGS_RESET_TABLES:
        if table not in existing:
            continue
        try:
            count_row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            count = int(count_row[0] if count_row else 0)
            conn.execute(f"DELETE FROM {table}")  # noqa: S608 — hardcoded allowlist
            cleared[table] = count
        except sqlite3.OperationalError as exc:
            logger.warning("Could not clear embedding table %s: %s", table, exc)

    conn.commit()

    return {
        "success": True,
        "cleared": cleared,
        "total_rows_cleared": sum(cleared.values()),
    }


@router.delete(
    "/reset",
    summary="Reset library data",
    description="Queue a destructive reset of the unified library database. "
                "A backup is automatically created before the reset, tables are "
                "cleared, and the database is compacted. Runs as an Activity job "
                "so the request returns immediately and progress is visible in the "
                "Activity panel.",
)
def reset_publications(
    user: dict = Depends(get_current_user),
):
    """Schedule the library reset as a background Activity job."""
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    operation_key = "library.reset"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Library reset is already running",
        )

    job_id = f"library_reset_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        processed=0,
        total=3,
        message="Queued library reset",
    )

    def _runner():
        try:
            summary = _run_reset(job_id)
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                processed=3,
                total=3,
                message="Library reset completed",
                result=summary,
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.error("Library reset failed: %s", exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message="Library reset failed",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Queued library reset",
        total=3,
    )


@router.post(
    "/deduplicate",
    summary="Deduplicate authors and publications",
    description="Run deduplication and stable-ID maintenance as a background job.",
)
def deduplicate_database(
    user: dict = Depends(get_current_user),
):
    """Schedule a background deduplication + stable ID pass."""
    from alma.api.scheduler import activity_envelope, find_active_job, schedule_immediate, set_job_status

    operation_key = "library.deduplicate"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Deduplication already running",
        )

    job_id = f"dedup_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message="Running database deduplication",
    )

    def _runner():
        from alma.library.deduplication import run_deduplication
        from alma.api.scheduler import set_job_status

        try:
            conn = open_db_connection()
            try:
                summary = run_deduplication(conn, job_id=job_id)
            finally:
                conn.close()
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                message="Database deduplication completed",
                result=summary,
            )
        except Exception as exc:
            logger.exception("Deduplication job %s failed: %s", job_id, exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message="Database deduplication failed",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Deduplication queued",
    )
