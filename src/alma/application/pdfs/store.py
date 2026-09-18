"""The PDF store: a human-readable file tree plus the rows that point into it.

Layout (under ``config.get_pdf_store_dir()``, i.e. ``<db dir>/pdfs``)::

    pdfs/
      2024/Maniquet 2024 - Recurrent issues with deep neural network models.pdf
      undated/Doe - A paper without a year.pdf
      .incoming/<uuid>.part          staging: every write lands here first

A file's name is built ONCE, when it is stored, and never renamed; a name
already taken gets `` [<paper id>]`` appended. Nothing is ever overwritten:
placement links the staged file into the tree and fails if the target exists,
so a replacement always gets a fresh name and the old file is unlinked only
after the row pointing at the new one has committed.

Rows (``paper_pdfs``: one per paper; ``paper_pdf_attempts``: the latest outcome
per paper × source; ``paper_pdf_rejections``: files the user marked wrong) are
written by the row helpers below, which never commit — callers run them inside
their own ``write_section`` / ``run_write_unit``. No network here.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import sqlite3
import unicodedata
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from alma.core.author_names import parse_author_names, surname
from alma.core.time import utcnow

PDFS_TABLE = "paper_pdfs"
ATTEMPTS_TABLE = "paper_pdf_attempts"
REJECTIONS_TABLE = "paper_pdf_rejections"

DDL: tuple[str, ...] = (
    f"""CREATE TABLE IF NOT EXISTS {PDFS_TABLE} (
        paper_id TEXT PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
        rel_path TEXT NOT NULL UNIQUE,
        sha256 TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        pages INTEGER,
        origin TEXT NOT NULL,
        verification TEXT NOT NULL,
        source_id TEXT,
        source_plugin TEXT,
        source_url TEXT,
        original_filename TEXT,
        version TEXT,
        license TEXT,
        stored_at TEXT NOT NULL
    )""",
    f"CREATE INDEX IF NOT EXISTS idx_{PDFS_TABLE}_sha256 ON {PDFS_TABLE}(sha256)",
    f"""CREATE TABLE IF NOT EXISTS {ATTEMPTS_TABLE} (
        paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        outcome TEXT NOT NULL,
        http_status INTEGER,
        detail TEXT,
        candidate_url TEXT,
        job_id TEXT,
        attempted_at TEXT NOT NULL,
        PRIMARY KEY (paper_id, source_id)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {REJECTIONS_TABLE} (
        paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
        sha256 TEXT NOT NULL,
        rejected_at TEXT NOT NULL,
        PRIMARY KEY (paper_id, sha256)
    )""",
)

INCOMING_DIRNAME = ".incoming"
UNDATED_DIRNAME = "undated"
_TITLE_MAX_CHARS = 90
#: Largest PDF ALMa accepts from any source (download, upload, import).
MAX_PDF_BYTES = 100 * 1024 * 1024


class PdfTooLargeError(Exception):
    """A streamed body exceeded the byte cap (the staged file is removed)."""


class UnsafeStorePathError(ValueError):
    """A stored path resolved outside the store root."""


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def store_root(*, create: bool = True) -> Path:
    """The store directory (``<db dir>/pdfs``), created on first use."""
    from alma.config import get_pdf_store_dir

    root = get_pdf_store_dir()
    if create:
        (root / INCOMING_DIRNAME).mkdir(parents=True, exist_ok=True)
    return root


def absolute_path(rel_path: str) -> Path:
    """The file for a stored ``rel_path``, refusing anything outside the root."""
    root = store_root(create=False).resolve()
    candidate = (root / rel_path).resolve()
    if candidate == root or root not in candidate.parents:
        raise UnsafeStorePathError(f"{rel_path!r} is outside the PDF store")
    return candidate


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

_FORBIDDEN_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]+')


def _clean_component(text: str) -> str:
    """A filename-safe fragment: NFC, no path/reserved/control characters.

    Also trims the trailing dots and spaces Windows / OneDrive reject, so the
    tree stays portable when the data volume is synced or copied.
    """
    value = unicodedata.normalize("NFC", str(text or ""))
    value = _FORBIDDEN_CHARS.sub(" ", value)
    value = " ".join(value.split())
    return value.strip(" .")


def _short_title(title: str) -> str:
    clean = _clean_component(title) or "Untitled"
    if len(clean) <= _TITLE_MAX_CHARS:
        return clean
    cut = clean[:_TITLE_MAX_CHARS].rsplit(" ", 1)[0]
    return (cut or clean[:_TITLE_MAX_CHARS]).strip(" .")


def filename_for(*, title: str, year: int | None, authors: str) -> str:
    """The relative path a paper's PDF is stored under (before clash handling).

    ``<Year>/<FirstAuthorSurname> <Year> - <Short title>.pdf``; papers without
    a year live in ``undated/`` and drop the year from the name.
    """
    names = parse_author_names(authors or "")
    lead = _clean_component(surname(names[0])) if names else ""
    lead = lead or "Unknown"
    short = _short_title(title)
    if year:
        return f"{int(year)}/{lead} {int(year)} - {short}.pdf"
    return f"{UNDATED_DIRNAME}/{lead} - {short}.pdf"


# ---------------------------------------------------------------------------
# Staging and placement
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StagedFile:
    """A body written to ``.incoming`` and hashed on the way in."""

    path: Path
    sha256: str
    bytes: int

    def discard(self) -> None:
        self.path.unlink(missing_ok=True)


def new_staging_path() -> Path:
    return store_root() / INCOMING_DIRNAME / f"{uuid.uuid4().hex}.part"


class StagingWriter:
    """Incrementally write one body into ``.incoming``, hashing as it goes.

    Staging lives INSIDE the store (never ``/tmp``, a 128 MB tmpfs in the
    Docker image) so placement is a same-filesystem link. The one writer for
    every source of bytes: a streamed download (:func:`stage_chunks`) and a
    streamed request body (an upload route feeds it ``await``-ed chunks).
    """

    def __init__(self, *, max_bytes: int = MAX_PDF_BYTES) -> None:
        self.max_bytes = int(max_bytes)
        self.path = new_staging_path()
        self._fh = self.path.open("wb")
        self._digest = hashlib.sha256()
        self._written = 0

    def write(self, chunk: bytes) -> None:
        """Append ``chunk``; past the cap, remove the partial file and raise."""
        if not chunk:
            return
        self._written += len(chunk)
        if self._written > self.max_bytes:
            self.abort()
            raise PdfTooLargeError(f"PDF exceeds the {self.max_bytes // (1024 * 1024)} MB limit")
        self._digest.update(chunk)
        self._fh.write(chunk)

    def finish(self) -> StagedFile:
        self._fh.close()
        return StagedFile(path=self.path, sha256=self._digest.hexdigest(), bytes=self._written)

    def abort(self) -> None:
        if not self._fh.closed:
            self._fh.close()
        self.path.unlink(missing_ok=True)


def stage_chunks(chunks: Iterable[bytes], *, max_bytes: int = MAX_PDF_BYTES) -> StagedFile:
    """Stage an iterable of chunks (a streamed download). See :class:`StagingWriter`.

    Raises :class:`PdfTooLargeError` — after removing the partial file — once
    the body passes ``max_bytes``.
    """
    writer = StagingWriter(max_bytes=max_bytes)
    try:
        for chunk in chunks:
            writer.write(chunk)
    except BaseException:
        writer.abort()
        raise
    return writer.finish()


# A staged upload awaiting a decision (PDF-first import that could not be
# identified yet) is addressed by its token: the staging file's uuid.
_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")


def staged_token(staged: StagedFile) -> str:
    return staged.path.stem


def keep_staged(staged: StagedFile, *, filename: str) -> str:
    """Record an upload's original filename beside it; return its token."""
    meta = {"filename": filename, "sha256": staged.sha256, "bytes": staged.bytes}
    staged.path.with_suffix(".json").write_text(json.dumps(meta))
    return staged_token(staged)


def load_staged(token: str) -> tuple[StagedFile, str] | None:
    """The staged upload for ``token`` and its original filename, if it still exists.

    Tokens are validated as bare hex, so a token can never address a path
    outside ``.incoming``.
    """
    if not _TOKEN_RE.match(token or ""):
        return None
    base = store_root() / INCOMING_DIRNAME / token
    part, meta_path = base.with_suffix(".part"), base.with_suffix(".json")
    if not part.exists() or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text())
    except (OSError, ValueError):
        return None
    staged = StagedFile(path=part, sha256=str(meta.get("sha256") or ""), bytes=int(meta.get("bytes") or 0))
    return staged, str(meta.get("filename") or "")


def release_staged(staged: StagedFile) -> None:
    """Drop a staged upload and its sidecar (after it was stored or given up)."""
    staged.discard()
    staged.path.with_suffix(".json").unlink(missing_ok=True)


def read_head(path: Path, size: int = 1024) -> bytes:
    with path.open("rb") as fh:
        return fh.read(size)


def place(staged: StagedFile, rel_path: str, *, paper_id: str) -> str:
    """Move a staged file into the tree under ``rel_path`` — never overwriting.

    If the name is taken the paper id is appended (`` [ab12cd34]``), then a
    counter. The link-then-unlink is atomic per attempt: ``os.link`` fails if
    the target exists, so two writers can never clobber each other. Returns
    the relative path actually used.
    """
    store_root()  # ensure the tree exists
    base = Path(rel_path)
    stem, suffix = base.stem, base.suffix or ".pdf"
    tag = paper_id.replace("-", "")[:8]
    names = [base.name, f"{stem} [{tag}]{suffix}"]
    names += [f"{stem} [{tag}-{n}]{suffix}" for n in range(2, 100)]
    for name in names:
        target_rel = str(base.with_name(name))
        target = absolute_path(target_rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(staged.path, target)
        except FileExistsError:
            continue
        except OSError as exc:
            if exc.errno not in (errno.EPERM, errno.EXDEV, errno.EMLINK, errno.ENOTSUP):
                raise
            # Filesystem without hard links: check-then-replace (the staged
            # file is ours alone, so the only race is a concurrent same-name
            # writer, which the existence check narrows to a window of µs).
            if target.exists():
                continue
            os.replace(staged.path, target)
            return target_rel
        staged.path.unlink(missing_ok=True)
        return target_rel
    raise FileExistsError(f"No free file name for {rel_path!r}")


#: A tree file younger than this is never pruned (a job may be about to
#: commit the row that points at it).
ORPHAN_GRACE_SECONDS = 3600
#: A staged upload awaiting a retry is kept this long.
STAGING_TTL_SECONDS = 24 * 3600


def prune_orphan_files(conn: sqlite3.Connection, *, now: float | None = None) -> int:
    """Remove store files no row points at; return how many were removed.

    Two kinds of leftovers: tree PDFs whose row is gone (a paper deleted, a
    merge that kept the root's own file, a crash between placing and
    committing) — older than :data:`ORPHAN_GRACE_SECONDS`; and staging files
    (abandoned uploads, unresolved imports never retried) — older than
    :data:`STAGING_TTL_SECONDS`. Reads rows, never writes them.
    """
    import time

    root = store_root(create=False)
    if not root.is_dir():
        return 0
    referenced = {str(row[0]) for row in conn.execute(f"SELECT rel_path FROM {PDFS_TABLE}")}
    clock = time.time() if now is None else now
    removed = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        age = clock - path.stat().st_mtime
        if rel.startswith(f"{INCOMING_DIRNAME}/"):
            stale = age > STAGING_TTL_SECONDS
        else:
            stale = path.suffix.lower() == ".pdf" and rel not in referenced and age > ORPHAN_GRACE_SECONDS
        if stale:
            path.unlink(missing_ok=True)
            removed += 1
    return removed


def unlink_quietly(rel_path: str | None) -> None:
    """Remove a stored file if present. Used only AFTER the row change commits."""
    if not rel_path:
        return
    try:
        absolute_path(rel_path).unlink(missing_ok=True)
    except (OSError, UnsafeStorePathError):
        pass


# ---------------------------------------------------------------------------
# Rows (never commit — run inside the caller's write unit / section)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StoredPdf:
    """One ``paper_pdfs`` row."""

    paper_id: str
    rel_path: str
    sha256: str
    bytes: int
    pages: int | None
    origin: str
    verification: str
    source_id: str | None = None
    source_plugin: str | None = None
    source_url: str | None = None
    original_filename: str | None = None
    version: str | None = None
    license: str | None = None
    stored_at: str = ""

    @property
    def filename(self) -> str:
        return Path(self.rel_path).name

    def to_wire(self) -> dict:
        return {
            "filename": self.filename,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "pages": self.pages,
            "origin": self.origin,
            "verification": self.verification,
            "source_id": self.source_id,
            "source_plugin": self.source_plugin,
            "source_url": self.source_url,
            "version": self.version,
            "license": self.license,
            "stored_at": self.stored_at,
        }


_PDF_COLUMNS = (
    "paper_id, rel_path, sha256, bytes, pages, origin, verification, source_id, "
    "source_plugin, source_url, original_filename, version, license, stored_at"
)


def _row_to_pdf(row: sqlite3.Row | tuple) -> StoredPdf:
    return StoredPdf(*tuple(row))


def read_pdf(conn: sqlite3.Connection, paper_id: str) -> StoredPdf | None:
    row = conn.execute(
        f"SELECT {_PDF_COLUMNS} FROM {PDFS_TABLE} WHERE paper_id = ?", (paper_id,)
    ).fetchone()
    return _row_to_pdf(row) if row else None


def upsert_pdf(conn: sqlite3.Connection, record: StoredPdf) -> StoredPdf | None:
    """Point ``record.paper_id`` at ``record``'s file; return the row it replaced.

    The caller unlinks the replaced file AFTER its write unit commits.
    """
    previous = read_pdf(conn, record.paper_id)
    conn.execute(
        f"""INSERT INTO {PDFS_TABLE} ({_PDF_COLUMNS})
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(paper_id) DO UPDATE SET
              rel_path = excluded.rel_path, sha256 = excluded.sha256,
              bytes = excluded.bytes, pages = excluded.pages,
              origin = excluded.origin, verification = excluded.verification,
              source_id = excluded.source_id, source_plugin = excluded.source_plugin,
              source_url = excluded.source_url,
              original_filename = excluded.original_filename,
              version = excluded.version, license = excluded.license,
              stored_at = excluded.stored_at""",
        (
            record.paper_id,
            record.rel_path,
            record.sha256,
            int(record.bytes),
            record.pages,
            str(record.origin),
            str(record.verification),
            record.source_id,
            record.source_plugin,
            record.source_url,
            record.original_filename,
            record.version,
            record.license,
            record.stored_at or utcnow().isoformat(),
        ),
    )
    if previous is not None and previous.rel_path == record.rel_path:
        return None
    return previous


def delete_pdf(conn: sqlite3.Connection, paper_id: str) -> StoredPdf | None:
    """Drop the paper's row; return it so the caller unlinks after commit."""
    previous = read_pdf(conn, paper_id)
    if previous is not None:
        conn.execute(f"DELETE FROM {PDFS_TABLE} WHERE paper_id = ?", (paper_id,))
    return previous


def find_by_sha(conn: sqlite3.Connection, sha256: str) -> StoredPdf | None:
    """The stored PDF with exactly these bytes, if any paper already has them."""
    row = conn.execute(
        f"SELECT {_PDF_COLUMNS} FROM {PDFS_TABLE} WHERE sha256 = ? LIMIT 1", (sha256,)
    ).fetchone()
    return _row_to_pdf(row) if row else None


def is_referenced(conn: sqlite3.Connection, rel_path: str) -> bool:
    row = conn.execute(f"SELECT 1 FROM {PDFS_TABLE} WHERE rel_path = ? LIMIT 1", (rel_path,)).fetchone()
    return row is not None


def record_attempt(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    source_id: str,
    outcome: str,
    http_status: int | None = None,
    detail: str | None = None,
    candidate_url: str | None = None,
    job_id: str | None = None,
) -> None:
    """Upsert the latest outcome of ``source_id`` for ``paper_id``."""
    conn.execute(
        f"""INSERT INTO {ATTEMPTS_TABLE}
              (paper_id, source_id, outcome, http_status, detail, candidate_url, job_id, attempted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(paper_id, source_id) DO UPDATE SET
              outcome = excluded.outcome, http_status = excluded.http_status,
              detail = excluded.detail, candidate_url = excluded.candidate_url,
              job_id = excluded.job_id, attempted_at = excluded.attempted_at""",
        (
            paper_id,
            source_id,
            str(outcome),
            http_status,
            (detail or "")[:500] or None,
            candidate_url,
            job_id,
            utcnow().isoformat(),
        ),
    )


def list_attempts(conn: sqlite3.Connection, paper_id: str) -> list[dict]:
    rows = conn.execute(
        f"""SELECT source_id, outcome, http_status, detail, candidate_url, job_id, attempted_at
            FROM {ATTEMPTS_TABLE} WHERE paper_id = ? ORDER BY attempted_at""",
        (paper_id,),
    ).fetchall()
    keys = ("source_id", "outcome", "http_status", "detail", "candidate_url", "job_id", "attempted_at")
    return [dict(zip(keys, tuple(row), strict=True)) for row in rows]


def reject_sha(conn: sqlite3.Connection, paper_id: str, sha256: str) -> None:
    conn.execute(
        f"INSERT OR IGNORE INTO {REJECTIONS_TABLE} (paper_id, sha256, rejected_at) VALUES (?, ?, ?)",
        (paper_id, sha256, utcnow().isoformat()),
    )


def rejected_shas(conn: sqlite3.Connection, paper_id: str) -> set[str]:
    rows = conn.execute(
        f"SELECT sha256 FROM {REJECTIONS_TABLE} WHERE paper_id = ?", (paper_id,)
    ).fetchall()
    return {str(row[0]) for row in rows}
