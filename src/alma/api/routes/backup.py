"""Backup and export API endpoints.

Provides data export capabilities:
- Full database backup (SQLite file)
- BibTeX export of library papers
- JSON export of library papers with metadata
"""

import json
import logging
import os
import re
import sqlite3
import tempfile
import unicodedata
from collections.abc import Iterator
from datetime import datetime
from itertools import count, product
from pathlib import Path
from string import ascii_lowercase
from typing import BinaryIO

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse

from alma.api.deps import get_current_user, get_db
from alma.api.helpers import raise_internal
from alma.config import get_db_path
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.sqlite_backup import online_sqlite_backup

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/backup",
    tags=["backup"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


_SNAPSHOT_PREFIX = ".alma-export-"
_SNAPSHOT_SUFFIX = ".tmp"


def clear_stale_export_snapshots(db_path: Path) -> None:
    """Delete export snapshots that a crash left beside the database.

    Startup calls this: no export is running yet, so any such file is a leftover
    copy of the library. Only this folder is searched, by the exact name
    ``mkstemp`` was given. A file that cannot be deleted must not stop the app.
    """
    for stale in db_path.parent.glob(f"{_SNAPSHOT_PREFIX}*{_SNAPSHOT_SUFFIX}"):
        try:
            stale.unlink()
            logger.warning("Removed an export snapshot left by a crash: %s", stale)
        except OSError as exc:
            logger.warning("Could not remove the stale export snapshot %s: %s", stale, exc)


def _snapshot_database(db_path: Path) -> BinaryIO:
    """Return an open handle on a consistent copy of the live database.

    The copy sits beside the database because Compose mounts ``/tmp`` as a
    128 MB tmpfs. SQLite's backup API needs a path, so the copy has a name while
    it is written. It is unlinked before this returns, and the handle keeps the
    bytes readable until the download ends, so a dropped connection leaves
    nothing. A crash during the copy does leave the file; the next startup
    removes it (:func:`clear_stale_export_snapshots`).
    """
    fd, name = tempfile.mkstemp(prefix=_SNAPSHOT_PREFIX, suffix=_SNAPSHOT_SUFFIX, dir=db_path.parent)
    os.close(fd)
    snapshot = Path(name)
    try:
        online_sqlite_backup(db_path, snapshot)
        return snapshot.open("rb")
    finally:
        snapshot.unlink(missing_ok=True)


def _read_chunks(stream: BinaryIO, size: int = 1 << 20) -> Iterator[bytes]:
    with stream:
        while chunk := stream.read(size):
            yield chunk


@router.get("/export")
def export_database():
    """Export the entire SQLite database as a downloadable file.

    The snapshot comes from SQLite's backup API, so it holds every committed
    transaction, including those still in the WAL, and writes can continue
    while it is taken. Filename: alma-backup-{date}.db
    """
    db_path = get_db_path()
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database file not found")

    try:
        stream = _snapshot_database(db_path)
    except Exception as e:
        raise_internal("Export failed", e)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_filename = f"alma-backup-{timestamp}.db"
    return StreamingResponse(
        _read_chunks(stream),
        # octet-stream keeps the response out of the gzip middleware.
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{backup_filename}"',
            "Content-Length": str(os.fstat(stream.fileno()).st_size),
        },
    )


def _bibtex_value(value: object) -> str:
    """Return ``value`` safe inside ``field = {...}``.

    A title holding `}` would close the field and could add its own, such as
    `file =`. Balanced braces are kept, because `{M}ozart` is how BibTeX titles
    protect capitals. Parsers disagree on whether `\\{` counts, so a value must
    balance read either way; otherwise its braces go. A trailing backslash goes
    in both cases, because it would turn the field's own closing brace into `\\}`.
    """
    text = str(value)

    def balanced(candidate: str) -> bool:
        depth = 0
        for ch in candidate:
            depth += (ch == "{") - (ch == "}")
            if depth < 0:
                return False
        return depth == 0

    unescaped = text.replace("\\{", "").replace("\\}", "")
    if balanced(text) and balanced(unescaped):
        return text.rstrip("\\")
    return text.replace("{", "").replace("}", "").rstrip("\\")


def _cite_key_text(text: str) -> str:
    """Keep `text` as cite-key characters: Latin letters plain, other scripts as written.

    `Müller` folds to `Muller`, the ASCII key everyone types. A letter with no
    ASCII form (`Ø`, `И`, `王`) stays, since Biber and modern BibTeX read UTF-8
    keys, and deleting it would reduce a Chinese author's key to the bare year.
    """
    folded = ""
    for ch in text:
        plain = "".join(
            part for part in unicodedata.normalize("NFKD", ch) if not unicodedata.combining(part)
        )
        folded += plain if plain.isascii() else ch
    return re.sub(r"[^\w:-]", "", folded)


def _unique_cite_key(key: str, used: set[str]) -> str:
    """`key`, or `key` plus the first free suffix: a, b, ... z, aa, ab.

    BibTeX reports `Li2020` and `LI2020` as a repeated entry, so `used` holds
    case-folded keys. The export order is fixed, so an export is reproducible.
    """
    letters = ("".join(tail) for size in count(1) for tail in product(ascii_lowercase, repeat=size))
    candidate = key
    while candidate.casefold() in used:
        candidate = f"{key}{next(letters)}"
    used.add(candidate.casefold())
    return candidate


@router.get("/export/bibtex")
def export_bibtex(conn: sqlite3.Connection = Depends(get_db)):
    """Export library papers as BibTeX.

    Returns all papers with status='library' formatted as valid BibTeX entries.
    """
    try:
        cursor = conn.execute(
            f"""SELECT id, title, authors, year, journal, doi, url, abstract, volume, issue, first_page, last_page
               FROM papers
               WHERE status = 'library'
                 AND {standalone_paper_sql('papers')}
               ORDER BY year DESC, title ASC, id ASC"""
        )
        papers = cursor.fetchall()

        bibtex_entries = []
        used_keys: set[str] = set()
        for paper in papers:
            # Generate citation key (simple: first_author_year_title_word)
            authors_str = paper["authors"] or ""
            first_author = authors_str.split(",")[0].strip() if authors_str else "Unknown"
            first_author_lastname = first_author.split()[-1] if first_author else "Unknown"
            year = paper["year"] or "0000"
            title_words = (paper["title"] or "").split()
            first_title_word = title_words[0].strip(".,;:!?()[]{}\"'") if title_words else "paper"
            cite_key = _unique_cite_key(
                _cite_key_text(f"{first_author_lastname}{year}{first_title_word}"),
                used_keys,
            )

            # Build BibTeX entry
            entry = f"@article{{{cite_key},\n"
            entry += f'  title = {{{_bibtex_value(paper["title"] or "Untitled")}}},\n'
            if authors_str:
                entry += f'  author = {{{_bibtex_value(authors_str)}}},\n'
            if paper["year"]:
                entry += f'  year = {{{_bibtex_value(paper["year"])}}},\n'
            if paper["journal"]:
                entry += f'  journal = {{{_bibtex_value(paper["journal"])}}},\n'
            if paper["volume"]:
                entry += f'  volume = {{{_bibtex_value(paper["volume"])}}},\n'
            if paper["issue"]:
                entry += f'  number = {{{_bibtex_value(paper["issue"])}}},\n'
            if paper["first_page"]:
                pages = paper["first_page"]
                if paper["last_page"]:
                    pages += f'--{paper["last_page"]}'
                entry += f'  pages = {{{_bibtex_value(pages)}}},\n'
            if paper["doi"]:
                entry += f'  doi = {{{_bibtex_value(paper["doi"])}}},\n'
            if paper["url"]:
                entry += f'  url = {{{_bibtex_value(paper["url"])}}},\n'
            if paper["abstract"]:
                entry += f'  abstract = {{{_bibtex_value(paper["abstract"])}}},\n'
            entry = entry.rstrip(",\n") + "\n}\n"
            bibtex_entries.append(entry)

        bibtex_content = "\n".join(bibtex_entries)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"alma-library-{timestamp}.bib"

        return Response(
            content=bibtex_content,
            media_type="text/plain",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        logger.error(f"Failed to export BibTeX: {e}")
        raise HTTPException(status_code=500, detail=f"BibTeX export failed: {str(e)}")


@router.get("/export/json")
def export_json(conn: sqlite3.Connection = Depends(get_db)):
    """Export library papers as JSON.

    Returns all papers with status='library', including their tags, collections,
    authors, and topics.
    """
    try:
        # Get all library papers
        cursor = conn.execute(
            f"""SELECT id, title, authors, year, journal, doi, url, abstract,
                      openalex_id, work_type, language, is_oa, oa_status, oa_url,
                      cited_by_count, rating, notes, added_at, added_from
               FROM papers
               WHERE status = 'library'
                 AND {standalone_paper_sql('papers')}
               ORDER BY year DESC, title ASC"""
        )
        papers = cursor.fetchall()

        result = []
        for paper in papers:
            paper_id = paper["id"]
            paper_dict = dict(paper)

            # Get tags
            tag_cursor = conn.execute(
                """SELECT t.name, t.color
                   FROM publication_tags pt
                   JOIN tags t ON pt.tag_id = t.id
                   WHERE pt.paper_id = ?""",
                (paper_id,)
            )
            paper_dict["tags"] = [dict(row) for row in tag_cursor.fetchall()]

            # Get collections
            coll_cursor = conn.execute(
                """SELECT c.name, c.description, c.color
                   FROM collection_items ci
                   JOIN collections c ON ci.collection_id = c.id
                   WHERE ci.paper_id = ?""",
                (paper_id,)
            )
            paper_dict["collections"] = [dict(row) for row in coll_cursor.fetchall()]

            # Get authors (structured)
            auth_cursor = conn.execute(
                """SELECT openalex_id, display_name, orcid, position, is_corresponding, institution
                   FROM publication_authors
                   WHERE paper_id = ?
                   ORDER BY position""",
                (paper_id,)
            )
            paper_dict["structured_authors"] = [dict(row) for row in auth_cursor.fetchall()]

            # Get topics
            topic_cursor = conn.execute(
                """SELECT term, score, domain, field, subfield
                   FROM publication_topics
                   WHERE paper_id = ?
                   ORDER BY score DESC""",
                (paper_id,)
            )
            paper_dict["topics"] = [dict(row) for row in topic_cursor.fetchall()]

            result.append(paper_dict)

        export_data = {
            "export_date": datetime.now().isoformat(),
            "total_papers": len(result),
            "papers": result,
        }

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"alma-library-{timestamp}.json"

        return Response(
            content=json.dumps(export_data, indent=2, default=str),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        logger.error(f"Failed to export JSON: {e}")
        raise HTTPException(status_code=500, detail=f"JSON export failed: {str(e)}")
