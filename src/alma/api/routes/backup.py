"""Backup and export API endpoints.

Provides data export capabilities:
- Full database backup (SQLite file)
- BibTeX export of library papers
- JSON export of library papers with metadata
"""

import logging
import shutil
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, Response

from alma.api.deps import get_db, get_current_user
from alma.config import get_db_path

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/backup",
    tags=["backup"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


@router.get("/export")
def export_database():
    """Export the entire SQLite database as a downloadable file.

    Creates a snapshot copy of the database file and returns it.
    Filename: alma-backup-{date}.db
    """
    try:
        db_path = get_db_path()
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="Database file not found")

        # Create a temporary snapshot
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_filename = f"alma-backup-{timestamp}.db"
        temp_backup = Path("/tmp") / backup_filename

        # Copy the database file
        shutil.copy2(db_path, temp_backup)

        return FileResponse(
            path=str(temp_backup),
            media_type="application/x-sqlite3",
            filename=backup_filename,
            headers={"Content-Disposition": f'attachment; filename="{backup_filename}"'},
        )
    except Exception as e:
        logger.error(f"Failed to export database: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get("/export/bibtex")
def export_bibtex(conn: sqlite3.Connection = Depends(get_db)):
    """Export library papers as BibTeX.

    Returns all papers with status='library' formatted as valid BibTeX entries.
    """
    try:
        cursor = conn.execute(
            """SELECT id, title, authors, year, journal, doi, url, abstract, volume, issue, first_page, last_page
               FROM papers
               WHERE status = 'library'
               ORDER BY year DESC, title ASC"""
        )
        papers = cursor.fetchall()

        bibtex_entries = []
        for paper in papers:
            # Generate citation key (simple: first_author_year_title_word)
            authors_str = paper["authors"] or ""
            first_author = authors_str.split(",")[0].strip() if authors_str else "Unknown"
            first_author_lastname = first_author.split()[-1] if first_author else "Unknown"
            year = paper["year"] or "0000"
            title_words = (paper["title"] or "").split()
            first_title_word = title_words[0].strip(".,;:!?()[]{}\"'") if title_words else "paper"
            cite_key = f"{first_author_lastname}{year}{first_title_word}".replace(" ", "")

            # Build BibTeX entry
            entry = f"@article{{{cite_key},\n"
            entry += f'  title = {{{paper["title"] or "Untitled"}}},\n'
            if authors_str:
                entry += f'  author = {{{authors_str}}},\n'
            if paper["year"]:
                entry += f'  year = {{{paper["year"]}}},\n'
            if paper["journal"]:
                entry += f'  journal = {{{paper["journal"]}}},\n'
            if paper["volume"]:
                entry += f'  volume = {{{paper["volume"]}}},\n'
            if paper["issue"]:
                entry += f'  number = {{{paper["issue"]}}},\n'
            if paper["first_page"]:
                pages = paper["first_page"]
                if paper["last_page"]:
                    pages += f'--{paper["last_page"]}'
                entry += f'  pages = {{{pages}}},\n'
            if paper["doi"]:
                entry += f'  doi = {{{paper["doi"]}}},\n'
            if paper["url"]:
                entry += f'  url = {{{paper["url"]}}},\n'
            if paper["abstract"]:
                # Escape special BibTeX characters in abstract
                abstract_clean = paper["abstract"].replace("{", "\\{").replace("}", "\\}")
                entry += f'  abstract = {{{abstract_clean}}},\n'
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
            """SELECT id, title, authors, year, journal, doi, url, abstract,
                      openalex_id, work_type, language, is_oa, oa_status, oa_url,
                      cited_by_count, rating, notes, added_at, added_from
               FROM papers
               WHERE status = 'library'
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
