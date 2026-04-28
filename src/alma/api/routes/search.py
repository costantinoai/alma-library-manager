"""Global search endpoint for command palette."""

import json
import logging
import sqlite3
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, Query

from alma.api.deps import get_current_user, get_db

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


class SearchResult:
    """A single search result."""

    def __init__(self, id: str, name: str, type: str, url: str, extra: Dict[str, Any] = None):
        self.id = id
        self.name = name
        self.type = type
        self.url = url
        self.extra = extra or {}


@router.get("/search")
def global_search(
    q: str = Query(..., description="Search query"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Search across papers, authors, collections, and topics.

    Returns up to 5 results per category.
    """
    if not q or len(q.strip()) < 2:
        return {
            "papers": [],
            "authors": [],
            "collections": [],
            "topics": [],
        }

    query = f"%{q.strip()}%"
    limit = 5

    results = {
        "papers": [],
        "authors": [],
        "collections": [],
        "topics": [],
    }

    # Search papers (title, abstract)
    try:
        paper_rows = db.execute(
            """
            SELECT id, title, authors, year, status
            FROM papers
            WHERE title LIKE ? OR abstract LIKE ?
            ORDER BY
                CASE WHEN status = 'library' THEN 0 ELSE 1 END,
                year DESC
            LIMIT ?
            """,
            (query, query, limit),
        ).fetchall()

        for row in paper_rows:
            results["papers"].append({
                "id": row["id"],
                "name": row["title"],
                "type": "paper",
                "url": f"#/library?paper={row['id']}",
                "subtitle": f"{row['authors'] or 'Unknown'} ({row['year'] or 'n/a'})",
                "status": row["status"],
            })
    except Exception as e:
        logger.error(f"Error searching papers: {e}")

    # Search authors (display_name)
    try:
        author_rows = db.execute(
            """
            SELECT id, name, affiliation, works_count
            FROM authors
            WHERE name LIKE ?
            ORDER BY works_count DESC NULLS LAST
            LIMIT ?
            """,
            (query, limit),
        ).fetchall()

        for row in author_rows:
            results["authors"].append({
                "id": row["id"],
                "name": row["name"],
                "type": "author",
                "url": f"#/authors?author={row['id']}",
                "subtitle": row["affiliation"] or "No affiliation",
            })
    except Exception as e:
        logger.error(f"Error searching authors: {e}")

    # Search collections (name)
    try:
        collection_rows = db.execute(
            """
            SELECT c.id, c.name, c.description, COUNT(ci.paper_id) as item_count
            FROM collections c
            LEFT JOIN collection_items ci ON c.id = ci.collection_id
            WHERE c.name LIKE ? OR c.description LIKE ?
            GROUP BY c.id
            ORDER BY item_count DESC
            LIMIT ?
            """,
            (query, query, limit),
        ).fetchall()

        for row in collection_rows:
            results["collections"].append({
                "id": row["id"],
                "name": row["name"],
                "type": "collection",
                "url": f"#/library?collection={row['id']}",
                "subtitle": f"{row['item_count']} papers",
            })
    except Exception as e:
        logger.error(f"Error searching collections: {e}")

    # Search topics (term from publication_topics)
    try:
        topic_rows = db.execute(
            """
            SELECT term, COUNT(DISTINCT paper_id) as paper_count
            FROM publication_topics
            WHERE term LIKE ?
            GROUP BY term
            ORDER BY paper_count DESC
            LIMIT ?
            """,
            (query, limit),
        ).fetchall()

        for row in topic_rows:
            term = row["term"]
            results["topics"].append({
                "id": term,
                "name": term,
                "type": "topic",
                "url": f"#/library?topic={term}",
                "subtitle": f"{row['paper_count']} papers",
            })
    except Exception as e:
        logger.error(f"Error searching topics: {e}")

    return results
