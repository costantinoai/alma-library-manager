#!/usr/bin/env python3
"""
Created on Sat Oct  7 14:04:23 2023

@author: costantino_ai
"""
import logging
import os
import sqlite3

from alma.core.sqlite_config import SQLITE_CONNECT_TIMEOUT_S, apply_busy_timeout

logger = logging.getLogger(__name__)


def connect_scholar_db(
    path: str, *, timeout: float = SQLITE_CONNECT_TIMEOUT_S
) -> sqlite3.Connection:
    """Open a scholar.db connection FROM A PATH, with the app's row contract.

    The one factory for code that receives a database *path* instead of a live
    connection (CLI entry points, the OpenAlex ingest writers, the legacy
    fetcher). It exists because every shared paper/author helper reads columns
    by NAME — ``row["doi"]``, ``row["name"]`` — which raises
    ``TypeError: tuple indices must be integers or slices, not str`` against the
    default tuple row factory. That is exactly how the periodic author refresh
    died. ``sqlite3.Row`` also still supports positional access, so
    ``name, oa = row`` keeps working.

    Request-path code keeps using ``alma.api.deps.open_db_connection`` (same row
    contract plus the WAL/pragma assertions for the app's own DB file).
    """
    conn = sqlite3.connect(path, timeout=timeout)
    conn.row_factory = sqlite3.Row
    apply_busy_timeout(conn)
    return conn


def _init_authors_db(authors_path: str) -> sqlite3.Connection:
    """Ensure the authors database exists and return a connection."""

    conn = connect_scholar_db(authors_path)
    # On the app DB the full-shape `authors` table already exists
    # (init_db_schema); this minimal CREATE only fires for a standalone
    # CLI-created file and is a no-op everywhere else.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS authors (
                name TEXT,
                id TEXT PRIMARY KEY,
                openalex_id TEXT
            )"""
    )
    return conn


def clean_pubs(fetched_pubs, from_year=2023, exclude_not_cited_papers=False):
    """
    Filters and processes the fetched publications based on specified criteria.

    Parameters:
        fetched_pubs (list): List of raw publication data fetched from Google Scholar.
        from_year (int, optional): Minimum year to include publications. Defaults to 2023.
        exclude_not_cited_papers (bool, optional): If True, excludes papers that haven't been cited. Defaults to False.

    Returns:
        list: A list of dictionaries containing cleaned and processed publication data.

    Example:
        cleaned_publications = clean_pubs(raw_publications, 2020, True)
    """

    # Set to keep track of seen titles to avoid double publications
    seen_titles = set()

    # Initialize a list to hold publications that match the specified criteria
    relevant_pubs = []

    for pub in fetched_pubs:
        # Check if the publication meets the year criterion and hasn't been seen before
        if (
            pub["bib"].get("pub_year")  # if the publication has a 'year' field
            and int(pub["bib"]["pub_year"]) == int(from_year)
            and (
                not exclude_not_cited_papers or pub["num_citations"] > 0
            )  # if exclude_not_cited_papers is True, then we select only papers with citations
            and pub["bib"]["title"] not in seen_titles
        ):  # only add the pub if it is the only pub with this title (avoid dupes)

            # Add the title to the seen set
            seen_titles.add(pub["bib"]["title"])

            # Append the relevant publication to the list
            relevant_pubs.append(
                {
                    "title": pub["bib"]["title"],
                    "authors": pub["bib"]["author"].replace(" and ", ", "),
                    "abstract": pub["bib"].get("abstract", "No abstract available"),
                    "year": pub["bib"]["pub_year"],
                    "num_citations": pub["num_citations"],
                    "journal": pub["bib"]["citation"],
                    "pub_url": pub["pub_url"] if "pub_url" in pub.keys() else None,
                }
            )

    # Sort the list of relevant publications by the number of citations in descending order
    sorted_pubs = sorted(relevant_pubs, key=lambda x: x["num_citations"], reverse=True)

    # Return the cleaned and sorted list of publications
    return sorted_pubs


def ensure_output_folder(output_folder):
    """
    Checks for the existence of the output folder, and if it doesn't exist, creates it.

    Raises:
    - Exception: If there's any error during folder creation.
    """
    if not os.path.exists(output_folder):  # Check if directory exists
        logger.info(f"Output folder '{output_folder}' does not exist. Creating it.")
        os.makedirs(output_folder)  # Create directory
