#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  7 14:04:23 2023

@author: costantino_ai
"""
import os
import shutil
import logging
import sqlite3
import json

# Scholarly is optional (used only for Google Scholar backend). Avoid importing
# it as a hard dependency at module import time to keep OpenAlex-only flows
# working without the package installed.
try:
    from scholarly import scholarly  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    scholarly = None

logger = logging.getLogger(__name__)


def migrate_legacy_files(root: str = "./src") -> None:
    """Migrate legacy JSON caches and authors list to SQLite if present.

    The project originally stored author and publication information in JSON
    files under ``googleapi_cache`` and ``authors.json``. This helper checks
    for those legacy locations and, when found, imports their contents into the
    SQLite databases before archiving the old files under ``obsolete``.

    Args:
        root: Base directory containing the legacy files. Defaults to ``./src``.
    """

    cache_dir = os.path.join(root, "googleapi_cache")
    authors_json = os.path.join(root, "authors.json")
    backup_dir = os.path.join(root, "googleapi_cache_bkp")

    # If no legacy artifacts exist, there's nothing to migrate.
    if not any(os.path.exists(path) for path in [cache_dir, authors_json, backup_dir]):
        return

    logger.info("Legacy cache detected in %s; starting migration.", root)

    # Prepare publications database and insert cached publications if available.
    from alma.core.utils import generate_paper_id

    pub_db_path = os.path.join(root, "publications.db")
    with sqlite3.connect(pub_db_path) as conn_pub:
        conn_pub.execute(
            """CREATE TABLE IF NOT EXISTS papers (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    year INTEGER,
                    abstract TEXT,
                    url TEXT,
                    cited_by_count INTEGER
                )"""
        )
        conn_pub.execute(
            """CREATE TABLE IF NOT EXISTS publication_authors (
                    paper_id TEXT NOT NULL,
                    openalex_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    orcid TEXT DEFAULT '',
                    position TEXT DEFAULT '',
                    PRIMARY KEY (paper_id, openalex_id)
                )"""
        )
        if os.path.exists(cache_dir):
            for fname in os.listdir(cache_dir):
                if not fname.endswith(".json"):
                    continue
                author_id = os.path.splitext(fname)[0]
                with open(os.path.join(cache_dir, fname), "r", encoding="utf-8") as f:
                    pubs = json.load(f)
                for pub in pubs:
                    title = pub["bib"]["title"]
                    year = pub["bib"].get("pub_year")
                    year_val = int(year) if year else None
                    abstract = pub["bib"].get("abstract")
                    url = pub.get("pub_url")
                    cited_by_count = pub.get("num_citations")
                    paper_id = generate_paper_id()
                    conn_pub.execute(
                        "INSERT INTO papers (id, title, year, abstract, url, cited_by_count) VALUES (?, ?, ?, ?, ?, ?)",
                        (paper_id, title, year_val, abstract, url, cited_by_count),
                    )
                    conn_pub.execute(
                        "INSERT OR IGNORE INTO publication_authors (paper_id, openalex_id, display_name) VALUES (?, ?, '')",
                        (paper_id, author_id),
                    )
                logger.info("Migrated cache for author %s", author_id)
            conn_pub.commit()

    # Prepare authors database and import legacy authors list if present.
    if os.path.exists(authors_json):
        authors_db_path = os.path.join(root, "authors.db")
        with sqlite3.connect(authors_db_path) as conn_auth:
            conn_auth.execute(
                """CREATE TABLE IF NOT EXISTS authors (
                        name TEXT,
                        id TEXT PRIMARY KEY
                    )"""
            )
            with open(authors_json, "r", encoding="utf-8") as f:
                authors = json.load(f)
            for author in authors:
                conn_auth.execute(
                    "INSERT OR REPLACE INTO authors (name, id) VALUES (?, ?)",
                    (author["name"], author["id"]),
                )
            conn_auth.commit()
        logger.info("Migrated authors list to SQLite")

    # Move legacy files into an ``obsolete`` folder for safekeeping.
    obsolete_dir = os.path.join(root, "obsolete")
    os.makedirs(obsolete_dir, exist_ok=True)
    if os.path.exists(authors_json):
        shutil.move(authors_json, os.path.join(obsolete_dir, "authors.json"))
        logger.info("Archived legacy authors.json to %s", obsolete_dir)
    if os.path.exists(cache_dir):
        shutil.move(cache_dir, os.path.join(obsolete_dir, "googleapi_cache"))
        logger.info("Archived legacy cache to %s", obsolete_dir)
    if os.path.exists(backup_dir):
        shutil.move(
            backup_dir, os.path.join(obsolete_dir, os.path.basename(backup_dir))
        )
        logger.info("Archived legacy backup cache to %s", obsolete_dir)


def delete_temp_cache(args):
    if os.path.isdir(args.temp_cache_path):
        try:
            shutil.rmtree(args.temp_cache_path)
            logger.debug("Temporary cache cleared.")
        except Exception as e:
            logger.error(
                f"Failed to delete cache at {args.temp_cache_path}. Please delete the folder manually."
            )
            logger.error(f"Reason: {str(e)}")
    else:
        logger.debug(f"Temporary cache not found at {args.temp_cache_path}.")


def confirm_temp_cache(
    temp_cache_path="./src/temp_cache", old_cache_path="./src/googleapi_cache"
):
    """
    Moves the contents of the temporary cache directory to the old cache directory.

    After ensuring that both paths exist, this function moves every file from the
    temp_cache_path to the old_cache_path, overwriting files with the same name.
    Once all files are moved, the temporary cache directory is deleted.

    Parameters:
    - temp_cache_path (str): Path to the temporary cache directory.
    - old_cache_path (str): Path to the old cache directory (to be updated with new files).

    Returns:
    None
    """

    # Check if temp_cache_path exists
    if not os.path.exists(temp_cache_path):
        logger.warning(
            f"Temporary cache path '{temp_cache_path}' does not exist. New articles are NOT saved to cache."
        )
        return

    # Check if old_cache_path exists
    if not os.path.exists(old_cache_path):
        logger.info(f"Cache path '{old_cache_path}' does not exist. Creating.")
        os.makedirs(old_cache_path, exist_ok=True)

    # Log the beginning of the process
    logger.debug("Starting to move files from temporary cache to old cache.")

    # Iterate over every file in the temporary cache path
    for file_name in os.listdir(temp_cache_path):
        source_path = os.path.join(temp_cache_path, file_name)
        destination_path = os.path.join(old_cache_path, file_name)

        # Move file from temporary cache to old cache, overwriting if necessary
        shutil.move(source_path, destination_path)
        logger.debug(f"Moved '{file_name}' from temporary cache to old cache.")

    # After moving all files, remove the temporary cache directory
    os.rmdir(temp_cache_path)
    logger.debug(f"Temporary cache path '{temp_cache_path}' has been deleted.")

    return


def _init_authors_db(authors_path: str) -> sqlite3.Connection:
    """Ensure the authors database exists and return a connection."""

    conn = sqlite3.connect(authors_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS authors (
                name TEXT,
                id TEXT PRIMARY KEY
            )"""
    )
    # Add optional columns if missing (e.g., openalex_id)
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(authors)").fetchall()]
        if 'openalex_id' not in cols:
            conn.execute("ALTER TABLE authors ADD COLUMN openalex_id TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            logger.warning("Failed to add openalex_id column to authors: %s", e)
    return conn


def add_new_author_to_json(authors_path: str, scholar_id: str) -> dict:
    """Insert a new author into the SQLite database.

    Despite the legacy name, this function now writes to a SQLite database
    located at ``authors_path``.

    Args:
        authors_path: Path to the database file (``scholar.db``).
        scholar_id: Google Scholar identifier for the author to add.

    Returns:
        dict: A dictionary with the inserted author's name and id.

    Raises:
        Exception: Propagates exceptions raised while fetching author data.
    """

    logger.info(f"Adding author ID {scholar_id} to {authors_path}.")
    conn = _init_authors_db(authors_path)
    try:
        if scholarly is None:
            raise RuntimeError(
                "Google Scholar backend not available: 'scholarly' package is not installed."
            )
        try:
            author_fetched = scholarly.search_author_id(scholar_id)
        except Exception as e:
            logger.error(f"Error encountered: {e}")
            raise

        author_name = author_fetched["name"]
        conn.execute(
            "INSERT OR IGNORE INTO authors (name, id) VALUES (?, ?)",
            (author_name, scholar_id),
        )
        conn.commit()
        logger.debug(f"Author {author_name} added to {authors_path}.")
        return {"name": author_name, "id": scholar_id}
    finally:
        conn.close()


def convert_json_to_tuple(authors_json: list) -> list:
    """Convert a list of author dictionaries into tuples.

    Args:
        authors_json: Sequence of dictionaries with ``name`` and ``id`` keys.

    Returns:
        list: Each author represented as ``(name, id)``.
    """

    authors_list = []

    # Iterate through each author in the JSON
    for author in authors_json:
        # Formatting the authors as tuples
        author_tuple = (author["name"], author["id"])
        authors_list.append(author_tuple)

    return authors_list


def get_authors_json(authors_path: str) -> list:
    """Retrieve authors from the SQLite database.

    The function name is kept for backward compatibility; however, it now
    reads from a ``.db`` file rather than a JSON document.

    Args:
        authors_path: Path to the database file (``scholar.db``).

    Returns:
        list[dict]: Authors stored in the database.
    """

    conn = _init_authors_db(authors_path)
    try:
        cursor = conn.execute("SELECT name, id FROM authors")
        return [
            {"name": name, "id": author_id} for name, author_id in cursor.fetchall()
        ]
    finally:
        conn.close()


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
