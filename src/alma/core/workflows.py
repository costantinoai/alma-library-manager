#!/usr/bin/env python3
"""
Created on Fri Oct 20 17:02:03 2023

@author: costantino_ai
"""
import logging
import os
import shutil

from alma.core.backend import fetch_from_json
from alma.core.database import (
    add_new_author_to_json,
    confirm_temp_cache,
    convert_json_to_tuple,
)

logger = logging.getLogger(__name__)


def update_cache_only(args):
    """Move fetched publications from the temp directory to cache.

    Args:
        temp_cache_path (str): Path to the temporary cache.
        cache_path (str): Path to the actual cache.
    """
    confirm_temp_cache(args.temp_cache_path, args.cache_path)
    logger.info("Fetched pubs successfully moved to cache and temporary cache cleared.")


def refetch_and_update(args):
    """
    Refetch author and publication details, and update the cache.

    This function deletes the old cache, refetches all the authors and
    their publication details, and subsequently updates the cache with
    the new fetched data.

    Parameters:
    - args: Arguments containing paths for cache, temp cache, and other relevant data.

    Returns:
    None
    """

    # Attempt to delete the old cache.
    if os.path.isdir(args.temp_cache_path):
        try:
            shutil.rmtree(args.cache_path)
            logger.debug(f"Deleted old cache at {args.cache_path}")
        except Exception as e:  # Handle specific exception to avoid broad except.
            logger.error(
                f"Failed to delete old cache at {args.cache_path}. Reason: {str(e)}"
            )

    # Refetch all the author and publication details.
    _ = fetch_from_json(args)

    # Update the cache with newly fetched data.
    update_cache_only(args)
    logger.info(
        "Re-fetched all publications. Data successfully moved to cache and temporary cache cleared."
    )


def add_scholar_and_fetch(args):
    """Add a new scholar, fetch publications, and update the cache.

    The author roster is now stored in a SQLite database. This helper inserts a
    new scholar into that database, retrieves their publications, and persists
    the results to the cache.

    Args:
        args: Object containing paths for the authors database, cache, and the
            identifier of the new author to add.
    """

    json_filename = f"{args.add_scholar_id}.json"
    json_filepath = os.path.join(args.cache_path, json_filename)

    if os.path.exists(json_filepath):
        logger.info(
            f"Author with scholar ID {args.add_scholar_id} already has cached publications. Fetching is skipped."
        )
        return

    author_dict = add_new_author_to_json(args.authors_path, args.add_scholar_id)
    logger.debug(
        f"Added new author with scholar ID {args.add_scholar_id} to authors database."
    )

    authors_json = [author_dict]
    authors = convert_json_to_tuple(authors_json)
    logger.debug("Converted new author's record into tuple representation.")

    # Provide a compat wrapper so tests can patch streams_funcs.fetch_pubs_dictionary
    articles = fetch_pubs_dictionary(authors, args)
    logger.info(f"Fetched {len(articles)} articles for the new author.")

    update_cache_only(args)
    logger.info(
        "Added author to database. Cache successfully updated with new author's data."
    )


def fetch_pubs_dictionary(authors, args, output_dir="./src"):
    """Compat wrapper proxying to the backend implementation.

    Tests patch streams_funcs.fetch_pubs_dictionary; keep this thin indirection
    so the patch point remains stable.
    """
    try:
        from alma.core.fetcher import fetch_pubs_dictionary as _fetch
    except Exception:
        # Fallback to backend (if implemented there)
        from alma.core.backend import fetch_publications_by_id as _alt
        # If only per-author is available, iterate authors
        results = []
        for _, aid in authors or []:
            results.extend(_alt(aid, output_folder=output_dir, args=args) or [])
        return results
    else:
        return _fetch(authors, args, output_dir=output_dir)
