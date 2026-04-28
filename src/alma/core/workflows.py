#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 17:02:03 2023

@author: costantino_ai
"""
import os
import shutil
import logging
from alma.core.database import (
    confirm_temp_cache,
    add_new_author_to_json,
    convert_json_to_tuple,
)
from alma.core.backend import fetch_from_json
from alma.plugins.registry import get_global_registry
from alma.plugins.slack.plugin import SlackPlugin


def send_to_slack(channel_name: str, message: str, token: str):
    """Compat shim to send a message to Slack via the plugin system.

    Tests may patch this function. Production code uses it to route messages
    through the Slack plugin.

    Args:
        channel_name: Slack channel or user to send to
        token: Bot token
        message: Preformatted message string

    Returns:
        bool: True if Slack API acknowledges the message
    """
    registry = get_global_registry()
    if "slack" not in registry.list_plugins():
        registry.register(SlackPlugin)
    plugin = registry.create_instance("slack", {"api_token": token, "default_channel": channel_name}, cache=True)
    ok = bool(plugin.send_message(message, channel_name))
    return {"ok": ok}

logger = logging.getLogger(__name__)


def update_cache_only(args):
    """Move fetched publications from the temp directory to cache.

    Args:
        temp_cache_path (str): Path to the temporary cache.
        cache_path (str): Path to the actual cache.
    """
    confirm_temp_cache(args.temp_cache_path, args.cache_path)
    logger.info("Fetched pubs successfully moved to cache and temporary cache cleared.")


def test_fetch_and_message(args, ch_name, token, limit: int = 2) -> None:
    """Fetch a limited number of authors and send test messages to Slack.

    The helper exercises the full fetching and messaging workflow without
    persisting any results to the cache. It is intended for dry runs where a
    small subset of authors is processed and their publications are posted to
    Slack with a clear test header.

    Args:
        args: Arguments passed through to :func:`fetch_from_json`.
        ch_name: Target Slack channel or user.
        token: Slack API token used for authentication.
        limit: Maximum number of authors to include in the test run. Defaults
            to ``2`` so the call remains lightweight.

    Returns:
        None

    For each fetched article, a test message is created and sent.
    """
    from alma.plugins.base import Author, Publication

    # Fetch a limited number of authors from the database.
    authors, articles = fetch_from_json(args, idx=limit)

    # Format messages using plugin system
    registry = get_global_registry()
    if "slack" not in registry.list_plugins():
        registry.register(SlackPlugin)
    plugin = registry.create_instance("slack", {"api_token": token, "default_channel": ch_name}, cache=True)

    # Convert to plugin data structures
    author_objects = [Author(name=name, scholar_id=sid) for name, sid in authors]
    pub_objects = [
        Publication(
            title=art.get("title", ""),
            authors=art.get("authors", ""),
            year=art.get("year", ""),
            abstract=art.get("abstract", ""),
            pub_url=art.get("pub_url", ""),
            journal=art.get("journal", ""),
            citations=art.get("num_citations", 0)
        )
        for art in articles.values() if articles
        for art in (articles if isinstance(articles, list) else [art])
    ]

    # Format messages
    authors_msg = plugin.format_authors(author_objects)
    pubs_msg = plugin.format_publications(pub_objects)

    logger.info(f"Formatted test messages for {len(authors)} authors.")

    test_header = "!!! This is a test message !!!"
    success = True  # To track if all messages are sent successfully.

    # Send messages with test header
    for formatted_message in [authors_msg, pubs_msg]:
        formatted_message = f"```\n{test_header}\n{formatted_message}\n```"
        ok = send_to_slack(ch_name, formatted_message, token)
        okval = ok.get("ok") if isinstance(ok, dict) else bool(ok)
        if not okval:
            success = False
            logger.warning("Failed to send a test message via Slack plugin")

    # Log overall success or failure.
    if success:
        logger.info("All test messages sent successfully.")
    else:
        logger.error("There was a problem sending one or more test messages.")


def regular_fetch_and_message(args, ch_name, token):
    """
    Regularly fetch articles and send messages to a Slack channel.
    If all messages are sent successfully, the cache will be updated.
    If any message fails, the temporary cache will be cleared.

    This function operates under the following conditions:
    - Not adding a scholar by ID (`add_scholar_id` is not provided).
    - Not updating the cache only (`update_cache` is False).
    - `test_message` is False.

    Args:
        args (argparse.Namespace): The argument object.
        ch_name (str): The channel name to send messages to.
        token (str): The token used for communication with Slack.

    """
    from alma.plugins.base import Author, Publication

    logger.info(
        "Starting fetch & send workflow: target=%s (messages will be sent)", ch_name
    )
    # Fetch all authors' details from the provided path.
    authors, articles = fetch_from_json(args)

    # Format messages using plugin system
    registry = get_global_registry()
    if "slack" not in registry.list_plugins():
        registry.register(SlackPlugin)
    plugin = registry.create_instance("slack", {"api_token": token, "default_channel": ch_name}, cache=True)

    # Convert to plugin data structures
    author_objects = [Author(name=name, scholar_id=sid) for name, sid in authors]
    pub_objects = [
        Publication(
            title=art.get("title", ""),
            authors=art.get("authors", ""),
            year=art.get("year", ""),
            abstract=art.get("abstract", ""),
            pub_url=art.get("pub_url", ""),
            journal=art.get("journal", ""),
            citations=art.get("num_citations", 0)
        )
        for art in articles.values() if articles
        for art in (articles if isinstance(articles, list) else [art])
    ]

    # Format messages
    authors_msg = plugin.format_authors(author_objects)
    pubs_msg = plugin.format_publications(pub_objects)
    formatted_messages = [authors_msg, pubs_msg]

    logger.info(f"Formatted messages for {len(authors)} authors.")

    # Initialize a success flag to track message sending process.
    success = True
    error_message = None  # To store any error encountered.

    for formatted_message in formatted_messages:
        ok = send_to_slack(ch_name, formatted_message, token)
        okval = ok.get("ok") if isinstance(ok, dict) else bool(ok)
        if not okval:
            success = False
            error_message = (ok.get("error") if isinstance(ok, dict) else None) or "send_message returned False"
            logger.warning(f"Failed to send a message due to: {error_message}")

    # Handle post-message actions based on the success flag.
    if success:
        confirm_temp_cache(args.temp_cache_path, args.cache_path)
        logger.info(
            "All messages sent. Moved fetched publications to cache and cleared temp cache."
        )
    else:
        # Clear the temporary cache due to the failure in sending messages.
        logger.error(
            f"Problem sending one or more messages to Slack. Cache was not updated. Error: {error_message}"
        )


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
