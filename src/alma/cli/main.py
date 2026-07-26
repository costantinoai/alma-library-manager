#!/usr/bin/env python3
"""
Created on Sat Oct  7 02:46:59 2023

@author: costantino_ai
"""

import argparse
import logging
import os
import shutil

from alma.core.database import delete_temp_cache
from alma.core.fetcher import fetch_author_details
from alma.core.logging import setup_logging
from alma.core.workflows import add_scholar_and_fetch, refetch_and_update

logger = logging.getLogger(__name__)


def handle_add_author(args: argparse.Namespace) -> None:
    """Validate and add a new scholar, then update the cache."""
    add_scholar_and_fetch(args)


def handle_update_cache(args: argparse.Namespace) -> None:
    """Re-fetch publications for all authors and update the cache."""
    refetch_and_update(args)


def handle_test_fetch(args: argparse.Namespace) -> None:
    """Fetch publications for a single author without side effects."""
    # Retrieve publications for the provided scholar ID and log how many were found.
    pubs = fetch_author_details(args.scholar_id)
    logger.info(
        "Fetched %d publications for scholar %s without caching or messaging.",
        len(pubs),
        args.scholar_id,
    )


def get_args():
    """Build the CLI parser and return parsed arguments.

    The ``fetch`` / ``send`` / ``test-run`` subcommands are gone with the
    plain-text Slack digest they existed to post (task 55). Notifications are the
    alerts engine's job — rules, schedules, per-channel dedup, delivery history —
    and it has never gone through this CLI. What remains here is cache
    maintenance, which never touched Slack.
    """
    parser = argparse.ArgumentParser(
        description="Maintain the local publication cache."
    )

    # Global options available to all subcommands.
    parser.add_argument(
        "--authors_path", default="./data/scholar.db", help="Path to the unified scholar database"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output.")

    # Define subcommands replacing the previous boolean flags.
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser(
        "add-author",
        help="Add a scholar by Google Scholar ID and update the cache without messaging.",
    )
    add_parser.add_argument("add_scholar_id", help="Google Scholar ID to add.")
    add_parser.set_defaults(
        func=handle_add_author, test_message=False, update_cache=False
    )

    update_parser = subparsers.add_parser(
        "update-cache",
        help="Re-fetch publications for all authors and update the cache only.",
    )
    update_parser.set_defaults(
        func=handle_update_cache,
        test_message=False,
        update_cache=True,
        add_scholar_id=None,
    )

    test_fetch_parser = subparsers.add_parser(
        "test-fetch",
        help="Fetch publications for a scholar without caching or messaging.",
    )
    test_fetch_parser.add_argument("scholar_id", help="Google Scholar ID to fetch.")
    test_fetch_parser.set_defaults(
        func=handle_test_fetch,
        test_message=False,
        update_cache=False,
        add_scholar_id=None,
    )

    args = parser.parse_args()
    return parser, args


def initialize_args():
    """Parse command-line arguments and configure logging.

    The function always relies on the command-line interface, even when the
    script is launched without additional arguments (such as from an IDE).
    Default values defined in :func:`get_args` are therefore applied.

    Returns:
        argparse.Namespace: Object holding parsed command-line arguments.
    """

    # Parse the arguments using the standard CLI interface.
    parser, args = get_args()
    logger.info("Parsed command-line arguments.")

    # Configure logging based on requested verbosity.
    setup_logging(verbose=args.verbose)
    if args.verbose:
        logger.debug("Verbose log mode activated.")
    else:
        logger.info("Minimal log mode activated.")

    # Display the arguments being used to aid debugging.
    for arg, value in vars(args).items():
        logger.debug(f"Argument {arg} = {value}")

    return args


def main():
    """Maintain the local publication cache from the command line.

    Workflow:
    1. Parse arguments and configure logging.
    2. Resolve the cache directories from ``--authors_path``.
    3. Run the selected subcommand (add author / refresh cache / preview a fetch).
    4. Clean the temporary cache on the way in and out.

    No Slack credentials are read here. Messaging moved to the alerts engine,
    which resolves its own credentials from the secret store (task 55).
    """
    setup_logging()
    logger.info("Initializing...")

    # Get the arguments
    args = initialize_args()

    # Set importnant directories
    root = os.path.dirname(args.authors_path)
    args.cache_path = os.path.join(root, "googleapi_cache")
    args.temp_cache_path = os.path.join(args.cache_path, "tmp")

    # Attempt to clean the old temporary cache if present
    delete_temp_cache(args)

    # Delegate execution to the subcommand handler selected by the user.
    args.func(args)

    # Attempt to clean the new temporary cache
    delete_temp_cache(args)

    logger.info("Done.")
    return args


if __name__ == "__main__":
    args = None
    try:
        args = main()
    except Exception as e:
        # If an error occurs, attempt to delete the folder at args.temp_cache_path
        if args is not None and hasattr(args, "temp_cache_path") and os.path.exists(args.temp_cache_path):
            shutil.rmtree(args.temp_cache_path)
        # Re-raise the exception after cleanup
        raise e
