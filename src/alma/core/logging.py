#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Logging configuration for scholar-slack-bot."""

import logging
import logging.config


def setup_logging(verbose: bool = False) -> None:
    """Configure root logger using ``logging.config.dictConfig``.

    Parameters
    ----------
    verbose: bool
        When ``True`` set the logging level to ``DEBUG``; otherwise ``INFO``.
    """
    level = "DEBUG" if verbose else "INFO"
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": "DEBUG",
            }
        },
        "root": {"handlers": ["console"], "level": level},
    }
    logging.config.dictConfig(config)

