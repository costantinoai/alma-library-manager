#!/usr/bin/env python3
"""Logging configuration for scholar-slack-bot."""

import logging
import logging.config
import numbers
from typing import Any

from alma.core.redaction import redact_sensitive_data, redact_sensitive_text

# Uvicorn keeps its own handlers and does not propagate, so a traceback from
# "Exception in ASGI application" never reaches the root console handler.
_SERVER_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def _safe_arg(arg: Any) -> Any:
    """Redact one ``%`` argument. Numbers stay numbers, or ``%d`` would break."""
    if arg is None or isinstance(arg, numbers.Number):
        return arg
    return redact_sensitive_text(str(arg))


class RedactSecretsFilter(logging.Filter):
    """Remove credentials from a record before a handler prints it.

    The belt under every logger call: there are hundreds of ``logger.x(..., exc)``
    sites, and fixing them one by one leaves the next one armed. The record keeps
    its shape, because uvicorn's access formatter unpacks ``record.args`` by
    position. A filter that fails must not lose the log line.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.msg = redact_sensitive_text(str(record.msg))
            if isinstance(record.args, dict):
                record.args = {k: _safe_arg(v) for k, v in redact_sensitive_data(record.args).items()}
            elif record.args:
                record.args = tuple(_safe_arg(arg) for arg in record.args)
            if record.exc_info and not record.exc_text:
                record.exc_text = logging.Formatter().formatException(record.exc_info)
            if record.exc_text:
                record.exc_text = redact_sensitive_text(record.exc_text)
            if record.stack_info:
                record.stack_info = redact_sensitive_text(record.stack_info)
        except Exception:  # noqa: BLE001 - a log line outranks a tidy one
            return True
        return True


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
        "filters": {"redact_secrets": {"()": RedactSecretsFilter}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "filters": ["redact_secrets"],
                "level": "DEBUG",
            }
        },
        "root": {"handlers": ["console"], "level": level},
    }
    logging.config.dictConfig(config)
    for name in _SERVER_LOGGERS:
        for handler in logging.getLogger(name).handlers:
            if not any(isinstance(f, RedactSecretsFilter) for f in handler.filters):
                handler.addFilter(RedactSecretsFilter())

