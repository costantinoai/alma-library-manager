"""Small helpers for checking live optional-module availability."""

from __future__ import annotations

import importlib.util
import sys


def module_available(module_name: str) -> bool:
    """Return whether a module is available in the current interpreter.

    This check does not import new modules. It uses ``find_spec()`` first, then
    falls back to the loaded-module table for runtimes that leave an
    already-imported module with ``__spec__ = None``.
    """
    try:
        return importlib.util.find_spec(module_name) is not None
    except ValueError:
        return module_name in sys.modules
