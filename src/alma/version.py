"""Canonical application version — single source of truth.

The release process (`chore(release): vX.Y.Z`) bumps ONLY ``pyproject.toml``,
so that file is the one authoritative version string. Every other historical
constant (``alma.__version__``, ``alma.api.app.APP_VERSION``, the frontend
``package.json``) drifted out of sync precisely because they were hand-kept.

``importlib.metadata`` is unreliable here: the package is installed editable,
so its recorded metadata lags the working tree (e.g. reports 0.14.0 while
``pyproject.toml`` already says 0.16.1). We therefore read ``pyproject.toml``
directly and fall back to installed metadata only when the file isn't on disk
(a wheel install).
"""

from __future__ import annotations

import importlib.metadata
import re
from functools import lru_cache
from pathlib import Path

# version.py lives at src/alma/version.py → repo root is two parents up.
_PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


@lru_cache(maxsize=1)
def get_app_version() -> str:
    """Return the current application version (e.g. ``"0.16.1"``)."""
    try:
        text = _PYPROJECT.read_text(encoding="utf-8")
        match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
        if match:
            return match.group(1)
    except OSError:
        pass
    try:
        return importlib.metadata.version("alma")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"
