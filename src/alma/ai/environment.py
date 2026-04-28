"""Dependency environment management for optional AI packages.

Allows users to select where AI dependencies should be loaded from
(`system`, `venv`/`uv`, or `conda`-style environments), validates the
selection, and provides package inspection helpers.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import json
import logging
import site
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from alma.ai.import_state import module_available

logger = logging.getLogger(__name__)

SUPPORTED_ENV_TYPES = {
    "system",
    "venv",
    "uv",
    "conda",
    "miniconda",
    "miniforge",
}
_VENV_TYPES = {"venv", "uv"}
_CONDA_TYPES = {"conda", "miniconda", "miniforge"}
_ACTIVE_SITE_PACKAGES: list[str] = []

_PYTHON_PROBE_SCRIPT = """
import json
import site
import sys
import sysconfig

paths = []
getsite = getattr(site, "getsitepackages", None)
if callable(getsite):
    try:
        for p in getsite() or []:
            if isinstance(p, str):
                paths.append(p)
    except Exception:
        pass

getusersite = getattr(site, "getusersitepackages", None)
if callable(getusersite) and bool(getattr(site, "ENABLE_USER_SITE", False)):
    try:
        p = getusersite()
        if isinstance(p, str):
            paths.append(p)
    except Exception:
        pass

cfg_paths = sysconfig.get_paths()
for key in ("purelib", "platlib"):
    p = cfg_paths.get(key)
    if isinstance(p, str):
        paths.append(p)

deduped = []
for p in paths:
    if p and p not in deduped:
        deduped.append(p)

print(json.dumps({
    "python_version": sys.version.split()[0],
    "prefix": sys.prefix,
    "site_packages": deduped,
}))
"""

_PACKAGE_CHECK_SCRIPT = """
import importlib.metadata
import importlib.util
import json
import sys

def module_installed(module_name):
    try:
        return importlib.util.find_spec(module_name) is not None
    except ValueError:
        # Some runtime loaders leave an already-imported module with
        # __spec__ = None (observed with `adapters` after live SPECTER2 use).
        # In that case the package is present in the running interpreter even
        # though find_spec() cannot rediscover it.
        return module_name in sys.modules

try:
    package_map = json.loads(sys.argv[1])
    dist_name_map = json.loads(sys.argv[2])
except Exception as exc:
    print(json.dumps({"ok": False, "error": f"Invalid input: {exc}"}))
    raise SystemExit(0)

result = {}
for display_name, module_name in package_map.items():
    installed = module_installed(module_name)
    version = None
    if installed:
        dist_name = dist_name_map.get(module_name, module_name)
        try:
            version = importlib.metadata.version(dist_name)
        except Exception:
            version = None
    result[display_name] = {"installed": installed, "version": version}

print(json.dumps({"ok": True, "dependencies": result}))
"""


@dataclass
class DependencyEnvironment:
    """Resolved dependency environment metadata."""

    configured_type: str
    configured_path: str
    valid: bool
    message: Optional[str]
    detected_type: Optional[str] = None
    resolved_path: Optional[str] = None
    selected_python_executable: Optional[str] = None
    selected_python_version: Optional[str] = None
    selected_site_packages: list[str] = field(default_factory=list)
    using_fallback: bool = False
    fallback_reason: Optional[str] = None
    effective_python_executable: str = sys.executable
    effective_python_version: str = sys.version.split()[0]

    def as_dict(self) -> dict:
        """Serialize to an API-safe dict."""
        backend_exec, backend_version = _current_python_info()
        selected_major_minor = ".".join((self.selected_python_version or "").split(".")[:2])
        backend_major_minor = ".".join((backend_version or "").split(".")[:2])
        return {
            "type": self.configured_type,
            "path": self.configured_path,
            "valid": self.valid,
            "message": self.message,
            "detected_type": self.detected_type,
            "resolved_path": self.resolved_path,
            "selected_python_executable": self.selected_python_executable,
            "selected_python_version": self.selected_python_version,
            "using_fallback": self.using_fallback,
            "fallback_reason": self.fallback_reason,
            "effective_python_executable": self.effective_python_executable,
            "effective_python_version": self.effective_python_version,
            "backend_python_executable": backend_exec,
            "backend_python_version": backend_version,
            "selected_site_packages": self.selected_site_packages,
            "active_site_packages": list(_ACTIVE_SITE_PACKAGES),
            "python_version_match": (
                bool(selected_major_minor)
                and bool(backend_major_minor)
                and selected_major_minor == backend_major_minor
            ),
        }


def _current_python_info() -> tuple[str, str]:
    """Return executable + version for the current process."""
    return sys.executable, sys.version.split()[0]


def _read_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    """Read one key from discovery_settings."""
    try:
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = ?",
            (key,),
        ).fetchone()
    except sqlite3.OperationalError:
        return default

    if row is None:
        return default
    return row["value"] if isinstance(row, sqlite3.Row) else row[0]


def _detect_environment_layout(env_dir: Path) -> Optional[str]:
    """Detect whether a directory looks like a venv or conda env."""
    if (env_dir / "conda-meta").is_dir():
        return "conda"
    if (env_dir / "pyvenv.cfg").is_file():
        return "venv"
    return None


def _guess_env_root_from_python(python_path: Path) -> Path:
    """Infer environment root from a python executable path."""
    parent = python_path.parent
    if parent.name.lower() in {"bin", "scripts"}:
        return parent.parent
    return parent


def _find_python_executable(env_dir: Path) -> Optional[Path]:
    """Find python executable inside an environment folder."""
    candidates = [
        env_dir / "bin" / "python",
        env_dir / "bin" / "python3",
        env_dir / "Scripts" / "python.exe",
        env_dir / "python.exe",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _probe_python_environment(python_executable: str) -> tuple[Optional[dict], Optional[str]]:
    """Query interpreter metadata from a python executable."""
    try:
        proc = subprocess.run(
            [python_executable, "-c", _PYTHON_PROBE_SCRIPT],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
    except Exception as exc:
        return None, str(exc)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return None, err or f"Exit code {proc.returncode}"

    try:
        payload = json.loads(proc.stdout.strip() or "{}")
    except Exception as exc:
        return None, f"Invalid probe output: {exc}"

    if not isinstance(payload, dict):
        return None, "Probe output is not a JSON object"

    return payload, None


def _invalid_with_fallback(
    env_type: str,
    env_path: str,
    message: str,
    resolved_path: Optional[str] = None,
    detected_type: Optional[str] = None,
) -> DependencyEnvironment:
    """Build an invalid env result that falls back to current process."""
    current_exec, current_version = _current_python_info()
    return DependencyEnvironment(
        configured_type=env_type,
        configured_path=env_path,
        valid=False,
        message=message,
        detected_type=detected_type,
        resolved_path=resolved_path,
        using_fallback=True,
        fallback_reason=message,
        effective_python_executable=current_exec,
        effective_python_version=current_version,
    )


def resolve_dependency_environment(env_type: str, env_path: str) -> DependencyEnvironment:
    """Validate and resolve a dependency environment selection."""
    normalized_type = (env_type or "system").strip().lower()
    normalized_path = (env_path or "").strip()

    if normalized_type not in SUPPORTED_ENV_TYPES:
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            (
                f"Unsupported environment type '{normalized_type}'. "
                f"Use one of: {', '.join(sorted(SUPPORTED_ENV_TYPES))}."
            ),
        )

    current_exec, current_version = _current_python_info()
    if normalized_type == "system":
        return DependencyEnvironment(
            configured_type="system",
            configured_path="",
            valid=True,
            message="Using the server's current Python environment.",
            detected_type="system",
            resolved_path=None,
            selected_python_executable=current_exec,
            selected_python_version=current_version,
            effective_python_executable=current_exec,
            effective_python_version=current_version,
        )

    if not normalized_path:
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            "Environment path is required for non-system environments.",
        )

    original_path = Path(normalized_path).expanduser()
    if not original_path.exists():
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            f"Path does not exist: {normalized_path}",
        )

    selected_python: Optional[Path] = None
    env_dir: Path
    if original_path.is_file():
        selected_python = original_path
        env_dir = _guess_env_root_from_python(original_path)
    elif original_path.is_dir():
        env_dir = original_path
    else:
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            f"Path is neither a file nor a directory: {normalized_path}",
        )

    try:
        resolved_env_dir = str(env_dir.resolve())
    except Exception:
        resolved_env_dir = str(env_dir)

    detected = _detect_environment_layout(env_dir)
    if normalized_type in _VENV_TYPES and detected != "venv":
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            (
                "Folder is not recognized as a venv/uv environment. "
                "Expected a 'pyvenv.cfg' file."
            ),
            resolved_path=resolved_env_dir,
            detected_type=detected,
        )

    if normalized_type in _CONDA_TYPES and detected != "conda":
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            (
                "Folder is not recognized as a conda environment. "
                "Expected a 'conda-meta/' directory."
            ),
            resolved_path=resolved_env_dir,
            detected_type=detected,
        )

    if selected_python is None:
        selected_python = _find_python_executable(env_dir)

    if selected_python is None:
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            (
                "Could not find a Python executable in this environment. "
                "Expected 'bin/python' or 'Scripts/python.exe'."
            ),
            resolved_path=resolved_env_dir,
            detected_type=detected,
        )

    selected_python_str = str(selected_python)
    probe_payload, probe_error = _probe_python_environment(selected_python_str)
    if probe_payload is None:
        return _invalid_with_fallback(
            normalized_type,
            normalized_path,
            f"Could not inspect environment Python: {probe_error}",
            resolved_path=resolved_env_dir,
            detected_type=detected,
        )

    selected_version = str(probe_payload.get("python_version") or "")
    site_packages = probe_payload.get("site_packages") or []
    if not isinstance(site_packages, list):
        site_packages = []

    return DependencyEnvironment(
        configured_type=normalized_type,
        configured_path=normalized_path,
        valid=True,
        message="Environment validated successfully.",
        detected_type=detected,
        resolved_path=resolved_env_dir,
        selected_python_executable=selected_python_str,
        selected_python_version=selected_version or None,
        selected_site_packages=[str(p) for p in site_packages if isinstance(p, str)],
        effective_python_executable=selected_python_str,
        effective_python_version=selected_version or current_version,
    )


def get_dependency_environment(conn: sqlite3.Connection) -> DependencyEnvironment:
    """Resolve dependency environment from discovery_settings."""
    env_type = _read_setting(conn, "ai.python_env_type", "system")
    env_path = _read_setting(conn, "ai.python_env_path", "")
    return resolve_dependency_environment(env_type, env_path)


def activate_dependency_environment(conn: sqlite3.Connection) -> DependencyEnvironment:
    """Resolve selected env and activate its site-packages on sys.path."""
    env = get_dependency_environment(conn)
    _activate_site_packages(env)
    return env


def _activate_site_packages(env: DependencyEnvironment) -> None:
    """Ensure selected env site-packages are prioritized on sys.path."""
    global _ACTIVE_SITE_PACKAGES

    # Remove paths inserted from a previous environment selection.
    if _ACTIVE_SITE_PACKAGES:
        sys.path[:] = [p for p in sys.path if p not in _ACTIVE_SITE_PACKAGES]
        _ACTIVE_SITE_PACKAGES = []

    if not env.valid or env.using_fallback or env.configured_type == "system":
        importlib.invalidate_caches()
        return

    inserted: list[str] = []
    for path in reversed(env.selected_site_packages):
        if not path:
            continue
        if path in sys.path:
            continue
        if not Path(path).exists():
            continue
        sys.path.insert(0, path)
        inserted.append(path)

    _ACTIVE_SITE_PACKAGES = list(reversed(inserted))
    importlib.invalidate_caches()


def check_packages_in_environment(
    package_map: dict[str, str],
    dist_name_map: Optional[dict[str, str]],
    env: DependencyEnvironment,
) -> tuple[dict[str, dict], Optional[str]]:
    """Check package install status in the selected env and backend runtime.

    ``installed``/``version`` describe the selected dependency environment.
    ``runtime_importable``/``runtime_version`` describe what the currently
    running backend process can import after activating the selected
    site-packages. A package can be installed in the selected venv yet still
    fail at runtime when the backend was started from another Python ABI.
    """
    resolved_dist_map = dist_name_map or {}
    effective_exec = env.effective_python_executable
    runtime_dependencies = _check_packages_current_process(package_map, resolved_dist_map)

    selected_dependencies, error = _check_packages_via_subprocess(
        effective_exec,
        package_map,
        resolved_dist_map,
    )
    if selected_dependencies is not None:
        return _merge_dependency_checks(selected_dependencies, runtime_dependencies), None

    warning = (
        "Could not inspect selected environment with its python executable "
        f"({error}). Falling back to the server environment."
    )
    logger.warning(warning)
    return _merge_dependency_checks(runtime_dependencies, runtime_dependencies), warning


def _merge_dependency_checks(
    selected_dependencies: dict[str, dict],
    runtime_dependencies: dict[str, dict],
) -> dict[str, dict]:
    """Return a stable package-status shape for selected env + runtime."""
    result: dict[str, dict] = {}
    names = sorted(set(selected_dependencies) | set(runtime_dependencies))
    for name in names:
        selected = selected_dependencies.get(name) or {}
        runtime = runtime_dependencies.get(name) or {}
        selected_installed = bool(selected.get("installed"))
        runtime_importable = bool(runtime.get("installed"))
        selected_version = selected.get("version")
        runtime_version = runtime.get("version")
        result[name] = {
            "installed": selected_installed,
            "version": selected_version,
            "selected_installed": selected_installed,
            "selected_version": selected_version,
            "runtime_importable": runtime_importable,
            "runtime_version": runtime_version,
            "runtime_matches_selected": selected_installed == runtime_importable,
        }
    return result


def _check_packages_via_subprocess(
    python_executable: str,
    package_map: dict[str, str],
    dist_name_map: dict[str, str],
) -> tuple[Optional[dict[str, dict]], Optional[str]]:
    """Run dependency checks in a target python interpreter."""
    try:
        proc = subprocess.run(
            [
                python_executable,
                "-c",
                _PACKAGE_CHECK_SCRIPT,
                json.dumps(package_map),
                json.dumps(dist_name_map),
            ],
            capture_output=True,
            text=True,
            timeout=12,
            check=False,
        )
    except Exception as exc:
        return None, str(exc)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return None, err or f"Exit code {proc.returncode}"

    try:
        payload = json.loads(proc.stdout.strip() or "{}")
    except Exception as exc:
        return None, f"Invalid JSON output: {exc}"

    if not isinstance(payload, dict):
        return None, "Dependency check output is not a JSON object"
    if not payload.get("ok"):
        return None, str(payload.get("error") or "Unknown dependency check error")

    dependencies = payload.get("dependencies")
    if not isinstance(dependencies, dict):
        return None, "Dependency check output missing 'dependencies' object"

    return dependencies, None


def _check_packages_current_process(
    package_map: dict[str, str],
    dist_name_map: dict[str, str],
) -> dict[str, dict]:
    """Check packages in the running Python process."""
    result: dict[str, dict] = {}
    for display_name, module_name in package_map.items():
        installed = module_available(module_name)
        version: Optional[str] = None
        if installed:
            dist_name = dist_name_map.get(module_name, module_name)
            try:
                version = importlib.metadata.version(dist_name)
            except Exception:
                version = None
        result[display_name] = {"installed": installed, "version": version}
    return result
