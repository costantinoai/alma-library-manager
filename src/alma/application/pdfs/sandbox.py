"""Run the PDF parser where a hostile file cannot hurt the server.

Every PDF ALMa keeps — fetched from a site we do not control, uploaded or
imported — is opened by ``pypdf``, a pure-Python parser of an adversarial
format. It never runs in the server process: :func:`inspect_pdf` starts
``_pdf_worker.py`` in a child process that

* is started by path with ``-I -B`` (isolated: no ``PYTHON*`` variables, no
  user site, no ``alma`` import) and a minimal environment (no secrets);
* caps its own memory (1 GiB), CPU (20 s), output file size and open files
  before reading the input (POSIX; on Windows only the wall-clock timeout
  below applies);
* is killed after :data:`WORKER_TIMEOUT` seconds, and may say at most
  :data:`MAX_REPLY_BYTES` on stdout;
* runs at most :data:`MAX_WORKERS` at a time.

Its one JSON reply is validated field by field before anything is used.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from alma.core.url_safety import clean_remote_text

WORKER = Path(__file__).with_name("_pdf_worker.py")
WORKER_TIMEOUT = 45.0
MAX_REPLY_BYTES = 2_000_000
MAX_WORKERS = 2
MAX_PAGES = 100_000
_TEXT_CHARS = 40_000
_META_CHARS = 1_000
_META_STRINGS = 50
_SLOTS = threading.BoundedSemaphore(MAX_WORKERS)
_UNSAFE = "The PDF could not be read safely (too large, too slow or malformed)"


@dataclass(frozen=True)
class PdfInspection:
    """What the sandboxed worker could say about one file (all fields validated)."""

    readable: bool = False
    pages: int | None = None
    encrypted: bool = False
    locked: bool = False
    text: str = ""
    metadata_title: str = ""
    metadata_strings: tuple[str, ...] = ()
    active: tuple[str, ...] = ()
    cleaned: bool = False
    error: str = ""


def _environment() -> dict[str, str]:
    """Just enough to start Python — no API keys, no ALMa settings paths."""
    env = {"PATH": os.environ.get("PATH", os.defpath), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    if sys.platform == "win32":
        env["SYSTEMROOT"] = os.environ.get("SYSTEMROOT", r"C:\Windows")
    return env


def inspect_pdf(
    path: Path,
    *,
    clean_to: Path | None = None,
    pages: int = 2,
    timeout: float = WORKER_TIMEOUT,
) -> PdfInspection:
    """Read ``path`` in the sandbox; with ``clean_to``, write a copy without active content.

    Never raises for anything the file does: a crash, a limit hit, a timeout
    or an unusable reply comes back as ``PdfInspection(error=…)``. When the
    reply says ``cleaned``, ``clean_to`` holds the rewritten file; otherwise
    ``clean_to`` is removed if the worker left anything there.
    """
    command = [sys.executable, "-I", "-B", str(WORKER), str(path), "--pages", str(int(pages))]
    if clean_to is not None:
        command += ["--clean-to", str(clean_to)]
    with _SLOTS:
        raw, finished = _run(command, timeout)
    inspection = _parse(raw) if finished else PdfInspection(error="The PDF took too long to read")
    if clean_to is not None and not inspection.cleaned:
        clean_to.unlink(missing_ok=True)
    return inspection


def _run(command: list[str], timeout: float) -> tuple[bytes, bool]:
    """Run the worker; return (stdout up to the cap, finished in time)."""
    started = time.monotonic()
    process = subprocess.Popen(  # noqa: S603 — fixed interpreter + our own script, no shell
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=_environment(),
        close_fds=True,
    )
    deadline = started + timeout
    chunks: list[bytes] = []
    reader = threading.Thread(target=lambda: chunks.append(process.stdout.read(MAX_REPLY_BYTES + 1)), daemon=True)
    reader.start()
    reader.join(max(0.0, deadline - time.monotonic()))
    try:
        process.wait(timeout=max(0.0, deadline - time.monotonic()))
        finished = True
    except subprocess.TimeoutExpired:
        finished = False
    if finished:
        reader.join(1.0)  # the reply is complete once the process has exited
        finished = not reader.is_alive()
    else:
        process.kill()
        process.wait()
    process.stdout.close()
    return (chunks[0] if chunks else b""), finished


def _string(value: object, limit: int) -> str:
    return value[:limit] if isinstance(value, str) else ""


def _parse(raw: bytes) -> PdfInspection:
    """Validate the worker's reply field by field; anything off → an error inspection."""
    if not raw or len(raw) > MAX_REPLY_BYTES:
        return PdfInspection(error=_UNSAFE)
    try:
        reply = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return PdfInspection(error=_UNSAFE)
    if not isinstance(reply, dict):
        return PdfInspection(error=_UNSAFE)
    pages = reply.get("pages")
    if pages is not None and not (isinstance(pages, int) and 0 <= pages <= MAX_PAGES):
        return PdfInspection(error=_UNSAFE)
    strings = reply.get("metadata_strings")
    active = reply.get("active")
    return PdfInspection(
        readable=reply.get("readable") is True,
        pages=pages,
        encrypted=reply.get("encrypted") is True,
        locked=reply.get("locked") is True,
        text=_string(reply.get("text"), _TEXT_CHARS),
        metadata_title=_string(reply.get("metadata_title"), _META_CHARS),
        metadata_strings=tuple(
            _string(item, _META_CHARS) for item in (strings if isinstance(strings, list) else [])[:_META_STRINGS]
        ),
        active=tuple(clean_remote_text(item, 60) for item in (active if isinstance(active, list) else [])[:20]),
        cleaned=reply.get("cleaned") is True,
        error=clean_remote_text(_string(reply.get("error"), 200)),
    )
