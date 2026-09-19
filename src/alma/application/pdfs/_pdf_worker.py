"""Read — and optionally clean — ONE untrusted PDF, in a child process.

Run only by ``pdfs.sandbox`` as ``python -I -B _pdf_worker.py IN [--clean-to OUT]``.
It imports nothing but the standard library and pypdf (never ``alma``: that
would start the app), caps its own memory, CPU time, file size and open
files before touching the input, and prints ONE JSON object on stdout:

``readable, pages, encrypted, locked, text, metadata_title,
metadata_strings, active, cleaned, error``

* facts — page count, the text of the first pages, metadata strings — for
  identity verification in the parent;
* ``active`` — what in the file can act on its own when opened: scripts,
  launch / submit / import actions, embedded files, XFA forms, media
  annotations, links to local files, a document that is also another format
  (bytes before ``%PDF-`` or after the last ``%%EOF``);
* with ``--clean-to``: when anything was found, a rewritten copy without it
  (links to web pages and in-document jumps are kept), re-read and re-checked
  before it is reported ``cleaned``. A file that cannot be cleaned — locked by
  a password, too complex to walk, or still active after rewriting — gets an
  ``error`` and no output.

A crash, a limit hit or a timeout leaves no JSON; the parent treats that as
"could not be read safely".
"""

from __future__ import annotations

import json
import logging
import sys

MAX_PDF_BYTES = 100 * 1024 * 1024
TEXT_CHARS = 40_000
META_CHARS = 1_000
META_STRINGS = 50
WALK_BUDGET = 500_000
TAIL_WINDOW = 1024 * 1024
TRAILING_JUNK = 1024

_BAD_ACTIONS = {
    "/JavaScript": "JavaScript",
    "/Launch": "program launch",
    "/SubmitForm": "form submission",
    "/ImportData": "data import",
    "/GoToR": "link into another file",
    "/GoToE": "link into an embedded file",
    "/Rendition": "media playback",
    "/RichMediaExecute": "media playback",
    "/Movie": "media playback",
    "/Sound": "media playback",
}
_BAD_ANNOTATIONS = {
    "/FileAttachment": "embedded file",
    "/RichMedia": "embedded media",
    "/Movie": "embedded media",
    "/Sound": "embedded media",
    "/Screen": "embedded media",
    "/3D": "embedded media",
}
_SAFE_URI_PREFIXES = ("http://", "https://", "mailto:")


def _limit_self() -> None:
    """Cap this process before it reads a byte of the input (POSIX only)."""
    try:
        import resource
    except ImportError:  # Windows: the parent's wall-clock timeout is the limit
        return
    caps = (
        (resource.RLIMIT_AS, 1024 * 1024 * 1024),  # 1 GiB address space
        (resource.RLIMIT_CPU, 20),  # seconds of CPU
        (resource.RLIMIT_FSIZE, 2 * MAX_PDF_BYTES),  # the cleaned copy, at most
        (resource.RLIMIT_NOFILE, 32),
        (resource.RLIMIT_CORE, 0),
    )
    for limit, value in caps:
        try:
            resource.setrlimit(limit, (value, value))
        except (ValueError, OSError):
            pass


def _is_polyglot(path: str) -> bool:
    """True when bytes precede ``%PDF-`` or more than a KB follows the last ``%%EOF``."""
    with open(path, "rb") as handle:
        if not handle.read(5) == b"%PDF-":
            return True
        handle.seek(0, 2)
        size = handle.tell()
        handle.seek(max(0, size - TAIL_WINDOW))
        tail = handle.read()
    end = tail.rfind(b"%%EOF")
    return end < 0 or len(tail) - (end + 5) > TRAILING_JUNK


class _Walker:
    """Find (and, with ``fix``, remove) active content, walking from the catalog."""

    def __init__(self, *, fix: bool) -> None:
        self.fix = fix
        self.found: set[str] = set()
        self.exhausted = False

    def walk(self, root) -> None:
        from pypdf.generic import ArrayObject, DictionaryObject, IndirectObject

        seen: set[tuple[int, int]] = set()
        stack = [root]
        budget = WALK_BUDGET
        while stack:
            budget -= 1
            if budget < 0:
                self.exhausted = True
                return
            node = stack.pop()
            if isinstance(node, IndirectObject):
                ref = (node.idnum, node.generation)
                if ref in seen:
                    continue
                seen.add(ref)
                try:
                    node = node.get_object()
                except Exception:  # noqa: BLE001 — a broken reference leads nowhere
                    continue
            if isinstance(node, DictionaryObject):
                self._inspect(node)
                stack.extend(node.values())
            elif isinstance(node, ArrayObject):
                stack.extend(node)

    @staticmethod
    def _resolve(value):
        from pypdf.generic import IndirectObject

        try:
            return value.get_object() if isinstance(value, IndirectObject) else value
        except Exception:  # noqa: BLE001
            return None

    def _bad_action(self, value) -> str | None:
        from pypdf.generic import DictionaryObject

        action = self._resolve(value)
        if not isinstance(action, DictionaryObject):
            return None
        kind = action.get("/S")
        if kind in _BAD_ACTIONS:
            return _BAD_ACTIONS[kind]
        if kind == "/URI":
            uri = str(self._resolve(action.get("/URI")) or "").strip().lower()
            if not uri.startswith(_SAFE_URI_PREFIXES):
                return "link to a local file"
        return None

    def _drop(self, holder, key: str, label: str) -> None:
        self.found.add(label)
        if self.fix and key in holder:
            del holder[key]

    def _inspect(self, node) -> None:
        from pypdf.generic import ArrayObject, DictionaryObject, NameObject

        for key, label in (("/AA", "automatic action"), ("/JS", "JavaScript")):
            if key in node:
                self._drop(node, key, label)
        if "/OpenAction" in node:
            opening = self._resolve(node["/OpenAction"])
            harmless = isinstance(opening, ArrayObject) or (
                isinstance(opening, DictionaryObject) and opening.get("/S") == "/GoTo"
            )
            if not harmless:
                self._drop(node, "/OpenAction", self._bad_action(opening) or "automatic action on open")
        for key in ("/A", "/Next"):
            if key not in node:
                continue
            target = self._resolve(node[key])
            if isinstance(target, ArrayObject):  # a /Next chain
                keep = [item for item in target if not self._note(self._bad_action(item))]
                if self.fix and not keep:
                    del node[key]
                elif self.fix and len(keep) != len(target):
                    node[NameObject(key)] = ArrayObject(keep)
            else:
                label = self._bad_action(target)
                if label:
                    self._drop(node, key, label)
        names = self._resolve(node.get("/Names")) if "/Names" in node else None
        if isinstance(names, DictionaryObject):
            for key, label in (("/JavaScript", "JavaScript"), ("/EmbeddedFiles", "embedded file")):
                if key in names:
                    self._drop(names, key, label)
        form = self._resolve(node.get("/AcroForm")) if "/AcroForm" in node else None
        if isinstance(form, DictionaryObject) and "/XFA" in form:
            self._drop(form, "/XFA", "XFA form")
        annotations = self._resolve(node.get("/Annots")) if "/Annots" in node else None
        if isinstance(annotations, ArrayObject):
            keep = []
            for item in annotations:
                annotation = self._resolve(item)
                subtype = annotation.get("/Subtype") if isinstance(annotation, DictionaryObject) else None
                if subtype in _BAD_ANNOTATIONS:
                    self.found.add(_BAD_ANNOTATIONS[subtype])
                else:
                    keep.append(item)
            if self.fix and len(keep) != len(annotations):
                annotations[:] = keep

    def _note(self, label: str | None) -> bool:
        if label:
            self.found.add(label)
        return bool(label)


def _facts(reader, pages: int) -> dict:
    count = len(reader.pages)
    chunks: list[str] = []
    for index in range(min(pages, count)):
        try:
            chunks.append(reader.pages[index].extract_text() or "")
        except Exception:  # noqa: BLE001 — one bad page must not sink the rest
            continue
    title = ""
    strings: list[str] = []
    try:
        meta = reader.metadata or {}
        title = str(meta.get("/Title") or "")[:META_CHARS]
        strings.extend(str(value)[:META_CHARS] for value in meta.values() if value)
    except Exception:  # noqa: BLE001
        pass
    try:
        xmp = reader.xmp_metadata
        if xmp is not None and xmp.dc_identifier:
            strings.append(str(xmp.dc_identifier)[:META_CHARS])
    except Exception:  # noqa: BLE001
        pass
    return {
        "pages": count,
        "text": "\n".join(chunks)[:TEXT_CHARS],
        "metadata_title": title,
        "metadata_strings": strings[:META_STRINGS],
    }


def _clean(reader, out_path: str) -> str:
    """Write ``reader`` to ``out_path`` without active content; '' or an error."""
    from pypdf import PdfReader, PdfWriter

    writer = PdfWriter(clone_from=reader)
    fixer = _Walker(fix=True)
    fixer.walk(writer.root_object)
    if fixer.exhausted:
        return "Too complex to check for active content"
    with open(out_path, "wb") as handle:
        writer.write(handle)
    check = _Walker(fix=False)
    check.walk(PdfReader(out_path, strict=False).trailer["/Root"])
    if check.found or check.exhausted:
        return "Could not be made safe"
    return ""


def main(argv: list[str]) -> int:
    _limit_self()
    logging.getLogger("pypdf").setLevel(logging.CRITICAL)
    path = argv[1]
    clean_to = argv[argv.index("--clean-to") + 1] if "--clean-to" in argv else ""
    pages = int(argv[argv.index("--pages") + 1]) if "--pages" in argv else 2
    reply: dict = {
        "readable": False, "pages": None, "encrypted": False, "locked": False, "text": "",
        "metadata_title": "", "metadata_strings": [], "active": [], "cleaned": False, "error": "",
    }
    from pypdf import PasswordType, PdfReader

    try:
        reader = PdfReader(path, strict=False)
        if reader.is_encrypted:
            reply["encrypted"] = True
            if reader.decrypt("") == PasswordType.NOT_DECRYPTED:
                reply["locked"] = True
                reply["error"] = "Password-protected"
                print(json.dumps(reply))
                return 0
        reply.update(_facts(reader, pages))
        reply["readable"] = True
    except Exception:  # noqa: BLE001 — any parse failure means "not a usable PDF"
        print(json.dumps(reply))
        return 0

    walker = _Walker(fix=False)
    try:
        walker.walk(reader.trailer["/Root"])
    except Exception:  # noqa: BLE001
        walker.exhausted = True
    active = set(walker.found)
    if _is_polyglot(path):
        active.add("data outside the PDF")
    reply["active"] = sorted(active)
    if clean_to and (active or walker.exhausted):
        try:
            error = _clean(reader, clean_to)
        except Exception:  # noqa: BLE001
            error = "Could not be made safe"
        reply["error"] = error
        reply["cleaned"] = not error
    print(json.dumps(reply))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
