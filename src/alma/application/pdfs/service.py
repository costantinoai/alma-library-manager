"""Paper PDFs — the use cases: attach an upload, import PDF-first, (fetch).

Each use case is the BODY of one background Activity job (see ``jobs``); it
receives its own DB connection and a step logger, and it follows the SQLite
write discipline: network and pypdf work happen with no transaction open, and
every write is a short ``write_section``. A file is placed in the tree BEFORE
the row that points at it commits, and a replaced file is unlinked only AFTER.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field

from alma.application.pdf_schema import Origin, PaperRef, Verification
from alma.application.pdfs import store
from alma.application.pdfs.identify import IdentityHint, identify
from alma.application.pdfs.verify import (
    BodyKind,
    PdfFacts,
    classify_head,
    read_facts,
    verify_identity,
)
from alma.core.db_write import write_section
from alma.core.time import utcnow
from alma.core.utils import canonical_lookup_doi

logger = logging.getLogger(__name__)

#: ``log(step, message, data)`` — the job's Activity step logger.
StepLog = Callable[..., None]


def _no_log(step: str, message: str, **_kw) -> None:  # pragma: no cover - default
    logger.debug("[%s] %s", step, message)


# ---------------------------------------------------------------------------
# The paper a PDF belongs to
# ---------------------------------------------------------------------------


def build_paper_ref(conn: sqlite3.Connection, paper_id: str) -> PaperRef | None:
    """The :class:`PaperRef` of ``paper_id``'s ROOT, with its group's identifiers.

    A preprint child resolves to its published root, so a PDF always belongs
    to the paper the Library shows; the group's other members contribute their
    DOIs / arXiv ids (a preprint copy is still this paper). Component rows
    (datasets, figures) are excluded — their DOIs name other objects.
    Returns ``None`` for an unknown id or an orphan component.
    """
    from alma.core.paper_groups import (
        collect_paper_group_ids,
        is_component_row,
        resolve_action_paper_id,
    )
    from alma.services.preprint_abstract import arxiv_id_from_identifiers

    root_id = resolve_action_paper_id(conn, paper_id)
    if not root_id:
        return None
    group = sorted(collect_paper_group_ids(conn, root_id))
    placeholders = ",".join("?" for _ in group)
    rows = {
        str(row["id"]): row
        for row in conn.execute(f"SELECT * FROM papers WHERE id IN ({placeholders})", group).fetchall()
    }
    root = rows.get(root_id)
    if root is None:
        return None

    def _arxiv(row) -> str:
        urls = [str(row["url"] or ""), str(row["oa_url"] or "")]
        return arxiv_id_from_identifiers(row["doi"], [u for u in urls if u]) or ""

    members = [r for pid, r in rows.items() if pid != root_id and not is_component_row(r)]
    try:
        year = int(root["year"]) if root["year"] else None
    except (TypeError, ValueError):
        year = None
    return PaperRef(
        paper_id=root_id,
        title=str(root["title"] or ""),
        year=year,
        authors=str(root["authors"] or ""),
        doi=canonical_lookup_doi(root["doi"]) or "",
        openalex_id=str(root["openalex_id"] or ""),
        semantic_scholar_id=str(root["semantic_scholar_id"] or ""),
        arxiv_id=_arxiv(root),
        url=str(root["url"] or ""),
        oa_url=str(root["oa_url"] or ""),
        group_dois=tuple(d for d in (canonical_lookup_doi(r["doi"]) for r in members) if d),
        group_arxiv_ids=tuple(a for a in (_arxiv(r) for r in members) if a),
    )


def paper_dois(ref: PaperRef) -> list[str]:
    """Every DOI that names this paper (its own first, then its group's)."""
    return [d for d in (ref.doi, *ref.group_dois) if d]


def pdf_status(conn: sqlite3.Connection, root_id: str) -> dict:
    """The paper's PDF state for the detail payload — a PURE read.

    ``stored`` is the kept file (or ``None``); ``file_missing`` flags a row
    whose file is gone (e.g. a dev profile seeded from prod rows only) so the
    UI offers to fetch or attach again instead of a dead link; ``attempts``
    is the latest outcome per source.
    """
    record = store.read_pdf(conn, root_id)
    missing = False
    if record is not None:
        try:
            missing = not store.absolute_path(record.rel_path).is_file()
        except store.UnsafeStorePathError:
            missing = True
    return {
        "stored": record.to_wire() if record is not None else None,
        "file_missing": missing,
        "url": f"/api/v1/papers/{root_id}/pdf" if record is not None and not missing else None,
        "attempts": store.list_attempts(conn, root_id),
    }


# ---------------------------------------------------------------------------
# Storing a verified file (shared by every origin)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StoreOutcome:
    record: store.StoredPdf
    replaced: store.StoredPdf | None = None


def store_file(
    conn: sqlite3.Connection,
    *,
    staged: store.StagedFile,
    ref: PaperRef,
    facts: PdfFacts,
    origin: Origin,
    verification: Verification,
    source_id: str | None = None,
    source_plugin: str | None = None,
    source_url: str | None = None,
    original_filename: str | None = None,
    version: str | None = None,
    license: str | None = None,
    extra_writes: Callable[[sqlite3.Connection], None] | None = None,
) -> StoreOutcome:
    """Place ``staged`` in the tree as ``ref``'s PDF and point its row at it.

    Order matters: the file is linked into the tree first (no transaction
    open), then ONE short write section upserts the row (plus any
    ``extra_writes`` that must commit with it, e.g. the fetch attempt), then
    the file it replaced is unlinked — only once nothing references it.
    """
    rel = store.place(
        staged,
        store.filename_for(title=ref.title, year=ref.year, authors=ref.authors),
        paper_id=ref.paper_id,
    )
    record = store.StoredPdf(
        paper_id=ref.paper_id,
        rel_path=rel,
        sha256=staged.sha256,
        bytes=staged.bytes,
        pages=facts.pages,
        origin=str(origin),
        verification=str(verification),
        source_id=source_id,
        source_plugin=source_plugin,
        source_url=source_url,
        original_filename=original_filename,
        version=version or None,
        license=license or None,
        stored_at=utcnow().isoformat(),
    )
    try:
        with write_section(conn, label="pdf.store"):
            replaced = store.upsert_pdf(conn, record)
            if extra_writes is not None:
                extra_writes(conn)
    except BaseException:
        store.unlink_quietly(rel)  # the row never pointed at it
        raise
    if replaced is not None and not store.is_referenced(conn, replaced.rel_path):
        store.unlink_quietly(replaced.rel_path)
    return StoreOutcome(record=record, replaced=replaced)


class NotAPdfError(ValueError):
    """The bytes are not a readable PDF (the staged file has been removed)."""


def inspect_staged(staged: store.StagedFile) -> PdfFacts:
    """Confirm a staged body is a readable PDF and read its facts (CPU only)."""
    kind = classify_head(store.read_head(staged.path))
    if kind is not BodyKind.PDF or staged.bytes < 1024:
        raise NotAPdfError("That file is not a PDF")
    facts = read_facts(staged.path)
    if not facts.readable:
        raise NotAPdfError("That PDF cannot be opened (damaged or not a real PDF)")
    return facts


# ---------------------------------------------------------------------------
# Attach an upload to a known paper
# ---------------------------------------------------------------------------


def attach_upload(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    staged: store.StagedFile,
    filename: str,
    log: StepLog = _no_log,
) -> dict:
    """Store an uploaded PDF as ``paper_id``'s file (replacing any previous one).

    The user chose the paper, so their association is authoritative: a file
    whose text names a different paper is still stored, flagged ``mismatch``.
    """
    ref = build_paper_ref(conn, paper_id)
    if ref is None:
        store.release_staged(staged)
        raise LookupError("That paper no longer exists")
    facts = inspect_staged(staged)
    log("inspect", f"Read {facts.pages} page(s) from {filename or 'the upload'}", data={"pages": facts.pages})
    verification = verify_identity(facts, dois=paper_dois(ref), title=ref.title)
    log("verify", f"Checked the file against the paper: {verification}", data={"verification": str(verification)})
    outcome = store_file(
        conn,
        staged=staged,
        ref=ref,
        facts=facts,
        origin=Origin.UPLOADED,
        verification=verification,
        original_filename=filename or None,
    )
    log("store", f"Stored as {outcome.record.filename}", data={"replaced": bool(outcome.replaced)})
    return {
        "outcome": "attached",
        "paper_id": ref.paper_id,
        "title": ref.title,
        "verification": str(verification),
        "filename": outcome.record.filename,
        "replaced": outcome.replaced is not None,
    }


# ---------------------------------------------------------------------------
# PDF-first import (→ Library, D4) — never creates a duplicate paper
# ---------------------------------------------------------------------------


@dataclass
class ImportTrace:
    tried: list[str] = field(default_factory=list)


def _local_match(conn: sqlite3.Connection, hint: IdentityHint) -> str | None:
    """An existing paper for ``hint`` — the importer's own dedup (no network)."""
    from alma.library.importer import find_existing_paper

    if hint.doi or hint.openalex_id:
        return find_existing_paper(conn, hint.doi or "", hint.openalex_id or "", "", None)
    if hint.title:
        return find_existing_paper(conn, "", "", hint.title, None)
    return None


def _accepts(facts: PdfFacts, *, dois: list[str], title: str) -> Verification | None:
    """The verification if the file plausibly IS this paper, else ``None``."""
    verification = verify_identity(facts, dois=dois, title=title)
    return None if verification is Verification.MISMATCH else verification


def import_upload(
    conn: sqlite3.Connection,
    *,
    staged: store.StagedFile,
    filename: str,
    user_hint: IdentityHint | None = None,
    log: StepLog = _no_log,
) -> dict:
    """Identify a dropped PDF, save its paper to the Library, attach the file.

    Order is the user's rule — **never create a duplicate**:

    1. the same bytes already stored for a paper → report that paper;
    2. for each identity hint (a typed DOI/title first when retrying): an
       EXISTING paper (the importer's canonical dedup: openalex id → DOI →
       title) is promoted into the Library (D4) and gets the file;
    3. only when no hint matches locally: resolve online (read-only), check
       the resolved work against the file, and save it through the canonical
       online-save path (``added_from='import'``), whose upsert dedups again.

    A hint whose paper the file's text contradicts is skipped. Nothing
    matched → ``unresolved``: the staged file is KEPT for a retry with a
    typed DOI or title.
    """
    facts = inspect_staged(staged)
    log("inspect", f"Read {facts.pages} page(s) from {filename or 'the upload'}", data={"pages": facts.pages})

    same = store.find_by_sha(conn, staged.sha256)
    if same is not None:
        store.release_staged(staged)
        title = str((conn.execute("SELECT title FROM papers WHERE id = ?", (same.paper_id,)).fetchone() or [""])[0])
        log("dedupe", f"Already stored for “{title}”")
        return {"outcome": "already_stored", "paper_id": same.paper_id, "title": title}

    hints = identify(facts, filename=filename)
    if user_hint is not None:
        hints = [user_hint, *hints]
    log("identify", f"{len(hints)} identity hint(s)", data={"hints": [h.describe() for h in hints]})

    # 1) Local first — an existing paper always wins over creating one.
    for hint in hints:
        paper_id = _local_match(conn, hint)
        if not paper_id:
            continue
        ref = build_paper_ref(conn, paper_id)
        if ref is None:
            continue
        verification = _accepts(facts, dois=paper_dois(ref), title=ref.title)
        if verification is None and hint is not user_hint:
            log("match", f"Skipped “{ref.title}” ({hint.describe()}): the file names another paper")
            continue
        log("match", f"Matched an existing paper: “{ref.title}” ({hint.describe()})")
        return _attach_imported(conn, ref=ref, staged=staged, facts=facts, filename=filename,
                                verification=verification or Verification.MISMATCH, created=False, log=log)

    # 2) Online — read-only resolution, verified against the file BEFORE saving.
    from alma.application.openalex_manual import resolve_work_for_ingest, save_online_search_result

    for hint in hints:
        try:
            work, source = resolve_work_for_ingest(
                openalex_id=hint.openalex_id, doi=hint.doi, title=hint.title
            )
        except Exception as exc:  # upstream down: try the next hint, report at the end
            log("resolve", f"Lookup failed for {hint.describe()}: {exc}", level="WARNING")
            continue
        if not work:
            log("resolve", f"No record found for {hint.describe()}")
            continue
        work_title = str(work.get("title") or "")
        verification = _accepts(facts, dois=[str(work.get("doi") or "")], title=work_title)
        if verification is None and hint is not user_hint:
            log("resolve", f"Skipped “{work_title}” ({hint.describe()}): the file names another paper")
            continue
        saved = save_online_search_result(
            conn,
            openalex_id=str(work.get("openalex_id") or "") or None,
            doi=hint.doi,
            title=hint.title if not work.get("openalex_id") else None,
            action="add",
            added_from="import",
            override_added_from=True,
        )
        ref = build_paper_ref(conn, str(saved["id"]))
        if ref is None:
            break
        log("resolve", f"Saved “{ref.title}” to the Library (via {source})")
        return _attach_imported(conn, ref=ref, staged=staged, facts=facts, filename=filename,
                                verification=verification or Verification.MISMATCH, created=True, log=log)

    token = store.keep_staged(staged, filename=filename)
    log("unresolved", "Could not tell which paper this is — add its DOI or title to retry")
    return {"outcome": "unresolved", "upload_id": token, "filename": filename,
            "hints": [h.describe() for h in hints]}


def _attach_imported(
    conn: sqlite3.Connection,
    *,
    ref: PaperRef,
    staged: store.StagedFile,
    facts: PdfFacts,
    filename: str,
    verification: Verification,
    created: bool,
    log: StepLog,
) -> dict:
    """Promote the matched paper into the Library (D4) and store the file on it."""
    from alma.library.importer import promote_existing_import_target

    promoted = False
    if not created:
        with write_section(conn, label="pdf.import.promote"):
            save_id, promoted = promote_existing_import_target(conn, ref.paper_id, added_from="import")
        if save_id != ref.paper_id:  # a Library duplicate absorbed it: attach to the survivor
            ref = build_paper_ref(conn, save_id) or ref
    outcome = store_file(
        conn,
        staged=staged,
        ref=ref,
        facts=facts,
        origin=Origin.IMPORTED,
        verification=verification,
        original_filename=filename or None,
    )
    store.release_staged(staged)
    log("store", f"Stored as {outcome.record.filename}")
    return {
        "outcome": "imported" if created else "attached",
        "paper_id": ref.paper_id,
        "title": ref.title,
        "verification": str(verification),
        "filename": outcome.record.filename,
        "promoted_to_library": created or promoted,
    }
