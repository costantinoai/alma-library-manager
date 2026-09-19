"""Paper PDFs — the use cases: fetch on tap, attach an upload, import PDF-first.

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
from urllib.parse import urlsplit

from alma.application.pdf_schema import (
    Origin,
    Outcome,
    PaperRef,
    PdfSource,
    SourceBlockedError,
    SourceSkippedError,
    Verification,
)
from alma.application.pdfs import store
from alma.application.pdfs.download import DownloadResult, download_candidate
from alma.application.pdfs.identify import IdentityHint, identify
from alma.application.pdfs.sandbox import inspect_pdf
from alma.application.pdfs.verify import (
    BodyKind,
    PdfFacts,
    classify_head,
    facts_from,
    verify_identity,
)
from alma.core.db_write import write_section
from alma.core.http_sources import describe_transport_error
from alma.core.time import utcnow
from alma.core.url_safety import clean_remote_text
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
    source_sha256: str | None = None,
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
        source_sha256=source_sha256 or staged.sha256,
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


def inspect_staged(staged: store.StagedFile, *, log: StepLog = _no_log) -> PdfFacts:
    """Confirm a staged upload is a readable PDF and read its facts (in the sandbox).

    The user's own file is kept as they gave it; if it carries active content
    (scripts, launch actions, embedded files…) Activity says so.
    """
    kind = classify_head(store.read_head(staged.path))
    if kind is not BodyKind.PDF or staged.bytes < 1024:
        raise NotAPdfError("That file is not a PDF")
    inspection = inspect_pdf(staged.path)
    facts = facts_from(inspection)
    if not facts.readable:
        raise NotAPdfError(
            "That PDF is password-protected" if inspection.locked
            else inspection.error or "That PDF cannot be opened (damaged or not a real PDF)"
        )
    if inspection.active:
        log("inspect", f"This PDF contains active content ({', '.join(inspection.active)}); kept as you gave it",
            level="WARNING")
    return facts


# ---------------------------------------------------------------------------
# Fetch on tap — the source chain
# ---------------------------------------------------------------------------

_OUTCOME_WORDS = {
    Outcome.NO_CANDIDATE: "no copy listed",
    Outcome.NOT_PDF: "not a PDF",
    Outcome.BLOCKED: "blocked by a bot check",
    Outcome.HTTP_ERROR: "server refused",
    Outcome.TOO_LARGE: "file too large",
    Outcome.MISMATCH: "a different paper's PDF",
    Outcome.REJECTED: "the file you marked wrong",
    Outcome.SKIPPED: "not set up",
    Outcome.ERROR: "unreachable",
}


# When every candidate of a source missed, the attempt row carries the most
# telling outcome (a wrong file beats a wall beats a refusal beats "no PDF")
# and, for several candidates, what each one answered.
_MISS_PRIORITY = (
    Outcome.MISMATCH,
    Outcome.REJECTED,
    Outcome.TOO_LARGE,
    Outcome.BLOCKED,
    Outcome.HTTP_ERROR,
    Outcome.ERROR,
    Outcome.NOT_PDF,
    Outcome.NO_CANDIDATE,
)
_MISS_DETAIL_CHARS = 500


class FetchUnavailableError(RuntimeError):
    """Fetching cannot start (no source plugin on, network off, paper gone)."""


def _miss_words(result: DownloadResult) -> str:
    return result.detail or _OUTCOME_WORDS.get(result.outcome, str(result.outcome))


def _host(url: str) -> str:
    """The host a miss came from, as safe text (a hostile URL may not even parse)."""
    try:
        host = urlsplit(url or "").hostname
    except ValueError:
        host = None
    return clean_remote_text(host, 80) if host else "(invalid address)"


def summarize_misses(misses: list[DownloadResult]) -> tuple[DownloadResult, str]:
    """The attempt row for a source whose candidates all missed: ``(lead, detail)``.

    A single miss is recorded as is. Several (one per mirror, or per listed
    location) keep the most telling outcome and say what each host answered,
    so a walled mirror is not hidden behind the last one's "no PDF". Each
    part is already clean (a host via :func:`_host`, a detail via
    ``DownloadResult``), so no part can carry the ``"; "`` that separates them.
    """
    if len(misses) == 1:
        return misses[0], misses[0].detail
    rank = {outcome: index for index, outcome in enumerate(_MISS_PRIORITY)}
    lead = min(misses, key=lambda miss: rank.get(miss.outcome, len(rank)))
    detail = "; ".join(f"{_host(miss.final_url)}: {_miss_words(miss)}" for miss in misses)
    return lead, detail[:_MISS_DETAIL_CHARS]


def _record_attempt(conn: sqlite3.Connection, **fields) -> None:
    with write_section(conn, label="pdf.attempt"):
        store.record_attempt(conn, **fields)


def _take_candidate(candidate, *, rejected: set[str], dois, title: str):
    """Download one candidate, then read (and clean) it in the sandbox.

    Returns a miss (:class:`DownloadResult`) or ``(result, staged, facts,
    verification, inspection)``, where ``staged`` is the file to keep — the
    cleaned copy when the original carried active content. Network and a
    child process only; never the database.
    """
    result = download_candidate(candidate)
    if result.outcome is not Outcome.FOUND:
        return result
    original = result.staged
    if original.sha256 in rejected:
        original.discard()
        return DownloadResult(Outcome.REJECTED, http_status=result.http_status, final_url=result.final_url)
    clean_path = store.new_staging_path()
    inspection = inspect_pdf(original.path, clean_to=clean_path)
    facts = facts_from(inspection)
    if not facts.readable or inspection.error:
        original.discard()
        detail = "Password-protected" if inspection.locked else inspection.error or "Unreadable PDF"
        return DownloadResult(Outcome.NOT_PDF, detail=detail, final_url=result.final_url)
    kept = store.adopt_staged(clean_path) if inspection.cleaned else original
    verification = verify_identity(facts, dois=dois, title=title)
    if verification is Verification.MISMATCH or kept.sha256 in rejected:
        original.discard()
        if kept is not original:
            kept.discard()
        outcome = Outcome.MISMATCH if verification is Verification.MISMATCH else Outcome.REJECTED
        return DownloadResult(outcome, http_status=result.http_status, final_url=result.final_url)
    return result, kept, facts, verification, inspection


def fetch_for_paper(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    job_id: str | None = None,
    log: StepLog = _no_log,
    sources: list[tuple[str, PdfSource]] | None = None,
) -> dict:
    """Try every enabled PDF source, in run order, until one yields THIS paper.

    ``sources`` — ``(plugin_id, PdfSource)`` pairs in run order — defaults to
    the plugin registry's enabled sources; callers with their own set (tests,
    a future "try this source only") pass it explicitly.

    Per source: ask for candidates (network, no DB), download each (validated
    by its bytes), skip a file the user rejected before, verify the rest
    against the paper (DOI, then title) — a file naming another paper is a
    ``mismatch`` and the next candidate is tried. The first verified file is
    stored and ends the job; each source's outcome is recorded in its own
    short write section so the detail panel can say what was tried.
    """
    from alma.core.network_policy import network_access_enabled
    from alma.plugins.registry import get_plugin_registry

    ref = build_paper_ref(conn, paper_id)
    if ref is None:
        raise FetchUnavailableError("That paper no longer exists")
    if not network_access_enabled():
        raise FetchUnavailableError("Network access is switched off (Settings)")
    if sources is None:
        sources = [(manifest.id, source) for manifest, source in get_plugin_registry().pdf_sources()]
    pairs = list(sources)
    if not pairs:
        raise FetchUnavailableError("Switch on a PDF source in Settings → Plugins")

    rejected = store.rejected_shas(conn, ref.paper_id)
    dois = paper_dois(ref)
    tried: list[str] = []
    for index, (plugin_id, source) in enumerate(pairs, start=1):
        log("source", f"Asking {source.label}…", processed=index - 1, total=len(pairs))
        tried.append(source.label)
        attempt = {"paper_id": ref.paper_id, "source_id": source.id, "job_id": job_id}
        try:
            candidates = source.candidates(ref)
        except SourceSkippedError as exc:
            _record_attempt(conn, **attempt, outcome=Outcome.SKIPPED, detail=str(exc))
            log("source", f"{source.label}: skipped — {exc}")
            continue
        except SourceBlockedError as exc:
            _record_attempt(conn, **attempt, outcome=Outcome.BLOCKED, detail=str(exc))
            log("source", f"{source.label}: blocked — {exc}", level="WARNING")
            continue
        except Exception as exc:  # noqa: BLE001 — one source down must not end the chain
            detail = describe_transport_error(exc)
            _record_attempt(conn, **attempt, outcome=Outcome.ERROR, detail=detail)
            log("source", f"{source.label}: unreachable — {detail}", level="WARNING")
            continue
        if not candidates:
            _record_attempt(conn, **attempt, outcome=Outcome.NO_CANDIDATE)
            log("source", f"{source.label}: no copy listed")
            continue

        misses: list[DownloadResult] = []

        def _missed(miss: DownloadResult, *, _label: str = source.label, _many: bool = len(candidates) > 1) -> None:
            misses.append(miss)
            if _many:  # one line per mirror / location, so Activity shows each answer
                log("source", f"{_label} · {_host(miss.final_url)}: {_miss_words(miss)}",
                    level="WARNING" if miss.outcome in (Outcome.ERROR, Outcome.BLOCKED) else "INFO")

        for candidate in candidates:
            try:
                taken = _take_candidate(candidate, rejected=rejected, dois=dois, title=ref.title)
            except Exception as exc:  # noqa: BLE001 — one bad answer must not end the chain
                logger.warning("PDF candidate from %s failed: %s", source.id, describe_transport_error(exc))
                taken = DownloadResult(Outcome.ERROR, detail=describe_transport_error(exc))
            if isinstance(taken, DownloadResult):
                _missed(taken)
                continue
            result, staged, facts, verification, inspection = taken
            if inspection.cleaned:
                log("clean", f"{source.label}: removed active content — {', '.join(inspection.active)}",
                    level="WARNING")

            def _found(c: sqlite3.Connection, _r: DownloadResult = result, _v: Verification = verification) -> None:
                store.record_attempt(
                    c, **attempt, outcome=Outcome.FOUND, http_status=_r.http_status,
                    detail=str(_v), candidate_url=_r.safe_url,
                )

            outcome = store_file(
                conn,
                staged=staged,
                ref=ref,
                facts=facts,
                origin=Origin.FETCHED,
                verification=verification,
                source_id=source.id,
                source_plugin=plugin_id,
                source_url=result.safe_url,
                version=candidate.version,
                license=candidate.license,
                source_sha256=result.staged.sha256,
                extra_writes=_found,
            )
            if staged is not result.staged:
                result.staged.discard()  # the file as it arrived; its cleaned copy was kept
            via = f" · {_host(candidate.url)}" if len(candidates) > 1 else ""  # which mirror / location won
            log("store", f"{source.label}{via}: stored {outcome.record.filename} ({verification})",
                processed=index, total=len(pairs))
            return {
                "found": True,
                "paper_id": ref.paper_id,
                "title": ref.title,
                "source": source.label,
                "verification": str(verification),
                "filename": outcome.record.filename,
            }

        last, detail = summarize_misses(misses)
        _record_attempt(
            conn, **attempt, outcome=last.outcome, http_status=last.http_status,
            detail=detail or None, candidate_url=last.safe_url or None,
        )
        log("source", f"{source.label}: {_OUTCOME_WORDS.get(last.outcome, str(last.outcome))}",
            level="WARNING" if last.outcome in (Outcome.ERROR, Outcome.BLOCKED) else "INFO")

    return {"found": False, "paper_id": ref.paper_id, "title": ref.title, "tried": tried}


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
    facts = inspect_staged(staged, log=log)
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


def _arxiv_candidate(hint: IdentityHint) -> dict | None:
    """The arXiv record behind a hint that names an arXiv paper, if any."""
    from alma.core.resolution import extract_arxiv_id
    from alma.discovery.arxiv import fetch_work_by_id

    arxiv_id = hint.arxiv_id
    if not arxiv_id and (hint.doi or "").lower().startswith("10.48550/arxiv."):
        arxiv_id = extract_arxiv_id(hint.doi or "")
    return fetch_work_by_id(arxiv_id) if arxiv_id else None


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
    facts = inspect_staged(staged, log=log)
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
        # OpenAlex often folds an arXiv preprint into its venue version and has
        # no record under the arXiv DOI; the arXiv record itself is then the
        # multi-source candidate the online-save path falls back on.
        candidate = _arxiv_candidate(hint) if not work else None
        if not work and candidate is None:
            log("resolve", f"No record found for {hint.describe()}")
            continue
        record = work or candidate
        work_title = str(record.get("title") or "")
        verification = _accepts(facts, dois=[str(record.get("doi") or "")], title=work_title)
        if verification is None and hint is not user_hint:
            log("resolve", f"Skipped “{work_title}” ({hint.describe()}): the file names another paper")
            continue
        openalex_id = str((work or {}).get("openalex_id") or "") or None
        saved = save_online_search_result(
            conn,
            openalex_id=openalex_id,
            doi=hint.doi,
            title=None if openalex_id else (work_title or hint.title),
            candidate=candidate,
            action="add",
            added_from="import",
            override_added_from=True,
        )
        source = source if work else "arXiv"
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
