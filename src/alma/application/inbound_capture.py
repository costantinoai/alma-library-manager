"""Turn an :class:`InboundMessage` into a paper in the Inbox.

The channel-agnostic half of the Inbox. Slack, email, a share-sheet endpoint —
each hands in an `InboundMessage` and gets back a `CaptureResult`, and nothing
in here knows or cares which one it was talking to. Adding a channel means
writing an adapter, never touching this file.

The pipeline, in order:

1. **Dedupe** on ``(channel, external_id)``. Channel delivery is at-least-once,
   so this runs first and replays the recorded outcome instead of re-capturing.
2. **Extract** identifiers from the message text and the channel's own links.
3. **Resolve** upstream via OpenAlex, preferring the strongest identifier.
4. **Land** the paper in the corpus through the same upsert every other source
   uses, then promote it to ``status='inbox'``.
5. **Record** the message and its outcome in `inbox_messages`.

Deliberately NOT built on :func:`save_online_search_result`: that function
applies the add/like/love RATING contract and lands papers in the Library. A
capture has not been triaged yet — it gets no rating and no opinion.

See `docs/concepts/inbox.md`.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import uuid
from dataclasses import dataclass

from alma.application.inbox_schema import (
    CaptureResult,
    ExtractedIdentifiers,
    InboundMessage,
)
from alma.core.resolution import extract_arxiv_id, extract_biorxiv_doi
from alma.core.time import utcnow
from alma.core.utils import is_doi_shaped, normalize_doi, normalize_openalex_id

logger = logging.getLogger(__name__)

# A DOI anywhere in free text. Stops at whitespace and at the punctuation that
# realistically terminates a DOI in prose or a URL — angle brackets (Slack
# wraps links as `<url|label>`), quotes, parens, and a trailing sentence period.
_DOI_IN_TEXT = re.compile(r"\b(10\.\d{4,9}/[^\s<>\"'()\[\]]+)", re.IGNORECASE)

# Bare URLs in free text, for the no-DOI fallback.
_URL_IN_TEXT = re.compile(r"https?://[^\s<>\"'\]]+", re.IGNORECASE)

# `W123456789` in an OpenAlex URL or on its own.
_OPENALEX_IN_TEXT = re.compile(r"\b(W\d{6,})\b")

# Trailing punctuation a DOI never really ends with — a DOI at the end of a
# sentence otherwise resolves with the period attached and misses.
_DOI_TRAILING_JUNK = ".,;:"


def extract_identifiers(message: InboundMessage) -> ExtractedIdentifiers:
    """Pull every resolvable identifier out of one message.

    Searches the message text AND the channel's pre-parsed `urls`. The channel's
    own links matter: Slack sends `<https://doi.org/10.1/x|the paper>`, where a
    naive regex over display text would capture the label too.

    An arXiv id becomes its registered DOI (``10.48550/arXiv.<id>``), because
    OpenAlex indexes preprints under that and a DOI is a far stronger lookup key
    than a URL.
    """
    haystack = " ".join(
        [message.text or "", *(message.urls or ())]
    ).strip()
    if not haystack:
        return ExtractedIdentifiers()

    doi: str | None = None
    match = _DOI_IN_TEXT.search(haystack)
    if match:
        candidate = match.group(1).rstrip(_DOI_TRAILING_JUNK)
        if is_doi_shaped(candidate):
            doi = normalize_doi(candidate)

    # Preprints: prefer the registered DOI form over the raw id.
    arxiv_id = extract_arxiv_id(haystack)
    if not doi:
        biorxiv_doi = extract_biorxiv_doi(haystack)
        if biorxiv_doi:
            doi = biorxiv_doi
        elif arxiv_id:
            doi = f"10.48550/arXiv.{arxiv_id}"

    openalex_id: str | None = None
    oa_match = _OPENALEX_IN_TEXT.search(haystack)
    if oa_match:
        openalex_id = normalize_openalex_id(oa_match.group(1)) or None

    # First URL, channel-supplied links first — the fallback when there is no
    # identifier at all. OpenAlex can sometimes resolve a landing page.
    url: str | None = None
    for candidate_url in (*(message.urls or ()), *_URL_IN_TEXT.findall(haystack)):
        cleaned = str(candidate_url or "").strip().rstrip(_DOI_TRAILING_JUNK)
        if cleaned:
            url = cleaned
            break

    return ExtractedIdentifiers(
        doi=doi,
        arxiv_id=arxiv_id,
        openalex_id=openalex_id,
        url=url,
        title=None,
    )


def find_recorded(
    db: sqlite3.Connection, *, channel: str, external_id: str
) -> CaptureResult | None:
    """The already-processed outcome for this message, or None if it is new.

    The idempotency read. Polling is at-least-once — a re-poll, or a crash
    between landing the paper and recording the message, re-delivers — so every
    capture starts here and a replay is a cheap no-op instead of a second copy
    of the same paper.
    """
    row = db.execute(
        """
        SELECT outcome, paper_id, extracted_json, error
        FROM inbox_messages
        WHERE channel = ? AND external_id = ?
        """,
        (channel, external_id),
    ).fetchone()
    if row is None:
        return None
    try:
        extracted = ExtractedIdentifiers(**json.loads(row["extracted_json"] or "{}"))
    except (ValueError, TypeError):
        extracted = ExtractedIdentifiers()
    return CaptureResult(
        outcome=row["outcome"],
        paper_id=row["paper_id"],
        extracted=extracted,
        error=row["error"],
    )


def latest_cursor(db: sqlite3.Connection, *, channel: str) -> str | None:
    """Highest `external_id` already processed for *channel*, or None.

    Derived from the ledger rather than kept in settings, so the cursor can
    never claim progress the database does not actually hold.
    """
    row = db.execute(
        "SELECT MAX(external_id) AS cursor FROM inbox_messages WHERE channel = ?",
        (channel,),
    ).fetchone()
    return str(row["cursor"]) if row and row["cursor"] else None


def record_message(
    db: sqlite3.Connection,
    message: InboundMessage,
    result: CaptureResult,
) -> None:
    """Persist one message + its outcome. Caller owns the transaction.

    `INSERT OR REPLACE` keyed on the UNIQUE `(channel, external_id)`: a retry of
    a message that previously errored overwrites the stale failure rather than
    raising on the constraint.
    """
    db.execute(
        """
        INSERT OR REPLACE INTO inbox_messages (
            id, channel, external_id, received_at, raw_text,
            extracted_json, outcome, paper_id, error, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            message.channel,
            message.external_id,
            message.received_at,
            message.text or "",
            json.dumps(result.extracted.as_dict()),
            result.outcome,
            result.paper_id,
            result.error,
            json.dumps(message.metadata or {}),
            utcnow().isoformat(),
        ),
    )


@dataclass(frozen=True, slots=True)
class ResolvedCapture:
    """Outcome of the NETWORK half — everything needed to persist, no DB touched.

    Split out so a caller can honour SQLite write-discipline rule 2 (never hold
    a write transaction across network I/O): resolve first, then open one short
    write window and call :func:`persist_capture`.
    """

    extracted: ExtractedIdentifiers
    normalized: dict | None = None
    match_source: str = "not_found"
    #: Set when resolution itself failed (upstream unreachable). Distinct from
    #: "resolved to nothing", which is an ordinary `unresolved` outcome.
    error: str | None = None
    #: True when the message carried nothing resolvable at all.
    empty: bool = False


def resolve_message(message: InboundMessage) -> ResolvedCapture:
    """Extract identifiers and resolve upstream. NETWORK ONLY — no DB access.

    Never raises: an upstream failure comes back as a `ResolvedCapture` with
    ``error`` set, which :func:`persist_capture` turns into a retryable
    ``error`` outcome.
    """
    from alma.application.openalex_manual import resolve_work_for_ingest

    extracted = extract_identifiers(message)
    if extracted.is_empty():
        return ResolvedCapture(extracted=extracted, empty=True)

    try:
        normalized, match_source = resolve_work_for_ingest(
            openalex_id=extracted.openalex_id,
            doi=extracted.doi,
            link=extracted.url,
            title=extracted.title,
        )
    except Exception as exc:  # upstream down / transport failure
        logger.warning("Inbox capture upstream failure (%s): %s", message.channel, exc)
        return ResolvedCapture(
            extracted=extracted,
            error=f"Could not reach the metadata service: {exc}",
        )

    return ResolvedCapture(
        extracted=extracted, normalized=normalized, match_source=match_source
    )


def persist_capture(
    db: sqlite3.Connection,
    message: InboundMessage,
    resolved: ResolvedCapture,
) -> CaptureResult:
    """Land the resolved paper and park it in the Inbox. DB ONLY — no network.

    MUST run inside the caller's write window (`write_section` /
    `run_write_unit`). Both halves — the paper row and the `inbox_messages`
    ledger row — belong in the SAME window so a capture and its record commit
    together; a paper without its ledger row would be re-captured on the next
    sweep.

    Outcomes, all recorded, none raising:
      ``resolved``   paper is in the corpus at ``status='inbox'``
      ``duplicate``  already in your Library; left alone, nothing parked
      ``unresolved`` nothing upstream recognised it — kept for hand-fixing
      ``error``      resolution failed; retryable on a later sweep
    """
    from alma.application.feed import _upsert_candidate_paper
    from alma.application.paper_actions import LIBRARY_STATUS, promote_to_inbox
    from alma.openalex.client import _upsert_single_paper

    extracted = resolved.extracted
    if resolved.empty:
        return CaptureResult(
            outcome="unresolved",
            extracted=extracted,
            error="No DOI, arXiv id, OpenAlex id or link found in the message.",
        )
    if resolved.error:
        return CaptureResult(
            outcome="error", extracted=extracted, error=resolved.error
        )

    normalized = resolved.normalized
    match_source = resolved.match_source

    if normalized:
        paper_id = _upsert_single_paper(db, normalized)
        title = str(normalized.get("title") or "") or None
    else:
        # OpenAlex missed. Keep the capture if we have enough to make a row:
        # a DOI alone is a legitimate paper the enrichment chain can finish
        # later. Mirrors `save_online_search_result`'s candidate fallback.
        if not (extracted.doi or extracted.openalex_id):
            return CaptureResult(
                outcome="unresolved",
                extracted=extracted,
                error=(
                    "Could not identify a paper from this link. "
                    f"(tried: {match_source})"
                ),
            )
        paper_id = _upsert_candidate_paper(
            db,
            {
                "title": extracted.doi or extracted.url or "",
                "doi": extracted.doi,
                "openalex_id": extracted.openalex_id,
                "url": extracted.url,
                "source_api": message.channel,
            },
            now=utcnow().isoformat(),
        )
        title = None

    if not paper_id:
        return CaptureResult(
            outcome="unresolved",
            extracted=extracted,
            error="Resolved upstream but the paper could not be stored.",
        )

    paper_id = str(paper_id)
    row = db.execute(
        "SELECT status, title FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    current_status = str((row["status"] if row else "") or "").strip().lower()
    title = title or (str(row["title"]) if row and row["title"] else None)

    # Already saved: report it and leave the Library row untouched. The Inbox is
    # for papers awaiting a decision, and this one already has one.
    if current_status == LIBRARY_STATUS:
        return CaptureResult(
            outcome="duplicate",
            paper_id=paper_id,
            title=title,
            extracted=extracted,
        )

    promote_to_inbox(db, paper_id, source=message.channel)
    return CaptureResult(
        outcome="resolved",
        paper_id=paper_id,
        title=title,
        extracted=extracted,
    )


def capture_message(
    db: sqlite3.Connection,
    message: InboundMessage,
) -> CaptureResult:
    """Resolve and persist one message — the convenience form.

    Equivalent to :func:`resolve_message` followed by :func:`persist_capture`.
    The caller still owns the transaction. Production sweeps use the two-phase
    form directly so the network call happens strictly outside the write window
    (`services.inbox_sweep`); this wrapper is for callers that already hold no
    transaction and want one call.
    """
    return persist_capture(db, message, resolve_message(message))
