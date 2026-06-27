"""Paper component classification — the part-of model (2026-06-27).

A *component* is a sub-part of a paper that a metadata source (OpenAlex /
Crossref) indexes under its OWN DOI but which is NOT a paper to read on its
own:

  - ``figure``        — a figure image (PLOS ``…/journal.pbio.NNNN.g001``)
  - ``supplementary`` — a supporting-information file / table
                        (PLOS ``…​.s001`` / ``.t001``; OpenAlex
                        ``supplementary-materials`` / ``other`` / ``paratext``)
  - ``peer_review``   — a peer-review or author-response document
                        (eLife ``…​.sa0``; OpenAlex ``peer-review``)
  - ``dataset``       — a data / code / stimuli deposit (OpenAlex ``dataset``)

Components stay in the corpus (provenance + discovery signal) but are hidden
from the Feed inbox and shown only inside their parent paper's popup. This
module is the single source of truth for "is this a paper, or a part of one?".

``classify_component`` is PURE (no DB, no network) so it can run at feed-ingest
time, where ``work_type`` may still be unhydrated and only the DOI is reliable.
``resolve_component`` layers the DB parent-row lookup on top so feed ingest and
the one-time backfill share one code path (DRY).

Distinct from ``papers.canonical_paper_id`` (dedup: "the same work indexed
twice"); this is *part-of*.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Optional, Tuple

from alma.core.utils import (
    clean_display_text,
    is_doi_shaped,
    normalize_doi,
    resolve_existing_paper_id,
)

# Component sub-types — also the persisted ``papers.component_type`` values.
COMPONENT_FIGURE = "figure"
COMPONENT_SUPPLEMENTARY = "supplementary"
COMPONENT_PEER_REVIEW = "peer_review"
COMPONENT_DATASET = "dataset"


def not_component_sql(alias: str = "p") -> str:
    """SQL predicate selecting standalone papers (NOT components).

    The single source of truth for the read-gate, shared by the Feed inbox and
    Discovery retrieval so a figure / SI / dataset never surfaces in either.
    ``alias`` is the ``papers`` table alias in the caller's query.
    """
    return f"COALESCE({alias}.component_type, '') = ''"

# DOI-suffix patterns: a component DOI is its parent's DOI plus a publisher
# suffix. These fire at ingest even before ``work_type`` is hydrated, and they
# yield the parent DOI deterministically (strip the suffix). ``.saN``
# (peer-review) is tried before ``.sN`` (supplementary); they don't actually
# overlap (``\.s\d`` needs a digit right after ``s``), but the order documents
# intent. Keep this list small and add publishers deliberately.
_SUFFIX_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\.sa\d{1,4}$", re.I), COMPONENT_PEER_REVIEW),   # eLife author response
    (re.compile(r"\.g\d{1,4}$", re.I), COMPONENT_FIGURE),         # PLOS figure
    (re.compile(r"\.s\d{1,4}$", re.I), COMPONENT_SUPPLEMENTARY),  # PLOS supporting info
    (re.compile(r"\.t\d{1,4}$", re.I), COMPONENT_SUPPLEMENTARY),  # PLOS table
)

# OpenAlex ``type`` values that are never a standalone paper. Used when the DOI
# suffix doesn't match but the work type HAS been hydrated. ``other`` is
# included deliberately: PLOS figures/SI without a recognised suffix surface as
# ``other``, and OpenAlex reserves ``other`` for non-standard outputs.
_WORK_TYPE_COMPONENT: dict[str, str] = {
    "dataset": COMPONENT_DATASET,
    "peer-review": COMPONENT_PEER_REVIEW,
    "supplementary-materials": COMPONENT_SUPPLEMENTARY,
    "paratext": COMPONENT_SUPPLEMENTARY,
    "other": COMPONENT_SUPPLEMENTARY,
}

# eLife reviewed-preprint DOIs carry a trailing version segment
# (``10.7554/elife.108223.2.sa0``); after stripping ``.sa0`` we drop a trailing
# ``.<n>`` so the parent resolves to the article DOI.
_TRAILING_VERSION_RE = re.compile(r"\.\d{1,3}$")


def classify_component(
    doi: Optional[str], work_type: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """Classify a work as a paper or a component.

    Returns ``(component_type, parent_doi)``:
      - ``(None, None)`` — a normal, standalone paper.
      - ``(component_type, parent_doi)`` — a component. ``parent_doi`` is the
        derived parent DOI when the DOI-suffix heuristic applies, else ``None``
        (work-type-only signal: still a component, but the parent must be
        linked via a relation or left unlinked).

    PURE: no DB / network.
    """
    norm = normalize_doi(doi)
    if norm:
        for pattern, component_type in _SUFFIX_PATTERNS:
            if pattern.search(norm):
                parent_doi = pattern.sub("", norm)
                if component_type == COMPONENT_PEER_REVIEW:
                    parent_doi = _TRAILING_VERSION_RE.sub("", parent_doi)
                # Only trust the suffix when what's left is still a real DOI, so
                # a coincidental ``.s12`` tail on a genuine article DOI can't
                # misclassify it.
                if is_doi_shaped(parent_doi):
                    return component_type, parent_doi

    wt = (work_type or "").strip().lower()
    if wt in _WORK_TYPE_COMPONENT:
        return _WORK_TYPE_COMPONENT[wt], None

    return None, None


def resolve_component(
    conn: sqlite3.Connection,
    *,
    doi: Optional[str],
    work_type: Optional[str] = None,
    relation_parent_doi: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Classify, then resolve the parent DOI to an existing ``papers.id``.

    Returns ``(component_type, parent_paper_id)``. ``component_type`` is set for
    every component (so the read-gate hides it regardless); ``parent_paper_id``
    is set only when the derived parent DOI is already a row in the corpus —
    otherwise it stays ``None`` (an orphan component, still hidden from the
    Feed). Shared by feed ingest and the one-time backfill.

    ``relation_parent_doi`` is the Phase-2 Crossref ``is-supplement-to`` signal
    (see ``alma.discovery.crossref.parent_doi_from_relation``): a third
    classification signal that both names a parent and — when nothing else
    classified the work — marks it ``supplementary``. This is what links
    standalone datasets (no derivable DOI suffix) to their paper.
    """
    component_type, parent_doi = classify_component(doi, work_type)
    rel_parent = normalize_doi(relation_parent_doi)
    if component_type is None and rel_parent:
        component_type = COMPONENT_SUPPLEMENTARY
    if component_type is None:
        return None, None
    parent_doi = parent_doi or rel_parent
    parent_paper_id: Optional[str] = None
    if parent_doi:
        parent_paper_id = resolve_existing_paper_id(conn, doi=parent_doi)
    return component_type, parent_paper_id


def link_orphan_components(
    conn: sqlite3.Connection, *, parent_paper_id: str, parent_doi: Optional[str]
) -> int:
    """Adopt unlinked DOI-suffix components of a just-upserted parent paper.

    Called when a paper that could be a parent enters the corpus, to catch the
    case where its figure / SI children were ingested FIRST (so they classified
    as components but had no parent row yet). Scans only the small orphan set
    (``component_type`` set, ``parent_paper_id`` NULL) and links those whose own
    DOI re-derives to this parent's DOI. Returns the number linked.

    Only suffix-derivable components are reconciled here (the child's DOI names
    its parent). Relation-linked components (datasets) are linked on their own
    Crossref hydration pass instead — they carry no parent pointer in their DOI.
    """
    pdoi = normalize_doi(parent_doi)
    if not pdoi:
        return 0
    rows = conn.execute(
        "SELECT id, doi FROM papers "
        "WHERE component_type IS NOT NULL AND parent_paper_id IS NULL "
        "AND doi IS NOT NULL"
    ).fetchall()
    linked = 0
    for row in rows:
        _, derived_parent = classify_component(row["doi"], None)
        if derived_parent and normalize_doi(derived_parent) == pdoi:
            conn.execute(
                "UPDATE papers SET parent_paper_id = ? WHERE id = ?",
                (parent_paper_id, row["id"]),
            )
            linked += 1
    return linked


def backfill_components(conn: sqlite3.Connection) -> dict:
    """One-time reconcile of existing rows into the part-of model.

    For every paper: (1) classify it (set ``component_type`` /
    ``parent_paper_id`` when still NULL — never churns an already-set value),
    and (2) HTML-clean a legacy title that predates the write-layer
    ``clean_display_text``. Idempotent and safe to re-run.

    Caller owns the transaction — wrap in ``run_write_unit`` / ``write_section``
    (no commit here). Returns counts for logging/verification.
    """
    rows = conn.execute(
        "SELECT id, doi, work_type, title, parent_paper_id, component_type FROM papers"
    ).fetchall()
    classified = linked = cleaned = 0
    for row in rows:
        sets: list[str] = []
        params: list[object] = []

        if row["component_type"] is None:
            component_type, parent_paper_id = resolve_component(
                conn, doi=row["doi"], work_type=row["work_type"]
            )
            if component_type is not None:
                sets.append("component_type = ?")
                params.append(component_type)
                classified += 1
                if parent_paper_id and row["parent_paper_id"] is None:
                    sets.append("parent_paper_id = ?")
                    params.append(parent_paper_id)
                    linked += 1
        elif row["parent_paper_id"] is None:
            # Already classified but unlinked — a suffix-orphan whose parent may
            # have entered the corpus since. Re-derive the parent from its own
            # DOI and link if present (relation-orphans link on their rehydrate).
            _, parent_doi = classify_component(row["doi"], row["work_type"])
            if parent_doi:
                parent_paper_id = resolve_existing_paper_id(conn, doi=parent_doi)
                if parent_paper_id:
                    sets.append("parent_paper_id = ?")
                    params.append(parent_paper_id)
                    linked += 1

        title = row["title"]
        if title:
            cleaned_title = clean_display_text(title)
            if cleaned_title and cleaned_title != title:
                sets.append("title = ?")
                params.append(cleaned_title)
                cleaned += 1

        if sets:
            params.append(row["id"])
            conn.execute(f"UPDATE papers SET {', '.join(sets)} WHERE id = ?", params)

    return {"scanned": len(rows), "classified": classified, "linked": linked, "cleaned": cleaned}
