"""Citation-fabric scoring features for Discovery (task 47 §7, step 5).

Two graph-channel strengths, computed for a candidate against the user's
high-signal (loved / saved) set using ONLY local ``publication_references``
rows — no network, no per-candidate DB access. Both are precomputed once per
refresh as batched set intersections and handed to ``score_candidate`` via the
scoring context:

- **coupling_strength** — the candidate and a high-signal paper cite the *same
  works* (shared references / a shared past): bibliographic coupling.
- **cocitation_strength** — some corpus paper cites the candidate *together
  with* a high-signal paper (shared citers / a shared reception): co-citation.

Each raw count ``n`` is squashed to ``[0, 1)`` by ``n / (n + k)`` — a soft,
run-size-independent saturation (0 → 0, k → 0.5, ∞ → 1) rather than an
arbitrary hard cap. The map also carries the single best-matching high-signal
paper id per feature so the caller can resolve a title for the evidence string
("shares N references with …").

Id conventions (verified live 2026-07-25): ``publication_references.paper_id``
is the citing local paper id; ``referenced_work_id`` is the unprefixed numeric
OpenAlex work id; ``papers.openalex_id`` is the ``'W'`` + that number.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from typing import Any


def _chunk(seq: list[str], size: int = 500) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _refs_by_paper(
    db: sqlite3.Connection, paper_ids: list[str]
) -> dict[str, set[str]]:
    """paper_id → set of referenced_work_id (as str). Index-friendly (paper_id IN)."""
    out: dict[str, set[str]] = defaultdict(set)
    for chunk in _chunk(paper_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT paper_id, referenced_work_id
            FROM publication_references
            WHERE paper_id IN ({placeholders})
              AND TRIM(COALESCE(referenced_work_id, '')) <> ''
            """,
            chunk,
        ).fetchall()
        for r in rows:
            out[str(r[0])].add(str(r[1]))
    return out


def _work_ids(db: sqlite3.Connection, paper_ids: list[str]) -> dict[str, str]:
    """paper_id → unprefixed numeric OpenAlex work id (only canonical 'W…' rows)."""
    out: dict[str, str] = {}
    for chunk in _chunk(paper_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT id, openalex_id FROM papers
            WHERE id IN ({placeholders})
              AND TRIM(COALESCE(openalex_id, '')) <> ''
            """,
            chunk,
        ).fetchall()
        for r in rows:
            oa = str(r[1]).strip()
            if oa[:1] in ("W", "w") and oa[1:].isdigit():
                out[str(r[0])] = oa[1:]
    return out


def _citers_of(
    db: sqlite3.Connection, work_ids: list[str]
) -> dict[str, set[str]]:
    """referenced_work_id → set of citing paper_ids. Index-friendly (ref_id IN)."""
    out: dict[str, set[str]] = defaultdict(set)
    for chunk in _chunk(work_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT referenced_work_id, paper_id
            FROM publication_references
            WHERE referenced_work_id IN ({placeholders})
            """,
            [int(w) for w in chunk],
        ).fetchall()
        for r in rows:
            out[str(r[0])].add(str(r[1]))
    return out


def build_citation_fabric_maps(
    db: sqlite3.Connection,
    candidate_paper_ids: dict[str, str],
    positive_ids: list[str],
    *,
    title_lookup: dict[str, str] | None = None,
    coupling_saturation: float = 3.0,
    cocitation_saturation: float = 2.0,
) -> dict[str, dict[str, Any]]:
    """Return ``{scoring_key: {coupling_strength, cocitation_strength,
    coupling_count, cocitation_count, coupling_partner_id,
    cocitation_partner_id, coupling_partner_title, cocitation_partner_title}}``
    for every candidate that resolves to a local paper.

    ``candidate_paper_ids`` maps a scoring key → local paper id; ``positive_ids``
    is the high-signal (loved / saved) paper-id set. ``title_lookup`` (paper id →
    title) resolves the best-matching partner's title for the evidence string —
    single-owner here, so the UI never has to look it up. Returns ``{}`` when
    either side is empty or the references table is unavailable.
    """
    titles = title_lookup or {}
    if not candidate_paper_ids or not positive_ids:
        return {}

    pos_ids = [p for p in dict.fromkeys(positive_ids) if p]
    cand_pids = list(dict.fromkeys(candidate_paper_ids.values()))
    if not pos_ids or not cand_pids:
        return {}

    try:
        # --- Coupling: shared referenced works (candidate ∩ each positive) ---
        pos_refs = _refs_by_paper(db, pos_ids)          # positive pid → refs
        cand_refs = _refs_by_paper(db, cand_pids)       # candidate pid → refs
        pos_refs_all: set[str] = set()
        for s in pos_refs.values():
            pos_refs_all |= s

        # --- Co-citation: shared citers (papers citing candidate + a positive) ---
        cand_work = _work_ids(db, cand_pids)            # candidate pid → work id
        pos_work = _work_ids(db, pos_ids)               # positive pid → work id
        work_to_pos: dict[str, str] = {w: p for p, w in pos_work.items()}
        pos_citers = _citers_of(db, list(pos_work.values()))  # pos work → citers
        # citing paper → set of positive pids it cites (for partner attribution)
        citer_to_pos: dict[str, set[str]] = defaultdict(set)
        for work, citers in pos_citers.items():
            pp = work_to_pos.get(work)
            if pp is None:
                continue
            for c in citers:
                citer_to_pos[c].add(pp)
        citers_of_pos = set(citer_to_pos.keys())
        cand_citers = _citers_of(db, list(cand_work.values()))  # cand work → citers
    except sqlite3.OperationalError:
        return {}

    # Per distinct candidate paper id → feature dict.
    per_pid: dict[str, dict[str, Any]] = {}
    for pid in cand_pids:
        # Coupling count + best-matching positive paper.
        my_refs = cand_refs.get(pid, set())
        coupling_count = 0
        coupling_partner: str | None = None
        if my_refs and pos_refs_all:
            best = 0
            for ppid, prefs in pos_refs.items():
                if ppid == pid:
                    continue
                shared = len(my_refs & prefs)
                if shared > best:
                    best, coupling_partner = shared, ppid
            coupling_count = len(my_refs & pos_refs_all)

        # Co-citation count + best-matching positive paper.
        cocite_count = 0
        cocite_partner: str | None = None
        work = cand_work.get(pid)
        if work and citers_of_pos:
            shared_citers = cand_citers.get(work, set()) & citers_of_pos
            cocite_count = len(shared_citers)
            if shared_citers:
                tally: Counter[str] = Counter()
                for c in shared_citers:
                    for ppid in citer_to_pos.get(c, ()):  # positives co-cited by c
                        if ppid != pid:
                            tally[ppid] += 1
                if tally:
                    cocite_partner = tally.most_common(1)[0][0]

        per_pid[pid] = {
            "coupling_count": coupling_count,
            "cocitation_count": cocite_count,
            "coupling_strength": (
                coupling_count / (coupling_count + coupling_saturation)
                if coupling_count
                else 0.0
            ),
            "cocitation_strength": (
                cocite_count / (cocite_count + cocitation_saturation)
                if cocite_count
                else 0.0
            ),
            "coupling_partner_id": coupling_partner,
            "cocitation_partner_id": cocite_partner,
            "coupling_partner_title": titles.get(coupling_partner) if coupling_partner else None,
            "cocitation_partner_title": titles.get(cocite_partner) if cocite_partner else None,
        }

    # Fan out per-pid results to every scoring key that resolved to that pid.
    return {
        key: per_pid[pid]
        for key, pid in candidate_paper_ids.items()
        if pid in per_pid
    }
