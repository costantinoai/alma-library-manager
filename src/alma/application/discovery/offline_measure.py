"""Measure corpus papers through the REAL scoring path, outside a refresh.

Two callers need "what would the scorer read for these papers under this
taste profile": the calibration build (a random corpus sample, to derive the
percentile tables and family priors) and the outcome evaluation (papers the
user later judged, under a profile built only from what they had kept before).
Both must go through ``measure_candidate`` + ``build_feature_snapshot`` — the
path a lens refresh uses — or they would be measuring a scorer that does not
exist. This module is that one path.

Offline means no retrieval happened: a corpus paper has no channel evidence
and no citation-fabric entry, so the ``retrieval`` family and the graph atoms
of ``citation`` are unavailable here by construction, not by omission.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from alma.core.sql_helpers import standalone_paper_sql


@dataclass(frozen=True)
class ProfileInputs:
    """Everything ``measure_candidate`` needs about the user's taste."""

    profile: dict
    settings: dict
    positive_centroid: Any
    negative_centroid: Any
    positive_texts: list[str]
    negative_texts: list[str]
    exemplars: list
    lexical_profile: Any
    model: str


def build_profile_inputs(
    conn: sqlite3.Connection,
    positive_pubs: list[dict],
    negative_pubs: list[dict],
    *,
    scope_paper_ids: set[str] | None = None,
) -> ProfileInputs:
    """The taste profile and its embedding/lexical companions, one owner.

    ``scope_paper_ids`` is ``compute_preference_profile``'s own scoping rule
    (collection topics, tags and the followed-author prior only count papers
    in the set) — the evaluation passes the profile's papers so signals from
    later-judged papers cannot leak in through those aggregates.
    """
    from alma.application.discovery.lens_crud import read_settings
    from alma.discovery import similarity as sim
    from alma.discovery.scoring import compute_preference_profile

    settings = read_settings(conn)
    positive_texts = [t for t in (sim.build_similarity_text(p, conn=conn) for p in positive_pubs) if t]
    negative_texts = [t for t in (sim.build_similarity_text(p, conn=conn) for p in negative_pubs) if t]
    return ProfileInputs(
        profile=compute_preference_profile(
            conn, positive_pubs, negative_pubs, settings, scope_paper_ids=scope_paper_ids
        ),
        settings=settings,
        positive_centroid=sim.compute_embedding_centroid(positive_pubs, conn),
        negative_centroid=sim.compute_embedding_centroid(negative_pubs, conn) if negative_pubs else None,
        positive_texts=positive_texts,
        negative_texts=negative_texts,
        exemplars=sim.load_publication_example_embeddings(positive_pubs, conn, limit=12),
        lexical_profile=sim.build_lexical_profile(positive_texts, negative_texts) if positive_texts else None,
        model=sim.get_active_embedding_model(conn),
    )


def measure_corpus_papers(
    conn: sqlite3.Connection,
    paper_ids: Sequence[str],
    inputs: ProfileInputs,
    *,
    calibration: Any,
) -> Iterator[tuple[str, dict, dict]]:
    """Yield ``(paper_id, score_breakdown, reward_features)`` per paper.

    Papers are read with the local-candidate projection the lexical and graph
    retrieval lanes use, so an offline candidate carries exactly the fields a
    locally retrieved one would.
    """
    from alma.application.discovery.features import build_feature_snapshot
    from alma.application.discovery.retrieval._common import (
        LOCAL_PAPER_SELECT,
        local_paper_candidate,
    )
    from alma.core.vector_blob import decode_vector
    from alma.discovery.scoring import measure_candidate

    ids = list(paper_ids)
    for start in range(0, len(ids), 200):
        chunk = ids[start : start + 200]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT {LOCAL_PAPER_SELECT} FROM papers WHERE {standalone_paper_sql('papers')} AND id IN ({placeholders})", chunk
        ).fetchall()
        vectors = {
            r["paper_id"]: decode_vector(r["embedding"])
            for r in conn.execute(
                f"SELECT paper_id, embedding FROM publication_embeddings "
                f"WHERE model = ? AND paper_id IN ({placeholders})",
                (inputs.model, *chunk),
            )
        }
        for row in rows:
            candidate = local_paper_candidate(row)
            breakdown = measure_candidate(
                candidate,
                inputs.profile,
                inputs.positive_centroid,
                inputs.negative_centroid,
                inputs.positive_texts,
                inputs.negative_texts,
                conn,
                inputs.settings,
                candidate_embedding=vectors.get(candidate["paper_id"]),
                lexical_profile=inputs.lexical_profile,
                positive_example_embeddings=inputs.exemplars,
                calibration=calibration,
            )
            candidate["score_breakdown"] = breakdown
            yield candidate["paper_id"], breakdown, build_feature_snapshot(candidate)[0]
