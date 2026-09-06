"""Per-library scoring calibration — derived from THIS corpus, never hardcoded.

A cosine, an overlap fraction or a family value has no useful absolute scale:
measured over one corpus, similarity to the Library ran 0.785..0.956 between
the 1st and 99th percentiles, and a different library on a different field
will sit somewhere else entirely. So every calibrated input is read as its
**percentile in this install's own corpus**, and every family's imputation
prior is **what a random paper from this corpus scores** — both measured here,
stored as the ``scoring:calibration`` materialized view, and handed to the
ranker as a value object.

How it is built (:func:`build_scoring_calibration`): a seeded random sample of
corpus papers is scored through the real measurement path with the real
preference profile — the same code a lens refresh runs — and the raw inputs and
family values are collected. Two things fall out:

* ``cdf`` — per calibrated input, its quantile table (raw value → percentile),
  the similarity-to-centroid one taken over EVERY corpus vector so the top tail
  (where a Discovery deck lives) keeps its order;
* ``prior_mean`` — each family's mean value under that calibration.

Never measured on a Discovery deck: retrieval already selected the deck for the
very quantities being measured, and a prior taken from it would be biased in
exactly the direction that inflates missing-evidence papers.

How it is used: loaded ONCE per scoring pass (:func:`load_calibration` — a pure
row read — or :func:`ensure_calibration` inside a background job, which also
builds/refreshes it) and passed explicitly to ``measure_candidate`` and the
ranker, the same way the Signal Lab context travels. Every ranking snapshot
records the tables it was scored with, so a replay is exact even after the
calibration has been rebuilt.

Until an install has been measured (no library, no vectors, first ever refresh)
:data:`UNCALIBRATED` applies: inputs are read raw and families impute at their
declared fallback priors. The explanation says so (``calibration`` is ``None``).
"""

from __future__ import annotations

import logging
import random
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from alma.ai.graph_versions import with_version
from alma.application import materialized_views as mv
from alma.core.scoring_math import interpolate_calibration

logger = logging.getLogger(__name__)

CALIBRATION_VIEW_KEY = "scoring:calibration"

# Bump when the build logic or the set of calibrated inputs changes: the stored
# row must be rebuilt even though the corpus did not move.
CALIBRATION_VERSION = "2026.09-2"

#: The quantile grid every CDF table is sampled on. Dense at the top because a
#: Discovery deck is drawn from the top percent of the corpus.
_QUANTILES = (0.5, 1, 5, 10, 25, 50, 75, 90, 95, 99, 99.5, 99.9)

#: Inputs read through a CDF table, and the raw breakdown key each is measured
#: from. Anything not listed here is read raw.
CALIBRATED_INPUTS: tuple[str, ...] = (
    "semantic_similarity_centroid_raw",
    "semantic_similarity_exemplar_raw",
    "semantic_similarity_negative_raw",
    "semantic_similarity_raw",
    "lexical_similarity_word_raw",
    "lexical_similarity_char_raw",
    "lexical_similarity_term_raw",
    "lexical_similarity_negative_penalty",
    "lexical_similarity_raw",
)

#: Inputs whose own corpus distribution cannot be measured (a negative centroid
#: exists only once something was rated down) borrow another input's table.
_TABLE_ALIASES = {
    "semantic_similarity_negative_raw": "semantic_similarity_centroid_raw",
    "lexical_similarity_negative_penalty": "lexical_similarity_word_raw",
}

#: How many random corpus papers the build scores. Enough for a stable median
#: and 99th percentile of every family; the similarity tail uses all vectors.
SAMPLE_SIZE = 600
_MIN_LIBRARY = 3
_MIN_SAMPLE = 30


def _clip(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


@dataclass(frozen=True)
class ScoringCalibration:
    """What the ranker and measurer read inputs through. Immutable, explicit."""

    cdf: dict[str, tuple[tuple[float, float], ...]] = field(default_factory=dict)
    prior_mean: dict[str, float] = field(default_factory=dict)
    #: Provenance of the stored artifact; ``None`` means the uncalibrated fallback.
    generation: dict[str, Any] | None = None

    @property
    def calibrated(self) -> bool:
        return self.generation is not None and bool(self.cdf)

    def table(self, key: str) -> tuple[tuple[float, float], ...] | None:
        return self.cdf.get(key) or self.cdf.get(_TABLE_ALIASES.get(key, ""))

    def read(self, key: str, raw: float) -> float:
        """Raw input → its corpus percentile, or the clipped raw value when no
        table exists (honest: uncalibrated, not silently rescaled)."""

        table = self.table(key)
        if not table:
            return _clip(raw)
        return _clip(interpolate_calibration(_clip(raw), table))

    def prior(self, family: str, declared: float) -> float:
        return float(self.prior_mean.get(family, declared))

    def as_dict(self) -> dict[str, Any]:
        return {
            "cdf": {k: [list(p) for p in v] for k, v in self.cdf.items()},
            "prior_mean": dict(self.prior_mean),
            "generation": self.generation,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ScoringCalibration:
        if not data:
            return UNCALIBRATED
        cdf = {
            str(k): tuple((float(x), float(y)) for x, y in v)
            for k, v in (data.get("cdf") or {}).items()
        }
        return cls(
            cdf=cdf,
            prior_mean={str(k): float(v) for k, v in (data.get("prior_mean") or {}).items()},
            generation=data.get("generation"),
        )

    @classmethod
    def from_samples(
        cls,
        samples: dict[str, list[float]],
        prior_mean: dict[str, float],
        generation: dict[str, Any] | None,
    ) -> ScoringCalibration:
        """Build the tables from raw samples. Also the seam tests use."""

        return cls(
            cdf={key: quantile_table(values) for key, values in samples.items() if values},
            prior_mean=dict(prior_mean),
            generation=generation,
        )


UNCALIBRATED = ScoringCalibration()


def quantile_table(values: list[float]) -> tuple[tuple[float, float], ...]:
    """Raw value → percentile, as a monotone piecewise-linear table."""

    ordered = sorted(_clip(v) for v in values)
    n = len(ordered)
    points: list[tuple[float, float]] = [(0.0, 0.0)]
    for q in _QUANTILES:
        x = ordered[min(n - 1, int(n * q / 100.0))]
        y = q / 100.0
        # Keep it a function: a repeated x (ties at the floor) keeps the LAST y.
        if points and x <= points[-1][0]:
            points[-1] = (points[-1][0], max(points[-1][1], y))
        else:
            points.append((x, y))
    if points[-1][0] < 1.0:
        points.append((1.0, 1.0))
    return tuple(points)


# ── build ──────────────────────────────────────────────────────────────────


def build_scoring_calibration(conn: sqlite3.Connection) -> dict[str, Any]:
    """Score a random corpus sample through the REAL measurement path and
    derive the tables. Runs inside the materialized-view job, never on a GET."""

    import numpy as np

    from alma.application.discovery import load_library_preference_inputs
    from alma.application.discovery.features import build_feature_snapshot
    from alma.application.discovery.lens_crud import read_settings
    from alma.application.discovery.ranker import FAMILY_SPECS, _family_reading
    from alma.application.discovery.retrieval._common import (
        LOCAL_PAPER_SELECT,
        local_paper_candidate,
    )
    from alma.core.vector_blob import decode_vector
    from alma.discovery import similarity as sim
    from alma.discovery.scoring import compute_preference_profile, measure_candidate

    settings = read_settings(conn)
    model = sim.get_active_embedding_model(conn)

    _, positive_pubs, negative_pubs = load_library_preference_inputs(conn)
    if len(positive_pubs) < _MIN_LIBRARY:
        return {"ready": False, "reason": "library too small to measure against"}

    ids = [
        r["id"]
        for r in conn.execute(
            """SELECT p.id FROM papers p JOIN publication_embeddings pe ON pe.paper_id = p.id
               WHERE p.status NOT IN ('library', 'dismissed', 'removed') AND pe.model = ?""",
            (model,),
        )
    ]
    if len(ids) < _MIN_SAMPLE:
        return {"ready": False, "reason": "corpus has too few embedded papers to measure"}

    profile = compute_preference_profile(conn, positive_pubs, negative_pubs, settings)
    positive_texts = [t for t in (sim.build_similarity_text(p, conn=conn) for p in positive_pubs) if t]
    negative_texts = [t for t in (sim.build_similarity_text(p, conn=conn) for p in negative_pubs) if t]
    positive_centroid = sim.compute_embedding_centroid(positive_pubs, conn)
    negative_centroid = sim.compute_embedding_centroid(negative_pubs, conn) if negative_pubs else None
    exemplars = sim.load_publication_example_embeddings(positive_pubs, conn, limit=12)
    lexical_profile = sim.build_lexical_profile(positive_texts, negative_texts) if positive_texts else None

    # Seeded, so the same corpus yields the same tables: the fingerprint says
    # WHEN to rebuild, determinism says the rebuild means something changed.
    rng = random.Random(f"{len(ids)}:{model}")
    sample_ids = ids[:]
    rng.shuffle(sample_ids)
    sample_ids = sample_ids[:SAMPLE_SIZE]

    def measure_all(calibration: ScoringCalibration) -> tuple[dict[str, list[float]], list[dict]]:
        """Score the sample through the real path under ``calibration``."""

        raw_samples: dict[str, list[float]] = {key: [] for key in CALIBRATED_INPUTS}
        rewards: list[dict] = []
        for start in range(0, len(sample_ids), 200):
            chunk = sample_ids[start : start + 200]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT {LOCAL_PAPER_SELECT} FROM papers WHERE id IN ({placeholders})", chunk
            ).fetchall()
            vectors = {
                r["paper_id"]: decode_vector(r["embedding"])
                for r in conn.execute(
                    f"SELECT paper_id, embedding FROM publication_embeddings "
                    f"WHERE model = ? AND paper_id IN ({placeholders})",
                    (model, *chunk),
                )
            }
            for row in rows:
                candidate = local_paper_candidate(row)
                breakdown = measure_candidate(
                    candidate, profile, positive_centroid, negative_centroid,
                    positive_texts, negative_texts, conn, settings,
                    candidate_embedding=vectors.get(candidate["paper_id"]),
                    lexical_profile=lexical_profile,
                    positive_example_embeddings=exemplars,
                    calibration=calibration,
                )
                candidate["score_breakdown"] = breakdown
                if breakdown.get("candidate_embedding_ready"):
                    for key in CALIBRATED_INPUTS:
                        if key in breakdown and key not in _TABLE_ALIASES:
                            raw_samples[key].append(float(breakdown[key] or 0.0))
                rewards.append(build_feature_snapshot(candidate)[0])
        return raw_samples, rewards

    # Pass 1, uncalibrated: the RAW distributions the tables are built from.
    raw_samples, _ = measure_all(UNCALIBRATED)

    # The similarity-to-centroid table from EVERY corpus vector: the sample's
    # 99.9th percentile is one paper, the corpus's is dozens.
    if positive_centroid is not None:
        centroid = np.asarray(positive_centroid, dtype=np.float32)
        centroid = centroid / (np.linalg.norm(centroid) or 1.0)
        cosines: list[float] = []
        for start in range(0, len(ids), 2000):
            chunk = ids[start : start + 2000]
            placeholders = ",".join("?" for _ in chunk)
            for r in conn.execute(
                f"SELECT embedding FROM publication_embeddings WHERE model = ? AND paper_id IN ({placeholders})",
                (model, *chunk),
            ):
                v = decode_vector(r["embedding"]).astype(np.float32)
                norm = float(np.linalg.norm(v))
                if norm > 0:
                    cosines.append(float(v @ centroid / norm))
        if cosines:
            raw_samples["semantic_similarity_centroid_raw"] = cosines

    tables = ScoringCalibration.from_samples(
        {k: v for k, v in raw_samples.items() if v}, prior_mean={}, generation={}
    )
    # Pass 2, under the derived tables: priors must be the family values AS
    # SCORING WILL READ THEM. The feedback family calibrates its cosine inside
    # `measure_candidate`, so a prior taken from the raw pass would be for a
    # reading the ranker never sees (measured: 0.93 raw against 0.5 calibrated).
    _, rewards = measure_all(tables)
    prior_mean: dict[str, float] = {}
    for spec in FAMILY_SPECS:
        values = [
            value
            for value, available, _ in (_family_reading(r, spec, tables) for r in rewards)
            if available
        ]
        if values:
            prior_mean[spec.key] = round(sum(values) / len(values), 4)

    return {
        "ready": True,
        "cdf": {k: [list(p) for p in v] for k, v in tables.cdf.items()},
        "prior_mean": prior_mean,
        "sample": {
            "papers_scored": len(rewards),
            "corpus_vectors": len(ids),
            "library_papers": len(positive_pubs),
            "embedding_model": model,
            "version": CALIBRATION_VERSION,
        },
    }


# When to rebuild: when the corpus or the Library changed SIGNIFICANTLY, not on
# every save. The build scores hundreds of papers, so a fingerprint that moved
# on each new vector would refit constantly for a table that would not change.
# Each input is bucketed to a step that plausibly shifts a percentile table:
# the Library by 5 papers, the embedding set by 250 vectors or a new day of
# writes, feedback by 10 events, plus the model and the build version. A
# rebuild is also deduped and served-stale-meanwhile by the view layer.
_FINGERPRINT_SQL = with_version(
    """
    SELECT (SELECT COUNT(*) / 250 FROM publication_embeddings),
           (SELECT COALESCE(SUBSTR(MAX(created_at), 1, 10), '') FROM publication_embeddings),
           (SELECT COALESCE(value, '') FROM discovery_settings WHERE key = 'embedding_model'),
           (SELECT COUNT(*) / 5 FROM papers WHERE status = 'library'),
           (SELECT COUNT(*) / 10 FROM feedback_events)
    """,
    CALIBRATION_VERSION,
)

mv.register(
    mv.View(
        key=CALIBRATION_VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=build_scoring_calibration,
        operation_key="materialize.scoring.calibration",
    )
)


# ── consume ────────────────────────────────────────────────────────────────


def _from_envelope(envelope: dict[str, Any] | None) -> ScoringCalibration:
    payload = (envelope or {}).get("payload") or {}
    if not payload.get("ready"):
        return UNCALIBRATED
    return ScoringCalibration.from_dict(
        {
            "cdf": payload.get("cdf"),
            "prior_mean": payload.get("prior_mean"),
            "generation": {
                "fingerprint": envelope.get("fingerprint"),
                "computed_at": envelope.get("computed_at"),
                **(payload.get("sample") or {}),
            },
        }
    )


def load_calibration(conn: sqlite3.Connection) -> ScoringCalibration:
    """The stored calibration, or the uncalibrated fallback. Pure row read —
    safe on any request path; never builds."""

    return _from_envelope(mv.get_stored(conn, CALIBRATION_VIEW_KEY))


def ensure_calibration(conn: sqlite3.Connection) -> ScoringCalibration:
    """For background jobs: the fresh calibration, built now if it has never
    been, refreshed in the background if its inputs moved (the previous one is
    served meanwhile)."""

    try:
        return _from_envelope(mv.get(conn, CALIBRATION_VIEW_KEY))
    except Exception as exc:  # noqa: BLE001 — scoring must not die on a calibration hiccup
        logger.warning("scoring calibration unavailable, scoring uncalibrated: %s", exc)
        return load_calibration(conn)
