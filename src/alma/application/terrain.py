"""terrain — the preference field over the corpus substrate, as a FIELD.

What changed and why
--------------------

Until 2026-07-27 the terrain was a scatter of labels rendered as if it were a
landscape. Each point looked up its own signals through
:mod:`alma.core.signal_valence` and nothing else: 96.9% of the 9,736 substrate
points carried no signal at all and were drawn at exactly 0.0. A paper sitting
in the dead centre of forty loved papers rendered neutral, because it
personally had no label. That is a scatter plot with a blur applied, not a
field — "what sits here" was answered only where you had already answered it.

This module adds the missing half: a **spatial model**. Observed labels stay
exactly as they are (the hierarchy in ``signal_valence`` is untouched and is
still the only owner of what a signal is worth). Where a paper has no label,
the field *predicts* one from its neighbourhood in embedding space, and reports
how much it trusts that prediction.

The method, and why this one
----------------------------

**Gaussian-process regression (kernel ridge) over a cosine kernel.**

- **Fitted in 768-d SPECTER2 space, never in the 2-D UMAP projection.** UMAP
  distorts distance by construction — two adjacent dots can be semantically far
  apart, and a model fitted on the picture would learn the projection's
  artifacts. Inference happens in embedding space; only the *rendering* uses
  the 2-D coordinates.
- **Kernel, not plain ridge.** A linear head on 768 dimensions with ~350 labels
  is p≫n, the same trap the ranker was in. It also cannot express "I like A and
  C but not the B between them", which is the normal shape of a research
  interest. The kernel form has one effective parameter per label, not per
  dimension.
- **Tractable.** ``n`` labels is a few hundred, so the ``O(n³)`` solve is
  ~4×10⁷ operations — milliseconds. Prediction over the whole substrate is two
  matrix products.
- **Uncertainty is free.** The GP posterior variance falls straight out of the
  same inverse, which is the whole reason to prefer it to label propagation:
  "+0.7 inferred from forty nearby labels" and "+0.7 inferred from one label
  three clusters away" must not render identically, and only a model with a
  variance can tell them apart.

What is deliberately NOT in here
--------------------------------

- **Engine evidence does not train the field.** A recommendation score is the
  ranker's own output; the ranker reads terrain-adjacent features. Letting
  ``rec_score`` fit the field, then letting the field feed ranking, is a closed
  loop that would converge on its own opinion. Engine evidence still *renders*
  where it exists (unchanged from before), at its own reduced confidence, but
  the fit sees user evidence only.
- **Author / venue / topic affinity are not label sources.** Those are ranking
  signals. Terrain answers "what sits at this place in the literature"; a
  second recommender wearing a colour ramp is not that.
- **PPR proximity is not a label source.** It is derived from Library
  membership, which is already a label — adding it would count the same
  evidence twice with a different shape.

Geometry contract
-----------------

This is a **read-time tint** (``CLAUDE.md`` → "Geometry is corpus-intrinsic").
Nothing here writes a coordinate, a cluster id, or a community assignment; the
substrate is read and never modified. The module must stay OUT of the layout
builders' import closure, which ``tests/test_geometry_admission_contract.py``
checks by import graph.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from alma.core.scope import Scope
from alma.core.signal_valence import (
    NEGATIVE_REC_ACTIONS,
    VALENCE_NO_SIGNAL,
    ValenceSource,
    paper_valence_evidence,
)
from alma.core.time import utcnow

if TYPE_CHECKING:  # pragma: no cover — typing only
    import numpy as np

logger = logging.getLogger(__name__)

SUBSTRATE_SCOPE = str(Scope.corpus)

# ── The model's constants (named so they are greppable and tunable in ONE place)

LABEL_NOISE = 0.15
"""Ridge term ``λ`` — how much the fit distrusts a single label.

Human preference labels are noisy: the same paper on a different day can be a 4
or a 5, and a save is not a considered verdict. A small λ would interpolate
every label exactly and paint a large confident region from one click. This
value shrinks the fit so agreement between neighbours is what produces a strong
colour."""

BANDWIDTH_NEIGHBORS = 5
"""``k`` for the adaptive length-scale.

The kernel width is set to the median cosine distance from a label to its k-th
nearest fellow label — the scale at which your own evidence is actually dense.
A fixed global width would be wrong on both ends: too wide and one rating
stains a whole cluster, too narrow and nothing generalises at all. Deriving it
from the label geometry means the field adapts as the library grows."""

MIN_BANDWIDTH = 0.02
MAX_BANDWIDTH = 0.60
"""Bounds on the derived length-scale.

Degenerate label sets (five near-duplicates; one paper from each of five
fields) would otherwise produce a width that is effectively zero or effectively
infinite, and in both cases the prediction stops meaning anything."""

MIN_LABELS_TO_FIT = 12
"""Below this the field predicts nothing and says so.

A handful of labels cannot support a length-scale estimate, let alone a
posterior. Showing an unfitted field as flat-neutral is honest; showing a
confident-looking one from eight labels is not."""

EVIDENCE_HALF_LIFE_DAYS = 540.0
"""Age at which a dated user signal counts half.

Interests move. Eighteen months is long enough that a still-relevant paper is
not discounted for being old, short enough that an abandoned direction fades
rather than anchoring the map forever. Applied ONLY where a real timestamp
exists — a label with no recorded date is not decayed, because inventing one
would be exactly the fabricated-timestamp bug this codebase already has a rule
against."""

CONFIDENCE_PREDICTED_FLOOR = 0.0
CONFIDENCE_OBSERVED = 1.0
"""Confidence reported for a directly observed user signal: total. You said it."""


@dataclass(frozen=True)
class TerrainPoint:
    """One substrate point's colour and how much it is to be believed."""

    paper_id: str
    cluster_id: int
    x: float
    y: float
    value: float
    confidence: float
    source: ValenceSource | str
    rec_score: float | None

    @property
    def is_predicted(self) -> bool:
        return self.source == "predicted"


@dataclass(frozen=True)
class TerrainModelInfo:
    """What the field was fitted from — the reader's right to know."""

    fitted: bool
    n_labels: int
    n_observed: int
    n_predicted: int
    n_unknown: int
    bandwidth: float | None
    noise: float
    reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "fitted": self.fitted,
            "n_labels": self.n_labels,
            "n_observed": self.n_observed,
            "n_predicted": self.n_predicted,
            "n_unknown": self.n_unknown,
            "bandwidth": round(self.bandwidth, 4) if self.bandwidth is not None else None,
            "noise": self.noise,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class TerrainField:
    points: list[TerrainPoint]
    model: TerrainModelInfo


@dataclass(frozen=True)
class _SubstrateRow:
    """One row of the substrate join, before any modelling."""

    paper_id: str
    cluster_id: int
    x: float
    y: float
    value: float | None
    source: ValenceSource | None
    weight: float
    rec_score: float | None


def build_terrain_field(conn: sqlite3.Connection) -> TerrainField:
    """The whole field: observed labels, plus a prediction wherever there is none.

    Pure read. Every substrate point comes back with a value, a confidence and
    the name of where the value came from, so a caller can render prediction
    and observation differently without re-deriving which is which.
    """
    rows = _load_substrate(conn)
    if not rows:
        return TerrainField(
            points=[],
            model=TerrainModelInfo(False, 0, 0, 0, 0, None, LABEL_NOISE, "empty substrate"),
        )

    observed = [row for row in rows if row.value is not None]
    unlabelled = [row for row in rows if row.value is None]

    # Only USER evidence trains the field (see module docstring: engine
    # evidence would close the loop between ranking and terrain).
    trainable = [row for row in observed if row.source is not None and row.source != "engine"]

    prediction, model = _fit_and_predict(
        conn,
        labels=trainable,
        targets=unlabelled,
    )

    points: list[TerrainPoint] = []
    n_predicted = 0
    n_unknown = 0
    for row in rows:
        if row.value is not None:
            points.append(
                TerrainPoint(
                    paper_id=row.paper_id,
                    cluster_id=row.cluster_id,
                    x=row.x,
                    y=row.y,
                    value=row.value,
                    # An observed value's confidence is the evidence weight the
                    # valence owner already assigns it: a rating is total, an
                    # engine guess is a quarter of one.
                    confidence=min(CONFIDENCE_OBSERVED, row.weight),
                    source=row.source or "unknown",
                    rec_score=row.rec_score,
                )
            )
            continue
        guess = prediction.get(row.paper_id)
        if guess is None:
            n_unknown += 1
            points.append(
                TerrainPoint(
                    paper_id=row.paper_id,
                    cluster_id=row.cluster_id,
                    x=row.x,
                    y=row.y,
                    value=VALENCE_NO_SIGNAL,
                    confidence=CONFIDENCE_PREDICTED_FLOOR,
                    source="unknown",
                    rec_score=row.rec_score,
                )
            )
            continue
        value, confidence = guess
        n_predicted += 1
        points.append(
            TerrainPoint(
                paper_id=row.paper_id,
                cluster_id=row.cluster_id,
                x=row.x,
                y=row.y,
                value=value,
                confidence=confidence,
                source="predicted",
                rec_score=row.rec_score,
            )
        )

    return TerrainField(
        points=points,
        model=TerrainModelInfo(
            fitted=model.fitted,
            n_labels=model.n_labels,
            n_observed=len(observed),
            n_predicted=n_predicted,
            n_unknown=n_unknown,
            bandwidth=model.bandwidth,
            noise=model.noise,
            reason=model.reason,
        ),
    )


def _load_substrate(conn: sqlite3.Connection) -> list[_SubstrateRow]:
    """Substrate coordinates joined to every signal the valence owner reads.

    One query. `n_engagements` is the newest of those: `external_link_click`
    rows in `feedback_events` were previously read by nothing.

    Deliberately NOT wrapped in a missing-table guard. The route this feeds
    used to swallow `OperationalError` and return an empty field, which meant a
    schema problem rendered as "you have no opinions yet" — indistinguishable
    from the truth, on the surface whose entire job is to show your opinions.
    Migrations guarantee the shape; if they have not run, that should be loud
    (CLAUDE.md → forward-only code, decoupled validators + migrators).
    """
    neg_actions_sql = ",".join(f"'{action}'" for action in NEGATIVE_REC_ACTIONS)
    rows = conn.execute(
        f"""
        SELECT pc.paper_id, pc.cluster_id, pc.x, pc.y, p.status,
               COALESCE(p.rating, 0) AS rating,
               latest.score AS rec_score,
               COALESCE(neg.n_neg, 0) AS n_neg,
               COALESCE(clicks.n_click, 0) AS n_click,
               COALESCE(evidence.last_seen, p.added_at) AS evidence_at
        FROM publication_clusters pc
        JOIN papers p ON p.id = pc.paper_id
        LEFT JOIN (
            SELECT paper_id, score, MAX(created_at)
            FROM recommendations GROUP BY paper_id
        ) latest ON latest.paper_id = pc.paper_id
        LEFT JOIN (
            SELECT paper_id, COUNT(*) AS n_neg
            FROM recommendations
            WHERE COALESCE(user_action, '') IN ({neg_actions_sql})
            GROUP BY paper_id
        ) neg ON neg.paper_id = pc.paper_id
        LEFT JOIN (
            SELECT entity_id, COUNT(*) AS n_click
            FROM feedback_events
            WHERE event_type = 'external_link_click' AND entity_type = 'publication'
            GROUP BY entity_id
        ) clicks ON clicks.entity_id = pc.paper_id
        LEFT JOIN (
            SELECT entity_id, MAX(created_at) AS last_seen
            FROM feedback_events
            WHERE entity_type = 'publication'
            GROUP BY entity_id
        ) evidence ON evidence.entity_id = pc.paper_id
        WHERE pc.scope = ?
        """,
        (SUBSTRATE_SCOPE,),
    ).fetchall()

    now = utcnow()
    out: list[_SubstrateRow] = []
    for row in rows:
        evidence = paper_valence_evidence(
            status=str(row["status"] or ""),
            rating=int(row["rating"] or 0),
            n_negative_actions=int(row["n_neg"] or 0),
            n_engagements=int(row["n_click"] or 0),
            rec_score=row["rec_score"],
        )
        decay = _recency_weight(row["evidence_at"], now=now)
        out.append(
            _SubstrateRow(
                paper_id=str(row["paper_id"]),
                cluster_id=int(row["cluster_id"] or 0),
                x=float(row["x"]),
                y=float(row["y"]),
                value=evidence.value if evidence else None,
                source=evidence.source if evidence else None,
                weight=(evidence.weight * decay) if evidence else 0.0,
                rec_score=row["rec_score"] if row["rec_score"] is not None else None,
            )
        )
    return out


def _recency_weight(recorded_at: object, *, now: datetime) -> float:
    """Exponential decay on a REAL timestamp; 1.0 when there isn't one.

    A missing date is not treated as "long ago" or "just now" — it simply does
    not participate in the decay. Fabricating a date to make the arithmetic
    uniform is the failure mode this codebase already has a rule against.
    """
    text = str(recorded_at or "").strip()
    if not text:
        return 1.0
    try:
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 1.0
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=now.tzinfo)
    age = now - stamp
    if age <= timedelta(0):
        return 1.0
    half_lives = age.total_seconds() / (EVIDENCE_HALF_LIFE_DAYS * 86400.0)
    return float(0.5**half_lives)


def _fit_and_predict(
    conn: sqlite3.Connection,
    *,
    labels: list[_SubstrateRow],
    targets: list[_SubstrateRow],
) -> tuple[dict[str, tuple[float, float]], TerrainModelInfo]:
    """Fit the GP on ``labels`` and predict a (value, confidence) per target.

    Returns an empty prediction map with a stated reason whenever the fit
    cannot be trusted, rather than a degraded one — a field that quietly falls
    back to smoothing nothing looks identical to a field that worked.
    """
    empty = TerrainModelInfo(False, len(labels), 0, 0, 0, None, LABEL_NOISE, None)
    if len(labels) < MIN_LABELS_TO_FIT:
        return {}, replace(
            empty, reason=f"only {len(labels)} labels; need {MIN_LABELS_TO_FIT}"
        )
    if not targets:
        return {}, replace(empty, reason="nothing to predict")

    import numpy as np

    from alma.application.graph_substrate import load_vectors_by_id
    from alma.discovery.similarity import get_active_embedding_model

    model_key = get_active_embedding_model(conn)
    wanted = [row.paper_id for row in labels] + [row.paper_id for row in targets]
    vectors = load_vectors_by_id(conn, wanted, model_key)

    label_rows = [row for row in labels if row.paper_id in vectors]
    target_rows = [row for row in targets if row.paper_id in vectors]
    if len(label_rows) < MIN_LABELS_TO_FIT:
        return {}, replace(
            empty,
            n_labels=len(label_rows),
            reason=f"only {len(label_rows)} labels carry a vector",
        )
    if not target_rows:
        return {}, replace(
            empty, n_labels=len(label_rows), reason="no target has a vector"
        )

    x_label = _unit_matrix(np.stack([vectors[row.paper_id] for row in label_rows]))
    y_label = np.asarray([row.value for row in label_rows], dtype=np.float64)
    w_label = np.asarray([max(row.weight, 1e-3) for row in label_rows], dtype=np.float64)

    # Cosine distance among labels, then the adaptive bandwidth read off it.
    label_distance = np.clip(1.0 - (x_label @ x_label.T), 0.0, 2.0)
    bandwidth = _adaptive_bandwidth(label_distance)

    kernel = np.exp(-label_distance / bandwidth)
    # Heteroscedastic ridge: a decayed or weak label gets MORE noise, which is
    # how "believe this one less" is expressed in a GP without touching y.
    gram = kernel + np.diag(LABEL_NOISE / w_label)
    try:
        inverse = np.linalg.inv(gram)
    except np.linalg.LinAlgError:
        return {}, replace(
            empty, n_labels=len(label_rows), reason="kernel matrix is singular"
        )
    alpha = inverse @ y_label

    predictions: dict[str, tuple[float, float]] = {}
    # Chunked so the cross-kernel never materialises as one huge array; the
    # substrate is ~10k rows today and this keeps the shape independent of it.
    chunk = 2048
    for start in range(0, len(target_rows), chunk):
        window = target_rows[start : start + chunk]
        x_target = _unit_matrix(np.stack([vectors[row.paper_id] for row in window]))
        cross = np.exp(-np.clip(1.0 - (x_target @ x_label.T), 0.0, 2.0) / bandwidth)
        mean = cross @ alpha
        # GP posterior: var = k(x,x) - kᵀ A k, and k(x,x) == 1 for this kernel,
        # so the quadratic form IS the fraction of the prior variance the labels
        # explain — i.e. confidence, already on [0, 1].
        explained = np.einsum("ij,jk,ik->i", cross, inverse, cross)
        confidence = np.clip(explained, 0.0, 1.0)
        for row, value, conf in zip(window, mean, confidence, strict=True):
            predictions[row.paper_id] = (
                float(np.clip(value, -1.0, 1.0)),
                float(conf),
            )

    return predictions, TerrainModelInfo(
        fitted=True,
        n_labels=len(label_rows),
        n_observed=0,
        n_predicted=len(predictions),
        n_unknown=0,
        bandwidth=float(bandwidth),
        noise=LABEL_NOISE,
        reason=None,
    )


def _unit_matrix(matrix: np.ndarray) -> np.ndarray:
    """L2-normalise rows so a dot product is a cosine."""
    import numpy as np

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return (matrix / norms).astype(np.float64)


def _adaptive_bandwidth(distance: np.ndarray) -> float:
    """Median distance to the k-th nearest fellow label, clamped.

    This is the scale at which the user's own evidence is dense. It is read off
    the labels rather than configured, so the field tightens by itself as the
    library grows instead of needing a knob nobody would know how to set.
    """
    import numpy as np

    n = distance.shape[0]
    k = min(BANDWIDTH_NEIGHBORS, n - 1)
    # Column 0 of the sorted row is the label's distance to itself (0.0).
    partitioned = np.sort(distance, axis=1)[:, 1 : k + 1]
    typical = float(np.median(partitioned)) if partitioned.size else MIN_BANDWIDTH
    return float(min(MAX_BANDWIDTH, max(MIN_BANDWIDTH, typical)))
