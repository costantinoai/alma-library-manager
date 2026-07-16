"""Canonical author signal — one definition of "how much we like an author".

This module is the **single source of truth** for an author-level preference
signal. Before it existed the same idea was computed five incompatible ways
(the detail card's library-ratio score, discovery's name-prevalence
``author_affinity``, discovery's paper-feedback projection, the suggestion
rail's centroid cosine, and the suggestion bucket scorers) and none of them
agreed on what "signal" meant or what range it lived in.

Everything now routes through here. The signal blends every source we have
about an author:

- **library**     — did you save their work, and how much of it
- **rating**      — how highly you rated the work you saved
- **interaction** — direct + paper-projected feedback (likes / loves / saves /
                    dismisses / follows), position-weighted and time-decayed.
                    This is the same ``ProjectedPaperSignals.author`` signal the
                    discovery ranker consumes, so the card and discovery agree.
- **similarity**  — cosine of the author's SPECTER2 centroid against the Library
                    centroid (the embedding signal the suggestion rail uses)
- **neighborhood**— citation / co-author adjacency to your Library. A slot the
                    suggestion engine populates where it already computes graph
                    adjacency; absent (and its weight redistributed) otherwise.

Two numbers come out of the same components so consumers never diverge:

- ``affinity`` — signed, ``[-1, 1]``. What the **discovery ranker** consumes
  (negative = "less like this").
- ``score`` — ``max(0, affinity) * 100``. What the **UI / rankings / graphs**
  display.

Storage is compute-on-read: there is no materialized table. The component
inputs live in their existing tables (``papers``, ``publication_authors``,
``feedback_events``, ``author_centroids``); :class:`AuthorSignalContext` loads
the expensive shared inputs (Library centroid + the projected-feedback graph)
**once** so batch callers (followed-author list, discovery preference profile)
stay cheap while single-author callers (the detail popup) build a one-off
context transparently.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field

from alma.application.signal_projection import load_projected_paper_signals
from alma.core.scoring_math import clamp as _clamp
from alma.core.sql_helpers import standalone_paper_sql

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signal contract — component weights + display labels.
#
# Weights sum to 1.0 and are RENORMALIZED over the components that have data
# for a given author (graceful degradation): an author with only a Library
# footprint and no embedding still gets a meaningful score from the library
# weight alone, rather than being dragged toward zero by absent components.
# Change a weight here and every surface (card, detail, discovery, rankings)
# moves together — that is the whole point of centralizing.
# ---------------------------------------------------------------------------
_WEIGHTS: dict[str, float] = {
    "library": 0.30,
    "rating": 0.20,
    "interaction": 0.20,
    "similarity": 0.20,
    "neighborhood": 0.10,
}
_LABELS: dict[str, str] = {
    "library": "Saved",
    "rating": "Rating",
    "interaction": "Interaction",
    "similarity": "Similarity",
    "neighborhood": "Neighborhood",
}

# Author-centroid similarity has a narrower, higher raw-cosine distribution
# than paper-vs-paper discovery candidates. Do not reuse Discovery's calibrated
# curve here: it intentionally pushes good paper matches close to 1.0, which
# makes most author cards read as "Similarity 100". Instead, treat ordinary
# domain overlap as the floor and reserve 100 for near-identical centroids.
_AUTHOR_SIMILARITY_FLOOR = 0.35
_AUTHOR_SIMILARITY_CEILING = 1.00

# Neighborhood saturation uses a soft exponential curve rather than tiny hard
# caps. One shared paper should be visible, but it should not display as 100.
_NEIGHBORHOOD_COAUTHOR_SCALE = 6.0
_NEIGHBORHOOD_CITED_SCALE = 4.0

# A component value within ±this of zero reads as "neutral" (grey) rather than
# positive/negative in the UI breakdown.
_NEUTRAL_BAND = 0.05


@dataclass
class AuthorSignalComponent:
    """One named contributor to an author's signal.

    ``value`` is the signed contribution in ``[-1, 1]`` used to compute
    affinity; ``score`` is its display magnitude (``abs(value) * 100``) and
    ``tone`` carries the sign so the UI can render bar length + colour without
    re-deriving either.
    """

    key: str
    label: str
    value: float
    weight: float
    score: float
    tone: str  # "positive" | "negative" | "neutral"
    detail: str | None = None

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "label": self.label,
            "score": self.score,
            "tone": self.tone,
            "detail": self.detail,
        }


@dataclass
class AuthorSignal:
    """The canonical signal for one author."""

    score: float  # 0..100, display
    affinity: float  # -1..1, discovery
    library_papers: int
    total_papers: int
    avg_rating: float | None
    components: list[AuthorSignalComponent]

    def to_dict(self) -> dict:
        return {
            "score": self.score,
            "affinity": self.affinity,
            "library_papers": self.library_papers,
            "total_papers": self.total_papers,
            "avg_rating": self.avg_rating,
            "components": [c.to_dict() for c in self.components],
        }


@dataclass
class _PaperStats:
    """Per-author Library footprint, aggregated from ``papers``."""

    total: int = 0
    library_count: int = 0
    rating_sum: int = 0
    rating_n: int = 0


@dataclass
class AuthorSignalContext:
    """Shared, batch-loaded inputs for computing many author signals cheaply.

    Build once via :func:`build_author_signal_context`, then call
    :meth:`signal_for` per author. ``centroid_by_oid`` and
    ``neighborhood_by_oid`` start empty; callers that want the similarity /
    neighborhood components populate them for the OpenAlex IDs they care about
    via :meth:`load_centroids` / direct assignment, so we never decode every
    author centroid in the corpus when only a handful are on screen.
    """

    active_model: str | None
    library_centroid: object  # np.ndarray | None (lazy numpy import)
    lib_dim: int
    projected_author: dict[str, float]
    projected_author_name: dict[str, float]
    stats_by_oid: dict[str, _PaperStats]
    stats_by_name: dict[str, _PaperStats]
    # Largest per-author Library footprint in the corpus — the denominator that
    # normalizes the "Saved" component so the author with the most papers in
    # your Library scores 1.0 (→ 100). Floored at 1 to avoid a zero divide.
    max_library_count: int = 1
    names_by_oid: dict[str, set[str]] = field(default_factory=dict)
    centroid_by_oid: dict[str, object] = field(default_factory=dict)
    neighborhood_by_oid: dict[str, float] = field(default_factory=dict)
    _db: sqlite3.Connection | None = None
    _centroids_loaded: set[str] = field(default_factory=set)
    _neighborhood_loaded: bool = False

    # -- similarity inputs -------------------------------------------------
    def load_centroids(self, oids: Iterable[str]) -> None:
        """Decode the SPECTER2 centroids for ``oids`` into ``centroid_by_oid``.

        No-op when the Library centroid is unavailable (nothing to compare
        against) or numpy / the ``author_centroids`` table is missing. Only
        IDs not already attempted are fetched, so repeated calls are cheap.
        """
        if self._db is None or self.library_centroid is None or not self.active_model:
            return
        wanted = [
            oid
            for oid in {str(o or "").strip().lower() for o in oids if o}
            if oid and oid not in self._centroids_loaded
        ]
        if not wanted:
            return
        self._centroids_loaded.update(wanted)
        try:
            import numpy as np  # noqa: F401  (presence check; used in decode)

            from alma.core.vector_blob import decode_vector
        except ImportError:
            return
        placeholders = ",".join("?" * len(wanted))
        try:
            rows = self._db.execute(
                f"""
                SELECT lower(trim(author_openalex_id)) AS oid, centroid_blob
                FROM author_centroids
                WHERE model = ? AND lower(trim(author_openalex_id)) IN ({placeholders})
                """,
                (self.active_model, *wanted),
            ).fetchall()
        except sqlite3.OperationalError:
            return
        for row in rows:
            oid = str(row["oid"] or "").strip()
            blob = row["centroid_blob"]
            if not oid or not blob:
                continue
            try:
                vec = decode_vector(blob, expected_dim=self.lib_dim)
            except Exception:
                continue
            if vec.shape[0] != self.lib_dim:
                continue
            self.centroid_by_oid[oid] = vec

    # -- neighborhood inputs -----------------------------------------------
    def ensure_neighborhood(self) -> None:
        """Populate ``neighborhood_by_oid`` for the whole corpus, once.

        Lazy by design: the graph relations are NOT loaded by
        :func:`build_author_signal_context` (so a caller that doesn't render
        the neighborhood component never pays for them). The corrected queries
        ride the schema indexes, so a single corpus-wide pass is a few
        milliseconds — cheaper and simpler than per-oid scoping, and the result
        is shared by every author this context scores. Idempotent: repeated
        calls after the first are no-ops.
        """
        if self._neighborhood_loaded or self._db is None:
            return
        self._neighborhood_loaded = True
        self.neighborhood_by_oid = _load_neighborhood(self._db)

    # -- the canonical per-author computation ------------------------------
    def signal_for(
        self,
        *,
        openalex_id: str = "",
        author_name: str = "",
        exclude: frozenset[str] | None = None,
    ) -> AuthorSignal | None:
        """Compute the canonical signal for one author, or ``None`` if we have
        no data at all (the caller renders "no signal yet").

        ``exclude`` drops named components before blending. The discovery
        ranker passes ``{"interaction"}`` because it already consumes the
        interaction signal through its own feedback-adjustment / dismissal
        path — excluding it here keeps the two from double-counting the same
        feedback while still sharing this one definition.
        """
        exclude = exclude or frozenset()
        oid = str(openalex_id or "").strip().lower()
        name = str(author_name or "").strip().lower()
        stats = self.stats_by_oid.get(oid) or self.stats_by_name.get(name)

        components: list[AuthorSignalComponent] = []

        # library — normalized prevalence: how many of their papers you've saved
        # to Library, relative to the author with the most (→ 1.0). Present only
        # when you've actually saved ≥1 of their papers: a zero here is absence
        # of evidence (you've merely seen their work), not a negative, and would
        # otherwise dilute the renormalized blend for a cold but graph-adjacent
        # author. The "X lib / Y total" detail keeps the raw counts visible.
        if "library" not in exclude and stats and stats.library_count > 0:
            lib_value = _clamp(stats.library_count / self.max_library_count, 0.0, 1.0)
            components.append(
                _make_component(
                    "library",
                    lib_value,
                    detail=f"{stats.library_count} lib / {stats.total} total",
                )
            )

        # rating — signed: poorly-rated work is a negative preference.
        avg_rating: float | None = None
        if stats and stats.rating_n > 0:
            avg_rating = stats.rating_sum / stats.rating_n
            if "rating" not in exclude:
                rat_value = _clamp((avg_rating - 3.0) / 2.0, -1.0, 1.0)
                components.append(
                    _make_component("rating", rat_value, detail=f"★{avg_rating:.1f}")
                )

        # interaction — the same projected paper-feedback signal discovery uses.
        if "interaction" not in exclude:
            inter = self.projected_author.get(oid)
            if inter is None and name:
                inter = self.projected_author_name.get(name)
            if inter is not None and abs(inter) > 1e-6:
                components.append(
                    _make_component("interaction", _clamp(inter, -1.0, 1.0))
                )

        # similarity — author centroid vs Library centroid (embedding signal).
        if "similarity" not in exclude:
            sim_value = self._similarity_value(oid)
            if sim_value is not None:
                components.append(_make_component("similarity", sim_value))

        # neighborhood — populated by the suggestion engine where available.
        if "neighborhood" not in exclude:
            nb = self.neighborhood_by_oid.get(oid)
            if nb is not None and nb > 0:
                components.append(_make_component("neighborhood", _clamp(nb, 0.0, 1.0)))

        if not components:
            return None

        # Renormalize weights over the present components, then blend.
        weight_sum = sum(c.weight for c in components)
        affinity = (
            sum(c.weight * c.value for c in components) / weight_sum
            if weight_sum > 0
            else 0.0
        )
        affinity = _clamp(affinity, -1.0, 1.0)

        return AuthorSignal(
            score=round(max(0.0, affinity) * 100.0, 1),
            affinity=round(affinity, 4),
            library_papers=stats.library_count if stats else 0,
            total_papers=stats.total if stats else 0,
            avg_rating=round(avg_rating, 2) if avg_rating else None,
            components=components,
        )

    def _similarity_value(self, oid: str) -> float | None:
        if self.library_centroid is None:
            return None
        centroid = self.centroid_by_oid.get(oid)
        if centroid is None:
            return None
        from alma.core.vector_blob import cosine_similarity

        raw = cosine_similarity(centroid, self.library_centroid)  # [-1, 1]
        return _scale_author_similarity(raw)


def _make_component(
    key: str, value: float, *, detail: str | None = None
) -> AuthorSignalComponent:
    tone = (
        "positive"
        if value > _NEUTRAL_BAND
        else "negative"
        if value < -_NEUTRAL_BAND
        else "neutral"
    )
    return AuthorSignalComponent(
        key=key,
        label=_LABELS[key],
        value=round(value, 4),
        weight=_WEIGHTS[key],
        score=round(abs(value) * 100.0, 1),
        tone=tone,
        detail=detail,
    )


def _scale_author_similarity(raw_score: float) -> float | None:
    """Map author-centroid cosine into a display/ranking component.

    Discovery paper scoring uses a generous calibration curve because raw
    academic paper cosine values can look deceptively low. Author centroids are
    averages over already-related papers, so their raw cosine values sit much
    higher. A floor/ceiling stretch preserves relative distance on cards while
    still returning ``None`` for weak, non-evidential overlap.
    """
    try:
        raw = float(raw_score)
    except (TypeError, ValueError):
        return None
    raw = max(0.0, min(1.0, raw))
    if raw <= _AUTHOR_SIMILARITY_FLOOR:
        return None
    span = _AUTHOR_SIMILARITY_CEILING - _AUTHOR_SIMILARITY_FLOOR
    if span <= 0:
        return 1.0
    return _clamp((raw - _AUTHOR_SIMILARITY_FLOOR) / span, 0.0, 1.0)


def _soft_saturate_count(count: int, *, scale: float) -> float:
    """Convert an evidence count to ``[0, 1]`` without early hard saturation."""
    n = max(0, int(count or 0))
    if n == 0:
        return 0.0
    scale = max(1.0, float(scale))
    return _clamp(1.0 - math.exp(-n / scale), 0.0, 1.0)


# ---------------------------------------------------------------------------
# Shared inputs
# ---------------------------------------------------------------------------


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def library_centroid(
    db: sqlite3.Connection, model: str
) -> tuple[object, int]:
    """Mean (normalized) of active-model embeddings for ``status='library'``.

    The single definition of "the Library's embedding direction", shared by
    the author signal's similarity component and the suggestion rail's
    ``semantic_similar`` bucket so they can never drift apart. Returns
    ``(centroid, dim)`` or ``(None, 0)`` when there's nothing to average
    (cold start, no embeddings, or numpy unavailable).
    """
    if not model or not _table_exists(db, "publication_embeddings"):
        return None, 0
    try:
        import numpy as np

        from alma.core.vector_blob import decode_vectors_uniform
    except ImportError:
        return None, 0
    try:
        rows = db.execute(
            f"""
            SELECT pe.embedding AS embedding
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE p.status = 'library' AND pe.model = ?
              AND {standalone_paper_sql('p')}
            LIMIT 1000
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None, 0
    if not rows:
        return None, 0
    matrix, _ = decode_vectors_uniform(row["embedding"] for row in rows)
    if matrix.size == 0:
        return None, 0
    centroid = np.mean(matrix, axis=0)
    norm = float(np.linalg.norm(centroid))
    if norm <= 0.0:
        return None, 0
    centroid = centroid / norm
    return centroid, int(centroid.shape[0])


def _load_paper_stats(
    db: sqlite3.Connection,
) -> tuple[dict[str, _PaperStats], dict[str, _PaperStats], dict[str, set[str]]]:
    """Aggregate every author's Library footprint in two GROUP BY passes.

    Keyed by both OpenAlex ID and normalized display name so authors that lack
    a resolved ID still get a library/rating signal. Returns
    ``(stats_by_oid, stats_by_name, names_by_oid)``.
    """
    stats_by_oid: dict[str, _PaperStats] = {}
    stats_by_name: dict[str, _PaperStats] = {}
    names_by_oid: dict[str, set[str]] = {}
    if not (_table_exists(db, "papers") and _table_exists(db, "publication_authors")):
        return stats_by_oid, stats_by_name, names_by_oid

    def _aggregate(group_expr: str, where_expr: str, sink: dict[str, _PaperStats]) -> None:
        try:
            rows = db.execute(
                f"""
                SELECT {group_expr} AS k,
                       COUNT(DISTINCT p.id) AS total,
                       SUM(CASE WHEN p.status = 'library' THEN 1 ELSE 0 END) AS lib,
                       SUM(CASE WHEN p.status = 'library' AND p.rating > 0
                                THEN p.rating ELSE 0 END) AS rsum,
                       SUM(CASE WHEN p.status = 'library' AND p.rating > 0
                                THEN 1 ELSE 0 END) AS rn
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                WHERE {where_expr}
                GROUP BY {group_expr}
                """
            ).fetchall()
        except sqlite3.OperationalError:
            return
        for row in rows:
            key = str(row["k"] or "").strip()
            if not key:
                continue
            sink[key] = _PaperStats(
                total=int(row["total"] or 0),
                library_count=int(row["lib"] or 0),
                rating_sum=int(row["rsum"] or 0),
                rating_n=int(row["rn"] or 0),
            )

    _aggregate(
        "lower(trim(pa.openalex_id))",
        "COALESCE(TRIM(pa.openalex_id), '') <> ''",
        stats_by_oid,
    )
    _aggregate(
        "lower(trim(pa.display_name))",
        "COALESCE(TRIM(pa.display_name), '') <> ''",
        stats_by_name,
    )

    # name(s) per oid — lets the discovery affinity map register name keys.
    try:
        rows = db.execute(
            """
            SELECT DISTINCT lower(trim(openalex_id)) AS oid,
                            lower(trim(display_name)) AS name
            FROM publication_authors
            WHERE COALESCE(TRIM(openalex_id), '') <> ''
              AND COALESCE(TRIM(display_name), '') <> ''
            """
        ).fetchall()
        for row in rows:
            oid = str(row["oid"] or "").strip()
            name = str(row["name"] or "").strip()
            if oid and name:
                names_by_oid.setdefault(oid, set()).add(name)
    except sqlite3.OperationalError:
        pass

    return stats_by_oid, stats_by_name, names_by_oid


def _load_neighborhood(db: sqlite3.Connection) -> dict[str, float]:
    """Per-author citation + co-author adjacency to your Library, in batch.

    Two graph relations, computed once over the whole corpus and combined
    noisy-or so either kind of closeness surfaces an otherwise-cold author:

    - **co-authorship** — distinct papers (any status) where the author shares
      a byline with someone on a saved Library paper (your "library circle"),
      excluding the author themselves.
    - **cited-by-Library** — the author's locally-known works that one of your
      Library papers references.

    Both queries are written to ride the indexes that schema init guarantees,
    so this stays a handful of milliseconds regardless of corpus size:

    - author ids match ``lower(openalex_id)`` exactly (NOT ``lower(trim(...))``
      — there is no whitespace in stored ids, and the gratuitous ``trim`` would
      make the expression no longer match ``idx_pubauthors_oid_lower`` and force
      a full self-join scan).
    - the cited-by relation joins on the canonical reference shape
      ``papers.openalex_id = 'W' || referenced_work_id`` (references store the
      bare integer work id; ``papers.openalex_id`` is W-prefixed) which rides
      the unique ``idx_papers_openalex_id``. Comparing the two raw, mismatched
      shapes — as an earlier version did — matched zero rows AND scanned.

    Each guarded independently so a missing column/table (e.g. a corpus with
    no `publication_references`) simply drops that relation. Returns an
    ``oid -> [0, 1]`` map (only authors with some adjacency appear).
    """
    if not _table_exists(db, "publication_authors"):
        return {}

    coauthor: dict[str, int] = {}
    # CTEs keep this bounded: `lib_circle_papers` is just the papers that
    # include a Library-circle author, and the outer join restricts to those.
    try:
        rows = db.execute(
            """
            WITH lib_authors AS (
                SELECT DISTINCT lower(pa.openalex_id) AS oid
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                WHERE p.status = 'library'
                  AND COALESCE(TRIM(pa.openalex_id), '') <> ''
            ),
            lib_circle_papers AS (
                SELECT DISTINCT pa.paper_id, lower(pa.openalex_id) AS lib_oid
                FROM publication_authors pa
                JOIN lib_authors la ON la.oid = lower(pa.openalex_id)
            )
            SELECT lower(pa.openalex_id) AS oid,
                   COUNT(DISTINCT pa.paper_id) AS papers
            FROM publication_authors pa
            JOIN lib_circle_papers lcp
              ON lcp.paper_id = pa.paper_id
             AND lcp.lib_oid <> lower(pa.openalex_id)
            WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
            GROUP BY lower(pa.openalex_id)
            """
        ).fetchall()
        for row in rows:
            oid = str(row["oid"] or "").strip()
            if oid:
                coauthor[oid] = int(row["papers"] or 0)
    except sqlite3.OperationalError:
        pass

    cited: dict[str, int] = {}
    try:
        rows = db.execute(
            """
            WITH lib_refs AS (
                SELECT DISTINCT pr.referenced_work_id AS wid
                FROM publication_references pr
                JOIN papers p ON p.id = pr.paper_id
                WHERE p.status = 'library'
                  AND pr.referenced_work_id IS NOT NULL
            )
            SELECT lower(pa.openalex_id) AS oid,
                   COUNT(DISTINCT pa.paper_id) AS papers
            FROM lib_refs lr
            JOIN papers p ON p.openalex_id = ('W' || lr.wid)
            JOIN publication_authors pa ON pa.paper_id = p.id
            WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
            GROUP BY lower(pa.openalex_id)
            """
        ).fetchall()
        for row in rows:
            oid = str(row["oid"] or "").strip()
            if oid:
                cited[oid] = int(row["papers"] or 0)
    except sqlite3.OperationalError:
        pass

    out: dict[str, float] = {}
    for oid in set(coauthor) | set(cited):
        c = _soft_saturate_count(
            coauthor.get(oid, 0), scale=_NEIGHBORHOOD_COAUTHOR_SCALE
        )
        r = _soft_saturate_count(
            cited.get(oid, 0), scale=_NEIGHBORHOOD_CITED_SCALE
        )
        # Noisy-or: either relation lifts the signal; combined stays in [0, 1].
        value = 1.0 - (1.0 - c) * (1.0 - r)
        if value > 0:
            out[oid] = value
    return out


def build_author_signal_context(db: sqlite3.Connection) -> AuthorSignalContext:
    """Load the shared inputs needed to compute author signals in batch."""
    try:
        from alma.discovery.similarity import get_active_embedding_model

        model = get_active_embedding_model(db)
    except Exception:
        model = None

    centroid, lib_dim = (None, 0)
    if model:
        centroid, lib_dim = library_centroid(db, model)

    # Author signal consumes only the author + author-name projections, so skip
    # the paper-level fan-out (topics / venues / tags / semantic + citation
    # neighbours) — its semantic-neighbour pass is a ~0.7s numpy loop we'd
    # otherwise compute and discard.
    projected = load_projected_paper_signals(db, author_only=True)
    stats_by_oid, stats_by_name, names_by_oid = _load_paper_stats(db)
    # Largest Library footprint across every known author — the denominator
    # that makes the "Saved" component a relative 0..1 (1.0 = the author on the
    # most Library papers). Floored at 1 so a cold corpus can't divide by zero.
    max_library_count = max(
        1,
        max(
            (s.library_count for s in (*stats_by_oid.values(), *stats_by_name.values())),
            default=0,
        ),
    )

    # Neighborhood (the graph relations) is intentionally NOT loaded here — it
    # is the one expensive, optional component. Callers that render it opt in
    # via ``ctx.ensure_neighborhood()``; everyone else skips the graph queries.
    return AuthorSignalContext(
        active_model=model,
        library_centroid=centroid,
        lib_dim=lib_dim,
        projected_author=dict(projected.author),
        projected_author_name=dict(projected.author_name),
        stats_by_oid=stats_by_oid,
        stats_by_name=stats_by_name,
        max_library_count=max_library_count,
        names_by_oid=names_by_oid,
        _db=db,
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def compute_author_signal(
    db: sqlite3.Connection,
    *,
    author_id: str,
    author_name: str,
    openalex_id: str,
    ctx: AuthorSignalContext | None = None,
) -> dict | None:
    """Canonical signal for a single author, as a JSON-ready dict.

    Builds a one-off context when ``ctx`` is omitted (the author-detail popup
    path). ``author_id`` is accepted for signature compatibility with the
    legacy helper but the signal keys off ``openalex_id`` / ``author_name``.
    """
    if ctx is None:
        ctx = build_author_signal_context(db)
    if openalex_id:
        ctx.load_centroids([openalex_id])
    ctx.ensure_neighborhood()
    signal = ctx.signal_for(openalex_id=openalex_id, author_name=author_name)
    return signal.to_dict() if signal else None


def compute_author_signals(
    db: sqlite3.Connection,
    authors: list[dict],
    *,
    ctx: AuthorSignalContext | None = None,
) -> dict[str, dict | None]:
    """Batch signals keyed by ``authors[i]['id']``.

    Each entry needs ``id`` plus ``openalex_id`` / ``name``. Loads every
    referenced centroid in one pass so a grid of cards costs one context build
    and one centroid query, not N.
    """
    if ctx is None:
        ctx = build_author_signal_context(db)
    ctx.load_centroids(
        [a.get("openalex_id") for a in authors if a.get("openalex_id")]
    )
    ctx.ensure_neighborhood()
    out: dict[str, dict | None] = {}
    for author in authors:
        signal = ctx.signal_for(
            openalex_id=str(author.get("openalex_id") or ""),
            author_name=str(author.get("name") or ""),
        )
        out[str(author.get("id") or "")] = signal.to_dict() if signal else None
    return out


def build_discovery_author_affinity(
    db: sqlite3.Connection, ctx: AuthorSignalContext | None = None
) -> dict[str, float]:
    """Author affinity map for the discovery ranker, keyed by name match keys.

    Replaces discovery's bespoke name-prevalence ``author_affinity`` with the
    canonical signal's STABLE preference (library + rating + similarity +
    neighborhood). The volatile ``interaction`` component is excluded because
    discovery consumes it through its own feedback-adjustment / dismissal
    path (``_projected_feedback_adjustment`` reads ``projected.author``), so
    folding it in here too would double-count the same feedback.

    The ranker looks authors up by :func:`author_affinity_keys` (normalized
    name), so we register every known author's signed ``affinity`` under those
    keys. Built once per preference-profile computation.
    """
    from alma.discovery.scoring import author_affinity_keys

    if ctx is None:
        ctx = build_author_signal_context(db)
    # Similarity matters most for authors with little direct signal, so load
    # centroids for every author we know about (one query, amortized once).
    ctx.load_centroids(list(ctx.names_by_oid.keys()))
    ctx.ensure_neighborhood()

    stable = frozenset({"interaction"})
    affinity: dict[str, float] = {}

    def _register(name: str, value: float) -> None:
        for key in author_affinity_keys(name):
            # Keep the strongest-magnitude signal when names collide.
            if key not in affinity or abs(value) > abs(affinity[key]):
                affinity[key] = value

    # Authors with a resolved OpenAlex ID (can carry similarity).
    for oid, names in ctx.names_by_oid.items():
        signal = ctx.signal_for(
            openalex_id=oid, author_name=next(iter(names), ""), exclude=stable
        )
        if signal is None:
            continue
        for name in names:
            _register(name, signal.affinity)

    # Name-only authors (no resolved OpenAlex ID).
    for name in ctx.stats_by_name:
        if any(name in names for names in ctx.names_by_oid.values()):
            continue
        signal = ctx.signal_for(author_name=name, exclude=stable)
        if signal is not None:
            _register(name, signal.affinity)

    return affinity
