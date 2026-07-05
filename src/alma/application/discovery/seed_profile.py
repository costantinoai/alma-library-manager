"""Discovery seed loading, preference profiling & branch building.

Loads a lens's seed papers, mines keyword / author / venue preference signals
from the library, clusters seeds into branches (vector or lexical), runs the
deterministic branch-query planner, and builds the topic cold-start summary.
Split out of the discovery god-module (D-9): pure move, every name re-exported
from ``alma.application.discovery``.

Depends on the ``lens_crud`` leaf (branch-control lifecycle, settings, lens
reads); never on the retrieval layer, which depends on this one.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Optional

from alma.core.scoring_math import clamp
from alma.discovery import similarity as sim_module

from .lens_crud import (
    _apply_branch_auto_lifecycle,
    _apply_branch_controls,
    _enrich_branches_with_outcomes,
    _load_branch_outcome_map,
    _make_branch_id,
    _resolve_lens_branch_controls,
    get_lens,
    read_settings,
)

logger = logging.getLogger(__name__)

_clamp = clamp  # D-3: canonical clamp under the legacy local name

# SPECTER2 vector ops degrade gracefully when numpy is unavailable; mirror the
# orchestrator's module-level guard so the vector clustering path stays intact.
try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


def _planner_clamp(value: float, lo: float, hi: float) -> float:
    """Bound `value` into [lo, hi]. Used by the deterministic branch planner."""
    return clamp(value, lo, hi)


def _planner_sanitize_queries(values: list[Any], max_items: int) -> list[str]:
    """Deduplicate, normalise, length-clip a list of candidate query strings.

    Used by `_plan_branch_queries_deterministic` to scrub the queries it
    stitches together from branch topics + seed titles. Strips internal
    whitespace, drops duplicates (case-insensitive), enforces a 6..180
    char window, and caps the result at `max_items`.
    """
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        q = " ".join(value.replace("\n", " ").split()).strip()
        if len(q) < 6:
            continue
        if len(q) > 180:
            q = q[:180].strip()
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
        if len(out) >= max_items:
            break
    return out


def _plan_branch_queries_deterministic(
    branch: dict,
    *,
    temperature: float,
    max_core: int,
    max_explore: int,
) -> dict[str, Any]:
    """Stitch a small set of branch search queries from topics + seed titles.

    Lifted out of the (now-deleted) LLM-backed `discovery_query_planner`
    so Discovery branch retrieval has a single, deterministic, zero-LLM
    path. Builds:
      - `core_queries`: anchored on the branch's `core_topics`, optionally
        combined with the strongest seed title. Bounded to `max_core`.
      - `explore_queries`: blends `explore_topics` with the lead core
        topic to nudge retrieval slightly outside the cluster centre.
        Bounded to `max_explore`. Temperature picks a soft modifier
        (`benchmarks` for low temp, `applications` for higher).
    """
    max_core = max(1, min(4, int(max_core)))
    max_explore = max(1, min(4, int(max_explore)))
    temperature = _planner_clamp(float(temperature), 0.0, 1.0)

    core_topics = [str(x).strip() for x in (branch.get("core_topics") or []) if str(x).strip()]
    explore_topics = [str(x).strip() for x in (branch.get("explore_topics") or []) if str(x).strip()]
    seed_context = branch.get("seed_context") or []
    seed_titles: list[str] = []
    if isinstance(seed_context, list):
        for item in seed_context[:3]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if title:
                seed_titles.append(title)

    core: list[str] = []
    if core_topics:
        core.append(" ".join(core_topics[:3]))
    if seed_titles and core_topics:
        core.append(f"{seed_titles[0]} {core_topics[0]}")
    elif seed_titles:
        core.append(seed_titles[0])
    core = _planner_sanitize_queries(core, max_core)
    if not core and core_topics:
        core = _planner_sanitize_queries([" ".join(core_topics[:2])], max_core)

    explore: list[str] = []
    if explore_topics:
        explore.append(" ".join(explore_topics[:3]))
    if core_topics and explore_topics:
        explore.append(f"{core_topics[0]} {explore_topics[0]} methods")
    if seed_titles and explore_topics:
        explore.append(f"{seed_titles[0]} {explore_topics[0]}")
    if not explore and core_topics:
        soft = "applications" if temperature >= 0.4 else "benchmarks"
        explore.append(f"{core_topics[0]} {soft}")

    return {
        "core_queries": _planner_sanitize_queries(core, max_core),
        "explore_queries": _planner_sanitize_queries(explore, max_explore),
    }


def _load_seed_papers_for_lens(db: sqlite3.Connection, lens: dict) -> list[dict]:
    context_type = lens["context_type"]
    config = lens.get("context_config") or {}
    settings = read_settings(db)
    try:
        max_seeds = int(settings.get("lens.max_seeds", "500"))
    except (TypeError, ValueError):
        max_seeds = 500
    max_seeds = max(50, min(5000, max_seeds))
    if context_type == "library_global":
        rows = db.execute(
            """
            SELECT id, title, abstract, doi, openalex_id, authors, journal, year, cited_by_count, rating
            FROM papers
            WHERE status = 'library'
            ORDER BY COALESCE(rating, 0) DESC, COALESCE(cited_by_count, 0) DESC
            LIMIT ?
            """,
            (max_seeds,),
        ).fetchall()
        return [dict(r) for r in rows]
    if context_type == "collection":
        collection_id = str(config.get("collection_id") or "").strip()
        if not collection_id:
            return []
        rows = db.execute(
            """
            SELECT p.id, p.title, p.abstract, p.doi, p.openalex_id, p.authors, p.journal, p.year, p.cited_by_count, p.rating
            FROM papers p
            JOIN collection_items ci ON ci.paper_id = p.id
            WHERE ci.collection_id = ?
            ORDER BY COALESCE(p.rating, 0) DESC, COALESCE(p.cited_by_count, 0) DESC
            LIMIT ?
            """,
            (collection_id, max_seeds),
        ).fetchall()
        return [dict(r) for r in rows]
    if context_type == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        if not keyword:
            return []
        pattern = f"%{keyword}%"
        rows = db.execute(
            """
            SELECT id, title, abstract, doi, openalex_id, authors, journal, year, cited_by_count, rating
            FROM papers
            WHERE title LIKE ? OR abstract LIKE ?
            ORDER BY COALESCE(cited_by_count, 0) DESC
            LIMIT ?
            """,
            (pattern, pattern, max_seeds),
        ).fetchall()
        return [dict(r) for r in rows]
    if context_type == "tag":
        tag_id = str(config.get("tag_id") or "").strip()
        tag_name = str(config.get("tag") or "").strip()
        if tag_id:
            rows = db.execute(
                """
                SELECT p.id, p.title, p.abstract, p.doi, p.openalex_id, p.authors, p.journal, p.year, p.cited_by_count, p.rating
                FROM papers p
                JOIN publication_tags pt ON pt.paper_id = p.id
                WHERE pt.tag_id = ?
                ORDER BY COALESCE(p.cited_by_count, 0) DESC
                LIMIT ?
                """,
                (tag_id, max_seeds),
            ).fetchall()
            return [dict(r) for r in rows]
        if tag_name:
            row = db.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
            if row:
                return _load_seed_papers_for_lens(
                    db,
                    {**lens, "context_config": {"tag_id": row["id"]}},
                )
        return []
    return []


_KEYWORD_STOP_WORDS: frozenset[str] = frozenset({
    "the", "and", "for", "with", "from", "using", "towards", "into", "between",
    "study", "studies", "analysis", "approach", "model", "models", "data",
    "this", "that", "these", "those", "paper", "papers", "method", "methods",
    "result", "results", "research", "review", "reviews", "library",
    "scientific",
})


def _tokenize_for_keywords(text: str) -> list[str]:
    """Split a title+abstract blob into lowercase alnum tokens worth ≥4 chars."""
    out: list[str] = []
    for token in text.lower().replace("/", " ").replace("-", " ").split():
        t = "".join(ch for ch in token if ch.isalnum())
        if len(t) < 4 or t in _KEYWORD_STOP_WORDS:
            continue
        out.append(t)
    return out


def _seed_token_set(seed: dict) -> set[str]:
    """Distinct kept tokens for a seed's title + abstract (memoised per call)."""
    return set(_tokenize_for_keywords(f"{seed.get('title', '')} {seed.get('abstract', '')}"))


def _extract_keywords(
    seeds: list[dict],
    explicit: Optional[list[str]] = None,
    max_keywords: int = 12,
    *,
    background: Optional[list[dict]] = None,
) -> list[str]:
    """Return the top distinctive keywords for `seeds`.

    Two-mode behaviour:

    * **Plain frequency mode** (when `background` is None): the historical
      bag-of-words most-common-N. Used for one-shot extractors that just
      want "what does this seed set talk about" without a comparison
      corpus (e.g. cluster query planning when there's only one cluster).
    * **TF-IDF mode** (when `background` is provided): per-token frequency
      inside `seeds` is divided by the document-frequency that token
      reaches across the broader `background` corpus. Tokens that show
      up in *every* background document score near 0; tokens that are
      common in `seeds` but rare elsewhere score highest. This is the
      mode `_build_seed_branches` uses to pick *distinctive* per-cluster
      `core_topics` so labels stop converging on the same handful of
      universal words ("learning / cortex" everywhere).
    """
    counts: dict[str, int] = {}
    for seed in seeds:
        for t in _tokenize_for_keywords(f"{seed.get('title', '')} {seed.get('abstract', '')}"):
            counts[t] = counts.get(t, 0) + 1
    if not counts:
        return list(explicit or [])[:max_keywords]

    if background:
        # Document frequency across the broader corpus (background may
        # itself include `seeds` — that's fine; the score is monotonic
        # in the ratio).
        bg_doc_freq: dict[str, int] = {}
        bg_total_docs = max(1, len(background))
        for bg_seed in background:
            for t in _seed_token_set(bg_seed):
                bg_doc_freq[t] = bg_doc_freq.get(t, 0) + 1

        scored: list[tuple[float, str]] = []
        seed_total = sum(counts.values()) or 1
        for token, freq in counts.items():
            df = bg_doc_freq.get(token, 0)
            # IDF with +1 smoothing in both numerator and denominator.
            idf = math.log((bg_total_docs + 1) / (df + 1)) + 1.0
            tf = freq / seed_total
            scored.append((tf * idf, token))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        words = [token for _, token in scored]
    else:
        words = [w for w, _ in sorted(counts.items(), key=lambda x: (x[1], x[0]), reverse=True)]

    if explicit:
        for item in explicit:
            s = (item or "").strip().lower()
            if s and s not in words:
                words.insert(0, s)
    return words[:max_keywords]


def _load_library_preference_inputs(
    db: sqlite3.Connection,
) -> tuple[list[dict], list[dict], list[dict]]:
    rows = db.execute(
        """SELECT id, title, abstract, url, doi, authors, journal, year, rating, added_at
           FROM papers
           WHERE status = 'library'
           ORDER BY COALESCE(added_at, '') DESC"""
    ).fetchall()
    library_pubs = [dict(r) for r in rows]
    positive_pubs = [
        dict(r)
        for r in rows
        if (r["rating"] or 0) >= 4 or (r["rating"] or 0) == 0
    ]
    negative_pubs = [
        dict(r)
        for r in rows
        if 1 <= int(r["rating"] or 0) <= 2
    ]
    if library_pubs and not any((r["rating"] or 0) >= 4 for r in rows):
        positive_pubs = list(library_pubs)
    return library_pubs, positive_pubs, negative_pubs


def _top_profile_terms(
    weights: dict[str, float],
    *,
    limit: int,
    min_weight: float = 0.16,
) -> list[tuple[str, float]]:
    ranked = [
        ((term or "").strip(), float(weight))
        for term, weight in weights.items()
        if (term or "").strip() and float(weight or 0.0) >= min_weight
    ]
    ranked.sort(key=lambda item: (item[1], len(item[0])), reverse=True)
    return ranked[: max(1, limit)]


def _top_preferred_authors(
    db: sqlite3.Connection,
    *,
    limit: int,
    library_dominance_cap: float = 0.4,
) -> list[tuple[str, float]]:
    """Return the top N authors to fan external taste-author queries
    out to. Excludes authors who appear on more than
    `library_dominance_cap` of the library — sending an explicit
    `"<dominant author>"` query to OpenAlex/S2 just amplifies the same
    author and starves the secondary tail. The dominant author is
    still picked up by author_affinity scoring on candidates pulled
    via topic / keyword / journal lanes; we just don't make him the
    explicit search query.
    """
    scores: dict[str, float] = {}
    display_names: dict[str, str] = {}

    # Library author prevalence (count of library papers each author
    # appears on, divided by library size) — used to cap dominance.
    library_size = 0
    library_author_share: dict[str, float] = {}
    try:
        size_row = db.execute(
            "SELECT COUNT(*) AS n FROM papers WHERE status = 'library'"
        ).fetchone()
        library_size = int(size_row["n"] or 0) if size_row else 0
        if library_size > 0:
            share_rows = db.execute(
                """
                SELECT pa.display_name AS display_name, COUNT(DISTINCT pa.paper_id) AS n
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                WHERE p.status = 'library'
                  AND COALESCE(TRIM(pa.display_name), '') != ''
                GROUP BY LOWER(TRIM(pa.display_name))
                """
            ).fetchall()
            for row in share_rows:
                key = str(row["display_name"] or "").strip().lower()
                if not key:
                    continue
                share = float(row["n"] or 0) / float(library_size)
                library_author_share[key] = max(library_author_share.get(key, 0.0), share)
    except sqlite3.OperationalError:
        pass

    try:
        rows = db.execute(
            """
            SELECT entity_id, affinity_weight, confidence, interaction_count
            FROM preference_profiles
            WHERE entity_type = 'author'
              AND affinity_weight > 0
            ORDER BY (affinity_weight * confidence) DESC, interaction_count DESC
            LIMIT ?
            """,
            (max(limit * 3, 8),),
        ).fetchall()
        for row in rows:
            display_name = str(row["entity_id"] or "").strip()
            key = display_name.lower()
            if not key:
                continue
            score = float(row["affinity_weight"] or 0.0) * max(0.2, float(row["confidence"] or 0.0))
            current = scores.get(key, 0.0)
            if score > current:
                scores[key] = score
                display_names[key] = display_name
    except sqlite3.OperationalError:
        pass

    try:
        rows = db.execute(
            """
            SELECT a.name
            FROM followed_authors fa
            JOIN authors a ON a.id = fa.author_id
            WHERE COALESCE(TRIM(a.name), '') != ''
            LIMIT ?
            """,
            (max(limit * 2, 6),),
        ).fetchall()
        for row in rows:
            display_name = str(row["name"] or "").strip()
            key = display_name.lower()
            if not key:
                continue
            scores[key] = max(scores.get(key, 0.0), 0.7)
            display_names[key] = display_name
    except sqlite3.OperationalError:
        pass

    # Drop dominant authors from the explicit-query list. Keep them
    # available as scoring boosts (handled elsewhere) but don't fan
    # external API queries at them.
    if library_size >= 5:
        for key in list(scores.keys()):
            if library_author_share.get(key, 0.0) > library_dominance_cap:
                scores.pop(key, None)

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return [
        (display_names.get(key, key), score)
        for key, score in ranked[: max(1, limit)]
    ]


def _recent_positive_publications(
    db: sqlite3.Connection,
    fallback: list[dict],
    *,
    limit: int,
) -> list[dict]:
    try:
        rows = db.execute(
            """
            SELECT p.id, p.title, p.abstract, p.authors, p.journal, p.year, r.action_at
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE r.user_action IN ('save', 'like')
            ORDER BY COALESCE(r.action_at, r.created_at, '') DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        recent = [dict(r) for r in rows if str(r["title"] or "").strip()]
        if recent:
            return recent
    except sqlite3.OperationalError:
        pass
    return list(fallback[:limit])


def _build_recent_win_queries(
    db: sqlite3.Connection,
    positive_pubs: list[dict],
    *,
    limit: int,
) -> list[tuple[str, float]]:
    recent_pubs = _recent_positive_publications(db, positive_pubs, limit=max(limit * 2, 4))
    queries: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, pub in enumerate(recent_pubs):
        terms = _extract_keywords([pub], max_keywords=5)
        query = " ".join(terms[:3]).strip()
        if not query:
            continue
        if query in seen:
            continue
        seen.add(query)
        strength = _clamp(0.92 - (idx * 0.12), 0.45, 0.92)
        queries.append((query, strength))
        if len(queries) >= limit:
            break
    return queries


def _negative_preference_context(
    db: sqlite3.Connection,
    preference_profile: Optional[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    topic_weights = dict((preference_profile or {}).get("topic_weights") or {})
    journal_affinity = dict((preference_profile or {}).get("journal_affinity") or {})
    negative_topics = {
        str(term).strip().lower(): abs(float(weight))
        for term, weight in topic_weights.items()
        if str(term).strip() and float(weight or 0.0) <= -0.16
    }
    negative_journals = {
        str(journal).strip().lower(): abs(float(weight))
        for journal, weight in journal_affinity.items()
        if str(journal).strip() and float(weight or 0.0) <= -0.16
    }
    negative_authors: dict[str, float] = {}
    try:
        rows = db.execute(
            """
            SELECT entity_id, affinity_weight, confidence
            FROM preference_profiles
            WHERE entity_type = 'author'
              AND affinity_weight < 0
            ORDER BY affinity_weight ASC
            LIMIT 40
            """
        ).fetchall()
        for row in rows:
            name = str(row["entity_id"] or "").strip().lower()
            if not name:
                continue
            negative_authors[name] = abs(float(row["affinity_weight"] or 0.0)) * max(
                0.2,
                float(row["confidence"] or 0.0),
            )
    except sqlite3.OperationalError:
        pass
    return {
        "topics": negative_topics,
        "authors": negative_authors,
        "journals": negative_journals,
    }


def _top_negative_terms(
    values: dict[str, float],
    *,
    limit: int,
    field_name: str,
) -> list[dict[str, Any]]:
    ranked = [
        {field_name: key, "weight": round(float(weight), 4)}
        for key, weight in sorted(
            (
                (str(key).strip(), float(weight))
                for key, weight in values.items()
                if str(key).strip() and float(weight or 0.0) > 0.0
            ),
            key=lambda item: item[1],
            reverse=True,
        )[: max(1, limit)]
    ]
    return ranked


def _candidate_negative_preference_penalty(
    candidate: dict,
    negative_context: dict[str, dict[str, float]],
) -> float:
    if not negative_context:
        return 0.0

    penalty = 0.0
    title = str(candidate.get("title") or "").strip().lower()
    journal = str(candidate.get("journal") or "").strip().lower()
    authors_text = str(candidate.get("authors") or "").strip().lower()

    for term, weight in negative_context.get("topics", {}).items():
        if not term:
            continue
        if term in title:
            penalty += min(0.42, 0.12 + (weight * 0.28))

    for author, weight in negative_context.get("authors", {}).items():
        if author and author in authors_text:
            penalty += min(0.45, 0.18 + (weight * 0.25))

    for venue, weight in negative_context.get("journals", {}).items():
        if venue and venue == journal:
            penalty += min(0.35, 0.12 + (weight * 0.18))

    return _clamp(penalty, 0.0, 0.95)


def _resolve_branch_temperature(
    settings: Optional[dict[str, str]] = None,
    override: Optional[float] = None,
) -> float:
    if override is not None:
        return _clamp(float(override), 0.0, 1.0)
    cfg = settings or {}
    mode = str(cfg.get("recommendation_mode", "balanced") or "balanced").strip().lower()
    default_by_mode = {
        "exploit": 0.12,
        "balanced": 0.28,
        "explore": 0.55,
    }
    fallback = default_by_mode.get(mode, 0.28)
    raw = cfg.get("branches.temperature")
    try:
        return _clamp(float(raw), 0.0, 1.0) if raw is not None else fallback
    except (TypeError, ValueError):
        return fallback


# Branch-clustering granularity: mirrors the Insights graph "Cluster detail"
# knob and drives the SAME shared engine (`ai.clustering.cluster_publications`).
BRANCH_RESOLUTION_DEFAULT = 1.0
BRANCH_RESOLUTION_MIN = 0.5
BRANCH_RESOLUTION_MAX = 3.0


def _resolve_branch_resolution(
    override: Optional[float] = None,
    settings: Optional[dict[str, str]] = None,
) -> float:
    """Resolve the branch cluster-granularity (>1 finer, <1 coarser).

    Priority: explicit ``override`` (live slider / persisted lens control) →
    ``branches.resolution`` setting → engine default 1.0. Clamped to the same
    [0.5, 3.0] band as the graph so the two surfaces share one contract."""
    if override is not None:
        try:
            return _clamp(float(override), BRANCH_RESOLUTION_MIN, BRANCH_RESOLUTION_MAX)
        except (TypeError, ValueError):
            pass
    raw = (settings or {}).get("branches.resolution")
    try:
        return (
            _clamp(float(raw), BRANCH_RESOLUTION_MIN, BRANCH_RESOLUTION_MAX)
            if raw is not None
            else BRANCH_RESOLUTION_DEFAULT
        )
    except (TypeError, ValueError):
        return BRANCH_RESOLUTION_DEFAULT


def _seed_strength(seed: dict) -> float:
    """Seed-paper strength used to rank / cluster lens seeds.

    Prefers the composite `paper_signal.score_papers_batch` value
    when callers have stamped it on the seed via
    `_attach_signal_scores_to_seeds` — that read-once batch covers
    rating, topic alignment, embedding similarity, author alignment,
    signal-lab swipes, and recency through one shared primitive.
    Falls back to the legacy rating + citation + recency heuristic
    when the stamp is missing (ad-hoc callers, tests, or pipelines
    that have not yet been threaded through the batch).
    """
    stamped = seed.get("_signal_score")
    if stamped is not None:
        try:
            return float(stamped)
        except (TypeError, ValueError):
            pass
    rating_raw = int(seed.get("rating") or 0)
    rating_score = 0.6 if rating_raw <= 0 else _clamp(rating_raw / 5.0, 0.0, 1.0)
    citations = float(seed.get("cited_by_count") or 0.0)
    citation_score = _clamp(citations / 200.0, 0.0, 1.0)
    year_raw = int(seed.get("year") or 0)
    current_year = datetime.utcnow().year
    recency_score = _clamp((year_raw - (current_year - 12)) / 12.0, 0.0, 1.0)
    return (rating_score * 0.6) + (citation_score * 0.25) + (recency_score * 0.15)


def _attach_signal_scores_to_seeds(
    db: sqlite3.Connection, seeds: list[dict]
) -> list[dict]:
    """Stamp `_signal_score` on each seed via `paper_signal.score_papers_batch`.

    One batched call per refresh; the per-seed sort key inside
    `_seed_strength` then becomes a dict lookup. Seeds whose IDs
    can't be scored (missing from papers, etc.) keep their existing
    heuristic-only behavior.
    """
    if not seeds:
        return seeds
    paper_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    paper_ids = [pid for pid in paper_ids if pid]
    if not paper_ids:
        return seeds
    try:
        from alma.application.paper_signal import score_papers_batch
        scores = score_papers_batch(db, paper_ids)
    except Exception as exc:
        logger.debug("score_papers_batch unavailable for seed strength: %s", exc)
        return seeds
    for seed in seeds:
        pid = str(seed.get("id") or "").strip()
        if pid in scores:
            seed["_signal_score"] = scores[pid]
    return seeds


def _fetch_seed_embedding_vectors(
    db: sqlite3.Connection,
    seeds: list[dict],
) -> dict[str, "np.ndarray"]:
    if not _NUMPY_AVAILABLE:
        return {}
    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [sid for sid in seed_ids if sid]
    if not seed_ids:
        return {}
    active_model = sim_module.get_active_embedding_model(db)
    placeholders = ",".join("?" for _ in seed_ids)
    try:
        rows = db.execute(
            f"""
            SELECT paper_id, embedding
            FROM publication_embeddings
            WHERE model = ? AND paper_id IN ({placeholders})
            """,
            [active_model, *seed_ids],
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    from alma.core.vector_blob import decode_vector
    out: dict[str, "np.ndarray"] = {}
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id:
            continue
        try:
            vec = decode_vector(row["embedding"])
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            out[paper_id] = vec / norm
        except Exception:
            continue
    return out


def _cluster_seed_papers_vector(
    seeds: list[dict],
    vectors: dict[str, "np.ndarray"],
    max_clusters: int,
    resolution: float = BRANCH_RESOLUTION_DEFAULT,
) -> list[dict[str, Any]]:
    """Cluster lens seeds into branches via the SHARED clustering engine.

    Delegates to ``ai.clustering.cluster_publications`` — the SAME
    BERTopic / HDBSCAN recipe the Insights graph uses — so one ``resolution``
    knob (>1 finer, <1 coarser) governs branch granularity on both surfaces.
    This replaces the old bespoke spherical k-means + 0.85 centroid-merge,
    whose merge loop collapsed every coherent single-user library to its hard
    floor of 2 branches regardless of size (the reported bug).

    Cluster COUNT is inferred by the engine from ``resolution``; ``max_clusters``
    is only a display ceiling. Overflow clusters + density-noise outliers are
    absorbed into the nearest retained centroid (cosine) so NO seed is dropped
    from the branch view. Returns ``[{"seeds": [...], "centroid": np.ndarray}]``
    with unit-normalized raw-vector centroids for the downstream
    core-pull / explore-push neighbour geometry.
    """
    seed_by_id = {str(seed.get("id") or ""): seed for seed in seeds}
    emb: dict[str, list[float]] = {}
    for paper_id, vec in vectors.items():
        if paper_id in seed_by_id:
            emb[paper_id] = vec.tolist() if hasattr(vec, "tolist") else list(vec)
    if not emb:
        return []
    if len(emb) == 1:
        sid = next(iter(emb))
        return [{"seeds": [seed_by_id[sid]], "centroid": vectors[sid]}]

    def _unit(v: "np.ndarray") -> "np.ndarray":
        n = float(np.linalg.norm(v))
        return v / n if n > 0.0 else v

    from alma.ai.clustering import cluster_publications

    result = cluster_publications(emb, resolution=resolution)

    real: list[dict[str, Any]] = []
    for cluster in result.clusters:
        ids = [k for k in cluster.member_keys if k in seed_by_id]
        if not ids:
            continue
        real.append(
            {"ids": ids, "centroid": _unit(np.asarray(cluster.centroid, dtype=float))}
        )

    if not real:
        # The engine placed every seed in density-noise → one catch-all branch
        # (never collapse the whole library to an empty branch view).
        all_ids = list(emb.keys())
        centroid = _unit(np.mean(np.vstack([vectors[i] for i in all_ids]), axis=0))
        pooled = sorted((seed_by_id[i] for i in all_ids), key=_seed_strength, reverse=True)
        return [{"seeds": pooled, "centroid": centroid}]

    # Rank by size then aggregate seed strength; cap to the display ceiling.
    real.sort(
        key=lambda c: (len(c["ids"]), sum(_seed_strength(seed_by_id[i]) for i in c["ids"])),
        reverse=True,
    )
    cap = len(real) if max_clusters <= 0 else max(2, min(int(max_clusters), len(real)))
    retained = real[:cap]

    # Absorb overflow-cluster members + density outliers into the nearest
    # retained centroid so the branch view still covers every seed.
    leftover: list[str] = list(result.outliers)
    for cluster in real[cap:]:
        leftover.extend(cluster["ids"])
    for paper_id in leftover:
        vec = vectors.get(paper_id)
        if vec is None or paper_id not in seed_by_id:
            continue
        best = max(retained, key=lambda c: float(np.dot(vec, c["centroid"])))
        best["ids"].append(paper_id)

    clusters: list[dict[str, Any]] = []
    for cluster in retained:
        group_seeds = [seed_by_id[i] for i in cluster["ids"] if i in seed_by_id]
        if not group_seeds:
            continue
        group_seeds.sort(key=_seed_strength, reverse=True)
        clusters.append({"seeds": group_seeds, "centroid": cluster["centroid"]})

    clusters.sort(
        key=lambda c: (
            sum(_seed_strength(s) for s in c["seeds"]) / max(1, len(c["seeds"])),
            len(c["seeds"]),
        ),
        reverse=True,
    )
    return clusters


def _cluster_seed_papers_lexical(
    seeds: list[dict],
    max_clusters: int,
) -> list[dict[str, Any]]:
    """Cluster seeds when no embeddings exist (small / fresh libraries).

    The previous implementation assigned each seed to its first keyword
    that also appeared in the global top-N — which collapsed every seed
    whose top word was "neural" into the same cluster regardless of the
    paper's actual topic. The replacement uses TF-IDF: for each seed we
    pick the token with the highest `seed_freq * idf` against the rest
    of the corpus, so the *distinctive* term anchors the seed, not the
    *most-common* one.
    """
    if not seeds:
        return []
    if len(seeds) == 1:
        return [{"seeds": list(seeds), "centroid": None}]

    seed_token_sets = [(seed, _seed_token_set(seed)) for seed in seeds]
    seed_total = max(1, len(seeds))

    # Document frequency across the whole seed set.
    doc_freq: dict[str, int] = {}
    for _, tokens in seed_token_sets:
        for tok in tokens:
            doc_freq[tok] = doc_freq.get(tok, 0) + 1

    # Pick each seed's most distinctive token: highest tf*idf within
    # the seed itself, with a floor on idf so a token that appears in
    # every seed (idf≈0) cannot be an anchor — those tokens get pushed
    # below any halfway-distinctive runner-up.
    groups: dict[str, list[dict]] = defaultdict(list)
    for seed, tokens in seed_token_sets:
        token_counts = Counter(_tokenize_for_keywords(
            f"{seed.get('title', '')} {seed.get('abstract', '')}"
        ))
        if not token_counts:
            groups["__misc__"].append(seed)
            continue
        seed_total_tokens = max(1, sum(token_counts.values()))
        best_token = "__misc__"
        best_score = -1.0
        for token, freq in token_counts.items():
            df = doc_freq.get(token, 0)
            # Skip tokens that appear in every seed (common to everyone) —
            # those are exactly the "neural / cortex" universal words
            # the user wants the clusterer to look past.
            if df >= seed_total:
                continue
            idf = math.log((seed_total + 1) / (df + 1)) + 1.0
            tf = freq / seed_total_tokens
            score = tf * idf
            # Stable tiebreak by alphabetical order — keeps clusters
            # from re-shuffling between refreshes when scores tie.
            if score > best_score or (score == best_score and token < best_token):
                best_token = token
                best_score = score
        groups[best_token].append(seed)

    clusters: list[dict[str, Any]] = []
    for anchor, group in groups.items():
        group_sorted = sorted(group, key=_seed_strength, reverse=True)
        clusters.append({"seeds": group_sorted, "centroid": None, "anchor": anchor})

    clusters.sort(
        key=lambda c: (
            len(c["seeds"]),
            sum(_seed_strength(s) for s in c["seeds"]) / max(1, len(c["seeds"])),
        ),
        reverse=True,
    )

    if len(clusters) > max_clusters and max_clusters > 0:
        overflow: list[dict] = []
        for cluster in clusters[max_clusters:]:
            overflow.extend(cluster["seeds"])
        clusters = clusters[:max_clusters]
        if clusters and overflow:
            clusters[-1]["seeds"].extend(overflow)
            clusters[-1]["seeds"].sort(key=_seed_strength, reverse=True)
    return clusters


def _build_seed_branches(
    db: sqlite3.Connection,
    seeds: list[dict],
    *,
    settings: Optional[dict[str, str]] = None,
    max_branches: int = 6,
    temperature: Optional[float] = None,
    resolution: Optional[float] = None,
    lens_id: Optional[str] = None,
) -> list[dict]:
    if not seeds:
        return []
    effective_max = max(2, min(12, int(max_branches or 6)))
    effective_temp = _resolve_branch_temperature(settings, temperature)
    effective_resolution = _resolve_branch_resolution(resolution, settings)

    vectors = _fetch_seed_embedding_vectors(db, seeds)
    if len(vectors) >= 4:
        clusters = _cluster_seed_papers_vector(
            seeds, vectors, effective_max, resolution=effective_resolution
        )
    else:
        clusters = _cluster_seed_papers_lexical(seeds, effective_max)
    if not clusters:
        return []

    global_terms = _extract_keywords(seeds, max_keywords=40)
    # Build a "background corpus" of all *other* clusters' seeds so the
    # TF-IDF call below picks tokens that are distinctive to *this*
    # cluster, not common across the whole library. Without this every
    # cluster's `core_topics` converged on the same universal terms
    # ("learning / cortex / model") and every branch label looked the
    # same — defeating the entire point of clustering.
    branches: list[dict] = []
    for i, cluster in enumerate(clusters[:effective_max], start=1):
        cluster_seeds = cluster.get("seeds") or []
        if not cluster_seeds:
            continue
        background_seeds: list[dict] = []
        for j, other in enumerate(clusters[:effective_max]):
            if j == i - 1:
                continue
            background_seeds.extend(other.get("seeds") or [])
        cluster_terms = _extract_keywords(
            cluster_seeds,
            max_keywords=14,
            background=background_seeds or None,
        )
        if not cluster_terms:
            cluster_terms = global_terms

        core_count = max(2, min(4, int(round(4.0 - (effective_temp * 2.4)))))
        core_topics = cluster_terms[:core_count] if cluster_terms else []
        if not core_topics and global_terms:
            core_topics = global_terms[:2]
        if not core_topics:
            core_topics = [f"branch-{i}"]

        neighbor_terms: list[str] = []
        direction_hint: Optional[str] = None

        # explore_topics neighbour selection is temperature-gated:
        #   - cold lens (temperature < 0.5)  → nearest cluster, gradient
        #     discovery: explore terms are a small step away from core.
        #   - hot lens  (temperature ≥ 0.5)  → farthest cluster, leap
        #     discovery: explore terms come from the most-different
        #     cluster on the seed-embedding manifold, surfacing
        #     genuinely orthogonal threads.
        # Either way the user has full control via the temperature
        # setting (and per-lens override).
        prefer_far_neighbour = effective_temp >= 0.5
        own_centroid = cluster.get("centroid")
        if _NUMPY_AVAILABLE and own_centroid is not None and len(clusters) > 1:
            best_idx: Optional[int] = None
            best_sim = -2.0 if prefer_far_neighbour else float("-inf")
            for j, other in enumerate(clusters):
                if other is cluster:
                    continue
                other_centroid = other.get("centroid")
                if other_centroid is None:
                    continue
                sim = float(np.dot(own_centroid, other_centroid))
                if prefer_far_neighbour:
                    # We want the lowest similarity (= farthest direction).
                    # Initialise best_sim to +inf and pick min.
                    if best_idx is None or sim < best_sim:
                        best_sim = sim
                        best_idx = j
                else:
                    if sim > best_sim:
                        best_sim = sim
                        best_idx = j
            if best_idx is not None:
                neighbor_terms = _extract_keywords(clusters[best_idx].get("seeds") or [], max_keywords=12)
                if neighbor_terms:
                    direction_hint = " / ".join(neighbor_terms[:2])

        if not neighbor_terms and len(clusters) > 1:
            next_cluster = clusters[(i) % len(clusters)]
            neighbor_terms = _extract_keywords(next_cluster.get("seeds") or [], max_keywords=12)
            if neighbor_terms:
                direction_hint = " / ".join(neighbor_terms[:2])

        explore_count = max(1, min(5, int(round(1.0 + (effective_temp * 4.0)))))
        explore_pool: list[str] = []
        for term in [*neighbor_terms, *global_terms]:
            if term in core_topics or term in explore_pool:
                continue
            explore_pool.append(term)
        explore_topics = explore_pool[:explore_count]

        branch_score = sum(_seed_strength(seed) for seed in cluster_seeds) / max(1, len(cluster_seeds))
        label = " / ".join(core_topics[:2]) if core_topics else f"Branch {i}"

        sample_papers: list[dict] = []
        ranked_cluster = sorted(cluster_seeds, key=_seed_strength, reverse=True)
        for seed in ranked_cluster[:3]:
            sample_papers.append(
                {
                    "paper_id": seed.get("id"),
                    "title": seed.get("title") or "Untitled",
                    "year": seed.get("year"),
                    "rating": int(seed.get("rating") or 0),
                }
            )
        seed_context: list[dict] = []
        for seed in ranked_cluster[:6]:
            abstract = (seed.get("abstract") or "").strip()
            if len(abstract) > 1400:
                abstract = abstract[:1400].rstrip() + "..."
            seed_context.append(
                {
                    "paper_id": seed.get("id"),
                    "title": seed.get("title") or "Untitled",
                    "abstract": abstract,
                    "year": seed.get("year"),
                    "rating": int(seed.get("rating") or 0),
                    "cited_by_count": int(seed.get("cited_by_count") or 0),
                }
            )

        # Derive the branch identity from the sorted set of cluster seed
        # paper IDs (scoped by lens_id). This is stable across preview /
        # refresh cycles: labels can drift, but the cluster's seed set is
        # the cluster's identity. See `_make_branch_id` for the full D-AUDIT-5
        # rationale.
        cluster_seed_ids = [str(seed.get("id") or "") for seed in cluster_seeds]
        branches.append(
            {
                "id": _make_branch_id(lens_id, cluster_seed_ids, core_topics),
                "label": label,
                "seed_count": len(cluster_seeds),
                "branch_score": round(branch_score, 4),
                "core_topics": core_topics,
                "explore_topics": explore_topics,
                "direction_hint": direction_hint,
                "sample_papers": sample_papers,
                "seed_context": seed_context,
            }
        )

    branches.sort(key=lambda b: (float(b.get("branch_score") or 0.0), int(b.get("seed_count") or 0)), reverse=True)
    return branches[:effective_max]


def preview_lens_branches(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    max_branches: int = 6,
    temperature: Optional[float] = None,
    resolution: Optional[float] = None,
) -> Optional[dict]:
    """Build an explainable branch map for one lens (for UI visualization).

    Each branch carries its `auto_weight` — the continuous multiplier the
    refresh allocator will apply to its retrieval budget based on past
    save/dismiss outcomes. Manual pin/boost/mute remain available as hard
    overrides; with no override, `auto_weight` is what shapes allocation.

    ``resolution`` (live slider override → else the persisted
    ``branch_controls.resolution``) tunes cluster granularity via the shared
    engine and is echoed back so the UI can render the current knob value.
    """
    lens = get_lens(db, lens_id)
    if lens is None:
        return None
    seeds = _attach_signal_scores_to_seeds(db, _load_seed_papers_for_lens(db, lens))
    settings = read_settings(db)
    controls = _resolve_lens_branch_controls(lens)
    effective_temp = _resolve_branch_temperature(
        settings,
        temperature if temperature is not None else controls.get("temperature"),
    )
    effective_resolution = _resolve_branch_resolution(
        resolution if resolution is not None else controls.get("resolution"),
        settings,
    )
    branches = _build_seed_branches(
        db,
        seeds,
        settings=settings,
        max_branches=max_branches,
        temperature=effective_temp,
        resolution=effective_resolution,
        lens_id=lens_id,
    )
    branches = _apply_branch_controls(branches, controls, db=db, lens_id=lens_id)
    branch_outcomes = _load_branch_outcome_map(db, lens_id=lens_id, days=60)
    enriched_branches = _enrich_branches_with_outcomes(
        branches,
        branch_outcomes,
        db=db,
        lens_id=lens_id,
    )
    enriched_branches = _apply_branch_auto_lifecycle(enriched_branches)
    return {
        "lens_id": lens_id,
        "lens_name": lens.get("name"),
        "context_type": lens.get("context_type"),
        "seed_count": len(seeds),
        "temperature": round(effective_temp, 3),
        "resolution": round(effective_resolution, 3),
        "generated_at": datetime.utcnow().isoformat(),
        "branches": enriched_branches,
    }


def _build_topic_keyword_cold_start_summary(
    lens: dict[str, Any],
    *,
    seed_count: int,
    lexical_count: int,
    graph_count: int,
    external_lane_counts: dict[str, int],
) -> Optional[dict[str, Any]]:
    if str(lens.get("context_type") or "") != "topic_keyword":
        return None
    config = lens.get("context_config") or {}
    keyword = str(config.get("keyword") or config.get("query") or "").strip()
    cold_start_results = int(external_lane_counts.get("cold_start_topic") or 0)
    if seed_count <= 0 and cold_start_results >= 3:
        state = "validated"
    elif seed_count <= 0 and cold_start_results > 0:
        state = "partial"
    elif seed_count <= 0:
        state = "blocked"
    elif cold_start_results > 0:
        state = "hybrid"
    else:
        state = "seeded"
    return {
        "keyword": keyword,
        "seed_count": int(seed_count),
        "lexical_results": int(lexical_count),
        "graph_results": int(graph_count),
        "external_results": cold_start_results,
        "state": state,
    }
