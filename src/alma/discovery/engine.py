"""Similar-paper retrieval and the Library read helpers shared with it.

``discover_similar_with_meta`` backs ``POST /discovery/similar`` (OpenAlex related
works, a dense fallback, the shared 8-signal scorer and a diversity interleaver).
``application/openalex_manual.py`` reuses the settings, Library and preference
readers here. Lens refreshes do not run through this module:
they live in ``application/discovery``.

Star-rating weighted preferences: publications rated 4-5 are strong positive
signals, 1-2 are negative signals, and 3/unrated are neutral.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import uuid
from collections import defaultdict
from typing import Any

from alma.core.paper_groups import (
    resolve_paper_root_id,
)
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.sqlite_config import SQLITE_CONNECT_TIMEOUT_S, apply_busy_timeout
from alma.core.time import utcnow
from alma.core.utils import candidate_dedup_key
from alma.discovery import openalex_related
from alma.discovery import similarity as sim_module
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS
from alma.discovery.scoring import (
    compute_preference_profile,
    measure_candidate,
)

logger = logging.getLogger(__name__)


DEFAULTS: dict[str, str] = dict(DISCOVERY_SETTINGS_DEFAULTS)


def connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with row-factory enabled."""
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=SQLITE_CONNECT_TIMEOUT_S)
    conn.row_factory = sqlite3.Row
    apply_busy_timeout(conn)
    # Re-assert WAL (persisted in the header, but cheap to confirm) so this
    # lane matches the canonical contract in alma.api.deps.open_db_connection
    # and never silently runs against a rollback-journal DB.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def load_settings(conn: sqlite3.Connection) -> dict[str, str]:
    """Read discovery settings over the built-in defaults."""
    kv: dict[str, str] = dict(DEFAULTS)
    for r in conn.execute("SELECT key, value FROM discovery_settings").fetchall():
        kv[r["key"]] = r["value"]
    return kv


def get_library_papers(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all papers with status='library' (saved papers)."""
    try:
        rows = conn.execute(
            f"""SELECT id, title, abstract, url, doi, authors, journal, year
               FROM papers
               WHERE status = 'library'
                 AND {standalone_paper_sql("papers")}
               ORDER BY added_at DESC"""
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError as exc:
        logger.warning("Failed to query library papers: %s", exc)
        return []


def get_rated_publications(conn: sqlite3.Connection) -> tuple[list[dict], list[dict]]:
    """Fetch rated papers and split into positive and negative groups."""
    positive: list[dict] = []
    negative: list[dict] = []

    try:
        rows = conn.execute(
            f"""SELECT id, title, abstract, url, doi, authors, journal, year, rating
               FROM papers
               WHERE rating > 0
                 AND {standalone_paper_sql("papers")}"""
        ).fetchall()
        for r in rows:
            d = dict(r)
            rating = d.get("rating", 0)
            if rating >= 4:
                positive.append(d)
            elif rating <= 2:
                negative.append(d)
    except Exception as exc:
        logger.warning("Failed to fetch rated papers: %s", exc)

    return positive, negative


def build_preference_profile(
    conn: sqlite3.Connection,
    positive_pubs: list[dict],
    negative_pubs: list[dict],
    settings: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the discovery preference profile via scoring.py."""
    return compute_preference_profile(conn, positive_pubs, negative_pubs, settings)


def score_discovery_candidate(
    candidate: dict,
    preference_profile: dict[str, Any],
    positive_centroid,
    negative_centroid,
    positive_texts: list[str] | None,
    negative_texts: list[str] | None,
    conn: sqlite3.Connection,
    settings: dict[str, str] | None = None,
) -> tuple[float, dict[str, Any]]:
    """Measure then rank one discovery candidate, through the one ranker.

    The legacy engine takes the same two steps as the lens pipeline
    (``measure_candidate`` → ``rank_candidate``) so a paper it scores is
    directly comparable with one Discovery scored.
    """
    from alma.application.discovery.calibration import load_calibration
    from alma.application.discovery.ranker import rank_candidate

    calibration = load_calibration(conn)
    candidate["score_breakdown"] = measure_candidate(
        candidate,
        preference_profile,
        positive_centroid,
        negative_centroid,
        positive_texts,
        negative_texts,
        conn,
        settings,
        calibration=calibration,
    )
    return rank_candidate(
        candidate,
        timestamp=utcnow().isoformat(),
        scoring_settings=settings,
        calibration=calibration,
    )


_AUTHOR_SPLIT_RE = re.compile(r"\s*(?:[;,]|\band\b|\&)\s*", re.IGNORECASE)


def _candidate_authors_for_diversity(candidate: dict) -> set[str]:
    """Return the first + last author keys we use to detect dominance.

    Author strings arrive in inconsistent shapes — semicolon-separated
    from S2, comma-separated from OpenAlex normalisation, sometimes
    "et al" or " and " conjoined. Split on any of those delimiters so
    the cap actually catches dominant first/last authors.
    """
    authors_str = str(candidate.get("authors") or "").strip()
    if not authors_str:
        return set()
    parts = [p.strip().lower() for p in _AUTHOR_SPLIT_RE.split(authors_str) if p.strip()]
    parts = [p for p in parts if p and p != "et al" and p != "et al."]
    if not parts:
        return set()
    out = {parts[0]}
    if len(parts) > 1:
        out.add(parts[-1])
    return out


def diversity_interleave(
    candidates: list[dict],
    max_results: int,
    *,
    max_per_author: int = 2,
    max_per_source_key: int | None = None,
) -> list[dict]:
    """Interleave candidates from different source types for diversity,
    then apply author and per-source-key caps so no single dominant
    author or external query oversaturates the staged top-K. Candidates
    that exceed a cap are not dropped — they're moved to an overflow
    queue and appended after every other candidate has had a chance to
    place. This way the lens always staffs `max_results` if the raw
    pool is large enough; we only reorder.

    `max_per_author` defaults to 2 (first/last author, individually).
    `max_per_source_key` defaults to ceil(max_results / 4): no single
    external search query can supply more than ~25% of the staged set.
    """
    if max_per_source_key is None:
        max_per_source_key = max(1, (max_results + 3) // 4)

    groups: dict[str, list[dict]] = defaultdict(list)
    for c in candidates:
        groups[c.get("source_type", "unknown")].append(c)

    for key in groups:
        groups[key].sort(key=lambda x: x.get("score", 0), reverse=True)

    group_order = sorted(
        groups.keys(),
        key=lambda k: groups[k][0].get("score", 0) if groups[k] else 0,
        reverse=True,
    )

    primary: list[dict] = []
    overflow: list[dict] = []
    group_idx: dict[str, int] = {k: 0 for k in group_order}
    author_counts: dict[str, int] = {}
    source_key_counts: dict[str, int] = {}

    def _accept(cand: dict) -> bool:
        # Per-author cap (first OR last author exceeding the cap blocks).
        for author in _candidate_authors_for_diversity(cand):
            if author_counts.get(author, 0) >= max_per_author:
                return False
        # Per-source-key cap. The key is an external query identifier
        # (`taste_author:smith`, `taste_topic:visual cortex`, …) — a
        # generic lane label like "vector" or "lexical" is not a source
        # key in this sense and shouldn't share a 25% bucket. We skip
        # the cap when only the lane label is available so the vector /
        # lexical lanes don't artificially throttle each other.
        raw_source_key = (cand.get("source_key") or "").strip()
        cap_source_key = raw_source_key if raw_source_key not in {"", "lens_retrieval"} else None
        if cap_source_key and source_key_counts.get(cap_source_key, 0) >= max_per_source_key:
            return False
        for author in _candidate_authors_for_diversity(cand):
            author_counts[author] = author_counts.get(author, 0) + 1
        if cap_source_key:
            source_key_counts[cap_source_key] = source_key_counts.get(cap_source_key, 0) + 1
        return True

    # Round-robin across source_types so each lane gets representation,
    # but only commit a candidate if it doesn't violate the diversity
    # caps. Caps-violators land in overflow and backfill at the end.
    while len(primary) < max_results:
        added_this_round = False
        for key in group_order:
            if group_idx[key] >= len(groups[key]):
                continue
            cand = groups[key][group_idx[key]]
            group_idx[key] += 1
            added_this_round = True
            if _accept(cand):
                primary.append(cand)
                if len(primary) >= max_results:
                    break
            else:
                overflow.append(cand)
        if not added_this_round:
            break

    # Backfill from overflow (still ranked by original score) if the
    # diversity pass left empty slots — better to seat dominant-author
    # papers than to return fewer recs.
    if len(primary) < max_results:
        overflow.sort(key=lambda x: x.get("score", 0), reverse=True)
        for cand in overflow:
            if len(primary) >= max_results:
                break
            primary.append(cand)

    return primary


def get_existing_recommendation_titles(conn: sqlite3.Connection) -> set[str]:
    """Return titles already present in the recommendations table."""
    try:
        rows = conn.execute(
            f"""SELECT p.title
               FROM recommendations r
               JOIN papers p ON r.paper_id = p.id
               WHERE {standalone_paper_sql('p')}"""
        ).fetchall()
        return {(r["title"] or "").strip().lower() for r in rows}
    except sqlite3.OperationalError:
        return set()


# D-7: one canonical in-memory dedup key (canonical_doi -> doi -> openalex_id ->
# year+title -> url -> title), shared with discovery/source_search. The old
# DOI->URL->title variant ignored openalex_id + year, so the same paper keyed
# differently between the retrieval merge and this skip-set and dedup missed.
canonical_candidate_key = candidate_dedup_key


def get_existing_recommendation_keys(conn: sqlite3.Connection) -> set[str]:
    """Return canonical identity keys from existing recommendations."""
    try:
        rows = conn.execute(
            f"""SELECT p.title, p.url, p.doi, p.openalex_id, p.year
               FROM recommendations r
               JOIN papers p ON r.paper_id = p.id
               WHERE {standalone_paper_sql('p')}"""
        ).fetchall()
        return {
            candidate_dedup_key(
                {
                    "title": r["title"],
                    "url": r["url"],
                    "doi": r["doi"],
                    "openalex_id": r["openalex_id"],
                    "year": r["year"],
                }
            )
            for r in rows
        }
    except sqlite3.OperationalError:
        return set()


def publication_text(pub: dict) -> str:
    """Build a text string from title + abstract for embedding / TF-IDF use."""
    title = (pub.get("title") or "").strip()
    abstract = (pub.get("abstract") or "").strip()
    return f"{title} {abstract}".strip()


def get_local_titles(conn: sqlite3.Connection) -> set[str]:
    """Return lowercased titles of all papers already in the DB."""
    try:
        rows = conn.execute("SELECT title FROM papers").fetchall()
        return {(r["title"] or "").strip().lower() for r in rows}
    except sqlite3.OperationalError:
        return set()


def get_local_keys(conn: sqlite3.Connection) -> set[str]:
    """Return canonical identity keys from local papers."""
    try:
        rows = conn.execute(
            "SELECT title, url, doi, openalex_id, year FROM papers"
        ).fetchall()
        return {
            candidate_dedup_key(
                {
                    "title": r["title"],
                    "url": r["url"],
                    "doi": r["doi"],
                    "openalex_id": r["openalex_id"],
                    "year": r["year"],
                }
            )
            for r in rows
        }
    except sqlite3.OperationalError:
        return set()


def merge_candidate(
    merged: dict[str, dict],
    skip_titles: set[str],
    skip_keys: set[str],
    candidate: dict,
    source_type: str,
    source_key: str,
    now: str,
    score_override: float | None = None,
) -> None:
    """Merge one candidate into the result set, preferring higher scores."""
    title_lower = (candidate.get("title") or "").strip().lower()
    if not title_lower:
        return
    candidate_key = canonical_candidate_key(candidate)
    if title_lower in skip_titles or candidate_key in skip_keys:
        return

    score = score_override if score_override is not None else candidate.get("score", 0.5)

    if candidate_key not in merged or merged[candidate_key]["score"] < score:
        merged[candidate_key] = {
            "id": uuid.uuid4().hex,
            "source_type": source_type,
            "source_key": source_key,
            "title": (candidate.get("title") or "").strip(),
            "authors": (candidate.get("authors") or "").strip(),
            "url": (candidate.get("url") or "").strip(),
            "doi": (candidate.get("doi") or "").strip(),
            "abstract": (candidate.get("abstract") or "").strip(),
            "journal": (candidate.get("journal") or "").strip(),
            "publication_date": (candidate.get("publication_date") or "").strip(),
            "openalex_id": (candidate.get("openalex_id") or "").strip(),
            "semantic_scholar_id": (candidate.get("semantic_scholar_id") or "").strip(),
            "semantic_scholar_corpus_id": str(candidate.get("semantic_scholar_corpus_id") or "").strip(),
            "specter2_embedding": candidate.get("specter2_embedding"),
            "specter2_model": candidate.get("specter2_model"),
            "score": round(score, 4),
            "year": candidate.get("year"),
            "cited_by_count": candidate.get("cited_by_count", 0),
            "tldr": (candidate.get("tldr") or "").strip(),
            "influential_citation_count": candidate.get("influential_citation_count"),
            "topics": candidate.get("topics", []),
            "created_at": now,
        }


def _recommendation_skip_sets(conn: sqlite3.Connection) -> tuple[set[str], set[str]]:
    """Build deduplication sets for recommendation generation."""
    existing_titles = get_existing_recommendation_titles(conn)
    local_titles = get_local_titles(conn)
    existing_keys = get_existing_recommendation_keys(conn)
    local_keys = get_local_keys(conn)
    return existing_titles | local_titles, existing_keys | local_keys


def _dense_fallback_candidates(
    conn: sqlite3.Connection,
    seed_papers: list[dict],
    *,
    skip_titles: set[str],
    skip_keys: set[str],
    limit: int,
) -> list[dict]:
    """Nearest-neighbour fallback over `publication_embeddings`.

    Used when the network channels (OpenAlex + S2) all return empty for
    a `discover_similar` query — typically because every plausible
    candidate is already in the Library or already on the recommendations
    table. Returns candidates in the same merge-shape the rest of the
    engine emits.
    """
    if not sim_module.has_active_embeddings(conn):
        return []
    centroid = None
    try:
        centroid = sim_module.compute_embedding_centroid(seed_papers, conn)
    except Exception as exc:
        logger.debug("dense_fallback: centroid compute failed: %s", exc)
    if centroid is None:
        return []
    try:
        import numpy as np
    except Exception:
        return []
    from numpy.linalg import norm

    centroid_norm = float(norm(centroid))
    if centroid_norm == 0.0:
        return []

    seed_ids = {str(p.get("id") or "") for p in seed_papers}
    model = sim_module.get_active_embedding_model(conn)
    try:
        rows = conn.execute(
            f"""
            SELECT p.id, p.title, p.authors, p.year, p.doi, p.url, p.abstract,
                   p.journal, p.cited_by_count, pe.embedding
            FROM publication_embeddings pe
            JOIN papers p ON pe.paper_id = p.id
            WHERE pe.model = ?
              AND COALESCE(p.status, '') NOT IN ('library', 'removed', 'dismissed')
              AND {standalone_paper_sql('p')}
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.debug("dense_fallback: embedding join failed: %s", exc)
        return []

    ranked: list[tuple[float, dict]] = []
    for row in rows:
        paper_id = str(row["id"] or "")
        if not paper_id or paper_id in seed_ids:
            continue
        row_dict = dict(row)
        title_lower = (row_dict.get("title") or "").strip().lower()
        if not title_lower or title_lower in skip_titles:
            continue
        if canonical_candidate_key(row_dict) in skip_keys:
            continue
        try:
            from alma.core.vector_blob import decode_vector
            vec = decode_vector(row["embedding"])
        except Exception:
            continue
        vec_norm = float(norm(vec))
        if vec_norm == 0.0:
            continue
        score = float(np.dot(centroid, vec) / (centroid_norm * vec_norm))
        ranked.append((score, row_dict))

    ranked.sort(key=lambda pair: pair[0], reverse=True)
    now = utcnow().isoformat()
    out: list[dict] = []
    for score, row_dict in ranked[:limit]:
        real_paper_id = str(row_dict.get("id") or "").strip()
        out.append(
            {
                # `id` is the transient merge key used by merge_candidate;
                # `paper_id` carries the real `papers.id` so the Similarity
                # route can thread it through and the frontend can open
                # `PaperDetailPanel` on click.
                "id": real_paper_id or uuid.uuid4().hex,
                "paper_id": real_paper_id or None,
                "source_type": "dense_fallback",
                "source_key": "specter2_centroid",
                "title": (row_dict.get("title") or "").strip(),
                "authors": (row_dict.get("authors") or "").strip(),
                "url": (row_dict.get("url") or "").strip(),
                "doi": (row_dict.get("doi") or "").strip(),
                "abstract": (row_dict.get("abstract") or "").strip(),
                "semantic_scholar_id": "",
                "semantic_scholar_corpus_id": "",
                "specter2_embedding": None,
                "specter2_model": None,
                "score": round(max(0.0, min(1.0, score)), 4),
                "year": row_dict.get("year"),
                "cited_by_count": row_dict.get("cited_by_count") or 0,
                "topics": [],
                "created_at": now,
            }
        )
    return out


def _discover_similar_with_meta_and_conn(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    """Core discover-similar logic returning both candidates and channel meta.

    Meta dict shape:

        {
            "channels": [
                {"name": "openalex_related", "fetched": int,
                 "skipped_as_existing": int, "error": str | None},
                ...
            ],
            "dense_fallback_used": bool,
            "seeds_with_doi": int,
            "seeds_with_s2_id": int,
            "seed_count": int,
        }
    """
    settings = load_settings(conn)
    per_strategy = int(settings.get("limits.max_candidates_per_strategy", "20"))
    s2_source_enabled = (
        settings.get("sources.semantic_scholar.enabled", "true").lower() == "true"
    )

    seed_papers: list[dict] = []
    for paper_id in paper_ids:
        try:
            root_id = resolve_paper_root_id(conn, str(paper_id), strict=False)
            row = conn.execute(
                f"""SELECT id, title, abstract, url, doi, authors, journal, year,
                          semantic_scholar_id, semantic_scholar_corpus_id
                   FROM papers
                   WHERE id = ?
                     AND {standalone_paper_sql('papers')}
                   LIMIT 1""",
                (root_id,),
            ).fetchone()
            if row:
                seed_papers.append(dict(row))
        except sqlite3.OperationalError as exc:
            logger.debug("Failed to look up seed paper '%s': %s", paper_id, exc)

    meta: dict[str, Any] = {
        "channels": [],
        "dense_fallback_used": False,
        "seeds_with_doi": 0,
        "seeds_with_s2_id": 0,
        "seed_count": len(seed_papers),
    }

    if not seed_papers:
        logger.info("discover_similar: no seed papers found for the given IDs")
        return [], meta

    meta["seeds_with_doi"] = sum(
        1 for seed in seed_papers if (seed.get("doi") or "").strip()
    )
    meta["seeds_with_s2_id"] = sum(
        1
        for seed in seed_papers
        if (seed.get("semantic_scholar_id") or "").strip()
        or (seed.get("semantic_scholar_corpus_id") or "").strip()
    )

    skip_titles, skip_keys = _recommendation_skip_sets(conn)
    now = utcnow().isoformat()
    merged: dict[str, dict] = {}

    def _merge_batch(
        works: list[dict],
        *,
        source_type: str,
        source_key: str,
    ) -> tuple[int, int]:
        fetched = 0
        skipped = 0
        for work in works:
            fetched += 1
            title_lower = (work.get("title") or "").strip().lower()
            if not title_lower:
                skipped += 1
                continue
            cand_key = canonical_candidate_key(work)
            if title_lower in skip_titles or cand_key in skip_keys:
                skipped += 1
                continue
            merge_candidate(
                merged,
                skip_titles,
                skip_keys,
                work,
                source_type=source_type,
                source_key=source_key,
                now=now,
            )
        return fetched, skipped

    openalex_related_fetched = 0
    openalex_related_skipped = 0
    openalex_citing_fetched = 0
    openalex_citing_skipped = 0
    s2_forpaper_fetched = 0
    s2_forpaper_skipped = 0
    s2_forpaper_error: str | None = None

    for seed in seed_papers:
        doi = (seed.get("doi") or "").strip()
        s2_id = (seed.get("semantic_scholar_id") or "").strip()
        corpus_id = str(seed.get("semantic_scholar_corpus_id") or "").strip()

        if doi:
            try:
                related = openalex_related.fetch_related_works(doi, limit=per_strategy)
            except Exception as exc:
                related = []
                logger.debug(
                    "discover_similar: OpenAlex related_works failed for %s: %s",
                    doi,
                    exc,
                )
            f, s = _merge_batch(
                related, source_type="openalex_related", source_key=doi
            )
            openalex_related_fetched += f
            openalex_related_skipped += s

            try:
                citing = openalex_related.fetch_citing_works(doi, limit=per_strategy)
            except Exception as exc:
                citing = []
                logger.debug(
                    "discover_similar: OpenAlex citing_works failed for %s: %s",
                    doi,
                    exc,
                )
            f, s = _merge_batch(
                citing, source_type="citation_chain", source_key=doi
            )
            openalex_citing_fetched += f
            openalex_citing_skipped += s

        if s2_source_enabled and (s2_id or doi or corpus_id):
            # Prefer native S2 paperId; DOI: and CorpusID: forms also accepted.
            seed_id = s2_id or (f"DOI:{doi}" if doi else f"CorpusID:{corpus_id}")
            from alma.discovery import semantic_scholar as s2_mod

            try:
                s2_candidates = s2_mod.recommend_for_paper(
                    seed_id, limit=per_strategy
                )
            except Exception as exc:
                s2_candidates = []
                s2_forpaper_error = str(exc)
                logger.debug(
                    "discover_similar: S2 recommend_for_paper failed for %s: %s",
                    seed_id,
                    exc,
                )
            f, s = _merge_batch(
                s2_candidates,
                source_type="semantic_scholar_recommend",
                source_key=seed_id,
            )
            s2_forpaper_fetched += f
            s2_forpaper_skipped += s

    meta["channels"] = [
        {
            "name": "openalex_related",
            "fetched": openalex_related_fetched,
            "skipped_as_existing": openalex_related_skipped,
            "error": None,
        },
        {
            "name": "openalex_citing",
            "fetched": openalex_citing_fetched,
            "skipped_as_existing": openalex_citing_skipped,
            "error": None,
        },
        {
            "name": "semantic_scholar_recommend",
            "fetched": s2_forpaper_fetched,
            "skipped_as_existing": s2_forpaper_skipped,
            "error": s2_forpaper_error,
        },
    ]

    # Dense-fallback kicks in when every network channel returned zero
    # usable candidates — typically a saturated library where the
    # external lookups all hit the skip sets. Per `tasks/14` T2 plan +
    # D-AUDIT-4 resolution.
    if not merged:
        fallback = _dense_fallback_candidates(
            conn,
            seed_papers,
            skip_titles=skip_titles,
            skip_keys=skip_keys,
            limit=max(limit, per_strategy),
        )
        if fallback:
            meta["dense_fallback_used"] = True
            for candidate in fallback:
                merge_candidate(
                    merged,
                    skip_titles,
                    skip_keys,
                    candidate,
                    source_type="dense_fallback",
                    source_key="specter2_centroid",
                    now=now,
                )

    if not merged:
        logger.info("discover_similar: no candidates found (network + dense)")
        return [], meta

    preference_profile = build_preference_profile(
        conn,
        positive_pubs=seed_papers,
        negative_pubs=[],
        settings=settings,
    )

    positive_centroid = None
    negative_centroid = None
    positive_texts = [publication_text(pub) for pub in seed_papers]
    positive_texts = [text for text in positive_texts if text]
    negative_texts: list[str] = []
    if sim_module.has_active_embeddings(conn):
        try:
            positive_centroid = sim_module.compute_embedding_centroid(seed_papers, conn)
        except Exception as exc:
            logger.debug("discover_similar: failed to compute seed centroid: %s", exc)

    scored_candidates: list[dict] = []
    for candidate in merged.values():
        full_score, breakdown = score_discovery_candidate(
            candidate,
            preference_profile,
            positive_centroid,
            negative_centroid,
            positive_texts,
            negative_texts,
            conn,
            settings,
        )
        candidate["score"] = round(full_score, 4)
        candidate["score_breakdown"] = json.dumps(breakdown)
        scored_candidates.append(candidate)

    interleaved = diversity_interleave(scored_candidates, limit)
    logger.info(
        "discover_similar: returning %d results from %d candidates (dense_fallback=%s)",
        len(interleaved),
        len(merged),
        meta["dense_fallback_used"],
    )
    return interleaved, meta


def discover_similar_with_meta(
    db_path: str, paper_ids: list[str], limit: int = 20
) -> tuple[list[dict], dict[str, Any]]:
    """Discover candidates + channel metadata (new form used by the route).

    Returns ``(candidates, meta)`` where ``meta`` exposes per-channel
    fetch/skip counts, whether the dense-SPECTER2 fallback triggered,
    and how many seeds carried a DOI / S2 identifier.
    """
    conn = connect(db_path)
    try:
        return _discover_similar_with_meta_and_conn(conn, paper_ids, limit)
    except Exception as exc:
        logger.error("discover_similar_with_meta failed: %s", exc)
        return [], {
            "channels": [],
            "dense_fallback_used": False,
            "seeds_with_doi": 0,
            "seeds_with_s2_id": 0,
            "seed_count": 0,
            "error": str(exc),
        }
    finally:
        conn.close()

