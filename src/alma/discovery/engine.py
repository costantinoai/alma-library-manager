"""Discovery engine: generates paper recommendations from external sources.

Uses five retrieval strategies to surface papers the user does NOT already
have in their database:

1. **OpenAlex related works** -- for each positively-rated paper with a DOI.
2. **OpenAlex topic search**  -- using the user's preferred topics.
3. **Followed author works**  -- recent papers from explicitly followed authors.
4. **Co-author network**      -- papers by frequent co-authors of tracked authors.
5. **Citation chain**          -- papers that cite the user's 5-star papers.

Candidates are scored with an 8-signal system (source relevance, topic overlap,
semantic text similarity, author affinity, journal affinity, recency boost,
citation quality, preference affinity from Signal Lab) plus a feedback
adjustment from past recommendation interactions.  A diversity interleaver
ensures the final list draws from multiple source types rather than being
dominated by a single strategy.

Star-rating weighted preferences: publications rated 4-5 are strong positive
signals, 1-2 are negative signals, and 3/unrated are neutral.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import struct
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.discovery import openalex_related
from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL
from alma.discovery import similarity as sim_module
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS
from alma.discovery.scoring import (
    parse_author_names as _parse_author_names,
    author_affinity_keys as _author_affinity_keys,
    compute_centroid_from_ids as _compute_centroid_from_ids,
    compute_preference_profile,
    score_candidate,
)
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)


DEFAULTS: Dict[str, str] = dict(DISCOVERY_SETTINGS_DEFAULTS)


def _upsert_s2_specter2_embedding(conn: sqlite3.Connection, paper_id: str, rec: dict) -> None:
    vector = rec.get("specter2_embedding")
    if not isinstance(vector, list) or not vector:
        return
    try:
        values = [float(value) for value in vector]
        blob = struct.pack(f"<{len(values)}f", *values)
    except (TypeError, ValueError, struct.error):
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO publication_embeddings (paper_id, embedding, model, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            paper_id,
            blob,
            S2_SPECTER2_MODEL,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
            datetime.utcnow().isoformat(),
        ),
    )


def connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with row-factory enabled."""
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def load_settings(conn: sqlite3.Connection) -> Dict[str, str]:
    """Read discovery settings, falling back to defaults when needed."""
    kv: Dict[str, str] = dict(DEFAULTS)
    try:
        rows = conn.execute("SELECT key, value FROM discovery_settings").fetchall()
        for r in rows:
            kv[r["key"]] = r["value"]
    except sqlite3.OperationalError:
        pass
    return kv


def get_library_papers(conn: sqlite3.Connection) -> List[dict]:
    """Fetch all papers with status='library' (saved papers)."""
    try:
        rows = conn.execute(
            """SELECT id, title, abstract, url, doi, authors, journal, year
               FROM papers
               WHERE status = 'library'
               ORDER BY added_at DESC"""
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError as exc:
        logger.warning("Failed to query library papers: %s", exc)
        return []


def get_rated_publications(conn: sqlite3.Connection) -> Tuple[List[dict], List[dict]]:
    """Fetch rated papers and split into positive and negative groups."""
    positive: List[dict] = []
    negative: List[dict] = []

    try:
        rows = conn.execute(
            """SELECT id, title, abstract, url, doi, authors, journal, year, rating
               FROM papers
               WHERE rating > 0"""
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
    positive_pubs: List[dict],
    negative_pubs: List[dict],
    settings: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build the discovery preference profile via scoring.py."""
    return compute_preference_profile(conn, positive_pubs, negative_pubs, settings)


def score_discovery_candidate(
    candidate: dict,
    preference_profile: Dict[str, Any],
    positive_centroid,
    negative_centroid,
    positive_texts: Optional[List[str]],
    negative_texts: Optional[List[str]],
    conn: sqlite3.Connection,
    settings: Optional[Dict[str, str]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """Score one discovery candidate via scoring.py."""
    return score_candidate(
        candidate,
        preference_profile,
        positive_centroid,
        negative_centroid,
        positive_texts,
        negative_texts,
        conn,
        settings,
    )


def diversity_interleave(candidates: List[dict], max_results: int) -> List[dict]:
    """Interleave candidates from different source types for diversity."""
    groups: Dict[str, List[dict]] = defaultdict(list)
    for c in candidates:
        groups[c.get("source_type", "unknown")].append(c)

    for key in groups:
        groups[key].sort(key=lambda x: x.get("score", 0), reverse=True)

    group_order = sorted(
        groups.keys(),
        key=lambda k: groups[k][0].get("score", 0) if groups[k] else 0,
        reverse=True,
    )

    result: List[dict] = []
    group_idx: Dict[str, int] = {k: 0 for k in group_order}

    while len(result) < max_results:
        added_this_round = False
        for key in group_order:
            if group_idx[key] < len(groups[key]):
                result.append(groups[key][group_idx[key]])
                group_idx[key] += 1
                added_this_round = True
                if len(result) >= max_results:
                    break
        if not added_this_round:
            break

    return result


def get_existing_recommendation_titles(conn: sqlite3.Connection) -> Set[str]:
    """Return titles already present in the recommendations table."""
    try:
        rows = conn.execute(
            """SELECT p.title
               FROM recommendations r
               LEFT JOIN papers p ON r.paper_id = p.id"""
        ).fetchall()
        return {(r["title"] or "").strip().lower() for r in rows}
    except sqlite3.OperationalError:
        return set()


def canonical_candidate_key(candidate: dict) -> str:
    """Canonical recommendation identity key: DOI -> URL -> title."""
    doi = normalize_doi((candidate.get("doi") or "").strip())
    if doi:
        return f"doi:{doi.lower()}"
    url = (candidate.get("url") or "").strip().lower()
    if url:
        return f"url:{url}"
    title = (candidate.get("title") or "").strip().lower()
    return f"title:{title}"


def get_existing_recommendation_keys(conn: sqlite3.Connection) -> Set[str]:
    """Return canonical identity keys from existing recommendations."""
    try:
        rows = conn.execute(
            """SELECT p.title, p.url, p.doi
               FROM recommendations r
               LEFT JOIN papers p ON r.paper_id = p.id"""
        ).fetchall()
        return {
            canonical_candidate_key(
                {
                    "title": r["title"],
                    "url": r["url"],
                    "doi": r["doi"],
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


def insert_recommendations(conn: sqlite3.Connection, recs: List[dict]) -> int:
    """Insert recommendation dicts into the database."""
    count = 0
    for rec in recs:
        try:
            paper_id = uuid.uuid4().hex
            conn.execute(
                """INSERT INTO papers (id, title, authors, url, doi, year, cited_by_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(doi) DO UPDATE SET
                       title = excluded.title,
                       authors = excluded.authors,
                       url = excluded.url,
                       year = excluded.year,
                       cited_by_count = excluded.cited_by_count""",
                (
                    paper_id,
                    rec.get("title", ""),
                    rec.get("authors", ""),
                    rec.get("url", ""),
                    rec.get("doi", ""),
                    rec.get("year"),
                    rec.get("cited_by_count", 0),
                ),
            )
            row = conn.execute(
                "SELECT id FROM papers WHERE doi = ? OR (title = ? AND authors = ?)",
                (
                    rec.get("doi", ""),
                    rec.get("title", ""),
                    rec.get("authors", ""),
                ),
            ).fetchone()
            if row:
                paper_id = row["id"]
            try:
                if rec.get("semantic_scholar_id") or rec.get("semantic_scholar_corpus_id"):
                    conn.execute(
                        """
                        UPDATE papers
                        SET semantic_scholar_id = COALESCE(NULLIF(semantic_scholar_id, ''), ?),
                            semantic_scholar_corpus_id = COALESCE(NULLIF(semantic_scholar_corpus_id, ''), ?)
                        WHERE id = ?
                        """,
                        (
                            rec.get("semantic_scholar_id") or None,
                            rec.get("semantic_scholar_corpus_id") or None,
                            paper_id,
                        ),
                    )
                _upsert_s2_specter2_embedding(conn, paper_id, rec)
            except sqlite3.OperationalError:
                pass

            rec_id = uuid.uuid4().hex
            conn.execute(
                """INSERT OR IGNORE INTO recommendations
                   (id, suggestion_set_id, lens_id, paper_id, rank,
                    score, score_breakdown, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rec_id,
                    None,
                    None,
                    paper_id,
                    count,
                    rec["score"],
                    rec.get("score_breakdown", ""),
                    rec["created_at"],
                ),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass
        except Exception as exc:
            logger.debug("Failed to insert recommendation: %s", exc)
    conn.commit()
    return count


def get_local_titles(conn: sqlite3.Connection) -> Set[str]:
    """Return lowercased titles of all papers already in the DB."""
    try:
        rows = conn.execute("SELECT title FROM papers").fetchall()
        return {(r["title"] or "").strip().lower() for r in rows}
    except sqlite3.OperationalError:
        return set()


def get_local_keys(conn: sqlite3.Connection) -> Set[str]:
    """Return canonical identity keys from local papers."""
    try:
        rows = conn.execute("SELECT title, url, doi FROM papers").fetchall()
        return {
            canonical_candidate_key(
                {"title": r["title"], "url": r["url"], "doi": r["doi"]}
            )
            for r in rows
        }
    except sqlite3.OperationalError:
        return set()


def merge_candidate(
    merged: Dict[str, dict],
    skip_titles: Set[str],
    skip_keys: Set[str],
    candidate: dict,
    source_type: str,
    source_key: str,
    now: str,
    score_override: Optional[float] = None,
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
            "semantic_scholar_id": (candidate.get("semantic_scholar_id") or "").strip(),
            "semantic_scholar_corpus_id": str(candidate.get("semantic_scholar_corpus_id") or "").strip(),
            "specter2_embedding": candidate.get("specter2_embedding"),
            "specter2_model": candidate.get("specter2_model"),
            "score": round(score, 4),
            "year": candidate.get("year"),
            "cited_by_count": candidate.get("cited_by_count", 0),
            "topics": candidate.get("topics", []),
            "created_at": now,
        }


def _recommendation_skip_sets(conn: sqlite3.Connection) -> Tuple[Set[str], Set[str]]:
    """Build deduplication sets for recommendation generation."""
    existing_titles = get_existing_recommendation_titles(conn)
    local_titles = get_local_titles(conn)
    existing_keys = get_existing_recommendation_keys(conn)
    local_keys = get_local_keys(conn)
    return existing_titles | local_titles, existing_keys | local_keys


def _generate_with_conn(conn: sqlite3.Connection, max_results: int) -> List[dict]:
    """Core recommendation logic for a live DB connection."""
    settings = load_settings(conn)
    effective_max = int(settings.get("limits.max_results", "50"))
    if max_results and max_results < effective_max:
        effective_max = max_results
    per_strategy = int(settings.get("limits.max_candidates_per_strategy", "20"))

    strat_related = settings.get("strategies.related_works", "true").lower() == "true"
    strat_topic = settings.get("strategies.topic_search", "true").lower() == "true"
    strat_followed = settings.get("strategies.followed_authors", "true").lower() == "true"
    strat_coauthor = settings.get("strategies.coauthor_network", "true").lower() == "true"
    strat_citation = settings.get("strategies.citation_chain", "true").lower() == "true"
    strat_s2 = settings.get("strategies.semantic_scholar", "true").lower() == "true"

    positive, negative = get_rated_publications(conn)
    if not positive:
        saved = get_library_papers(conn)
        if not saved:
            logger.info("No library/rated papers found; cannot generate recommendations")
            return []
        positive = saved
        negative = []

    preference_profile = build_preference_profile(conn, positive, negative, settings)

    positive_centroid = None
    negative_centroid = None
    positive_texts = [publication_text(pub) for pub in positive]
    positive_texts = [text for text in positive_texts if text]
    negative_texts = [publication_text(pub) for pub in negative]
    negative_texts = [text for text in negative_texts if text]
    if sim_module.has_active_embeddings(conn):
        try:
            positive_centroid = sim_module.compute_embedding_centroid(positive, conn)
        except Exception as exc:
            logger.debug("Failed to compute positive centroid: %s", exc)
        if negative:
            try:
                negative_centroid = sim_module.compute_embedding_centroid(negative, conn)
            except Exception as exc:
                logger.debug("Failed to compute negative centroid: %s", exc)

    skip_titles, skip_keys = _recommendation_skip_sets(conn)
    now = datetime.utcnow().isoformat()
    merged: Dict[str, dict] = {}
    current_year = datetime.utcnow().year

    followed_author_ids: List[Tuple[str, str]] = []
    if strat_followed:
        try:
            for row in conn.execute("SELECT author_id FROM followed_authors").fetchall():
                author_table_id = row["author_id"]
                try:
                    author = conn.execute(
                        "SELECT openalex_id FROM authors WHERE id = ?",
                        (author_table_id,),
                    ).fetchone()
                    if author and (author["openalex_id"] or "").strip():
                        followed_author_ids.append(
                            (author_table_id, author["openalex_id"].strip())
                        )
                except Exception:
                    pass
        except Exception:
            pass

    coauthor_data: Optional[Tuple[str, Any]] = None
    if strat_coauthor:
        try:
            tracked_oa_ids: Set[str] = set()
            tracked_author_ids: Set[str] = set()
            for pub in positive:
                author_id = (pub.get("author_id") or "").strip()
                if author_id:
                    tracked_author_ids.add(author_id)
            for author_id in tracked_author_ids:
                try:
                    row = conn.execute(
                        "SELECT openalex_id FROM authors WHERE id = ?",
                        (author_id,),
                    ).fetchone()
                    if row and (row["openalex_id"] or "").strip():
                        tracked_oa_ids.add(row["openalex_id"].strip())
                except Exception:
                    pass

            coauthor_rows = conn.execute(
                """SELECT DISTINCT pa.openalex_id, pa.display_name
                   FROM publication_authors pa
                   JOIN papers p ON pa.paper_id = p.id
                   WHERE pa.openalex_id IS NOT NULL
                     AND pa.openalex_id != ''
                   LIMIT 20""",
            ).fetchall()
            coauthors_with_ids = [
                (row["openalex_id"], row["display_name"])
                for row in coauthor_rows
                if row["openalex_id"] not in tracked_oa_ids
            ][:10]

            if coauthors_with_ids:
                coauthor_data = ("ids", coauthors_with_ids)
            else:
                coauthor_names: Set[str] = set()
                for pub in positive:
                    authors_str = (pub.get("authors") or "").strip()
                    if not authors_str:
                        continue
                    for author_name in authors_str.split(","):
                        author_name = author_name.strip()
                        if author_name:
                            coauthor_names.add(author_name)
                coauthor_list = list(coauthor_names)[:10]
                if coauthor_list:
                    coauthor_data = ("names", coauthor_list)
        except Exception:
            pass

    def _strat_related() -> Dict[str, dict]:
        local_merged: Dict[str, dict] = {}
        dois_tried: Set[str] = set()
        for pub in positive:
            doi = (pub.get("doi") or "").strip()
            if not doi or doi in dois_tried:
                continue
            dois_tried.add(doi)
            for work in openalex_related.fetch_related_works(doi, limit=per_strategy):
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    work,
                    source_type="openalex_related",
                    source_key=doi,
                    now=now,
                )
            if len(dois_tried) >= 10:
                break
        return local_merged

    def _strat_topic() -> Dict[str, dict]:
        local_merged: Dict[str, dict] = {}
        topics = [
            topic
            for topic in preference_profile.get("topic_weights", {})
            if preference_profile["topic_weights"].get(topic, 0) > 0
        ]
        if not topics:
            return local_merged
        for work in openalex_related.search_works_by_topics(
            topics[:10],
            limit=per_strategy,
            from_year=current_year - 2,
        ):
            merge_candidate(
                local_merged,
                skip_titles,
                skip_keys,
                work,
                source_type="openalex_topic",
                source_key=",".join(topics[:5]),
                now=now,
                score_override=work.get("score", 0.3) * 0.8,
            )
        return local_merged

    def _strat_followed() -> Dict[str, dict]:
        local_merged: Dict[str, dict] = {}
        if not followed_author_ids:
            return local_merged
        openalex_to_author_id: Dict[str, str] = {}
        all_openalex_ids: List[str] = []
        for author_table_id, openalex_id in followed_author_ids:
            openalex_to_author_id[openalex_id] = author_table_id
            all_openalex_ids.append(openalex_id)
        if len(all_openalex_ids) == 1:
            openalex_id = all_openalex_ids[0]
            author_table_id = openalex_to_author_id.get(openalex_id, openalex_id)
            try:
                works = openalex_related.fetch_recent_works_for_author(
                    openalex_id,
                    from_year=current_year - 2,
                    limit=per_strategy,
                )
            except Exception:
                works = []
            for work in works:
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    work,
                    source_type="followed_author",
                    source_key=author_table_id,
                    now=now,
                )
            return local_merged

        batch_results = openalex_related.batch_fetch_recent_works_for_authors(
            all_openalex_ids,
            from_year=current_year - 2,
            per_author_limit=per_strategy,
        )
        seen_openalex_ids: Set[str] = set()
        for openalex_id, works in batch_results.items():
            seen_openalex_ids.add(openalex_id)
            author_table_id = openalex_to_author_id.get(openalex_id, openalex_id)
            for work in works:
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    work,
                    source_type="followed_author",
                    source_key=author_table_id,
                    now=now,
                )

        for author_table_id, openalex_id in followed_author_ids:
            if openalex_id in seen_openalex_ids:
                continue
            try:
                works = openalex_related.fetch_recent_works_for_author(
                    openalex_id,
                    from_year=current_year - 2,
                    limit=per_strategy,
                )
            except Exception:
                works = []
            for work in works:
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    work,
                    source_type="followed_author",
                    source_key=author_table_id,
                    now=now,
                )
        return local_merged

    def _strat_coauthor() -> Dict[str, dict]:
        local_merged: Dict[str, dict] = {}
        if not coauthor_data:
            return local_merged
        if coauthor_data[0] == "ids":
            coauthor_ids = coauthor_data[1]
            source_key = ",".join(name for _, name in coauthor_ids[:3])
            batch_results = openalex_related.batch_fetch_recent_works_for_authors(
                [openalex_id for openalex_id, _name in coauthor_ids],
                from_year=current_year - 2,
                per_author_limit=per_strategy,
            )
            for works in batch_results.values():
                for work in works:
                    merge_candidate(
                        local_merged,
                        skip_titles,
                        skip_keys,
                        work,
                        source_type="coauthor_network",
                        source_key=source_key,
                        now=now,
                    )
        else:
            coauthor_names = coauthor_data[1]
            for work in openalex_related.search_works_by_topics(
                coauthor_names,
                limit=per_strategy,
                from_year=current_year - 2,
            ):
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    work,
                    source_type="coauthor_network",
                    source_key=",".join(coauthor_names[:3]),
                    now=now,
                )
        return local_merged

    def _strat_citation() -> Dict[str, dict]:
        local_merged: Dict[str, dict] = {}
        five_star_dois: Set[str] = set()
        for pub in positive:
            rating = pub.get("rating", 0)
            if rating and rating >= 5:
                doi = (pub.get("doi") or "").strip()
                if doi:
                    five_star_dois.add(doi)
        for doi in list(five_star_dois)[:5]:
            for work in openalex_related.fetch_citing_works(doi, limit=per_strategy):
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    work,
                    source_type="citation_chain",
                    source_key=doi,
                    now=now,
                )
        return local_merged

    def _strat_semantic_scholar() -> Dict[str, dict]:
        local_merged: Dict[str, dict] = {}
        try:
            from alma.discovery import semantic_scholar as s2

            topics = [
                topic
                for topic in preference_profile.get("topic_weights", {})
                if preference_profile["topic_weights"].get(topic, 0) > 0
            ]
            if not topics:
                return local_merged
            query = " ".join(topics[:5])
            for paper in s2.search_papers(query, limit=per_strategy):
                merge_candidate(
                    local_merged,
                    skip_titles,
                    skip_keys,
                    paper,
                    source_type="semantic_scholar",
                    source_key=query,
                    now=now,
                    score_override=paper.get("score", 0.3) * 0.8,
                )
        except Exception as exc:
            logger.debug("Semantic Scholar strategy failed: %s", exc)
        return local_merged

    from concurrent.futures import ThreadPoolExecutor, as_completed

    strategy_fns = []
    if strat_related:
        strategy_fns.append(("related_works", _strat_related))
    if strat_topic:
        strategy_fns.append(("topic_search", _strat_topic))
    if strat_followed:
        strategy_fns.append(("followed_authors", _strat_followed))
    if strat_coauthor:
        strategy_fns.append(("coauthor_network", _strat_coauthor))
    if strat_citation:
        strategy_fns.append(("citation_chain", _strat_citation))
    if strat_s2:
        strategy_fns.append(("semantic_scholar", _strat_semantic_scholar))

    workers = min(len(strategy_fns), 6) if strategy_fns else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fn): name for name, fn in strategy_fns}
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                for key, candidate in result.items():
                    if key not in merged or merged[key]["score"] < candidate["score"]:
                        merged[key] = candidate
            except Exception as exc:
                logger.debug("Strategy %s failed: %s", name, exc)

    if not merged:
        logger.info("No new recommendations generated")
        return []

    scored_candidates: List[dict] = []
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

    interleaved = diversity_interleave(scored_candidates, effective_max)
    inserted = insert_recommendations(conn, interleaved)
    logger.info(
        "Generated %d new recommendations (%d inserted) from %d candidates",
        len(interleaved),
        inserted,
        len(merged),
    )
    return interleaved


def _dense_fallback_candidates(
    conn: sqlite3.Connection,
    seed_papers: List[dict],
    *,
    skip_titles: Set[str],
    skip_keys: Set[str],
    limit: int,
) -> List[dict]:
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
            """
            SELECT p.id, p.title, p.authors, p.year, p.doi, p.url, p.abstract,
                   p.journal, p.cited_by_count, pe.embedding
            FROM publication_embeddings pe
            JOIN papers p ON pe.paper_id = p.id
            WHERE pe.model = ?
              AND COALESCE(p.status, '') NOT IN ('library', 'removed', 'dismissed')
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.debug("dense_fallback: embedding join failed: %s", exc)
        return []

    ranked: List[Tuple[float, dict]] = []
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
            vec = np.frombuffer(row["embedding"], dtype=np.float32)
        except Exception:
            continue
        vec_norm = float(norm(vec))
        if vec_norm == 0.0:
            continue
        score = float(np.dot(centroid, vec) / (centroid_norm * vec_norm))
        ranked.append((score, row_dict))

    ranked.sort(key=lambda pair: pair[0], reverse=True)
    now = datetime.utcnow().isoformat()
    out: List[dict] = []
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


def _discover_similar_with_conn(
    conn: sqlite3.Connection,
    paper_ids: List[str],
    limit: int,
) -> List[dict]:
    """Core discover-similar logic — list-only backward-compat wrapper."""
    candidates, _meta = _discover_similar_with_meta_and_conn(conn, paper_ids, limit)
    return candidates


def _discover_similar_with_meta_and_conn(
    conn: sqlite3.Connection,
    paper_ids: List[str],
    limit: int,
) -> Tuple[List[dict], Dict[str, Any]]:
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

    seed_papers: List[dict] = []
    for paper_id in paper_ids:
        try:
            row = conn.execute(
                """SELECT id, title, abstract, url, doi, authors, journal, year,
                          semantic_scholar_id, semantic_scholar_corpus_id
                   FROM papers
                   WHERE id = ?
                   LIMIT 1""",
                (paper_id,),
            ).fetchone()
            if row:
                seed_papers.append(dict(row))
        except sqlite3.OperationalError as exc:
            logger.debug("Failed to look up seed paper '%s': %s", paper_id, exc)

    meta: Dict[str, Any] = {
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
    now = datetime.utcnow().isoformat()
    merged: Dict[str, dict] = {}

    def _merge_batch(
        works: List[dict],
        *,
        source_type: str,
        source_key: str,
    ) -> Tuple[int, int]:
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
    s2_forpaper_error: Optional[str] = None

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
    negative_texts: List[str] = []
    if sim_module.has_active_embeddings(conn):
        try:
            positive_centroid = sim_module.compute_embedding_centroid(seed_papers, conn)
        except Exception as exc:
            logger.debug("discover_similar: failed to compute seed centroid: %s", exc)

    scored_candidates: List[dict] = []
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


def _generate_recommendations_for_db_path(db_path: str, max_results: int) -> List[dict]:
    conn = connect(db_path)
    try:
        return _generate_with_conn(conn, max_results)
    except Exception as exc:
        logger.error("Discovery engine failed: %s", exc)
        return []
    finally:
        conn.close()


def generate_recommendations(db_path: str, max_results: int = 50) -> List[dict]:
    """Generate new global recommendations for the legacy discovery endpoint."""
    return _generate_recommendations_for_db_path(db_path, max_results)


def _refresh_recommendations_for_db_path(db_path: str) -> List[dict]:
    conn = connect(db_path)
    try:
        settings = load_settings(conn)
        max_results = int(settings.get("limits.max_results", "50"))
        conn.execute("DELETE FROM recommendations WHERE user_action IS NULL")
        conn.commit()
        logger.info("Cleared neutral recommendations for refresh")
        return _generate_with_conn(conn, max_results=max_results)
    except Exception as exc:
        logger.error("Failed to refresh recommendations: %s", exc)
        return []
    finally:
        conn.close()


def refresh_recommendations(db_path: str) -> List[dict]:
    """Refresh legacy global recommendations for one database path."""
    return _refresh_recommendations_for_db_path(db_path)


def _discover_similar_for_db_path(db_path: str, paper_ids: List[str], limit: int) -> List[dict]:
    conn = connect(db_path)
    try:
        return _discover_similar_with_conn(conn, paper_ids, limit)
    except Exception as exc:
        logger.error("discover_similar failed: %s", exc)
        return []
    finally:
        conn.close()


def discover_similar(db_path: str, paper_ids: List[str], limit: int = 20) -> List[dict]:
    """Discover candidates similar to a set of seed papers (list-only form)."""
    return _discover_similar_for_db_path(db_path, paper_ids, limit)


def discover_similar_with_meta(
    db_path: str, paper_ids: List[str], limit: int = 20
) -> Tuple[List[dict], Dict[str, Any]]:
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


class DiscoveryEngine:
    """Recommendation engine for academic publications.

    Produces recommendations by:
    1. Collecting positive / negative signals from rated & liked papers.
    2. Building a rich preference profile (topics, authors, journals,
       collections, tags, past feedback).
    3. Running 5 retrieval strategies to fetch external candidates.
    4. Filtering out papers already in the local database.
    5. Scoring each candidate with 7 weighted signals.
    6. Applying diversity interleaving across source types.
    7. Storing new recommendations in the ``recommendations`` table.
    """

    # Default settings used when the discovery_settings table is missing
    # or has missing keys.
    DEFAULTS: Dict[str, str] = dict(DEFAULTS)

    def __init__(self, db_path: str) -> None:
        """Initialize the discovery engine.

        Args:
            db_path: Path to the unified SQLite database.  This database
                     must contain the ``papers`` and ``recommendations``
                     tables (created by deps.py).
        """
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """Open a connection with row-factory enabled."""
        return connect(self.db_path)

    @staticmethod
    def _load_settings(conn: sqlite3.Connection) -> Dict[str, str]:
        """Read all rows from the ``discovery_settings`` table.

        Returns a flat ``{key: value}`` dict, falling back to class-level
        ``DEFAULTS`` for any missing keys.  If the table does not exist yet
        (e.g. in test databases that skip deps.py migration), all defaults
        are returned.
        """
        return load_settings(conn)

    def _get_library_papers(self, conn: sqlite3.Connection) -> List[dict]:
        """Fetch all papers with status='library' (saved papers)."""
        return get_library_papers(conn)

    def _get_rated_publications(
        self, conn: sqlite3.Connection
    ) -> Tuple[List[dict], List[dict]]:
        """Fetch papers with ratings and split into positive / negative.

        Positive: rating 4-5 (show more like this)
        Negative: rating 1-2 (show less like this)
        Neutral (3) and unrated (0) are excluded from both lists.

        Returns:
            Tuple of (positive_pubs, negative_pubs).
        """
        return get_rated_publications(conn)

    # ------------------------------------------------------------------
    # Preference profile
    # ------------------------------------------------------------------

    def _compute_preference_profile(
        self,
        conn: sqlite3.Connection,
        positive_pubs: List[dict],
        negative_pubs: List[dict],
        settings: Optional[Dict[str, str]] = None,
    ) -> Dict:
        """Compute a user preference profile. Delegates to scoring.py."""
        return build_preference_profile(conn, positive_pubs, negative_pubs, settings)

    # ------------------------------------------------------------------
    # Multi-signal scoring
    # ------------------------------------------------------------------

    def _score_candidate_full(
        self,
        candidate: dict,
        preference_profile: Dict,
        positive_centroid,
        negative_centroid,
        positive_texts: Optional[List[str]],
        negative_texts: Optional[List[str]],
        conn: sqlite3.Connection,
        settings: Optional[Dict[str, str]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """Score a candidate paper. Delegates to scoring.py."""
        return score_discovery_candidate(
            candidate,
            preference_profile,
            positive_centroid,
            negative_centroid,
            positive_texts,
            negative_texts,
            conn,
            settings,
        )

    # ------------------------------------------------------------------
    # Diversity interleaver
    # ------------------------------------------------------------------

    @staticmethod
    def _diversity_interleave(candidates: List[dict], max_results: int) -> List[dict]:
        """Interleave candidates from different source types for diversity.

        Groups candidates by ``source_type``, sorts each group by score
        descending, then round-robins one candidate from each non-empty
        group until ``max_results`` or all groups are exhausted.

        Args:
            candidates: List of candidate dicts, each with ``source_type``
                        and ``score`` keys.
            max_results: Maximum number of results to return.

        Returns:
            Interleaved list of candidates.
        """
        return diversity_interleave(candidates, max_results)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _get_existing_recommendation_titles(self, conn: sqlite3.Connection) -> Set[str]:
        """Return the set of titles already in the recommendations table (lowercased)."""
        return get_existing_recommendation_titles(conn)

    @staticmethod
    def _canonical_candidate_key(candidate: dict) -> str:
        """Canonical recommendation identity key: DOI -> URL -> title."""
        return canonical_candidate_key(candidate)

    def _get_existing_recommendation_keys(self, conn: sqlite3.Connection) -> Set[str]:
        """Return canonical identity keys from existing recommendations."""
        return get_existing_recommendation_keys(conn)

    @staticmethod
    def _pub_text(pub: dict) -> str:
        """Build a text string from title + abstract for embedding / TF-IDF vectorization."""
        return publication_text(pub)

    def _insert_recommendations(
        self, conn: sqlite3.Connection, recs: List[dict]
    ) -> int:
        """Insert recommendation dicts into the database.

        First inserts/updates papers table entries, then creates recommendation
        records linking to those papers.

        Args:
            conn: Database connection.
            recs: List of recommendation dicts with keys matching the
                  ``recommendations`` table columns.

        Returns:
            Number of rows inserted.
        """
        return insert_recommendations(conn, recs)

    def _get_local_titles(self, conn: sqlite3.Connection) -> Set[str]:
        """Return lowercased titles of all papers already in the DB."""
        return get_local_titles(conn)

    def _get_local_keys(self, conn: sqlite3.Connection) -> Set[str]:
        """Return canonical identity keys from local papers."""
        return get_local_keys(conn)

    # ------------------------------------------------------------------
    # Retrieval strategies
    # ------------------------------------------------------------------

    def _merge_candidate(
        self,
        merged: Dict[str, dict],
        skip_titles: Set[str],
        skip_keys: Set[str],
        candidate: dict,
        source_type: str,
        source_key: str,
        now: str,
        score_override: Optional[float] = None,
    ) -> None:
        """Merge a single candidate into the merged dict, skipping known titles.

        If the candidate is already present with a lower score, it is replaced.
        """
        merge_candidate(
            merged,
            skip_titles,
            skip_keys,
            candidate,
            source_type,
            source_key,
            now,
            score_override,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_recommendations(self, max_results: int = 50) -> List[dict]:
        """Generate new recommendations based on liked publications.

        Pipeline:
        1. Load positive / negative signal publications.
        2. Build preference profile (topics, authors, journals, collections,
           tags, feedback).
        3. Compute positive / negative embedding centroids.
        4. Run 5 retrieval strategies to fetch external candidates.
        5. Filter out papers already in the local database.
        6. Score each candidate with 7 weighted signals.
        7. Apply diversity interleaving across source types.
        8. Insert top N into the DB.
        9. Return the list of new recommendations.

        Args:
            max_results: Maximum number of new recommendations to generate.

        Returns:
            List of recommendation dicts that were inserted into the DB.
        """
        return generate_recommendations(self.db_path, max_results)

    def _generate(self, conn: sqlite3.Connection, max_results: int) -> List[dict]:
        """Core recommendation logic (called within a connection context).

        Reads configurable settings from the ``discovery_settings`` table.
        Strategies can be individually enabled/disabled, and per-strategy
        candidate limits as well as global max_results are configurable.

        Recommendations come exclusively from external sources (OpenAlex).
        Papers already in the local database are filtered out -- the point of
        discovery is to surface papers the user does NOT already have.
        """
        return _generate_with_conn(conn, max_results)

    def refresh_recommendations(self) -> List[dict]:
        """Clear existing neutral recommendations and regenerate.

        Keeps recommendations that the user has explicitly acted upon
        (liked or dismissed).  Only clears neutral recommendations so they
        can be replaced with fresh ones.  The ``max_results`` limit is read
        from the ``discovery_settings`` table.

        Returns:
            List of newly generated recommendations.
        """
        return refresh_recommendations(self.db_path)

    def discover_similar(
        self, paper_ids: List[str], limit: int = 20
    ) -> List[dict]:
        """Find papers similar to a set of seed papers.

        Uses only DOI-based retrieval strategies (related works and citation
        chain) to surface external papers related to the given seed papers.
        Candidates are scored against a mini preference profile built from
        the seeds alone and diversity-interleaved before being returned.

        Unlike ``generate_recommendations``, this method does NOT insert
        results into the ``recommendations`` table.

        Args:
            paper_ids: List of paper UUIDs to use as seeds.
            limit: Maximum number of results to return.

        Returns:
            Scored and interleaved list of candidate dicts.  Returns an
            empty list when no seed papers are found or no candidates are
            produced.
        """
        return discover_similar(self.db_path, paper_ids, limit)

    def _discover_similar_impl(
        self,
        conn: sqlite3.Connection,
        paper_ids: List[str],
        limit: int,
    ) -> List[dict]:
        """Core logic for ``discover_similar`` (called within a connection context)."""
        return _discover_similar_with_conn(conn, paper_ids, limit)
