"""Network-backed author suggestion buckets (D12 AUTH-SUG-3 / AUTH-SUG-4).

These two buckets surface authors who are NOT yet in our corpus by
reaching out to OpenAlex and Semantic Scholar and extracting authors
from papers the services say are related to the user's strongest
Library signal.

Architecture:
  - Seed selection uses the composite `paper_signal.score_papers_batch`
    — so we seed on topic + embedding + author + signal-lab + rating,
    not just "rating >= 4". This breaks the recency-locked feedback
    loop where unrated-but-strong Library papers couldn't drive
    author suggestions.
  - Results are cached in `author_suggestion_cache` keyed by
    `(source, hash(sorted(seed_ids)))`. Stale rows are served
    synchronously while the refresh runner recomputes in the
    background (Activity-envelope pattern).
  - Per-candidate scoring blends seed_cooccurrence + topic_overlap +
    centroid_sim + venue_overlap + recency_activity + h_index_soft
    (weights live in `DISCOVERY_SETTINGS_DEFAULTS.candidate_author_weights.*`).

Read helpers `_openalex_related_candidates` + `_s2_related_candidates`
are pure reads from the cache. They're called by
`application/authors.list_author_suggestions`. Never call the network
from a GET handler — that was the earlier mirror-sync-on-GET anti
pattern (see `lessons.md` "API endpoints: reads vs writes").
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from alma.application import paper_signal
from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_doi
from alma.discovery import semantic_scholar
from alma.discovery.defaults import merge_discovery_defaults
from alma.openalex import client as openalex_client

logger = logging.getLogger(__name__)


SOURCE_OPENALEX_RELATED = "openalex_related"
SOURCE_S2_RELATED = "s2_related"
_SOURCES = (SOURCE_OPENALEX_RELATED, SOURCE_S2_RELATED)
# One cache row per source — the refresh runner always overwrites
# this slot. Earlier revisions hashed the seed set into the key so
# stale/fresh could coexist, but that pushed expensive seed-selection
# into the GET read path. Keep the schema column but treat it as a
# constant.
_CACHE_SLOT = "default"

_SEED_AUTHOR_SIGNAL_MIN = 0.45       # composite paper_signal threshold for seed authors
_SEED_PAPER_SIGNAL_MIN = 0.50        # composite paper_signal threshold for seed DOIs
_MAX_SEED_AUTHORS = 20
_MAX_SEED_DOIS = 20
_MAX_WORKS_PER_SEED = 25
_MAX_RECS_PER_DOI = 25

_CANDIDATE_COMPONENTS = (
    "seed_cooccurrence",
    "topic_overlap",
    "centroid_sim",
    "venue_overlap",
    "recency_activity",
    "h_index_soft",
)


# -- cache I/O -------------------------------------------------------

def _cache_key(seed_ids: Any = None) -> str:
    """Cache slot is constant — signature kept for test back-compat."""

    return _CACHE_SLOT


def _cache_ttl_hours(db: sqlite3.Connection) -> float:
    try:
        rows = db.execute(
            "SELECT key, value FROM discovery_settings "
            "WHERE key = 'author_suggestion_cache_ttl_hours'"
        ).fetchall()
        stored = {row["key"]: row["value"] for row in rows}
    except sqlite3.OperationalError:
        stored = {}
    merged = merge_discovery_defaults(stored)
    try:
        return max(1.0, float(merged.get("author_suggestion_cache_ttl_hours", "24")))
    except (TypeError, ValueError):
        return 24.0


def load_cached_candidates(
    db: sqlite3.Connection,
    source: str,
    cache_key: str = _CACHE_SLOT,
) -> Optional[dict]:
    """Return the cached payload dict (fresh OR stale), or None if absent.

    O(1) single-row lookup — keeps the GET handler cheap. The `cache_key`
    positional is kept for back-compat; callers that ignore it get the
    current slot.
    """

    try:
        row = db.execute(
            """
            SELECT payload_json, computed_at, expires_at, seed_count
            FROM author_suggestion_cache
            WHERE source = ? AND cache_key = ?
            """,
            (source, cache_key),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    try:
        payload = json.loads(row["payload_json"] or "[]")
    except json.JSONDecodeError:
        payload = []
    return {
        "candidates": payload if isinstance(payload, list) else [],
        "computed_at": row["computed_at"],
        "expires_at": row["expires_at"],
        "seed_count": int(row["seed_count"] or 0),
    }


def is_cache_stale(
    db: sqlite3.Connection,
    source: str,
    cache_key: str = _CACHE_SLOT,
) -> bool:
    """True if the cache row is missing OR past its `expires_at`.

    Fast SQL-only probe — no seed selection, no state loading.
    """

    entry = load_cached_candidates(db, source, cache_key)
    if entry is None:
        return True
    expires_at = str(entry.get("expires_at") or "")
    if not expires_at:
        return True
    try:
        exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= exp


def _write_cache(
    db: sqlite3.Connection,
    source: str,
    candidates: list[dict],
    seed_count: int,
    ttl_hours: float,
    *,
    cache_key: str = _CACHE_SLOT,
) -> None:
    """Overwrite the single cache slot for `source` with the latest payload.

    Called incrementally by the refresh runner — every partial batch
    writes, commits, and releases the writer lock so concurrent GETs
    see the running progress (per-unit-of-work commits, per
    `lessons.md` "Background jobs must release writer lock").
    """

    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=ttl_hours)
    db.execute(
        """
        INSERT INTO author_suggestion_cache
            (source, cache_key, payload_json, seed_count, computed_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, cache_key) DO UPDATE SET
            payload_json = excluded.payload_json,
            seed_count   = excluded.seed_count,
            computed_at  = excluded.computed_at,
            expires_at   = excluded.expires_at
        """,
        (
            source,
            cache_key,
            json.dumps(candidates),
            seed_count,
            now.isoformat(),
            expires.isoformat(),
        ),
    )


# -- seed selection --------------------------------------------------

def _select_seed_authors(db: sqlite3.Connection, limit: int = _MAX_SEED_AUTHORS) -> list[str]:
    """Top-signal authors to seed OpenAlex co-author extension from.

    Union of (followed authors, authors of high-signal Library papers).
    High-signal = composite `paper_signal_score` above a threshold.
    Ranked by (a) cached author_centroid similarity to library centroid
    when available, (b) Library paper count as fallback.
    """

    try:
        rows = db.execute(
            """
            SELECT DISTINCT lower(trim(a.openalex_id)) AS oid
            FROM authors a
            JOIN followed_authors fa ON fa.author_id = a.id
            WHERE COALESCE(TRIM(a.openalex_id), '') <> ''
            """
        ).fetchall()
        followed = [str(row["oid"]) for row in rows if row["oid"]]
    except sqlite3.OperationalError:
        followed = []

    # Authors of high-signal Library papers, ordered by their centroid
    # alignment with the library when we have it.
    state = paper_signal.load_library_state(db)
    lib_paper_rows = db.execute(
        "SELECT id FROM papers WHERE status = 'library'"
    ).fetchall()
    lib_ids = [str(r["id"]) for r in lib_paper_rows]
    scores = paper_signal.score_papers_batch(db, lib_ids, state)
    strong_ids = [pid for pid, s in scores.items() if s >= _SEED_AUTHOR_SIGNAL_MIN]
    extra: list[str] = []
    if strong_ids:
        placeholders = ",".join("?" * len(strong_ids))
        try:
            pa_rows = db.execute(
                f"""
                SELECT lower(trim(pa.openalex_id)) AS oid, COUNT(DISTINCT pa.paper_id) AS n
                FROM publication_authors pa
                WHERE pa.paper_id IN ({placeholders})
                  AND COALESCE(TRIM(pa.openalex_id), '') <> ''
                GROUP BY lower(trim(pa.openalex_id))
                ORDER BY n DESC
                """,
                strong_ids,
            ).fetchall()
            extra = [str(r["oid"]) for r in pa_rows if r["oid"]]
        except sqlite3.OperationalError:
            extra = []

    # Merge; ranking: centroid-aligned first, then Library count.
    pool: list[str] = []
    seen: set[str] = set()
    for oid in followed + extra:
        if oid and oid not in seen:
            seen.add(oid)
            pool.append(oid)

    if state.has_author_centroids():
        pool.sort(
            key=lambda oid: state.author_centroid_sim.get(oid, 0.0),
            reverse=True,
        )
    return pool[:limit]


def _select_seed_dois(db: sqlite3.Connection, limit: int = _MAX_SEED_DOIS) -> list[str]:
    """Top-signal Library DOIs for S2-backed recommendation seeding."""

    try:
        rows = db.execute(
            """
            SELECT id, doi
            FROM papers
            WHERE status = 'library'
              AND COALESCE(NULLIF(TRIM(doi), ''), '') <> ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    if not rows:
        return []
    paper_ids = [str(r["id"]) for r in rows]
    state = paper_signal.load_library_state(db)
    scores = paper_signal.score_papers_batch(db, paper_ids, state)
    ranked = [
        (scores.get(str(r["id"]), 0.0), str(r["doi"] or "").strip())
        for r in rows
    ]
    ranked = [(s, d) for s, d in ranked if d and s >= _SEED_PAPER_SIGNAL_MIN]
    ranked.sort(reverse=True)
    return [d for _s, d in ranked[:limit]]


# -- candidate scoring -----------------------------------------------

def _resolve_candidate_weights(db: sqlite3.Connection) -> dict[str, float]:
    try:
        settings_rows = db.execute(
            "SELECT key, value FROM discovery_settings WHERE key LIKE 'candidate_author_weights.%'"
        ).fetchall()
        stored = {row["key"]: row["value"] for row in settings_rows}
    except sqlite3.OperationalError:
        stored = {}
    merged = merge_discovery_defaults(stored)
    out: dict[str, float] = {}
    for name in _CANDIDATE_COMPONENTS:
        raw = merged.get(f"candidate_author_weights.{name}", "0.0")
        try:
            out[name] = max(0.0, float(raw))
        except (TypeError, ValueError):
            out[name] = 0.0
    return out


def _compute_composite(
    components: dict[str, float],
    present: dict[str, bool],
    weights: dict[str, float],
) -> float:
    present_w = sum(weights[n] for n in _CANDIDATE_COMPONENTS if present.get(n))
    missing_w = sum(weights[n] for n in _CANDIDATE_COMPONENTS if not present.get(n))
    if present_w <= 0.0:
        return 0.0
    bonus = 1.0 + (missing_w / present_w)
    total = 0.0
    for name in _CANDIDATE_COMPONENTS:
        if not present.get(name):
            continue
        total += components[name] * weights[name] * bonus
    return max(0.0, min(1.0, total))


def _corpus_author_oids(db: sqlite3.Connection) -> set[str]:
    try:
        rows = db.execute(
            "SELECT lower(trim(openalex_id)) AS oid FROM authors "
            "WHERE COALESCE(TRIM(openalex_id), '') <> ''"
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(r["oid"]) for r in rows if r["oid"]}


def _score_from_buckets(
    per_candidate: dict[str, dict],
    *,
    total_seeds: int,
    library_state: paper_signal.LibraryState,
    top_topics: set[str],
    lib_venues: set[str],
    corpus_oids: set[str],
    weights: dict[str, float],
) -> list[dict]:
    """Pure-function rescore over an accumulated candidate pool.

    Takes precomputed library state so it can be called after every
    seed completes without re-reading embeddings each time.
    """

    scored: list[dict] = []
    for oid, ctx in per_candidate.items():
        if oid in corpus_oids:
            continue
        components: dict[str, float] = {n: 0.0 for n in _CANDIDATE_COMPONENTS}
        present: dict[str, bool] = {n: False for n in _CANDIDATE_COMPONENTS}

        freq = int(ctx.get("seed_cooccurrence") or 0)
        if freq > 0:
            components["seed_cooccurrence"] = min(1.0, freq / max(1.0, total_seeds / 2.0))
            present["seed_cooccurrence"] = True

        topics = set(t.lower() for t in (ctx.get("topics") or []))
        if topics and top_topics:
            components["topic_overlap"] = len(topics & top_topics) / max(1, len(topics | top_topics))
            present["topic_overlap"] = True

        centroid_sim = library_state.author_centroid_sim.get(oid)
        if centroid_sim is not None:
            components["centroid_sim"] = centroid_sim
            present["centroid_sim"] = True

        venues = set(v.lower() for v in (ctx.get("venues") or []))
        if venues and lib_venues:
            overlap = len(venues & lib_venues)
            components["venue_overlap"] = min(1.0, overlap / max(1, min(len(lib_venues), 5)))
            present["venue_overlap"] = True

        recent_ratio = float(ctx.get("recent_ratio") or 0.0)
        if ctx.get("has_activity_info"):
            components["recency_activity"] = max(0.0, min(1.0, recent_ratio))
            present["recency_activity"] = True

        h_index = ctx.get("h_index")
        if h_index is not None:
            components["h_index_soft"] = min(1.0, float(h_index) / 40.0)
            present["h_index_soft"] = True

        composite = _compute_composite(components, present, weights)
        if composite <= 0.0:
            continue
        scored.append(
            {
                "candidate_openalex_id": oid,
                "candidate_name": str(ctx.get("name") or "").strip() or oid,
                "composite_score": round(composite, 4),
                "seed_cooccurrence": freq,
                "topics": sorted(topics),
                "venues": sorted(venues),
                "recent_ratio": round(recent_ratio, 3),
                "h_index": h_index,
            }
        )
    scored.sort(key=lambda c: c["composite_score"], reverse=True)
    return scored


def _score_candidates(
    db: sqlite3.Connection,
    per_candidate: dict[str, dict],
    *,
    total_seeds: int,
    now: datetime,
) -> list[dict]:
    """Back-compat wrapper: load state + call `_score_from_buckets` once."""

    library_state = paper_signal.load_library_state(db)
    return _score_from_buckets(
        per_candidate,
        total_seeds=total_seeds,
        library_state=library_state,
        top_topics=set(list(library_state.topic_weights.keys())[:25]),
        lib_venues=_library_venue_set(db),
        corpus_oids=_corpus_author_oids(db),
        weights=_resolve_candidate_weights(db),
    )


def _library_venue_set(db: sqlite3.Connection) -> set[str]:
    try:
        rows = db.execute(
            """
            SELECT lower(trim(journal)) AS v
            FROM papers
            WHERE status = 'library'
              AND COALESCE(TRIM(journal), '') <> ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(r["v"]) for r in rows if r["v"]}


# -- OpenAlex runner -------------------------------------------------

def refresh_openalex_related_network(
    db_path: str, *, ctx: Optional[Any] = None
) -> dict:
    """Populate the `openalex_related` cache; stream progress per seed.

    Commit cadence: after every seed we (a) rescore the accumulated
    candidate pool, (b) write the partial cache row, (c) commit, (d)
    call `ctx.log_step` with `processed/total`. That way the user
    sees incremental suggestions appearing — not a 30s blank wait
    while the runner crunches — and a backend restart loses at most
    one seed's worth of work.
    """

    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        # Heavy seed selection runs ONCE in the runner (never in GET).
        _log_step(ctx, "seed_select", "Selecting seed authors")
        seeds = _select_seed_authors(conn)
        ttl = _cache_ttl_hours(conn)
        now_dt = datetime.now(timezone.utc)

        if not seeds:
            _write_cache(conn, SOURCE_OPENALEX_RELATED, [], 0, ttl)
            conn.commit()
            _log_step(ctx, "done", "No seed authors — cache written empty", 0, 0)
            return {"seeds": 0, "candidates": 0}

        _log_step(ctx, "openalex_fetch", f"Expanding {len(seeds)} seed authors", 0, len(seeds))
        # Precompute scoring primitives ONCE so every per-seed rescore is cheap.
        library_state = paper_signal.load_library_state(conn)
        top_topics = set(list(library_state.topic_weights.keys())[:25])
        lib_venues = _library_venue_set(conn)
        corpus_oids = _corpus_author_oids(conn)
        weights = _resolve_candidate_weights(conn)

        per_candidate: dict[str, dict] = {}
        errors = 0
        for idx, seed_oid in enumerate(seeds, 1):
            # Release writer lock before the remote call (lesson:
            # "Background jobs must release writer lock before every
            # remote call"). Works even though we have no open tx.
            if conn.in_transaction:
                conn.commit()
            try:
                page = openalex_client.fetch_works_page_for_author(
                    seed_oid, cursor="*", per_page=_MAX_WORKS_PER_SEED
                )
            except Exception as exc:
                logger.debug("OpenAlex seed works fetch failed %s: %s", seed_oid, exc)
                errors += 1
                _log_step(
                    ctx, "openalex_fetch",
                    f"Seed {idx}/{len(seeds)} failed — skipped",
                    idx, len(seeds),
                )
                continue
            _accumulate_openalex_coauthors(
                page.get("results") or [], seed_oid, per_candidate, now_dt
            )
            # Partial write: rescore and commit after every seed so
            # the UI sees the rail fill up (candidates appear in
            # `GET /authors/suggestions` as each seed completes).
            partial = _score_from_buckets(
                per_candidate,
                total_seeds=len(seeds),
                library_state=library_state,
                top_topics=top_topics,
                lib_venues=lib_venues,
                corpus_oids=corpus_oids,
                weights=weights,
            )
            _write_cache(conn, SOURCE_OPENALEX_RELATED, partial, len(seeds), ttl)
            conn.commit()
            _log_step(
                ctx, "openalex_fetch",
                f"Seed {idx}/{len(seeds)} · {len(partial)} candidates",
                idx, len(seeds),
            )
        return {
            "seeds": len(seeds),
            "candidates": len(per_candidate),
            "errors": errors,
        }
    finally:
        conn.close()


def _accumulate_openalex_coauthors(
    works: list[dict],
    seed_oid: str,
    per_candidate: dict[str, dict],
    now: datetime,
) -> None:
    """Walk a seed author's works; tally every co-author as a candidate."""

    seed_lower = str(seed_oid).strip().lower()
    for work in works:
        authorships = work.get("authorships") or []
        topics = [
            str(t.get("term") or "").strip().lower()
            for t in (work.get("topics") or [])
            if t.get("term")
        ]
        venue = str(work.get("journal") or "").strip().lower()
        pub_date = str(work.get("publication_date") or "")
        is_recent = _is_recent(pub_date, now)
        for ap in authorships:
            raw_oid = str(ap.get("openalex_id") or "").strip().lower()
            if not raw_oid or raw_oid == seed_lower:
                continue
            bucket = per_candidate.setdefault(
                raw_oid,
                {
                    "name": ap.get("display_name") or "",
                    "seed_cooccurrence": 0,
                    "topics": set(),
                    "venues": set(),
                    "recent_works": 0,
                    "total_works": 0,
                    "has_activity_info": True,
                    "h_index": None,
                },
            )
            bucket["seed_cooccurrence"] = int(bucket["seed_cooccurrence"]) + 1
            bucket["topics"].update(topics)
            if venue:
                bucket["venues"].add(venue)
            bucket["total_works"] += 1
            if is_recent:
                bucket["recent_works"] += 1
            bucket["name"] = bucket["name"] or ap.get("display_name") or ""
    for bucket in per_candidate.values():
        total = bucket.get("total_works") or 0
        bucket["recent_ratio"] = (bucket["recent_works"] / total) if total else 0.0
        # convert sets to lists in the inner structure for JSON serialization
        # (we return sorted() from the scorer anyway)


# -- Semantic Scholar runner ----------------------------------------

def refresh_s2_related_network(
    db_path: str, *, ctx: Optional[Any] = None
) -> dict:
    """Populate the `s2_related` cache; stream progress per DOI.

    Same cadence as the OpenAlex runner: commit + log_step after every
    seed DOI completes. A 20-DOI run should feel like it "fills in"
    the rail, not a 40s blank wait.
    """

    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        _log_step(ctx, "seed_select", "Selecting seed DOIs")
        seeds = _select_seed_dois(conn)
        ttl = _cache_ttl_hours(conn)
        now_dt = datetime.now(timezone.utc)

        if not seeds:
            _write_cache(conn, SOURCE_S2_RELATED, [], 0, ttl)
            conn.commit()
            _log_step(ctx, "done", "No seed DOIs — cache written empty", 0, 0)
            return {"seeds": 0, "candidates": 0}

        library_state = paper_signal.load_library_state(conn)
        top_topics = set(list(library_state.topic_weights.keys())[:25])
        lib_venues = _library_venue_set(conn)
        corpus_oids = _corpus_author_oids(conn)
        weights = _resolve_candidate_weights(conn)

        per_candidate: dict[str, dict] = {}
        errors = 0
        for idx, doi in enumerate(seeds, 1):
            # Release writer lock before every remote call.
            if conn.in_transaction:
                conn.commit()
            try:
                recs = _s2_recommendations(doi, limit=_MAX_RECS_PER_DOI)
            except Exception as exc:
                logger.debug("S2 recommend failed for %s: %s", doi, exc)
                recs = []
                errors += 1
            if recs:
                # Preferred path: hydrate via OpenAlex so candidates get
                # stable openalex_ids that dedup against the corpus.
                rec_dois = []
                for row in recs:
                    ext = row.get("externalIds") or {}
                    rd = normalize_doi(str(ext.get("DOI") or "").strip())
                    if rd:
                        rec_dois.append(rd)
                if conn.in_transaction:
                    conn.commit()
                oa_works: list[dict] = []
                if rec_dois:
                    try:
                        oa_works = _openalex_works_by_dois(rec_dois)
                    except Exception as exc:
                        logger.debug("OpenAlex hydrate failed: %s", exc)
                        errors += 1
                if oa_works:
                    _accumulate_openalex_coauthors(
                        oa_works, seed_oid="", per_candidate=per_candidate, now=now_dt,
                    )
                else:
                    # Fallback: OpenAlex hydration returned nothing (no
                    # API key, 401 burst, or papers without DOIs).
                    # Extract authors directly from the S2 rec rows —
                    # less precise (pseudo `s2:` ids, no topic
                    # metadata), but keeps the bucket useful when
                    # OpenAlex is unreachable.
                    _accumulate_s2_native_authors(recs, per_candidate, now_dt)
            partial = _score_from_buckets(
                per_candidate,
                total_seeds=len(seeds),
                library_state=library_state,
                top_topics=top_topics,
                lib_venues=lib_venues,
                corpus_oids=corpus_oids,
                weights=weights,
            )
            _write_cache(conn, SOURCE_S2_RELATED, partial, len(seeds), ttl)
            conn.commit()
            _log_step(
                ctx, "s2_fetch",
                f"DOI {idx}/{len(seeds)} · {len(partial)} candidates",
                idx, len(seeds),
            )
        return {
            "seeds": len(seeds),
            "candidates": len(per_candidate),
            "errors": errors,
        }
    finally:
        conn.close()


def _s2_recommendations(seed_doi: str, *, limit: int = 20) -> list[dict]:
    """Call S2 recommendations endpoint, return raw paper rows.

    Returns a list of dicts with `paperId`, `externalIds` (may carry
    `DOI`), `authors` (list of `{authorId, name}`), `title`, `year`,
    `venue`, `publicationDate`. Callers pick whether to hydrate via
    OpenAlex DOIs (preferred — gives stable openalex_ids for corpus
    dedup) or fall back to S2's own author rows when OpenAlex isn't
    reachable (typical in dev without an API key).

    NOTE: S2's recommendations API lives at a different host path than
    `/graph/v1/...`. The ALMa `semantic_scholar` source client is
    base-URL'd at `https://api.semanticscholar.org/graph/v1`, so we
    pass the full URL here to bypass the base-URL join.
    """

    doi = (seed_doi or "").strip()
    if not doi:
        return []
    client = get_source_http_client("semantic_scholar")
    try:
        resp = client.get(
            f"https://api.semanticscholar.org/recommendations/v1/papers/forpaper/{doi}",
            params={
                "fields": (
                    "paperId,externalIds,title,year,venue,publicationDate,"
                    "authors.authorId,authors.name"
                ),
                "limit": min(limit, 100),
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(
                "S2 recommendations HTTP %d for DOI %s", resp.status_code, doi
            )
            return []
        return (resp.json() or {}).get("recommendedPapers") or []
    except Exception as exc:
        logger.debug("S2 recommendations failed for DOI %s: %s", doi, exc)
        return []


def _s2_recommended_dois(seed_doi: str, *, limit: int = 20) -> list[str]:
    """Back-compat helper — extracts just the DOIs from recommendations."""

    out: list[str] = []
    for row in _s2_recommendations(seed_doi, limit=limit):
        ext = row.get("externalIds") or {}
        rec_doi = normalize_doi(str(ext.get("DOI") or "").strip())
        if rec_doi:
            out.append(rec_doi)
    return out


def _accumulate_s2_native_authors(
    recs: list[dict],
    per_candidate: dict[str, dict],
    now: datetime,
) -> None:
    """Fallback: extract author candidates from S2 rec rows directly.

    Used when OpenAlex DOI hydration fails (no API key / 401). S2
    gives us `authorId + name + year + venue`, enough to rank with
    the composite signal. Candidates get `s2:{authorId}` as a pseudo
    openalex_id so the cache keeps a stable identity; when the same
    author later gets picked up via an OpenAlex-hydrated pass, the
    refresh will overwrite the slot with the real `A...` id.
    """

    for paper in recs:
        title = str(paper.get("title") or "").strip()
        year = paper.get("year")
        venue = str(paper.get("venue") or "").strip().lower()
        pub_date = str(paper.get("publicationDate") or "")
        is_recent = _is_recent(pub_date, now) if pub_date else (
            bool(year and year >= (now.year - 2))
        )
        for author in (paper.get("authors") or []):
            author_id = str(author.get("authorId") or "").strip()
            name = str(author.get("name") or "").strip()
            if not author_id or not name:
                continue
            pseudo_oid = f"s2:{author_id}"
            bucket = per_candidate.setdefault(
                pseudo_oid,
                {
                    "name": name,
                    "seed_cooccurrence": 0,
                    "topics": set(),
                    "venues": set(),
                    "recent_works": 0,
                    "total_works": 0,
                    "has_activity_info": True,
                    "h_index": None,
                },
            )
            bucket["seed_cooccurrence"] = int(bucket["seed_cooccurrence"]) + 1
            if venue:
                bucket["venues"].add(venue)
            bucket["total_works"] += 1
            if is_recent:
                bucket["recent_works"] += 1
            bucket["name"] = bucket["name"] or name
    for bucket in per_candidate.values():
        total = bucket.get("total_works") or 0
        bucket["recent_ratio"] = (bucket["recent_works"] / total) if total else 0.0


def _openalex_works_by_dois(dois: list[str]) -> list[dict]:
    """Hydrate DOIs to normalized OpenAlex works (with structured authorships)."""

    if not dois:
        return []
    try:
        from alma.openalex.client import batch_fetch_works_by_dois, _normalize_work

        raw = batch_fetch_works_by_dois(dois)
    except Exception as exc:
        logger.debug("OpenAlex DOI hydration failed: %s", exc)
        return []
    # `batch_fetch_works_by_dois` returns RAW work dicts keyed by DOI —
    # run them through `_normalize_work` so every caller sees the same
    # shape (`authorships` structured with openalex_ids, `topics` list,
    # etc.).
    works: list[dict] = []
    source = raw.values() if isinstance(raw, dict) else raw or []
    for work in source:
        if isinstance(work, dict):
            try:
                works.append(_normalize_work(work))
            except Exception:
                logger.debug("work normalize failed", exc_info=True)
    return works


# -- read helpers for list_author_suggestions ------------------------

def _openalex_related_candidates(
    db: sqlite3.Connection,
    *,
    exclude_ids: set[str],
    limit: int,
) -> list[dict]:
    """Pure-read from the `openalex_related` cache row for the current seed set."""

    return _read_cached_bucket(db, SOURCE_OPENALEX_RELATED, exclude_ids, limit)


def _s2_related_candidates(
    db: sqlite3.Connection,
    *,
    exclude_ids: set[str],
    limit: int,
) -> list[dict]:
    """Pure-read from the `s2_related` cache row for the current seed set."""

    return _read_cached_bucket(db, SOURCE_S2_RELATED, exclude_ids, limit)


def _read_cached_bucket(
    db: sqlite3.Connection,
    source: str,
    exclude_ids: set[str],
    limit: int,
) -> list[dict]:
    """Pure cheap read of the cache slot for `source`.

    Single SQL fetch, then in-memory filtering. NO heavy seed selection
    or library-state loading in this path — `list_author_suggestions`
    is a hot GET and must stay <100ms end-to-end.
    """

    if limit <= 0:
        return []
    entry = load_cached_candidates(db, source)
    if not entry:
        return []

    # Apply exclusion + suppression filters at READ time so a new reject
    # propagates instantly even if the cache row is still warm.
    from alma.application.gap_radar import get_missing_author_feedback_state

    out: list[dict] = []
    for row in entry["candidates"]:
        oid = str(row.get("candidate_openalex_id") or "").strip().lower()
        if not oid or oid in exclude_ids:
            continue
        feedback = get_missing_author_feedback_state(db, oid)
        if feedback.get("suppressed"):
            continue
        out.append({
            "candidate_openalex_id": oid,
            "candidate_name": str(row.get("candidate_name") or "").strip() or oid,
            "suggestion_type": source,
            "composite_score": float(row.get("composite_score") or 0.0),
            "seed_cooccurrence": int(row.get("seed_cooccurrence") or 0),
            "topics": list(row.get("topics") or []),
            "venues": list(row.get("venues") or []),
            "recent_ratio": float(row.get("recent_ratio") or 0.0),
            "negative_signal": float(feedback.get("score") or 0.0),
            "last_removed_at": feedback.get("last_removed_at"),
        })
        if len(out) >= limit:
            break
    return out


# -- helpers ---------------------------------------------------------

def _is_recent(pub_date: str, now: datetime, *, years: int = 2) -> bool:
    raw = (pub_date or "").strip()
    if not raw:
        return False
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt) <= timedelta(days=365 * years)


def _log_step(
    ctx: Optional[Any], step: str, message: str,
    processed: Optional[int] = None, total: Optional[int] = None,
) -> None:
    if ctx is None:
        return
    try:
        ctx.log_step(step, message=message, processed=processed, total=total)
    except Exception:
        logger.debug("ctx.log_step failed on %s", step, exc_info=True)


__all__ = [
    "refresh_openalex_related_network",
    "refresh_s2_related_network",
    "_openalex_related_candidates",
    "_s2_related_candidates",
    "SOURCE_OPENALEX_RELATED",
    "SOURCE_S2_RELATED",
    "is_cache_stale",
    "load_cached_candidates",
]
