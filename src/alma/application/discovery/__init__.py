"""Discovery use-cases (settings, recommendations, lenses, and signals)."""

from __future__ import annotations

import json
import hashlib
import logging
import math
import sqlite3
import uuid
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Callable, Optional

from alma.discovery import openalex_related
from alma.discovery.semantic_scholar import upsert_specter2_embedding
from alma.discovery import similarity as sim_module
from alma.discovery import source_search
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS, merge_discovery_defaults
from alma.discovery.scoring import (
    compute_preference_profile,
    parse_author_names,
    score_candidate,
    load_settings as load_scoring_settings,
)
from alma.openalex.client import (
    _upsert_referenced_works,
    batch_fetch_referenced_works_for_openalex_ids,
    batch_fetch_works_by_openalex_ids,
)
from alma.core.scoring_math import age_decay, clamp

_clamp = clamp  # D-3: canonical clamp under the legacy local name


def _jsonable_numeric(value: Any) -> Any:
    """json.dumps default for numpy scalars / arrays.

    Score breakdowns are built across many lanes; any numeric path that
    forgets to call ``float()`` would otherwise crash lens refresh with
    "Object of type float32 is not JSON serializable" at staging time.
    """
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return tolist()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
from alma.core.utils import normalize_doi
from .. import library as library_app
from ..feed import _commit_if_pending

# --- D-9: re-exported from .lens_crud (moved out of this god-module) ---
from .lens_crud import (
    DEFAULT_BRANCH_CONTROLS,
    DEFAULT_CHANNEL_WEIGHTS,
    VALID_CONTEXT_TYPES,
    VALID_RECOMMENDATION_ACTIONS,
    _AUTO_WEIGHT_CEIL,
    _AUTO_WEIGHT_FLOOR,
    _AUTO_WEIGHT_HALF_LIFE_DAYS,
    _AUTO_WEIGHT_MUTE_THRESHOLD,
    _AUTO_WEIGHT_PRIOR_STRENGTH,
    _AUTO_WEIGHT_ROTATE_THRESHOLD,
    _PAPER_DISMISS_DECAY_HALF_LIFE_DAYS,
    _PAPER_DISMISS_HARD_HALF_LIFE_DAYS,
    _PAPER_DISMISS_HARD_THRESHOLD,
    _PAPER_DISMISS_SIGNAL_HARD,
    _PAPER_DISMISS_SIGNAL_SOFT,
    _PAPER_DISMISS_SUPPRESSION_THRESHOLD,
    _aggregate_branch_outcomes,
    _apply_branch_auto_lifecycle,
    _apply_branch_controls,
    _branch_control_state,
    _compute_branch_auto_weight,
    _decay_factor,
    _enrich_branches_with_outcomes,
    _json_dump,
    _json_load,
    _load_branch_outcome_map,
    _load_branch_seed_history,
    _make_branch_id,
    _map_lens_row,
    _normalize_branch_controls,
    _normalize_channel_weights,
    _normalize_recommendation,
    _paper_dismissal_scores,
    _parse_action_datetime,
    _resolve_branch_control_via_lineage,
    _resolve_lens_branch_controls,
    _safe_div,
    _table_exists,
    apply_branch_control_action,
    clear_recommendations,
    create_lens,
    default_channel_weights,
    delete_lens,
    get_lens,
    get_recommendation,
    list_lens_recommendations,
    list_lens_signals,
    list_lenses,
    list_recommendations,
    mark_recommendation_action,
    read_settings,
    recommendation_stats,
    record_lens_signal,
    reset_settings_to_defaults,
    update_lens,
    upsert_setting,
)

# --- D-9: re-exported from .seed_profile (moved out of this god-module) ---
from .seed_profile import (
    _KEYWORD_STOP_WORDS,
    _attach_signal_scores_to_seeds,
    _build_recent_win_queries,
    _build_seed_branches,
    _build_topic_keyword_cold_start_summary,
    _candidate_negative_preference_penalty,
    _cluster_seed_papers_lexical,
    _cluster_seed_papers_vector,
    _extract_keywords,
    _fetch_seed_embedding_vectors,
    _load_library_preference_inputs,
    _load_seed_papers_for_lens,
    _negative_preference_context,
    _plan_branch_queries_deterministic,
    _planner_clamp,
    _planner_sanitize_queries,
    _recent_positive_publications,
    _resolve_branch_temperature,
    _seed_strength,
    _seed_token_set,
    _tokenize_for_keywords,
    _top_negative_terms,
    _top_preferred_authors,
    _top_profile_terms,
    preview_lens_branches,
)


def _calibration_block(cal) -> dict:
    """Serialize an `OutcomeCalibration` into the retrieval_summary
    diagnostic shape — multipliers, quality, raw counts, impressions.
    Empty fields on a fresh DB: caller's contract."""
    return {
        "multipliers": dict(cal.multipliers),
        "quality": {k: round(v, 4) for k, v in cal.quality.items()},
        "positive_counts": {k: round(v, 2) for k, v in cal.positive_counts.items()},
        "negative_counts": {k: round(v, 2) for k, v in cal.negative_counts.items()},
        "impressions": dict(cal.impressions),
    }


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring cache — library-derived artifacts that are stable between refreshes
# ---------------------------------------------------------------------------

def _library_fingerprint(positive_ids: list[str], negative_ids: list[str]) -> str:
    """Compute a stable hash of the library state used for scoring."""
    payload = "|".join(sorted(positive_ids)) + "||" + "|".join(sorted(negative_ids))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _cache_get(db: sqlite3.Connection, cache_key: str, fingerprint: str) -> Optional[dict]:
    """Load a cached artifact if the fingerprint matches."""
    try:
        row = db.execute(
            "SELECT value_json, value_blob, fingerprint FROM scoring_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if row and str(row["fingerprint"]) == fingerprint:
            return {"json": row["value_json"], "blob": row["value_blob"]}
    except Exception:
        pass
    return None


def _cache_put(db: sqlite3.Connection, cache_key: str, cache_type: str,
               fingerprint: str, *, value_json: Optional[str] = None,
               value_blob: Optional[bytes] = None) -> None:
    """Store a cached artifact."""
    try:
        db.execute(
            """INSERT INTO scoring_cache (cache_key, cache_type, fingerprint, value_json, value_blob, created_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(cache_key) DO UPDATE SET
                   cache_type = excluded.cache_type,
                   fingerprint = excluded.fingerprint,
                   value_json = excluded.value_json,
                   value_blob = excluded.value_blob,
                   created_at = excluded.created_at""",
            (cache_key, cache_type, fingerprint, value_json, value_blob),
        )
    except Exception as exc:
        logger.debug("Cache write failed for %s: %s", cache_key, exc)

try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx:idx + size] for idx in range(0, len(items), size)]


def _derive_recommendation_provenance(candidate: dict, lens_id: str) -> dict[str, Any]:
    branch_mode = str(candidate.get("branch_mode") or "").strip() or None
    branch_id = str(candidate.get("branch_id") or "").strip() or None
    branch_label = str(candidate.get("branch_label") or "").strip() or None
    source_api = str(candidate.get("source_api") or "").strip() or None
    source_type = str(candidate.get("source_type") or "").strip() or None
    if not source_type:
        if branch_mode == "followed_author":
            source_type = "followed_author"
        elif branch_id or branch_label:
            source_type = "branch"
        elif source_api:
            source_type = "external_search"
        else:
            source_type = "lens_retrieval"
    source_key = str(candidate.get("source_key") or "").strip() or None
    if not source_key:
        source_key = branch_id or branch_mode or source_type or lens_id
    return {
        "source_type": source_type,
        "source_api": source_api,
        "source_key": source_key,
        "branch_id": branch_id,
        "branch_label": branch_label,
        "branch_mode": branch_mode,
    }


def refresh_lens_recommendations(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    trigger_source: str = "user",
    limit: int = 50,
    ctx=None,
) -> Optional[dict]:
    """Generate per-lens recommendations using 4 retrieval channels."""
    overall_start = perf_counter()
    phase_started = overall_start
    timings_ms: dict[str, int] = {}

    def _log(step: str, message: str, **kwargs):
        if ctx is not None:
            ctx.log_step(step, message, **kwargs)

    lens = get_lens(db, lens_id)
    if lens is None:
        return None

    lens_name = lens.get("name") or lens_id[:12]
    seeds = _attach_signal_scores_to_seeds(db, _load_seed_papers_for_lens(db, lens))
    timings_ms["seed_load"] = int(round((perf_counter() - phase_started) * 1000))
    if not seeds:
        return {
            "lens_id": lens_id,
            "context_type": lens["context_type"],
            "channels": {"lexical": 0, "vector": 0, "graph": 0, "external": 0},
            "weights": _normalize_channel_weights(lens.get("weights") or default_channel_weights(lens["context_type"])),
            "inserted": 0,
            "message": "No seed papers for lens context",
            "timings_ms": {"seed_load": timings_ms["seed_load"], "total": int(round((perf_counter() - overall_start) * 1000))},
        }

    _log("seeds", f"Lens '{lens_name}': loaded {len(seeds)} seed papers", data={"seeds": len(seeds)})

    weights = lens.get("weights") or default_channel_weights(lens["context_type"])
    channel_weights = _normalize_channel_weights(weights)

    scoring_settings = load_scoring_settings(db)
    _library_pubs, positive_pubs, negative_pubs = _load_library_preference_inputs(db)
    profile = compute_preference_profile(db, positive_pubs, negative_pubs, scoring_settings)
    phase_started = perf_counter()

    _log("retrieval", f"Lens '{lens_name}': running 4 retrieval channels (lexical, vector, graph, external)")

    # Each retrieval lane is wrapped as an Activity subtask so the
    # panel can show per-lane status, duration, and any partial
    # failures without burying them in the parent's log stream. The
    # subtasks are sequential here (the existing ordering); each lane
    # already has its own internal threading where it makes sense.
    parent_job_id = getattr(ctx, "job_id", None) if ctx is not None else None

    def _run_lane_subtask(
        lane_name: str,
        runner,
        *,
        label: Optional[str] = None,
    ):
        started = perf_counter()
        if not parent_job_id:
            return runner()
        from alma.api.scheduler import add_job_log as _add_job_log, set_job_status as _set_job_status
        pretty = label or lane_name
        subtask_id = f"{parent_job_id}_lane_{lane_name}"
        try:
            _set_job_status(
                subtask_id,
                status="running",
                operation_key=f"discovery.lens.refresh.lane.{lane_name}",
                trigger_source="subtask",
                parent_job_id=parent_job_id,
                stage=f"lane.{lane_name}",
                stage_label=pretty,
                started_at=datetime.utcnow().isoformat(),
                message=f"{pretty} retrieval running",
            )
            _add_job_log(
                parent_job_id,
                f"Subtask started: {pretty}",
                step=f"lane.{lane_name}.start",
                data={"subtask_job_id": subtask_id},
            )
        except Exception:  # never let subtask bookkeeping break the lane
            logger.debug("subtask start bookkeeping failed for %s", lane_name, exc_info=True)
        try:
            result = runner()
        except Exception as exc:
            try:
                _set_job_status(
                    subtask_id,
                    status="failed",
                    finished_at=datetime.utcnow().isoformat(),
                    error=str(exc),
                    message=f"{pretty} retrieval failed: {exc}",
                    parent_job_id=parent_job_id,
                )
                _add_job_log(
                    parent_job_id,
                    f"Subtask failed: {pretty}: {exc}",
                    level="ERROR",
                    step=f"lane.{lane_name}.failed",
                    data={"subtask_job_id": subtask_id},
                )
            except Exception:
                logger.debug("subtask failure bookkeeping failed for %s", lane_name, exc_info=True)
            raise
        duration_ms = int(round((perf_counter() - started) * 1000))
        # `result` may be a list (lexical / vector) or a (list, summary)
        # tuple (graph / external). Count the candidates in either case.
        if isinstance(result, tuple) and result and isinstance(result[0], list):
            count = len(result[0])
        elif isinstance(result, list):
            count = len(result)
        else:
            count = 0
        try:
            _set_job_status(
                subtask_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                processed=count,
                total=count,
                message=f"{pretty} retrieval completed: {count} candidate(s) in {duration_ms}ms",
                parent_job_id=parent_job_id,
            )
            _add_job_log(
                parent_job_id,
                f"Subtask completed: {pretty} ({count} candidates, {duration_ms}ms)",
                step=f"lane.{lane_name}.completed",
                data={
                    "subtask_job_id": subtask_id,
                    "count": count,
                    "duration_ms": duration_ms,
                },
            )
        except Exception:
            logger.debug("subtask completion bookkeeping failed for %s", lane_name, exc_info=True)
        timings_ms[f"lane_{lane_name}_ms"] = duration_ms
        return result

    # F3: run the 4 retrieval lanes CONCURRENTLY instead of sequentially.
    # Each lane gets its OWN SQLite connection (open_db_connection →
    # check_same_thread=False, WAL): the graph lane writes (reference
    # backfill) while the others read, so sharing one connection across
    # threads would be unsafe. Results merge after. Per-source HTTP clients
    # already gate their own concurrency (S2 is serialized at max_concurrency
    # =1 process-wide), so this does not stampede a rate-limited upstream.
    from alma.api.deps import open_db_connection as _open_lane_conn

    lane_specs = (
        ("lexical", "Lexical (OpenAlex topic search)",
         lambda c: _retrieve_lexical_channel(c, lens, seeds, limit=limit)),
        ("vector", "Vector (local SPECTER2 cosine)",
         lambda c: _retrieve_vector_channel(c, lens, seeds, limit=limit)),
        ("graph", "Graph (citation references)",
         lambda c: _retrieve_graph_channel(c, lens, seeds, limit=limit)),
        ("external", "External (taste-author / -topic / -venue / S2)",
         lambda c: _retrieve_external_channel(
             c, lens, seeds, limit=limit,
             preference_profile=profile, positive_pubs=positive_pubs)),
    )

    def _run_lane_with_conn(lane_name: str, label: str, fn):
        conn = _open_lane_conn()
        try:
            return _run_lane_subtask(lane_name, lambda: fn(conn), label=label)
        finally:
            try:
                conn.commit()  # persist graph-lane reference-backfill writes
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    lane_results: dict[str, Any] = {}
    lane_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lens-lane-top")
    try:
        fut_to_name = {
            lane_pool.submit(_run_lane_with_conn, name, label, fn): name
            for name, label, fn in lane_specs
        }
        for fut, name in fut_to_name.items():
            try:
                lane_results[name] = fut.result(timeout=_LANE_HARD_CAP_S)
            except Exception as exc:
                logger.warning("lens lane %s did not complete (%s); using empty result", name, exc)
                if not fut.done():
                    fut.cancel()
                lane_results[name] = ([], {}) if name in ("graph", "external") else []
    finally:
        lane_pool.shutdown(wait=False)

    lexical = lane_results.get("lexical") or []
    vector = lane_results.get("vector") or []
    _graph_pair = lane_results.get("graph") or ([], {})
    graph, graph_summary = _graph_pair if isinstance(_graph_pair, tuple) else (_graph_pair, {})
    _external_pair = lane_results.get("external") or ([], {})
    external, external_summary = _external_pair if isinstance(_external_pair, tuple) else (_external_pair, {})
    timings_ms["channel_retrieval"] = int(round((perf_counter() - phase_started) * 1000))
    _log(
        "retrieval_channels",
        f"Lens '{lens_name}': retrieval finished with {len(lexical) + len(vector) + len(graph) + len(external)} raw candidates",
        data={
            "channels": {
                "lexical": len(lexical),
                "vector": len(vector),
                "graph": len(graph),
                "external": len(external),
            },
            "graph_cache": graph_summary,
            "external_lanes": external_summary.get("external_lanes") or {},
        },
    )
    if (external_summary.get("lane_runs") or []) or graph_summary.get("fallback_sources"):
        _log(
            "retrieval_detail",
            f"Lens '{lens_name}': retrieval plan used {len(external_summary.get('lane_runs') or [])} external lane runs",
            data={
                "graph_fallback": graph_summary,
                "external_lane_runs": (external_summary.get("lane_runs") or [])[:20],
            },
        )

    _log(
        "merge",
        f"Lens '{lens_name}': merging candidates — lexical={len(lexical)}, vector={len(vector)}, graph={len(graph)}, external={len(external)}",
        data={"lexical": len(lexical), "vector": len(vector), "graph": len(graph), "external": len(external)},
    )

    phase_started = perf_counter()  # reset so `merge` times only the merge step, not retrieval (was double-counted)
    merged = _merge_channel_candidates(
        channel_weights=channel_weights,
        channels={
            "lexical": lexical,
            "vector": vector,
            "graph": graph,
            "external": external,
        },
    )
    timings_ms["merge"] = int(round((perf_counter() - phase_started) * 1000))
    _log(
        "merge_result",
        f"Lens '{lens_name}': merged into {len(merged)} unique candidates",
        data={
            "unique_candidates": len(merged),
            "channel_weights": channel_weights,
        },
    )

    cached_embeddings_available = sim_module.has_active_embeddings(db)

    _log(
        "scoring",
        f"Lens '{lens_name}': scoring {len(merged)} candidates with 10-signal hybrid ranker",
        data={
            "candidate_count": len(merged),
            "signals": 10,
            "positive_library_examples": len(positive_pubs),
            "negative_library_examples": len(negative_pubs),
            "cached_embeddings_available": cached_embeddings_available,
            "embeddings_available": cached_embeddings_available,
        },
    )

    # --- Apply 10-signal scoring to replace simple channel-weighted scores ---
    # Library fingerprint: used to cache artifacts that only change when library changes
    import numpy as np
    positive_ids = [str(p.get("id") or "") for p in positive_pubs if p.get("id")]
    negative_ids = [str(p.get("id") or "") for p in negative_pubs if p.get("id")]
    active_embedding_model = sim_module.get_active_embedding_model(db)
    lib_fp = f"{active_embedding_model}:{_library_fingerprint(positive_ids, negative_ids)}"

    # Compute/cache embedding centroids for text similarity
    phase_started = perf_counter()
    positive_centroid = None
    negative_centroid = None
    positive_texts = [sim_module.build_similarity_text(p, conn=db) for p in positive_pubs]
    positive_texts = [t for t in positive_texts if t]
    negative_texts = [sim_module.build_similarity_text(p, conn=db) for p in negative_pubs]
    negative_texts = [t for t in negative_texts if t]
    lexical_profile = sim_module.build_lexical_profile(positive_texts, negative_texts) if positive_texts else None
    timings_ms["lexical_profile"] = int(round((perf_counter() - phase_started) * 1000))

    phase_started = perf_counter()
    positive_example_embeddings = []
    negative_example_embeddings = []
    centroid_cache_hit = False
    if cached_embeddings_available and positive_pubs:
        # Try loading cached centroids
        cached_pos = _cache_get(db, "positive_centroid", lib_fp)
        if cached_pos and cached_pos["blob"]:
            try:
                positive_centroid = np.frombuffer(cached_pos["blob"], dtype=np.float32).copy()
                centroid_cache_hit = True
            except Exception:
                positive_centroid = None
        if positive_centroid is None:
            try:
                positive_centroid = sim_module.compute_embedding_centroid(positive_pubs, db)
                if positive_centroid is not None:
                    _cache_put(db, "positive_centroid", "centroid", lib_fp,
                               value_blob=positive_centroid.astype(np.float32).tobytes())
            except Exception as exc:
                logger.warning("Failed to compute positive centroid for lens scoring: %s", exc)

        cached_neg = _cache_get(db, "negative_centroid", lib_fp)
        if cached_neg and cached_neg["blob"]:
            try:
                negative_centroid = np.frombuffer(cached_neg["blob"], dtype=np.float32).copy()
            except Exception:
                negative_centroid = None
        if negative_centroid is None and negative_pubs:
            try:
                negative_centroid = sim_module.compute_embedding_centroid(negative_pubs, db)
                if negative_centroid is not None:
                    _cache_put(db, "negative_centroid", "centroid", lib_fp,
                               value_blob=negative_centroid.astype(np.float32).tobytes())
            except Exception as exc:
                logger.debug("Failed to compute negative centroid: %s", exc)

        # Exemplar embeddings (cached)
        cached_pos_ex = _cache_get(db, "positive_exemplars", lib_fp)
        if cached_pos_ex and cached_pos_ex["blob"]:
            try:
                raw = np.frombuffer(cached_pos_ex["blob"], dtype=np.float32)
                dim = positive_centroid.shape[0] if positive_centroid is not None else 384
                positive_example_embeddings = [row.copy() for row in raw.reshape(-1, dim)]
            except Exception:
                positive_example_embeddings = []
        if not positive_example_embeddings:
            try:
                positive_example_embeddings = sim_module.load_publication_example_embeddings(positive_pubs, db, limit=12)
                if positive_example_embeddings:
                    blob = np.stack(positive_example_embeddings).astype(np.float32).tobytes()
                    _cache_put(db, "positive_exemplars", "exemplars", lib_fp, value_blob=blob)
            except Exception as exc:
                logger.debug("Failed to load positive exemplar embeddings: %s", exc)

        if negative_pubs:
            cached_neg_ex = _cache_get(db, "negative_exemplars", lib_fp)
            if cached_neg_ex and cached_neg_ex["blob"]:
                try:
                    raw = np.frombuffer(cached_neg_ex["blob"], dtype=np.float32)
                    dim = positive_centroid.shape[0] if positive_centroid is not None else 384
                    negative_example_embeddings = [row.copy() for row in raw.reshape(-1, dim)]
                except Exception:
                    negative_example_embeddings = []
            if not negative_example_embeddings:
                try:
                    negative_example_embeddings = sim_module.load_publication_example_embeddings(negative_pubs, db, limit=8)
                    if negative_example_embeddings:
                        blob = np.stack(negative_example_embeddings).astype(np.float32).tobytes()
                        _cache_put(db, "negative_exemplars", "exemplars", lib_fp, value_blob=blob)
                except Exception as exc:
                    logger.debug("Failed to load negative exemplar embeddings: %s", exc)
    timings_ms["centroids"] = int(round((perf_counter() - phase_started) * 1000))

    phase_started = perf_counter()
    candidate_text_map: dict[str, str] = {}
    for key, candidate in merged.items():
        try:
            candidate_text = sim_module.build_similarity_text(
                candidate,
                conn=db,
                paper_topics=candidate.get("topics") or None,
            )
        except Exception:
            candidate_text = ""
        if candidate_text.strip():
            candidate_text_map[key] = candidate_text
    timings_ms["candidate_texts"] = int(round((perf_counter() - phase_started) * 1000))

    phase_started = perf_counter()
    candidate_embedding_map: dict[str, Any] = {}
    reused_embedding_count = 0
    if cached_embeddings_available and candidate_text_map:
        # Map each candidate key to a real DB paper_id for the
        # embedding lookup. External / graph lane candidates carry a
        # fresh UUID `id` rather than a paper_id — look them up via
        # openalex_id / doi / semantic_scholar_id so we can reuse the
        # existing embedding instead of treating them as embedding-less.
        # Without this, `text_similarity_mode` collapses to "lexical"
        # for every external candidate and semantic ranking goes dark.
        candidate_paper_ids: dict[str, str] = {}
        unresolved_keys: list[str] = []
        external_lookup_terms: dict[str, dict[str, str]] = {}
        for key, candidate in merged.items():
            if key not in candidate_text_map:
                continue
            pid = str(candidate.get("paper_id") or "").strip()
            if pid:
                candidate_paper_ids[key] = pid
                continue
            # Best-effort identity resolution for keys that have no
            # paper_id yet. The candidate may already exist in `papers`
            # under a different surrogate key.
            terms: dict[str, str] = {}
            oa = str(candidate.get("openalex_id") or "").strip()
            if oa:
                terms["openalex_id"] = oa
            doi = str(candidate.get("doi") or "").strip().lower()
            if doi:
                terms["doi"] = doi
            s2 = str(candidate.get("semantic_scholar_id") or "").strip()
            if s2:
                terms["semantic_scholar_id"] = s2
            if terms:
                external_lookup_terms[key] = terms
                unresolved_keys.append(key)

        if external_lookup_terms:
            for col in ("openalex_id", "doi", "semantic_scholar_id"):
                values = [
                    (k, terms[col])
                    for k, terms in external_lookup_terms.items()
                    if col in terms and k not in candidate_paper_ids
                ]
                if not values:
                    continue
                value_to_keys: dict[str, list[str]] = defaultdict(list)
                for k, v in values:
                    if col == "doi":
                        v = v.lower()
                    value_to_keys[v].append(k)
                for chunk in _chunked(list(value_to_keys.keys()), 200):
                    placeholders = ", ".join("?" for _ in chunk)
                    if col == "doi":
                        rows = db.execute(
                            f"SELECT id, LOWER(doi) AS lookup FROM papers "
                            f"WHERE LOWER(doi) IN ({placeholders})",
                            chunk,
                        ).fetchall()
                    else:
                        rows = db.execute(
                            f"SELECT id, {col} AS lookup FROM papers "
                            f"WHERE {col} IN ({placeholders})",
                            chunk,
                        ).fetchall()
                    for row in rows:
                        for matched_key in value_to_keys.get(str(row["lookup"] or ""), []):
                            if matched_key not in candidate_paper_ids:
                                candidate_paper_ids[matched_key] = str(row["id"])

        if candidate_paper_ids:
            pid_to_keys: dict[str, list[str]] = defaultdict(list)
            for key, pid in candidate_paper_ids.items():
                pid_to_keys[pid].append(key)
            for chunk in _chunked(list(pid_to_keys.keys()), 200):
                placeholders = ", ".join("?" for _ in chunk)
                rows = db.execute(
                    f"SELECT paper_id, embedding FROM publication_embeddings "
                    f"WHERE model = ? AND paper_id IN ({placeholders})",
                    [active_embedding_model, *chunk],
                ).fetchall()
                from alma.core.vector_blob import decode_vector
                for row in rows:
                    if not row["embedding"]:
                        continue
                    try:
                        decoded = decode_vector(row["embedding"])
                    except Exception:
                        continue
                    for matched_key in pid_to_keys.get(str(row["paper_id"]), []):
                        candidate_embedding_map[matched_key] = decoded
                        reused_embedding_count += 1
    timings_ms["candidate_embedding_batch"] = int(round((perf_counter() - phase_started) * 1000))
    _log(
        "scoring_inputs",
        f"Lens '{lens_name}': prepared scoring inputs ({len(positive_texts)} positive docs, {len(negative_texts)} negative docs)",
        data={
            "positive_texts": len(positive_texts),
            "negative_texts": len(negative_texts),
            "positive_centroid_ready": positive_centroid is not None,
            "negative_centroid_ready": negative_centroid is not None,
            "positive_examples_ready": len(positive_example_embeddings),
            "negative_examples_ready": len(negative_example_embeddings),
            "lexical_profile_ready": lexical_profile is not None,
            "candidate_texts": len(candidate_text_map),
            "candidate_embeddings_ready": len(candidate_embedding_map),
            "candidate_embeddings_reused": reused_embedding_count,
            "candidate_embeddings_computed": 0,
            "centroid_cache_hit": centroid_cache_hit,
            "library_fingerprint": lib_fp,
            "centroid_prep_ms": timings_ms["centroids"],
            "lexical_profile_ms": timings_ms["lexical_profile"],
            "candidate_text_ms": timings_ms["candidate_texts"],
            "candidate_embedding_batch_ms": timings_ms["candidate_embedding_batch"],
            "cached_embeddings_available": cached_embeddings_available,
            "embeddings_available": cached_embeddings_available,
        },
    )

    # Batch-compute lexical similarity for all candidates at once
    # (single matrix transform + cosine instead of per-candidate calls)
    phase_started = perf_counter()
    precomputed_lexical_map: dict[str, dict] = {}
    if lexical_profile is not None and candidate_text_map:
        try:
            precomputed_lexical_map = sim_module.batch_compute_lexical_similarity(
                candidate_text_map, lexical_profile,
            )
        except Exception as exc:
            logger.warning("Batch lexical similarity failed, falling back to per-candidate: %s", exc)
    timings_ms["batch_lexical"] = int(round((perf_counter() - phase_started) * 1000))

    # D-AUDIT-10 (2026-04-24): pre-embed every user-topic term ONCE per
    # refresh. Inside `compute_topic_overlap`, the semantic fallback
    # previously re-embedded every user_topic for every candidate
    # (nested loop: O(candidates × unmatched_paper_topics ×
    # user_topics)), even though the module-level LRU cache absorbed
    # repeated calls. That nested call graph was the prime suspect for
    # the 27-min / 31-rec baseline: with e.g. 500 candidates × 5
    # unmatched topics × 50 user topics = 125 000 `_get_topic_embedding`
    # lookups per refresh, the per-call overhead dominates even with a
    # warm cache. Hoisting the `user_topic_embeddings` dict one level
    # up collapses that to `O(user_topics)` provider calls + a cheap
    # dict lookup inside the hot loop. Returns `None` when no
    # embedding provider is configured — the semantic fallback bails
    # out via its existing `provider is None` guard.
    phase_started = perf_counter()
    user_topic_embeddings: Optional[dict[str, Any]] = None
    user_topic_weights = profile.get("topic_weights") or {}
    # Resolve the embedding provider ONCE per refresh and reuse it for the
    # whole scoring loop. `get_active_provider()` probes the configured
    # dependency env via a SUBPROCESS; the scoring loop otherwise called it
    # per candidate (inside `compute_topic_overlap` + the topic_match_mode
    # check), which cProfile showed costing ~22 s/refresh in 385 subprocess
    # probes (task 19 follow-up, uncovered once F1 cut the embedding cost).
    try:
        from alma.ai.providers import get_active_provider
        _topic_provider = get_active_provider(db)
    except Exception:
        _topic_provider = None
    if user_topic_weights and _topic_provider is not None:
        user_topic_embeddings = {}
        for ut in user_topic_weights:
            try:
                user_topic_embeddings[ut] = sim_module._get_topic_embedding(
                    _topic_provider, ut,
                )
            except Exception:
                user_topic_embeddings[ut] = None
    timings_ms["user_topic_embeddings"] = int(round((perf_counter() - phase_started) * 1000))

    # D-AUDIT-10 follow-up (2026-04-24): batch-embed every candidate
    # topic term ONCE up front so `_get_topic_embedding` inside the
    # scoring loop hits the module cache every time. Before this, the
    # semantic fallback inside `compute_topic_overlap` called
    # `provider.embed([term])` one term at a time for every unmatched
    # paper topic — at 500 candidates × ~5 unmatched topics each that's
    # ~2500 sequential provider round-trips, which even a local
    # SPECTER2 model takes tens of seconds to satisfy. Doing one big
    # `provider.embed(all_terms)` call warms the cache in ~O(1) network
    # round-trip, after which the per-term lookup is a dict hit.
    phase_started = perf_counter()
    if user_topic_embeddings is not None and _topic_provider is not None:
        candidate_topic_terms: set[str] = set()
        for candidate in merged.values():
            for t in (candidate.get("topics") or []):
                term = (t.get("term") or "").strip().lower()
                if term and term not in sim_module._topic_embedding_cache:
                    candidate_topic_terms.add(term)
        if candidate_topic_terms:
            # Bound the batch size so the embedding provider's own
            # request budget isn't exceeded on huge refreshes. 256 is
            # a safe default for OpenAI / SPECTER2; bump later if we
            # see throughput headroom.
            terms = sorted(candidate_topic_terms)
            for chunk_start in range(0, len(terms), 256):
                chunk = terms[chunk_start:chunk_start + 256]
                try:
                    embeddings = _topic_provider.embed(chunk)
                except Exception:
                    embeddings = []
                if not embeddings:
                    # Provider refused — mark the chunk as "attempted"
                    # so we don't retry inside the hot loop.
                    for term in chunk:
                        sim_module._topic_embedding_cache[term] = None
                    continue
                import numpy as np
                for term, vec in zip(chunk, embeddings):
                    if vec:
                        try:
                            sim_module._topic_embedding_cache[term] = np.array(
                                vec, dtype=np.float32,
                            )
                        except Exception:
                            sim_module._topic_embedding_cache[term] = None
                    else:
                        sim_module._topic_embedding_cache[term] = None
    timings_ms["candidate_topic_embeddings"] = int(round((perf_counter() - phase_started) * 1000))

    # D-AUDIT-10a (2026-04-24): preload preference_profiles + candidate
    # authors once per refresh. `get_preference_affinity_signal` inside
    # `score_candidate` otherwise makes 4 DB round trips per candidate
    # (`SUM(interaction_count)` + topic affinity lookup + per-candidate
    # `publication_authors` + author affinity lookup) — on a 500-candidate
    # refresh that's ~2 000 trips under the SQLite writer lock. Hoisting
    # to one preload + an `IN (?, ?, …)` authors batch collapses the
    # hot-loop cost to cheap dict hits.
    phase_started = perf_counter()
    from alma.services.signal_lab import (
        preload_candidate_authors as _preload_authors,
        preload_preference_profile_maps as _preload_pref,
    )
    preloaded_preference_profile = _preload_pref(db)
    if preloaded_preference_profile is not None:
        candidate_paper_id_list = [
            str(candidate.get("paper_id") or candidate.get("id") or "").strip()
            for candidate in merged.values()
        ]
        preloaded_preference_profile["authors_by_paper"] = _preload_authors(
            db, candidate_paper_id_list,
        )
    timings_ms["preference_profile_preload"] = int(round((perf_counter() - phase_started) * 1000))

    # Outcome calibration: per-dimension multipliers on `source_relevance`
    # based on observed save/dismiss outcomes from prior refreshes. Three
    # calibration axes compose multiplicatively per candidate:
    #   - source_api  (which API surfaced it: openalex / s2 / …)
    #   - branch_mode (which retrieval lane: core / explore / safe)
    #   - branch_id   (which specific branch within the lens)
    # On a fresh DB all three return empty maps → multiplier 1.0 (no
    # behavior change). After enough events accumulate, axes where
    # dismisses dominate get pulled toward 0.5x, axes where saves
    # dominate get pushed toward 1.5x. Composite is clamped to the
    # same band so three positives can't push past 1.5x.
    from alma.application.outcome_calibration import (
        calibration_multiplier_for,
        compose_calibration_multipliers,
        compute_outcome_calibration,
    )
    calibration_source = compute_outcome_calibration(db, dimension="source_api")
    calibration_branch_mode = compute_outcome_calibration(db, dimension="branch_mode")
    calibration_branch_id = compute_outcome_calibration(db, dimension="branch_id")

    # Score each candidate with full 10-signal system
    phase_started = perf_counter()
    signal_names = (
        "source_relevance",
        "topic_score",
        "text_similarity",
        "author_affinity",
        "journal_affinity",
        "recency_boost",
        "citation_quality",
        "feedback_adj",
        "preference_affinity",
        "usefulness_boost",
    )
    signal_value_sums = {name: 0.0 for name in signal_names}
    signal_weighted_sums = {name: 0.0 for name in signal_names}
    text_mode_counts: dict[str, int] = {}
    topic_mode_counts: dict[str, int] = {}
    raw_semantic_scores: list[float] = []
    raw_semantic_exemplar_scores: list[float] = []
    raw_semantic_support_scores: list[float] = []
    raw_lexical_scores: list[float] = []
    raw_lexical_word_scores: list[float] = []
    raw_lexical_char_scores: list[float] = []
    raw_lexical_term_scores: list[float] = []
    final_scores: list[float] = []
    embedding_ready_count = 0
    compressed_similarity_count = 0
    low_similarity_count = 0
    for key, candidate in merged.items():
        # Channel score becomes source_relevance (normalized to 0-1).
        # Then scale by the calibration multiplier for this candidate's
        # source. The clamp at 1.0 stays — a 1.5x multiplier on a 0.8
        # source_relevance lifts to 1.2 → clamped to 1.0; multipliers
        # below 1.0 just shrink. The pre-calibration value is kept on
        # the candidate so the breakdown can show the adjustment.
        raw_source_relevance = min(1.0, candidate["score"] / 100.0)
        source_mul = calibration_multiplier_for(
            calibration_source,
            candidate.get("source_api"),
            candidate.get("source_type"),
        )
        branch_mode_mul = calibration_multiplier_for(
            calibration_branch_mode,
            candidate.get("branch_mode"),
            None,
        )
        branch_id_mul = calibration_multiplier_for(
            calibration_branch_id,
            candidate.get("branch_id"),
            None,
        )
        multiplier = compose_calibration_multipliers(
            source_mul, branch_mode_mul, branch_id_mul
        )
        candidate["source_relevance"] = min(1.0, raw_source_relevance * multiplier)
        candidate["source_calibration_multiplier"] = multiplier
        candidate["source_calibration_components"] = {
            "source_api": round(source_mul, 4),
            "branch_mode": round(branch_mode_mul, 4),
            "branch_id": round(branch_id_mul, 4),
        }
        candidate["source_relevance_pre_calibration"] = raw_source_relevance
        final_score, breakdown = score_candidate(
            candidate, profile,
            positive_centroid, negative_centroid,
            positive_texts, negative_texts,
            db, scoring_settings,
            candidate_text=candidate_text_map.get(key),
            candidate_embedding=candidate_embedding_map.get(key),
            lexical_profile=lexical_profile,
            positive_example_embeddings=positive_example_embeddings,
            negative_example_embeddings=negative_example_embeddings,
            precomputed_lexical_details=precomputed_lexical_map.get(key),
            user_topic_embeddings=user_topic_embeddings,
            preloaded_preference_profile=preloaded_preference_profile,
            topic_provider=_topic_provider,
        )
        candidate["score"] = final_score
        # Fold retrieval provenance ("why this paper surfaced") into the
        # persisted breakdown so the UI can explain more than the branch
        # label: the actual query string that found it, and the core /
        # explore topic hints that defined the branch.
        matched_query = str(candidate.get("matched_query") or "").strip()
        if matched_query:
            breakdown["matched_query"] = matched_query
        branch_core = [t for t in (candidate.get("branch_core_topics") or []) if t]
        if branch_core:
            breakdown["branch_core_topics"] = branch_core
        branch_explore = [t for t in (candidate.get("branch_explore_topics") or []) if t]
        if branch_explore:
            breakdown["branch_explore_topics"] = branch_explore
        # Outcome calibration provenance — composed multiplier + the
        # per-axis components + the pre-calibration value, so the
        # breakdown explains *which* axes pushed the candidate up or
        # down rather than collapsing it into one opaque number.
        breakdown["source_calibration_multiplier"] = round(
            float(candidate.get("source_calibration_multiplier") or 1.0), 4
        )
        breakdown["source_calibration_components"] = candidate.get(
            "source_calibration_components"
        ) or {"source_api": 1.0, "branch_mode": 1.0, "branch_id": 1.0}
        breakdown["source_relevance_pre_calibration"] = round(
            float(candidate.get("source_relevance_pre_calibration") or 0.0), 4
        )

        # T4: promote the "truthful provenance" numbers into a clean
        # sub-dict the UI can consume without inspecting the full 60+
        # raw-diagnostic keys. Every number here already exists
        # somewhere in `breakdown` (raw diagnostics) or `candidate`
        # (scoring inputs) — we're just giving the frontend a single
        # canonical place to look.
        specter_cosine = float(breakdown.get("semantic_similarity_raw") or 0.0)
        lexical_similarity_raw = float(breakdown.get("lexical_similarity_raw") or 0.0)
        negative_hit_raw = float(breakdown.get("semantic_similarity_negative_raw") or 0.0)
        candidate_author_text = str(candidate.get("authors") or "").lower()
        profile_authors = [
            str(name or "").lower()
            for name in (profile.get("author_affinity") or {}).keys()
            if name
        ]
        shared_authors: list[str] = []
        if candidate_author_text and profile_authors:
            for name in profile_authors[:50]:
                if len(name) >= 4 and name in candidate_author_text:
                    shared_authors.append(name)
                    if len(shared_authors) >= 5:
                        break
        breakdown["provenance"] = {
            # Normalized 0..1 for the frontend. Legacy rows that
            # persisted 0..100 still coerce cleanly on read.
            "score_pct": round(float(final_score or 0.0) / 100.0, 4),
            "specter_cosine": round(specter_cosine, 4) if specter_cosine else None,
            "lexical_similarity": round(lexical_similarity_raw, 4) if lexical_similarity_raw else None,
            "negative_hit": round(negative_hit_raw, 4) if negative_hit_raw >= 0.35 else None,
            "shared_authors_count": len(shared_authors) if shared_authors else None,
            "shared_authors_sample": shared_authors[0] if shared_authors else None,
        }

        candidate["score_breakdown"] = breakdown
        final_scores.append(float(final_score or 0.0))
        if breakdown.get("candidate_embedding_ready"):
            embedding_ready_count += 1
        text_mode = str(breakdown.get("text_similarity_mode") or "none")
        topic_mode = str(breakdown.get("topic_match_mode") or "none")
        text_mode_counts[text_mode] = int(text_mode_counts.get(text_mode) or 0) + 1
        topic_mode_counts[topic_mode] = int(topic_mode_counts.get(topic_mode) or 0) + 1
        try:
            raw_semantic_scores.append(float(breakdown.get("semantic_similarity_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        try:
            raw_semantic_exemplar_scores.append(float(breakdown.get("semantic_similarity_exemplar_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        try:
            support_value = float(breakdown.get("semantic_similarity_support_raw") or 0.0)
            raw_semantic_support_scores.append(support_value)
        except (TypeError, ValueError):
            pass
        try:
            raw_lexical_scores.append(float(breakdown.get("lexical_similarity_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        for target, key_name in (
            (raw_lexical_word_scores, "lexical_similarity_word_raw"),
            (raw_lexical_char_scores, "lexical_similarity_char_raw"),
            (raw_lexical_term_scores, "lexical_similarity_term_raw"),
        ):
            try:
                target.append(float(breakdown.get(key_name) or 0.0))
            except (TypeError, ValueError):
                pass
        try:
            if float((breakdown.get("text_similarity") or {}).get("value") or 0.0) < 0.24:
                low_similarity_count += 1
        except Exception:
            pass
        try:
            if float(breakdown.get("semantic_similarity_raw") or 0.0) > 0.0 and float(breakdown.get("semantic_similarity_raw") or 0.0) < 0.14:
                compressed_similarity_count += 1
        except Exception:
            pass
        for signal_name in signal_names:
            signal_detail = breakdown.get(signal_name) or {}
            if not isinstance(signal_detail, dict):
                continue
            signal_value_sums[signal_name] += float(signal_detail.get("value") or 0.0)
            signal_weighted_sums[signal_name] += float(signal_detail.get("weighted") or 0.0)
    timings_ms["scoring"] = int(round((perf_counter() - phase_started) * 1000))
    avg_signal_values = {
        name: round(signal_value_sums[name] / max(1, len(merged)), 4)
        for name in signal_names
    }
    avg_signal_weighted = {
        name: round(signal_weighted_sums[name] / max(1, len(merged)), 4)
        for name in signal_names
    }
    top_driver_names = [
        name
        for name, _value in sorted(
            avg_signal_weighted.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
    ]
    _log(
        "scoring_profile",
        f"Lens '{lens_name}': scoring finished in {timings_ms['scoring']}ms; average drivers were {', '.join(top_driver_names) or 'n/a'}",
        data={
            "candidate_count": len(merged),
            "scoring_ms": timings_ms["scoring"],
            "score_range": {
                "min": round(min(final_scores), 3) if final_scores else 0.0,
                "avg": round(sum(final_scores) / max(1, len(final_scores)), 3) if final_scores else 0.0,
                "max": round(max(final_scores), 3) if final_scores else 0.0,
            },
            "avg_signal_values": avg_signal_values,
            "avg_signal_weighted": avg_signal_weighted,
            "text_similarity_modes": text_mode_counts,
            "topic_match_modes": topic_mode_counts,
            "candidate_embeddings_used": embedding_ready_count,
            "raw_similarity": {
                "semantic_avg": round(sum(raw_semantic_scores) / max(1, len(raw_semantic_scores)), 4) if raw_semantic_scores else 0.0,
                "semantic_exemplar_avg": round(sum(raw_semantic_exemplar_scores) / max(1, len(raw_semantic_exemplar_scores)), 4) if raw_semantic_exemplar_scores else 0.0,
                "semantic_support_avg": round(sum(raw_semantic_support_scores) / max(1, len(raw_semantic_support_scores)), 4) if raw_semantic_support_scores else 0.0,
                "lexical_avg": round(sum(raw_lexical_scores) / max(1, len(raw_lexical_scores)), 4) if raw_lexical_scores else 0.0,
                "lexical_word_avg": round(sum(raw_lexical_word_scores) / max(1, len(raw_lexical_word_scores)), 4) if raw_lexical_word_scores else 0.0,
                "lexical_char_avg": round(sum(raw_lexical_char_scores) / max(1, len(raw_lexical_char_scores)), 4) if raw_lexical_char_scores else 0.0,
                "lexical_term_avg": round(sum(raw_lexical_term_scores) / max(1, len(raw_lexical_term_scores)), 4) if raw_lexical_term_scores else 0.0,
                "compressed_rate": round(compressed_similarity_count / max(1, len(merged)), 3) if merged else 0.0,
                "low_text_similarity_rate": round(low_similarity_count / max(1, len(merged)), 3) if merged else 0.0,
            },
        },
    )

    full_ranked = sorted(merged.values(), key=lambda x: x["score"], reverse=True)
    staging_limit = min(
        len(full_ranked),
        max(max(1, limit), min(max(1, limit) * 3, max(1, limit) + 60)),
    )
    ranked, diversity_summary = _select_diverse_recommendation_candidates(
        full_ranked,
        limit=max(1, limit),
        staging_limit=staging_limit,
    )
    _log(
        "scoring_result",
        f"Lens '{lens_name}': selected {len(ranked)} diverse candidates after scoring",
        data={
            "ranked": len(ranked),
            "candidate_pool": len(full_ranked),
            "diversity": diversity_summary,
            "top_candidates": [
                {
                    "title": str(item.get("title") or "")[:120],
                    "score": round(float(item.get("score") or 0.0), 3),
                    "source_type": item.get("source_type"),
                    "branch_label": item.get("branch_label"),
                }
                for item in ranked[:5]
            ],
        },
    )

    suggestion_set_id = uuid.uuid4().hex
    now = datetime.utcnow().isoformat()
    external_lane_counts: dict[str, int] = {}
    for item in external:
        if str(item.get("branch_id") or "").strip():
            branch_mode = str(item.get("branch_mode") or "branch").strip() or "branch"
            lane = f"branch_{branch_mode}"
        else:
            lane = str(item.get("source_type") or item.get("branch_mode") or "external").strip() or "external"
        external_lane_counts[lane] = external_lane_counts.get(lane, 0) + 1
    graph_lane_counts: dict[str, int] = {}
    for item in graph:
        lane = str(item.get("source_type") or "graph").strip() or "graph"
        graph_lane_counts[lane] = graph_lane_counts.get(lane, 0) + 1
    retrieval_summary = {
        "seed_count": len(seeds),
        "recommendation_mode": external_summary.get("recommendation_mode", "balanced"),
        "temperature": external_summary.get("temperature"),
        "channels": {
            "lexical": len(lexical),
            "vector": len(vector),
            "graph": len(graph),
            "external": len(external),
        },
        "graph_lanes": graph_lane_counts,
        "graph_cache": graph_summary,
        "external_lanes": external_lane_counts,
        "weights": channel_weights,
        "taste_profile": external_summary.get("taste_profile") or {},
        "negative_profile": external_summary.get("negative_profile") or {},
        "budgets": external_summary.get("budgets") or {},
        "lane_runs": external_summary.get("lane_runs") or [],
        "diversity": diversity_summary,
        # Outcome calibration snapshot — three axes (source_api,
        # branch_mode, branch_id), each empty on a fresh DB. Per-axis
        # block carries quality `0..1`, the resulting `[0.5, 1.5]`
        # multiplier, and the raw counts so a developer can read this
        # and tell whether an estimate is grounded in real traffic
        # or still mostly Bayesian prior. Composed multiplicatively in
        # log-space at scoring time, see `compose_calibration_multipliers`.
        "calibration": {
            "source_api": _calibration_block(calibration_source),
            "branch_mode": _calibration_block(calibration_branch_mode),
            "branch_id": _calibration_block(calibration_branch_id),
        },
    }
    cold_start_summary = _build_topic_keyword_cold_start_summary(
        lens,
        seed_count=len(seeds),
        lexical_count=len(lexical),
        graph_count=len(graph),
        external_lane_counts=external_lane_counts,
    )
    if cold_start_summary is not None:
        retrieval_summary["cold_start"] = cold_start_summary
        _log(
            "cold_start",
            f"Lens '{lens_name}': topic cold-start state is {cold_start_summary['state']}",
            data=cold_start_summary,
        )

    # NOTE: the recommendations provenance columns (source_type/source_api/
    # source_key/branch_id/branch_label/branch_mode) are guaranteed by the
    # schema/migrator layer (`api.deps.init_db_schema`), not patched in here —
    # this hot path is forward-only and assumes the current shape (D-10).
    #
    # NOTE: old recommendations are deleted atomically with the insert below,
    # NOT here — so a crash during scoring doesn't wipe existing recommendations.

    _log("insert", f"Lens '{lens_name}': staging top {len(ranked)} recommendations")

    phase_started = perf_counter()
    staged_candidates: list[tuple[int, dict, str]] = []
    staged_paper_ids: list[str] = []
    for idx, candidate in enumerate(ranked, start=1):
        paper_id = library_app.upsert_paper(
            db,
            # S-4/S-9: defer hydration scheduling — staging up to ~110 papers
            # auto-scheduled (and re-scanned operation_status for) a sweep PER
            # paper. Write the ledger rows here, fire ONE sweep after the loop.
            auto_schedule_hydration=False,
            title=candidate["title"],
            authors=candidate.get("authors"),
            abstract=candidate.get("abstract"),
            year=candidate.get("year"),
            journal=candidate.get("journal"),
            url=candidate.get("url"),
            doi=candidate.get("doi"),
            openalex_id=candidate.get("openalex_id"),
            semantic_scholar_id=candidate.get("semantic_scholar_id"),
            semantic_scholar_corpus_id=candidate.get("semantic_scholar_corpus_id"),
            cited_by_count=int(candidate.get("cited_by_count") or 0),
            # T5 — persist S2 TLDR + influential citation count so Library +
            # PaperCard + citation_quality scoring can use them without
            # re-fetching. Falsy values are skipped by `upsert_paper` so
            # existing rows don't get their TLDRs clobbered by later
            # non-S2 lanes.
            tldr=(candidate.get("tldr") or None),
            influential_citation_count=(
                int(candidate["influential_citation_count"])
                if candidate.get("influential_citation_count") is not None
                else None
            ),
            status="tracked",
            added_from="discovery",
        )
        upsert_specter2_embedding(db, paper_id, candidate)
        candidate["paper_id"] = paper_id
        staged_candidates.append((idx, candidate, paper_id))
        staged_paper_ids.append(paper_id)
    timings_ms["paper_upsert"] = int(round((perf_counter() - phase_started) * 1000))
    # S-4/S-9: one bounded hydration sweep for everything staged this refresh,
    # instead of an auto-scheduled job + operation_status scan per paper.
    if staged_paper_ids:
        try:
            from alma.services.corpus_rehydrate import schedule_pending_hydration_sweep

            schedule_pending_hydration_sweep(
                reason="lens_refresh",
                target_paper_ids=list(dict.fromkeys(staged_paper_ids)),
            )
        except Exception as exc:
            logger.debug("Lens refresh hydration sweep skipped: %s", exc)
    # Commit the tracked-paper upserts independently of the rec swap below.
    # Per `lessons.md` → "Background jobs must release the writer lock ...
    # AND between phases" + "commit per unit of work": the paper rows are
    # useful on their own (Corpus, Feed, Library backfill) even if a later
    # phase fails. Keeping them in one txn with the swap means an 11-min
    # refresh that crashes at the swap discards the entire upsert phase.
    _commit_if_pending(db)

    phase_started = perf_counter()
    status_by_paper: dict[str, str] = {}
    reading_status_by_paper: dict[str, str] = {}
    actioned_paper_ids: set[str] = set()
    unique_paper_ids = [paper_id for paper_id in dict.fromkeys(staged_paper_ids) if str(paper_id).strip()]
    for chunk in _chunked(unique_paper_ids, 200):
        placeholders = ", ".join("?" for _ in chunk)
        status_rows = db.execute(
            f"SELECT id, status, reading_status FROM papers WHERE id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in status_rows:
            status_by_paper[str(row["id"])] = str(row["status"] or "tracked")
            reading_status_by_paper[str(row["id"])] = str(row["reading_status"] or "").strip()
        # Only block re-surfacing while paper dismissals are still in their
        # cooldown window. Saves drive status='library'; reading-list handoffs
        # are caught by the reading-status filter; like/love/dislike are
        # rating signals and should not hide the paper.
        action_rows = db.execute(
            f"""
            SELECT paper_id, user_action, action_at, created_at
            FROM recommendations
            WHERE paper_id IN ({placeholders})
              AND user_action IN ('dismiss', 'dismissed', 'remove', 'removed')
            """,
            chunk,
        ).fetchall()
        for paper_id, score in _paper_dismissal_scores(action_rows).items():
            if score <= _PAPER_DISMISS_SUPPRESSION_THRESHOLD:
                actioned_paper_ids.add(paper_id)

    rec_rows: list[tuple] = []
    inserted_paper_ids: list[str] = []
    seen_paper_ids: set[str] = set()
    skipped_library = 0
    skipped_actioned = 0
    skipped_duplicate_paper = 0
    for idx, candidate, paper_id in staged_candidates:
        paper_status = status_by_paper.get(paper_id, "tracked")
        if paper_status in ("library", "dismissed", "removed") or reading_status_by_paper.get(paper_id):
            skipped_library += 1
            continue
        if paper_id in actioned_paper_ids:
            skipped_actioned += 1
            continue
        # Two distinct candidate keys (e.g. one matched by DOI, another
        # by title) can resolve to the same DB paper_id after the
        # candidate→paper upsert. The recommendations table has a
        # UNIQUE (lens_id, paper_id, suggestion_set_id) constraint, so
        # a second insert for the same paper would crash the whole
        # batch. Keep the higher-ranked candidate (lower idx) only.
        if paper_id in seen_paper_ids:
            skipped_duplicate_paper += 1
            continue
        seen_paper_ids.add(paper_id)
        provenance = _derive_recommendation_provenance(candidate, lens_id)
        display_rank = len(rec_rows) + 1
        rec_rows.append(
            (
                uuid.uuid4().hex,
                suggestion_set_id,
                lens_id,
                paper_id,
                display_rank,
                float(candidate["score"]),
                json.dumps(
                    candidate.get("score_breakdown", {}),
                    default=_jsonable_numeric,
                ),
                provenance.get("source_type"),
                provenance.get("source_api"),
                provenance.get("source_key"),
                provenance.get("branch_id"),
                provenance.get("branch_label"),
                provenance.get("branch_mode"),
                now,
            )
        )
        inserted_paper_ids.append(paper_id)
        if len(rec_rows) >= max(1, limit):
            break
    timings_ms["filter_existing"] = int(round((perf_counter() - phase_started) * 1000))

    retrieval_summary["filters"] = {
        "ranked": len(ranked),
        "staged": len(staged_candidates),
        "skipped_library_or_sunk": skipped_library,
        "skipped_previously_actioned": skipped_actioned,
        "skipped_duplicate_paper": skipped_duplicate_paper,
        "insertable": len(rec_rows),
    }
    retrieval_summary["final_mix"] = _recommendation_mix_summary(rec_rows, ranked_by_paper=ranked)
    _log(
        "filter_result",
        f"Lens '{lens_name}': {len(rec_rows)} recommendations remained after library/action filters",
        data=retrieval_summary["filters"],
    )
    phase_started = perf_counter()
    # Atomic swap: delete old un-actioned recommendations and insert new ones together.
    # This prevents data loss if the operation crashes during scoring above.
    db.execute("DELETE FROM recommendations WHERE lens_id = ? AND user_action IS NULL", (lens_id,))
    db.execute(
        """
        INSERT INTO suggestion_sets (
            id, lens_id, context_type, trigger_source, retrieval_summary, ranker_version, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            suggestion_set_id,
            lens_id,
            lens["context_type"],
            trigger_source,
            json.dumps(retrieval_summary),
            "lens-v2-9signal",
            now,
        ),
    )
    if rec_rows:
        db.executemany(
            """
            INSERT INTO recommendations (
                id, suggestion_set_id, lens_id, paper_id, rank, score, score_breakdown,
                source_type, source_api, source_key, branch_id, branch_label, branch_mode,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rec_rows,
        )
    timings_ms["recommendation_insert"] = int(round((perf_counter() - phase_started) * 1000))

    timings_ms["total"] = int(round((perf_counter() - overall_start) * 1000))
    retrieval_summary["timings_ms"] = dict(timings_ms)

    db.execute(
        "UPDATE suggestion_sets SET retrieval_summary = ? WHERE id = ?",
        (json.dumps(retrieval_summary), suggestion_set_id),
    )

    db.execute(
        "UPDATE discovery_lenses SET last_refreshed_at = ? WHERE id = ?",
        (now, lens_id),
    )
    inserted = len(rec_rows)
    _log(
        "done",
        f"Lens '{lens_name}': refresh complete with {inserted} retained recommendations",
        data={
            "inserted": inserted,
            "timings_ms": timings_ms,
            "channels": retrieval_summary["channels"],
        },
    )
    return {
        "lens_id": lens_id,
        "suggestion_set_id": suggestion_set_id,
        "context_type": lens["context_type"],
        "channels": retrieval_summary["channels"],
        "weights": channel_weights,
        "retrieval_summary": retrieval_summary,
        "inserted": inserted,
    }


def _retrieve_lexical_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    config = lens.get("context_config") or {}
    explicit_topics = config.get("topics") if isinstance(config.get("topics"), list) else None
    if lens["context_type"] == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        explicit_topics = [keyword] if keyword else []
    topics = _extract_keywords(seeds, explicit=explicit_topics, max_keywords=10)
    if not topics:
        return []
    results = openalex_related.search_works_by_topics(
        topics, limit=limit, from_year=datetime.utcnow().year - 3
    )
    # Stamp provenance so downstream `_derive_recommendation_provenance`
    # routes these to the `lexical` bucket instead of the un-tagged
    # `lens_retrieval` catch-all. `source_key` carries the actual query
    # so the per-source-key diversity cap can group same-query results.
    source_key = " OR ".join(topics[:10])
    for item in results:
        if not str(item.get("source_type") or "").strip():
            item["source_type"] = "lexical"
        if not str(item.get("source_api") or "").strip():
            item["source_api"] = "openalex"
        if not str(item.get("source_key") or "").strip():
            item["source_key"] = source_key
    return results


def _retrieve_vector_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    if not _NUMPY_AVAILABLE:
        return []

    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [sid for sid in seed_ids if sid]
    if not seed_ids:
        return []

    active_model = sim_module.get_active_embedding_model(db)
    placeholders = ",".join("?" for _ in seed_ids)
    seed_rows = db.execute(
        f"""
        SELECT paper_id, embedding
        FROM publication_embeddings
        WHERE model = ? AND paper_id IN ({placeholders})
        """,
        [active_model, *seed_ids],
    ).fetchall()
    if not seed_rows:
        return []

    from alma.core.vector_blob import decode_vector
    seed_vecs: list["np.ndarray"] = []
    for row in seed_rows:
        try:
            vec = decode_vector(row["embedding"])
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            seed_vecs.append(vec / norm)
        except Exception:
            continue
    if not seed_vecs:
        return []

    centroid = np.mean(np.vstack(seed_vecs), axis=0)
    centroid_norm = float(np.linalg.norm(centroid))
    if centroid_norm <= 0.0:
        return []
    centroid = centroid / centroid_norm

    rows = db.execute(
        """
        SELECT pe.paper_id, pe.embedding, p.title, p.authors, p.url, p.doi, p.year, p.journal, p.cited_by_count
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = ? AND p.status NOT IN ('dismissed', 'removed')
        """,
        [active_model],
    ).fetchall()

    # Score every embedded paper against the centroid — there used to
    # be a `max_scan` cap that stopped after `limit*20` rows in
    # arbitrary SQLite row order, which meant the lane returned the
    # best-N-of-an-arbitrary-1000 rather than the best-N-of-the-
    # corpus. With float16-encoded vectors and numpy dot, scoring 5–10k
    # rows takes well under a second; the previous "performance" cap
    # was actively producing worse retrieval at noticeable cost to
    # quality.
    seed_set = set(seed_ids)
    scored: list[tuple[float, dict]] = []
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id or paper_id in seed_set:
            continue
        try:
            vec = decode_vector(row["embedding"])
            if vec.shape != centroid.shape:
                continue
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            vec = vec / norm
            sim = float(np.dot(centroid, vec))
            score = max(0.0, (sim + 1.0) / 2.0)
            if score <= 0.0:
                continue
            scored.append(
                (
                    score,
                    {
                        # paper_id MUST be carried so the downstream
                        # embedding lookup can short-circuit (the lookup
                        # falls back to openalex_id/doi/s2_id resolution
                        # but those won't match for purely-internal
                        # corpus papers that haven't been backfilled
                        # with their OpenAlex ID yet). Without this the
                        # vector lane's own candidates couldn't get
                        # their cached embeddings reused at scoring,
                        # collapsing text_similarity_mode to "lexical".
                        "paper_id": paper_id,
                        # `source_type` drives the diversity_interleave
                        # round-robin so the vector lane gets fair air
                        # time. We deliberately leave `source_key` unset
                        # — the per-source-key cap is meant for
                        # external-query identifiers (taste_author:smith,
                        # taste_topic:visual_cortex), not for lane
                        # labels. With `source_key=""` the diversity
                        # cap skips these candidates entirely.
                        "source_type": "vector",
                        "title": row["title"] or "",
                        "authors": row["authors"] or "",
                        "url": row["url"] or "",
                        "doi": row["doi"] or "",
                        "score": score,
                        "year": row["year"],
                        "journal": row["journal"] or "",
                        "cited_by_count": row["cited_by_count"] or 0,
                    },
                )
            )
        except Exception:
            continue

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[: max(1, limit)]]


# Wall-clock cap on the graph lane's external citation fallbacks (OpenAlex
# related/citing/referenced + S2 related). The graph lane previously had NO
# overall deadline — when Semantic Scholar rate-limited (429 → up to ~2 min
# of retry+cooldown PER call, see core/http_sources S2 policy), the
# `with ThreadPoolExecutor` (shutdown=wait) fallbacks waited out the full
# budget × calls, ballooning a normally-4 s lane to minutes (task 19 F2
# live-reproduced a 7.3 min hang). Mirrors source_search.DEFAULT_LANE_DEADLINE_S.
_GRAPH_FALLBACK_DEADLINE_S: float = 8.0

# Backstop wall-clock cap per retrieval lane when the 4 lanes run
# concurrently (F3). Lanes are bounded internally (external: 8 s source-lane
# deadline; graph fallbacks: _GRAPH_FALLBACK_DEADLINE_S), so this only catches
# a pathological hang in a section not otherwise bounded (e.g. an OpenAlex
# batch-fetch helper). The lane pool is shut down wait=False so an abandoned
# lane can't block the refresh.
_LANE_HARD_CAP_S: float = 60.0


def _drain_futures_within_deadline(
    executor: ThreadPoolExecutor,
    future_map: dict,
    deadline_s: float,
) -> dict:
    """Return the subset of ``future_map`` that completed within ``deadline_s``.

    Cancels any futures still pending at the deadline and shuts the executor
    down with ``wait=False`` so a worker stuck in a slow/rate-limited HTTP
    call (e.g. S2 429 retry/cooldown) does not block the caller — it exits on
    its own per-request HTTP timeout. Same bounded-fan-out contract as
    ``discovery/source_search.search_across_sources``.
    """
    done_map: dict = {}
    try:
        for fut in as_completed(list(future_map.keys()), timeout=max(0.1, deadline_s)):
            done_map[fut] = future_map[fut]
    except TimeoutError:
        for fut in future_map:
            if not fut.done():
                fut.cancel()
    finally:
        executor.shutdown(wait=False)
    return done_map


def _retrieve_graph_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    def _seed_graph_identifier(seed: dict) -> str:
        openalex_id = str(seed.get("openalex_id") or "").strip()
        if openalex_id:
            return openalex_id
        return str(seed.get("doi") or "").strip()

    graph_summary: dict[str, Any] = {
        "seed_total": len(seeds),
        "seed_local_reference_ready": 0,
        "seed_reference_backfilled": 0,
        "local_reference_candidates": 0,
        "fallback_candidates": 0,
        "fallback_used": False,
        "semantic_related_candidates": 0,
        "fallback_sources": [],
    }

    def _backfill_local_references() -> None:
        seed_rows = [
            (
                str(seed.get("id") or "").strip(),
                str(seed.get("openalex_id") or "").strip(),
            )
            for seed in seeds
            if str(seed.get("id") or "").strip()
        ]
        if not seed_rows:
            return
        seed_ids = [paper_id for paper_id, _openalex_id in seed_rows]
        placeholders = ", ".join("?" for _ in seed_ids)
        ref_counts: dict[str, int] = {}
        try:
            rows = db.execute(
                f"""
                SELECT paper_id, COUNT(*) AS ref_count
                FROM publication_references
                WHERE paper_id IN ({placeholders})
                GROUP BY paper_id
                """,
                seed_ids,
            ).fetchall()
            ref_counts = {str(row["paper_id"]): int(row["ref_count"] or 0) for row in rows}
        except sqlite3.OperationalError:
            ref_counts = {}

        graph_summary["seed_local_reference_ready"] = sum(
            1 for paper_id, _openalex_id in seed_rows if int(ref_counts.get(paper_id) or 0) > 0
        )
        missing_pairs = [
            (paper_id, openalex_id)
            for paper_id, openalex_id in seed_rows
            if openalex_id and int(ref_counts.get(paper_id) or 0) <= 0
        ]
        if not missing_pairs:
            return
        try:
            reference_map = batch_fetch_referenced_works_for_openalex_ids(
                [openalex_id for _paper_id, openalex_id in missing_pairs],
                batch_size=25,
                max_workers=4,
            )
        except Exception:
            return

        backfilled = 0
        for paper_id, openalex_id in missing_pairs:
            referenced_ids = reference_map.get(openalex_id) or []
            if not referenced_ids:
                continue
            backfilled += _upsert_referenced_works(db, paper_id, referenced_ids)
        if backfilled > 0:
            graph_summary["seed_reference_backfilled"] = backfilled
            graph_summary["seed_local_reference_ready"] = min(
                len(seed_rows),
                int(graph_summary["seed_local_reference_ready"] or 0)
                + sum(1 for _paper_id, openalex_id in missing_pairs if reference_map.get(openalex_id)),
            )

    def _local_reference_candidates() -> list[dict]:
        seed_ids = [str(seed["id"]) for seed in seeds if seed.get("id")]
        if not seed_ids:
            return []
        seed_placeholders = ", ".join("?" for _ in seed_ids)
        try:
            # Lens-adjacency guarantee: a reference must be cited by at
            # least one seed paper to be considered. Within that pool,
            # rank by how many papers in the **entire local corpus**
            # cite it (not just seeds), tie-break by seed_overlap.
            #
            # Why corpus-wide instead of seeds-only: pure seed_overlap
            # systematically penalizes recent references — a 2024 paper
            # cited by 1 seed and 4 other corpus papers (corpus_overlap=5)
            # otherwise loses to a 2010 paper cited by 1 seed and 0
            # others (corpus_overlap=1) when both have seed_overlap=1.
            # Widening the count to the corpus gives newer references a
            # fair shot at the top-K. The scorer's recency_boost takes
            # over from there once OpenAlex enrichment supplies the
            # publication_date.
            rows = db.execute(
                f"""
                SELECT
                    pr.referenced_work_id,
                    COUNT(DISTINCT pr.paper_id) AS corpus_overlap,
                    SUM(CASE WHEN pr.paper_id IN ({seed_placeholders}) THEN 1 ELSE 0 END) AS seed_overlap
                FROM publication_references pr
                WHERE pr.referenced_work_id IS NOT NULL
                  AND pr.referenced_work_id IN (
                      SELECT DISTINCT referenced_work_id
                      FROM publication_references
                      WHERE paper_id IN ({seed_placeholders})
                        AND referenced_work_id IS NOT NULL
                  )
                GROUP BY pr.referenced_work_id
                ORDER BY corpus_overlap DESC, seed_overlap DESC, pr.referenced_work_id ASC
                LIMIT ?
                """,
                [*seed_ids, *seed_ids, limit],
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        # publication_references stores the bare integer ID; OpenAlex
        # batch fetch expects W-prefixed strings.
        work_ids = [
            f"W{r['referenced_work_id']}"
            for r in rows
            if r["referenced_work_id"]
        ]
        if not work_ids:
            return []
        works = batch_fetch_works_by_openalex_ids(work_ids, batch_size=50, max_workers=4)
        out: list[dict] = []
        for idx, work_id in enumerate(work_ids):
            work = works.get(work_id)
            if not work:
                continue
            title = (work.get("display_name") or "").strip()
            if not title:
                continue
            authorships = work.get("authorships") or []
            authors = ", ".join((a.get("author") or {}).get("display_name", "") for a in authorships)
            primary_loc = work.get("primary_location") or {}
            source = primary_loc.get("source") or {}
            out.append(
                {
                    "openalex_id": work_id,
                    "title": title,
                    "authors": authors,
                    "url": primary_loc.get("landing_page_url") or primary_loc.get("pdf_url") or work.get("id") or "",
                    "doi": work.get("doi") or "",
                    "score": max(0.1, 1.0 - (idx / max(1, len(work_ids)))),
                    "year": work.get("publication_year"),
                    "journal": source.get("display_name") if isinstance(source, dict) else "",
                    "cited_by_count": work.get("cited_by_count") or 0,
                    "source_type": "graph_reference",
                    "source_api": "openalex",
                    "source_key": "local_references",
                }
            )
            if len(out) >= limit:
                break
        graph_summary["local_reference_candidates"] = len(out)
        return out

    _backfill_local_references()
    local_candidates = _local_reference_candidates()
    if len(local_candidates) >= limit:
        return local_candidates[:limit], graph_summary

    merged: dict[str, dict] = {}
    for item in local_candidates:
        merged[_candidate_key(item)] = item

    fallback_budget = max(limit, 8)
    identifiers = [
        identifier
        for identifier in (_seed_graph_identifier(seed) for seed in seeds[:10])
        if identifier
    ]
    seed_dois = [
        str(seed.get("doi") or "").strip()
        for seed in seeds[:10]
        if str(seed.get("doi") or "").strip()
    ]
    # Parallelize the 3-call OA fallback fan-out across all seed identifiers.
    # Pre-refactor this was up to 30 sequential OpenAlex HTTP calls; bounded
    # pool keeps peak concurrent requests at max_workers=6.
    if identifiers:
        graph_summary["fallback_used"] = True
        graph_summary["fallback_sources"] = sorted(set([*graph_summary.get("fallback_sources", []), "openalex"]))
        relation_calls = (
            ("graph_reference", openalex_related.fetch_referenced_works, 0.72),
            ("graph_citing", openalex_related.fetch_citing_works, 0.58),
            ("graph_related", openalex_related.fetch_related_works, 0.44),
        )
        call_keys: list[tuple[str, str, float]] = [
            (identifier, relation, weight)
            for identifier in identifiers
            for relation, _fn, weight in relation_calls
        ]
        fn_map = {relation: fn for relation, fn, _ in relation_calls}
        # Bounded fan-out: drain up to the deadline, abandon (shutdown
        # wait=False) any OpenAlex call still pending so a slow/429 source
        # can't stall the lane (F2).
        gpool = ThreadPoolExecutor(max_workers=min(6, max(1, len(call_keys))), thread_name_prefix="graph-oa")
        future_map = {
            gpool.submit(fn_map[rel], identifier, 6): (identifier, rel, weight)
            for identifier, rel, weight in call_keys
        }
        done_map = _drain_futures_within_deadline(gpool, future_map, _GRAPH_FALLBACK_DEADLINE_S)
        if len(done_map) < len(future_map):
            graph_summary["oa_fallback_timed_out"] = True
        for fut, (identifier, rel, weight) in done_map.items():
            if len(merged) >= fallback_budget:
                continue
            try:
                items = fut.result() or []
            except Exception as exc:
                logger.debug("graph OA fallback (%s) failed for %s: %s", rel, identifier, exc)
                items = []
            for idx, item in enumerate(items):
                candidate = dict(item)
                candidate["source_type"] = rel
                candidate["source_api"] = str(candidate.get("source_api") or "openalex")
                candidate["source_key"] = identifier
                base = float(candidate.get("score", 0.25) or 0.25)
                rank_factor = _clamp(1.0 - (idx / max(1, len(items) * 1.6)), 0.12, 1.0)
                candidate["score"] = round(_clamp((base * weight) + (rank_factor * (1.0 - weight)), 0.05, 1.0), 4)
                key = _candidate_key(candidate)
                existing = merged.get(key)
                if existing is None or float(candidate.get("score") or 0.0) > float(existing.get("score") or 0.0):
                    merged[key] = candidate
                if len(merged) >= fallback_budget:
                    break

    if len(merged) < fallback_budget and seed_dois:
        from alma.discovery import semantic_scholar

        graph_summary["fallback_used"] = True
        graph_summary["fallback_sources"] = sorted(set([*graph_summary.get("fallback_sources", []), "semantic_scholar"]))
        # Bounded fan-out (F2): S2 is the rate-limit-prone source — abandon
        # any call still in 429 retry/cooldown at the deadline (this is the
        # exact site of the live-reproduced 7.3 min hang).
        s2pool = ThreadPoolExecutor(max_workers=min(4, max(1, len(seed_dois))), thread_name_prefix="graph-s2")
        future_map = {s2pool.submit(semantic_scholar.fetch_related_papers, doi, 6): doi for doi in seed_dois}
        done_map = _drain_futures_within_deadline(s2pool, future_map, _GRAPH_FALLBACK_DEADLINE_S)
        if len(done_map) < len(future_map):
            graph_summary["s2_fallback_timed_out"] = True
        for fut, doi in done_map.items():
            if len(merged) >= fallback_budget:
                continue
            try:
                items = fut.result() or []
            except Exception as exc:
                logger.debug("graph S2 related fetch failed for %s: %s", doi, exc)
                items = []
            graph_summary["semantic_related_candidates"] = int(graph_summary.get("semantic_related_candidates") or 0) + len(items)
            for idx, item in enumerate(items):
                candidate = dict(item)
                candidate["source_type"] = "graph_semantic_related"
                candidate["source_api"] = "semantic_scholar"
                candidate["source_key"] = doi
                base = float(candidate.get("score", 0.25) or 0.25)
                rank_factor = _clamp(1.0 - (idx / max(1, len(items) * 1.5)), 0.12, 1.0)
                candidate["score"] = round(_clamp((base * 0.52) + (rank_factor * 0.48), 0.05, 1.0), 4)
                key = _candidate_key(candidate)
                existing = merged.get(key)
                if existing is None or float(candidate.get("score") or 0.0) > float(existing.get("score") or 0.0):
                    merged[key] = candidate

    ranked = sorted(merged.values(), key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
    graph_summary["fallback_candidates"] = max(0, len(ranked) - len(local_candidates))
    return ranked[:limit], graph_summary


def _retrieve_external_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
    preference_profile: Optional[dict[str, Any]] = None,
    positive_pubs: Optional[list[dict]] = None,
) -> tuple[list[dict], dict[str, Any]]:
    def _setting_bool(key: str, default: bool) -> bool:
        raw = settings.get(key)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _setting_int(key: str, default: int, lo: int, hi: int) -> int:
        raw = settings.get(key)
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, value))

    out: list[dict] = []
    settings = read_settings(db)
    branch_enabled = _setting_bool("strategies.branch_explorer", True)
    topic_search_enabled = _setting_bool("strategies.topic_search", True)
    taste_topics_enabled = _setting_bool("strategies.taste_topics", True)
    taste_authors_enabled = _setting_bool("strategies.taste_authors", True)
    taste_venues_enabled = _setting_bool("strategies.taste_venues", True)
    recent_wins_enabled = _setting_bool("strategies.recent_wins", True)
    recommendation_mode = str(settings.get("recommendation_mode", "balanced") or "balanced").strip().lower()
    branch_controls = _resolve_lens_branch_controls(lens)
    # The old discrete pin/boost/mute "smart suggestion" panel was replaced by
    # continuous per-branch auto_weight (see _compute_branch_auto_weight),
    # which is applied directly inside the branch budget allocator below.
    # Manual branch_controls remain the only user-driven overrides.
    effective_branch_controls = branch_controls
    temperature = _resolve_branch_temperature(settings, branch_controls.get("temperature"))
    current_year = datetime.utcnow().year
    profile = preference_profile or {}
    preferred_topics = _top_profile_terms(
        dict(profile.get("topic_weights") or {}),
        limit=_setting_int("limits.taste_topic_queries", 3, 1, 6),
    )
    preferred_authors = _top_preferred_authors(
        db,
        limit=_setting_int("limits.taste_author_queries", 3, 1, 6),
    )
    preferred_venues = _top_profile_terms(
        dict(profile.get("journal_affinity") or {}),
        limit=_setting_int("limits.taste_venue_queries", 2, 1, 4),
        min_weight=0.14,
    )
    recent_win_queries = _build_recent_win_queries(
        db,
        list(positive_pubs or []),
        limit=_setting_int("limits.recent_win_queries", 2, 1, 4),
    )
    negative_context = _negative_preference_context(db, profile)

    # Cache repeated source searches within one refresh run.  Each cache entry
    # is a `Future` returned by the shared lane executor so that every
    # submission is dispatched in parallel; consumers block with `.result()`
    # at the point of use.  `_lane_timings` records per-call wall-clock and
    # `_lane_diagnostics` captures per-source timing + timeouts so that
    # `lane_runs` can surface a `duration_ms` and `slowest_source` value
    # even though each future was fired concurrently (D-AUDIT-10b / -10c).
    query_cache: dict[tuple[str, str, int, int], "Future[list[dict]]"] = {}
    _lane_timings: dict[tuple, int] = {}
    _lane_diagnostics: dict[tuple, dict[str, Any]] = {}
    lane_runs: list[dict[str, Any]] = []

    def _lane_diag_fields(cache_key: tuple) -> dict[str, Any]:
        """Pull the slowest-source + timeout info from diagnostics for a lane_run."""
        diag = _lane_diagnostics.get(cache_key) or {}
        fields: dict[str, Any] = {}
        slowest = diag.get("slowest_source")
        if isinstance(slowest, dict):
            fields["slowest_source"] = slowest.get("source")
            fields["slowest_source_ms"] = slowest.get("duration_ms")
        per_source = diag.get("per_source_ms")
        if per_source:
            fields["per_source_ms"] = dict(per_source)
        timed_out = diag.get("timed_out_sources") or []
        if timed_out:
            fields["timed_out_sources"] = list(timed_out)
        return fields

    # S-6: 12 lane workers (was 6). A single refresh queues ~26-30 lane futures
    # (branch core/explore + taste + graph + external + S2-recommend +, before
    # S-2, the per-author fetches); 6 workers serialized them into ~5 waves. The
    # rate-limited sources can't be sped up by more workers — S2 is gated to 1
    # rps process-wide by SourceHttpClient._concurrency_slot, arXiv to 1 req/3s —
    # so widening only overlaps the FAST sources (OpenAlex 100 req/s, Crossref).
    # Worst case 12 × 2 OpenAlex sub-calls (search_works_hybrid) ≈ 3 OA req/s
    # averaged over the 8s lane deadline, far under the 100 req/s key ceiling.
    lane_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="lens-lane")

    def _submit_source_search(
        cache_key: tuple,
        query: str,
        per_query: int,
        from_year: int,
        *,
        mode: str,
    ) -> "Future[list[dict]]":
        """Submit a `search_across_sources` call to the lane executor.

        Stores the Future in `query_cache` keyed by `cache_key` so repeat
        requests in the same refresh piggyback on the same in-flight call.
        Wraps the fn with a timer that records wall-clock duration into
        `_lane_timings` so `lane_runs` can carry `duration_ms` without a
        second timing pass.  Per-source diagnostics (which of the 5
        sources was slowest, which timed out) feed into
        `_lane_diagnostics` so `lane_runs` expose them for profiling.
        """
        existing = query_cache.get(cache_key)
        if existing is not None:
            return existing

        diagnostics: dict[str, Any] = {}

        def _timed_call() -> list[dict]:
            started = perf_counter()
            try:
                return source_search.search_across_sources(
                    query,
                    limit=per_query,
                    from_year=from_year,
                    settings=settings,
                    mode=mode,
                    temperature=temperature,
                    semantic_scholar_mode="bulk",
                    diagnostics=diagnostics,
                )
            finally:
                _lane_timings[cache_key] = int(round((perf_counter() - started) * 1000))
                _lane_diagnostics[cache_key] = diagnostics

        future = lane_executor.submit(_timed_call)
        query_cache[cache_key] = future
        return future

    def _resolve_lane(cache_entry: "Future[list[dict]] | list[dict]") -> list[dict]:
        if isinstance(cache_entry, Future):
            try:
                return cache_entry.result() or []
            except Exception as exc:
                logger.warning("lens lane source search failed: %s", exc)
                return []
        return cache_entry or []

    # Branch exploration: cluster seed papers, use AI to craft search
    # queries from seed titles+abstracts, then search across all sources.
    #
    # Two-pass structure so every lane submission is queued on the shared
    # lane_executor before any `.result()` blocks.  Pass 1 builds one
    # `branch_plan` per branch (with LLM query plans precomputed) and
    # submits every source search.  Pass 2 iterates the plans and consumes
    # results from the cache — the executor has been working on them
    # concurrently in the meantime.
    branch_plans: list[dict[str, Any]] = []
    if branch_enabled and seeds:
        max_branches = _setting_int("branches.max_clusters", 6, 2, 12)
        max_active = _setting_int("branches.max_active_for_retrieval", 4, 1, 12)
        core_variants = _setting_int("branches.query_core_variants", 2, 1, 4)
        explore_variants = _setting_int("branches.query_explore_variants", 2, 1, 4)

        branches = _build_seed_branches(
            db,
            seeds,
            settings=settings,
            max_branches=max_branches,
            temperature=temperature,
            lens_id=str(lens.get("id") or "") or None,
        )
        branches = _apply_branch_controls(
            branches,
            effective_branch_controls,
            db=db,
            lens_id=str(lens.get("id") or "").strip() or None,
        )
        # Enrich each branch with its outcome history so the budget allocator
        # can read `auto_weight` (continuous multiplier derived from past
        # save/dismiss patterns) alongside the manual pin/boost/mute flags.
        branch_outcome_map = _load_branch_outcome_map(
            db,
            lens_id=str(lens.get("id") or "").strip() or None,
            days=60,
        )
        branches = _enrich_branches_with_outcomes(
            branches,
            branch_outcome_map,
            db=db,
            lens_id=str(lens.get("id") or "").strip() or None,
        )
        # Auto-lifecycle: branches whose auto_weight crossed the rotate
        # or mute threshold get their topics rotated or get auto-muted.
        # Self-correcting on the next refresh — if the rotation pulls
        # saves, auto_weight rises and the branch returns to its core
        # angle automatically.
        branches = _apply_branch_auto_lifecycle(branches)
        active_branches = [branch for branch in branches if branch.get("is_active")]
        if active_branches:
            prioritized = active_branches[:max_active]
            branch_budget_weights = []
            for branch in prioritized:
                # auto_weight is the continuous multiplier from past outcomes
                # (range AUTO_WEIGHT_FLOOR..AUTO_WEIGHT_CEIL, neutral 1.0 when
                # signal is thin). Pin/Boost are hard floors that prevent the
                # auto-weight from starving a branch the user explicitly
                # endorsed; without an override, auto_weight rules.
                weight = float(branch.get("auto_weight") or 1.0)
                if branch.get("is_pinned"):
                    weight = max(weight, 1.65)
                elif branch.get("is_boosted"):
                    weight = max(weight, 1.3)
                branch_budget_weights.append(weight)
            core_ratio = _clamp(0.82 - (0.36 * temperature), 0.42, 0.82)
            total_branch_weight = sum(branch_budget_weights) or 1.0
            # Absolute minimum per-branch budget. A branch starved
            # below this floor never gets enough recommendations to
            # generate calibration signal — making its weakness
            # self-fulfilling. The proportional share still applies on
            # top, but no active branch drops below `min_budget`.
            min_per_branch = max(
                4,
                _setting_int("branches.min_budget_per_branch", 8, 1, 32),
            )
            for branch, branch_weight in zip(prioritized, branch_budget_weights):
                branch_id = str(branch.get("id") or "")
                branch_label = str(branch.get("label") or branch_id)
                core_topics = list(branch.get("core_topics") or [])
                explore_topics = list(branch.get("explore_topics") or [])
                proportional = int(round((limit * branch_weight) / total_branch_weight))
                per_branch = max(min_per_branch, proportional)
                branch_score_bonus = 0.0
                if branch.get("is_pinned"):
                    branch_score_bonus = 0.1
                elif branch.get("is_boosted"):
                    branch_score_bonus = 0.05

                # Branch query planning is deterministic — stitches queries
                # from core / explore topics + seed titles. The LLM-backed
                # planner was removed in 2026-04 (see tasks/01_LLM_PRODUCTION_EXIT.md);
                # the deterministic stitcher is now the only path.
                query_plan = _plan_branch_queries_deterministic(
                    branch,
                    temperature=temperature,
                    max_core=core_variants,
                    max_explore=explore_variants,
                )

                core_queries = [q for q in (query_plan.get("core_queries") or []) if str(q).strip()][:core_variants]
                explore_queries = [q for q in (query_plan.get("explore_queries") or []) if str(q).strip()][:explore_variants]
                if not core_queries and core_topics:
                    core_queries = [" ".join(core_topics[:3])]
                if not explore_queries and explore_topics:
                    explore_queries = [" ".join(explore_topics[:3])]

                core_limit_total = max(1, int(round(per_branch * core_ratio)))
                explore_limit_total = max(0, per_branch - core_limit_total)

                core_per_query = (
                    max(2, core_limit_total // max(1, len(core_queries)))
                    if core_queries and core_limit_total > 0
                    else 0
                )
                explore_per_query = (
                    max(1, explore_limit_total // max(1, len(explore_queries)))
                    if explore_queries and explore_limit_total > 0
                    else 0
                )
                from_year_core = current_year - (2 if temperature <= 0.35 else 4)
                from_year_explore = current_year - (3 if temperature <= 0.35 else 6)

                # Queue all submissions for this branch onto the lane executor.
                # Submissions are non-blocking, so queuing proceeds to the next
                # branch immediately.
                if core_per_query > 0:
                    for query in core_queries:
                        _submit_source_search(
                            ("core", query, core_per_query, from_year_core),
                            query,
                            core_per_query,
                            from_year_core,
                            mode="core",
                        )
                if explore_per_query > 0:
                    for query in explore_queries:
                        _submit_source_search(
                            ("explore", query, explore_per_query, from_year_explore),
                            query,
                            explore_per_query,
                            from_year_explore,
                            mode="explore",
                        )

                branch_plans.append(
                    {
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "core_topics": core_topics,
                        "explore_topics": explore_topics,
                        "core_queries": core_queries,
                        "explore_queries": explore_queries,
                        "core_per_query": core_per_query,
                        "explore_per_query": explore_per_query,
                        "from_year_core": from_year_core,
                        "from_year_explore": from_year_explore,
                        "branch_score_bonus": branch_score_bonus,
                    }
                )

    taste_budget_factor = {
        "explore": 0.30,
        "balanced": 0.34 if temperature <= 0.35 else 0.28,
        "exploit": 0.38,
    }.get(recommendation_mode, 0.34 if temperature <= 0.35 else 0.28)
    taste_budget_total = max(6, int(round(limit * taste_budget_factor)))
    topic_hint = preferred_topics[0][0] if preferred_topics else ""
    lane_specs: list[dict[str, Any]] = []
    explicit_topic_keyword = ""

    if topic_search_enabled and lens["context_type"] == "topic_keyword":
        config = lens.get("context_config") or {}
        explicit_topic_keyword = str(config.get("keyword") or config.get("query") or "").strip()
        if explicit_topic_keyword:
            lane_specs.append(
                {
                    "lane_type": "cold_start_topic",
                    "query": explicit_topic_keyword,
                    "source_key": explicit_topic_keyword,
                    "strength": 0.76 if not seeds else 0.58,
                    "budget": max(4, int(round(limit * (0.40 if not seeds else 0.18)))),
                    "from_year": current_year - 5,
                    "mode": "core" if not seeds else "explore",
                }
            )
        elif not seeds:
            lane_specs.append(
                {
                    "lane_type": "cold_start_topic",
                    "query": str(lens.get("name") or "").strip(),
                    "source_key": str(lens.get("name") or "").strip(),
                    "strength": 0.62,
                    "budget": max(4, int(round(limit * 0.34))),
                    "from_year": current_year - 5,
                    "mode": "core",
                }
            )

    if taste_topics_enabled:
        topic_budget = max(2, int(round(taste_budget_total * 0.34)))
        for topic, strength in preferred_topics:
            lane_specs.append(
                {
                    "lane_type": "taste_topic",
                    "query": topic,
                    "source_key": topic,
                    "strength": _clamp(0.58 + (float(strength) * 0.35), 0.45, 1.0),
                    "budget": topic_budget,
                    "from_year": current_year - 4,
                    "mode": "core",
                }
            )

    if taste_authors_enabled:
        author_budget = max(2, int(round(taste_budget_total * 0.28)))
        for author, strength in preferred_authors:
            author_query = author if not topic_hint else f"{author} {topic_hint}"
            lane_specs.append(
                {
                    "lane_type": "taste_author",
                    "query": author_query,
                    "source_key": author,
                    "strength": _clamp(0.54 + (float(strength) * 0.32), 0.42, 1.0),
                    "budget": author_budget,
                    "from_year": current_year - 4,
                    "mode": "core",
                }
            )

    if taste_venues_enabled:
        venue_budget = max(1, int(round(taste_budget_total * 0.16)))
        for venue, strength in preferred_venues:
            venue_query = venue if not topic_hint else f"{venue} {topic_hint}"
            lane_specs.append(
                {
                    "lane_type": "taste_venue",
                    "query": venue_query,
                    "source_key": venue,
                    "strength": _clamp(0.5 + (float(strength) * 0.3), 0.38, 0.94),
                    "budget": venue_budget,
                    "from_year": current_year - 5,
                    "mode": "core",
                }
            )

    if recent_wins_enabled:
        recent_budget = max(2, int(round(taste_budget_total * 0.22)))
        for query, strength in recent_win_queries:
            lane_specs.append(
                {
                    "lane_type": "recent_win",
                    "query": query,
                    "source_key": query,
                    "strength": float(strength),
                    "budget": recent_budget,
                    "from_year": current_year - 3,
                    "mode": "explore" if temperature >= 0.4 else "core",
                }
            )

    # --- Prefetch: submit every remaining source-search + followed-author
    # --- fetch + S2 recommend to the lane executor BEFORE any consumer blocks
    # --- on a future.  This unlocks cross-section parallelism (branch queries,
    # --- taste lanes, followed-author fetches, and S2 recommend all share the
    # --- same pool of workers and run concurrently).
    for lane in lane_specs:
        query = str(lane.get("query") or "").strip()
        if not query:
            continue
        per_query = max(1, int(lane.get("budget") or 1))
        from_year = int(lane.get("from_year") or current_year - 4)
        mode = str(lane.get("mode") or "core")
        cache_key = (str(lane.get("lane_type") or "taste"), query, per_query, from_year)
        _submit_source_search(cache_key, query, per_query, from_year, mode=mode)

    # S2 recommend lane — resolve seeds from DB, then submit the network call
    # as a Future so it overlaps with the other lanes.  The consume block
    # later reads the result and emits the lane_run + `out` entries.
    s2_recommend_enabled = _setting_bool("strategies.s2_recommend", True)
    s2_source_enabled = _setting_bool("sources.semantic_scholar.enabled", True)
    s2_recommend_future: "Future[list[dict]] | None" = None
    s2_recommend_budget = 0
    s2_positive_seed_ids: list[str] = []
    s2_negative_seed_ids: list[str] = []
    s2_holder: dict[str, Any] = {}
    if s2_recommend_enabled and s2_source_enabled:
        from alma.discovery import semantic_scholar as _s2_lane

        pos_rows = db.execute(
            """
            SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi
            FROM papers
            WHERE status = 'library' AND COALESCE(rating, 0) >= 4
              AND (
                COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(doi), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(semantic_scholar_corpus_id), ''), '') != ''
              )
            ORDER BY COALESCE(rating, 0) DESC, COALESCE(added_at, '') DESC
            LIMIT 50
            """
        ).fetchall()
        neg_rows = db.execute(
            """
            SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi
            FROM papers
            WHERE (
                status IN ('removed', 'dismissed')
                OR COALESCE(rating, 0) BETWEEN 1 AND 2
              )
              AND (
                COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(doi), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(semantic_scholar_corpus_id), ''), '') != ''
              )
            LIMIT 100
            """
        ).fetchall()

        def _row_to_s2_seed_id(row) -> str:
            s2 = str(row["semantic_scholar_id"] or "").strip()
            if s2:
                return s2
            doi = (row["doi"] or "").strip()
            if doi:
                return f"DOI:{doi}"
            corpus = str(row["semantic_scholar_corpus_id"] or "").strip()
            if corpus:
                return f"CorpusID:{corpus}"
            return ""

        s2_positive_seed_ids = [seed for row in pos_rows if (seed := _row_to_s2_seed_id(row))]
        s2_negative_seed_ids = [seed for row in neg_rows if (seed := _row_to_s2_seed_id(row))]
        s2_recommend_budget = max(6, int(round(limit * 0.18)))
        if s2_positive_seed_ids:

            def _s2_recommend_call(
                pos: list[str] = s2_positive_seed_ids,
                neg: list[str] = s2_negative_seed_ids,
                budget: int = s2_recommend_budget,
                holder: dict[str, Any] = s2_holder,
            ) -> list[dict]:
                started = perf_counter()
                try:
                    return _s2_lane.recommend_from_seeds(pos, neg, limit=budget) or []
                except Exception as exc:
                    holder["error"] = str(exc)
                    return []
                finally:
                    holder["ms"] = int(round((perf_counter() - started) * 1000))

            s2_recommend_future = lane_executor.submit(_s2_recommend_call)

    # Followed-author OpenAlex fetches — submit each in parallel, consume
    # once branches and taste lanes have been drained.
    followed_rows = db.execute(
        """
        SELECT a.openalex_id
        FROM followed_authors fa
        JOIN authors a ON a.id = fa.author_id
        WHERE a.openalex_id IS NOT NULL AND TRIM(a.openalex_id) != ''
        LIMIT 10
        """
    ).fetchall()
    year_floor = current_year - 2
    follow_budget_base = {
        "explore": 0.14,
        "balanced": 0.20 + (0.25 * temperature),
        "exploit": 0.28 + (0.18 * temperature),
    }.get(recommendation_mode, 0.20 + (0.25 * temperature))
    follow_budget = max(3, int(round(limit * follow_budget_base)))
    follow_author_limit = min(6, follow_budget)
    # S-2: ONE batched OpenAlex OR-filter call (author.id:A1|A2|...) for all
    # followed authors instead of one request per author. Mirrors the Feed
    # path (batch_fetch_recent_works_for_authors); frees the per-author worker
    # slots on lane_executor for the branch/taste/S2 lanes. A pipe filter is a
    # single OpenAlex list call regardless of author count (≤100 ids/filter),
    # far under the 100 req/s key limit.
    followed_author_ids = [
        (row["openalex_id"] or "").strip()
        for row in followed_rows
        if (row["openalex_id"] or "").strip()
    ]
    followed_author_holder: dict[str, int] = {}
    followed_author_future: "Future[dict[str, list[dict]]] | None" = None
    if followed_author_ids:

        def _timed_batch_author_fetch(
            ids: list[str] = followed_author_ids,
            from_year: int = year_floor,
            per_fetch: int = follow_author_limit,
            holder: dict[str, int] = followed_author_holder,
        ) -> dict[str, list[dict]]:
            started = perf_counter()
            try:
                return openalex_related.batch_fetch_recent_works_for_authors(
                    ids,
                    from_year=from_year,
                    per_author_limit=per_fetch,
                )
            finally:
                holder["ms"] = int(round((perf_counter() - started) * 1000))

        followed_author_future = lane_executor.submit(_timed_batch_author_fetch)

    # --- Consume pass: branch plans first (each resolve blocks only on the
    # --- slowest single future because the rest are already mid-flight).
    for plan in branch_plans:
        branch_id = plan["branch_id"]
        branch_label = plan["branch_label"]
        core_topics = plan["core_topics"]
        explore_topics = plan["explore_topics"]
        core_queries = plan["core_queries"]
        explore_queries = plan["explore_queries"]
        core_per_query = plan["core_per_query"]
        explore_per_query = plan["explore_per_query"]
        from_year_core = plan["from_year_core"]
        from_year_explore = plan["from_year_explore"]
        branch_score_bonus = plan["branch_score_bonus"]

        if core_per_query > 0:
            for query in core_queries:
                cache_key = ("core", query, core_per_query, from_year_core)
                core_results = _resolve_lane(query_cache[cache_key])
                lane_runs.append(
                    {
                        "lane_type": "branch_core",
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "query": query,
                        "from_year": from_year_core,
                        "result_count": len(core_results),
                        "duration_ms": int(_lane_timings.get(cache_key, 0)),
                        **_lane_diag_fields(cache_key),
                    }
                )
                for idx, item in enumerate(core_results):
                    rank_factor = _clamp(1.0 - (idx / max(1, core_per_query * 1.6)), 0.2, 1.0)
                    base = float(item.get("score", 0.35) or 0.35)
                    score = _clamp((base * 0.78) + (rank_factor * 0.22), 0.03, 1.0)
                    out.append(
                        {
                            **item,
                            "score": round(_clamp(score + branch_score_bonus, 0.03, 1.0), 4),
                            "source_type": "branch",
                            "source_key": str(item.get("source_key") or branch_label),
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "branch_mode": "core",
                            "matched_query": query,
                            "branch_core_topics": list(core_topics),
                            "branch_explore_topics": list(explore_topics),
                        }
                    )

        if explore_per_query > 0:
            for query in explore_queries:
                cache_key = ("explore", query, explore_per_query, from_year_explore)
                explore_results = _resolve_lane(query_cache[cache_key])
                lane_runs.append(
                    {
                        "lane_type": "branch_explore",
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "query": query,
                        "from_year": from_year_explore,
                        "result_count": len(explore_results),
                        "duration_ms": int(_lane_timings.get(cache_key, 0)),
                        **_lane_diag_fields(cache_key),
                    }
                )
                for idx, item in enumerate(explore_results):
                    rank_factor = _clamp(1.0 - (idx / max(1, explore_per_query * 1.8)), 0.1, 1.0)
                    base = float(item.get("score", 0.2) or 0.2)
                    score = _clamp(
                        (base * (0.45 + (0.35 * temperature)))
                        + (rank_factor * (0.18 + (0.22 * temperature))),
                        0.02,
                        0.98,
                    )
                    out.append(
                        {
                            **item,
                            "score": round(_clamp(score + branch_score_bonus, 0.02, 0.98), 4),
                            "source_type": "branch",
                            "source_key": str(item.get("source_key") or branch_label),
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "branch_mode": "explore",
                            "matched_query": query,
                            "branch_core_topics": list(core_topics),
                            "branch_explore_topics": list(explore_topics),
                        }
                    )

    # --- Consume pass: taste lane_specs.
    for lane in lane_specs:
        query = str(lane.get("query") or "").strip()
        if not query:
            continue
        per_query = max(1, int(lane.get("budget") or 1))
        from_year = int(lane.get("from_year") or current_year - 4)
        cache_key = (str(lane.get("lane_type") or "taste"), query, per_query, from_year)
        lane_results = _resolve_lane(query_cache.get(cache_key))
        source_key = str(lane.get("source_key") or query)
        lane_runs.append(
            {
                "lane_type": str(lane.get("lane_type") or "taste"),
                "query": query,
                "source_key": source_key,
                "from_year": from_year,
                "result_count": len(lane_results),
                "duration_ms": int(_lane_timings.get(cache_key, 0)),
                **_lane_diag_fields(cache_key),
            }
        )
        lane_strength = float(lane.get("strength") or 0.5)
        lane_type = str(lane.get("lane_type") or "taste_topic")
        for idx, item in enumerate(lane_results):
            rank_factor = _clamp(1.0 - (idx / max(1, per_query * 1.8)), 0.12, 1.0)
            base = float(item.get("score", 0.22) or 0.22)
            score = _clamp((base * 0.62) + (rank_factor * 0.18) + (lane_strength * 0.20), 0.02, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": lane_type,
                    "source_key": source_key,
                    "taste_strength": round(lane_strength, 4),
                    "branch_mode": str(item.get("branch_mode") or ""),
                }
            )

    # --- Consume pass: followed-author fetches (one batched OpenAlex call).
    if followed_author_future is not None:
        try:
            # F4: bound the wait (was unbounded → only the ~20-30 s HTTP
            # timeout). TimeoutError is an Exception subclass in 3.11, so the
            # handler below degrades a slow fetch to empty without stalling.
            recs_by_author = (
                followed_author_future.result(timeout=source_search.DEFAULT_LANE_DEADLINE_S) or {}
            )
        except Exception as exc:
            logger.warning("followed-author batch works fetch failed: %s", exc)
            recs_by_author = {}
        # One call served every author, so the timing is shared across the
        # per-author lane_runs entries below.
        batch_ms = int(followed_author_holder.get("ms") or 0)
        for author_key, recs in recs_by_author.items():
            for item in recs:
                base = float(item.get("score", 0.3) or 0.3)
                score = _clamp(base, 0.05, 1.0)
                out.append(
                    {
                        **item,
                        "score": round(score, 4),
                        "source_type": "followed_author",
                        "source_key": author_key,
                        "branch_mode": "followed_author",
                        "source_api": "openalex",
                    }
                )
            lane_runs.append(
                {
                    "lane_type": "followed_author",
                    "query": author_key,
                    "source_key": author_key,
                    "from_year": year_floor,
                    "result_count": len(recs),
                    "duration_ms": batch_ms,
                }
            )
            if len(out) >= (limit + follow_budget):
                break

    # S2 list-mode recommendations lane — consume the future submitted above
    # (runs concurrently with branch + taste + followed-author lanes on the
    # shared lane_executor).
    if s2_recommend_enabled and s2_source_enabled:
        recommended: list[dict] = []
        if s2_recommend_future is not None:
            try:
                # F4: bound the wait — S2 is the rate-limit-prone source.
                recommended = s2_recommend_future.result(timeout=source_search.DEFAULT_LANE_DEADLINE_S) or []
            except Exception as exc:
                s2_holder.setdefault("error", str(exc))
                recommended = []
        for idx, item in enumerate(recommended):
            rank_factor = _clamp(
                1.0 - (idx / max(1, s2_recommend_budget * 1.4)), 0.2, 1.0
            )
            base = float(item.get("score", 0.45) or 0.45)
            score = _clamp((base * 0.68) + (rank_factor * 0.22), 0.04, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": "semantic_scholar_recommend",
                    "source_key": f"pos={len(s2_positive_seed_ids)}/neg={len(s2_negative_seed_ids)}",
                    "source_api": "semantic_scholar",
                    "branch_mode": "s2_recommend",
                }
            )
        lane_runs.append(
            {
                "lane_type": "semantic_scholar_recommend",
                "query": f"pos={len(s2_positive_seed_ids)} neg={len(s2_negative_seed_ids)}",
                "source_key": "s2_recommend",
                "from_year": 0,
                "result_count": len(recommended),
                "duration_ms": int(s2_holder.get("ms") or 0),
                **({"error": s2_holder["error"]} if s2_holder.get("error") else {}),
            }
        )

    if negative_context:
        filtered: list[dict] = []
        for item in out:
            penalty = _candidate_negative_preference_penalty(item, negative_context)
            if penalty >= 0.72:
                continue
            if penalty > 0.0:
                item = dict(item)
                item["score"] = round(float(item.get("score", 0.0) or 0.0) * (1.0 - (penalty * 0.55)), 4)
                item["negative_pref_penalty"] = round(penalty, 4)
            filtered.append(item)
        out = filtered

    # Dedupe by candidate identity and keep highest score.
    merged: dict[str, dict] = {}
    for item in out:
        key = _candidate_key(item)
        if key not in merged or float(item.get("score", 0.0) or 0.0) > float(merged[key].get("score", 0.0) or 0.0):
            merged[key] = item

    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    summary = {
        "recommendation_mode": recommendation_mode,
        "temperature": round(temperature, 3),
        "taste_profile": {
            "topics": [
                {"term": topic, "weight": round(float(weight), 4)}
                for topic, weight in preferred_topics
            ],
            "authors": [
                {"name": author, "weight": round(float(weight), 4)}
                for author, weight in preferred_authors
            ],
            "venues": [
                {"name": venue, "weight": round(float(weight), 4)}
                for venue, weight in preferred_venues
            ],
            "recent_wins": [
                {"query": query, "strength": round(float(strength), 4)}
                for query, strength in recent_win_queries
            ],
        },
        "negative_profile": {
            "topics": _top_negative_terms(dict(negative_context.get("topics") or {}), limit=4, field_name="term"),
            "authors": _top_negative_terms(dict(negative_context.get("authors") or {}), limit=4, field_name="name"),
            "venues": _top_negative_terms(dict(negative_context.get("journals") or {}), limit=3, field_name="name"),
        },
        "budgets": {
            "taste_budget_total": int(taste_budget_total),
            "followed_author_budget": int(follow_budget),
            "branch_explorer_enabled": bool(branch_enabled),
            "branch_controls": branch_controls,
            "effective_branch_controls": effective_branch_controls,
            "taste_lanes_enabled": {
                "topics": bool(taste_topics_enabled),
                "authors": bool(taste_authors_enabled),
                "venues": bool(taste_venues_enabled),
                "recent_wins": bool(recent_wins_enabled),
            },
        },
        "lane_runs": lane_runs[:24],
        "cold_start_topic": {
            "keyword": explicit_topic_keyword,
            "seed_count": len(seeds),
            "enabled": bool(explicit_topic_keyword) or (lens["context_type"] == "topic_keyword"),
        } if lens["context_type"] == "topic_keyword" else None,
    }
    # All lane futures have been resolved above — release worker threads.
    lane_executor.shutdown(wait=False)
    return ranked[:limit], summary


def _candidate_source_bucket(candidate: dict) -> str:
    source_type = str(candidate.get("source_type") or "").strip()
    if source_type:
        return source_type
    if str(candidate.get("branch_id") or "").strip():
        return "branch"
    source_api = str(candidate.get("source_api") or "").strip()
    if source_api:
        return f"external:{source_api}"
    return "lens_retrieval"


def _candidate_author_keys(candidate: dict) -> list[str]:
    names = parse_author_names(str(candidate.get("authors") or ""))
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = " ".join(str(name or "").lower().split())
        if len(key) < 3 or key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= 8:
            break
    return out


def _candidate_topic_keys(candidate: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    raw = candidate.get("topics") or []
    raw_topics = [raw] if isinstance(raw, str) else list(raw)
    for topic in raw_topics:
        term = ""
        if isinstance(topic, dict):
            term = str(topic.get("term") or topic.get("name") or "").strip().lower()
        else:
            term = str(topic or "").strip().lower()
        if term and term not in seen:
            seen.add(term)
            out.append(term)
        if len(out) >= 5:
            break
    raw_core = candidate.get("branch_core_topics") or []
    raw_explore = candidate.get("branch_explore_topics") or []
    core_topics = [raw_core] if isinstance(raw_core, str) else list(raw_core)
    explore_topics = [raw_explore] if isinstance(raw_explore, str) else list(raw_explore)
    for topic in core_topics + explore_topics:
        term = str(topic or "").strip().lower()
        if term and term not in seen:
            seen.add(term)
            out.append(term)
        if len(out) >= 5:
            break
    return out


def _candidate_venue_key(candidate: dict) -> str:
    return " ".join(str(candidate.get("journal") or "").lower().split())


def _select_diverse_recommendation_candidates(
    candidates: list[dict],
    *,
    limit: int,
    staging_limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    """Select a high-relevance but non-monopolistic staging list.

    The scorer still decides candidate quality. This pass only chooses among
    score-qualified candidates so the persisted suggestion set has
    source/channel coverage and does not collapse onto one prolific author,
    venue, or topic. It stages more than the visible limit because later
    lifecycle filters can remove rows after paper upsert resolves candidate
    identity.
    """
    pool = [item for item in candidates if isinstance(item, dict)]
    if not pool:
        return [], {
            "candidate_pool": 0,
            "selected": 0,
            "staging_limit": 0,
            "source_counts_preview": {},
            "branch_counts_preview": {},
        }

    target = max(1, min(len(pool), int(staging_limit or limit or 1)))
    visible_limit = max(1, int(limit or target))
    top_score = max(float(item.get("score") or 0.0) for item in pool)
    relevance_floor = max(24.0, top_score * 0.48)

    source_groups = {
        _candidate_source_bucket(item)
        for item in pool
        if float(item.get("score") or 0.0) >= relevance_floor
    }
    branch_groups = {
        str(item.get("branch_id") or "").strip()
        for item in pool
        if str(item.get("branch_id") or "").strip()
        and float(item.get("score") or 0.0) >= relevance_floor
    }
    branch_target = min(len(branch_groups), max(1, int(math.ceil(visible_limit * 0.20)))) if branch_groups else 0
    source_target = min(len(source_groups), max(2, int(math.ceil(visible_limit * 0.30)))) if len(source_groups) > 1 else len(source_groups)

    source_cap = max(4, int(math.ceil(target * 0.45)))
    author_cap = max(3, int(math.ceil(target * 0.16)))
    venue_cap = max(4, int(math.ceil(target * 0.22)))
    topic_cap = max(5, int(math.ceil(target * 0.30)))

    remaining = list(pool)
    selected: list[dict] = []
    selected_ids: set[int] = set()
    source_counts: Counter[str] = Counter()
    branch_counts: Counter[str] = Counter()
    author_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    topic_counts: Counter[str] = Counter()

    def _would_exceed_caps(item: dict) -> bool:
        source = _candidate_source_bucket(item)
        if len(source_groups) > 1 and source_counts[source] >= source_cap:
            return True
        for author in _candidate_author_keys(item):
            if author_counts[author] >= author_cap:
                return True
        venue = _candidate_venue_key(item)
        if venue and venue_counts[venue] >= venue_cap:
            return True
        for topic in _candidate_topic_keys(item)[:3]:
            if topic_counts[topic] >= topic_cap:
                return True
        return False

    def _adjusted_score(item: dict) -> float:
        score = float(item.get("score") or 0.0)
        source = _candidate_source_bucket(item)
        branch_id = str(item.get("branch_id") or "").strip()
        authors = _candidate_author_keys(item)
        topics = _candidate_topic_keys(item)
        venue = _candidate_venue_key(item)

        adjusted = score
        if branch_id and branch_counts[branch_id] == 0 and sum(branch_counts.values()) < branch_target:
            adjusted += 10.0
        if source_counts[source] == 0 and sum(1 for v in source_counts.values() if v > 0) < source_target:
            adjusted += 6.0
        adjusted -= source_counts[source] * 1.35
        if branch_id:
            adjusted -= branch_counts[branch_id] * 0.8
        if authors:
            adjusted -= max(author_counts[a] for a in authors) * 2.25
        if venue:
            adjusted -= venue_counts[venue] * 1.5
        if topics:
            adjusted -= max(topic_counts[t] for t in topics[:3]) * 1.0
        return adjusted

    while len(selected) < target and remaining:
        best_idx: int | None = None
        best_value = float("-inf")
        for idx, item in enumerate(remaining):
            if id(item) in selected_ids:
                continue
            score = float(item.get("score") or 0.0)
            if score < relevance_floor and len(selected) < visible_limit:
                continue
            if _would_exceed_caps(item):
                continue
            value = _adjusted_score(item)
            if value > best_value:
                best_idx = idx
                best_value = value

        if best_idx is None:
            # Relax caps/floor only for the overflow staging tail. The visible
            # portion should stay quality-gated; the tail exists to survive
            # later lifecycle filters.
            if len(selected) < visible_limit:
                candidates_left = [
                    (idx, item)
                    for idx, item in enumerate(remaining)
                    if id(item) not in selected_ids and float(item.get("score") or 0.0) >= relevance_floor
                ]
            else:
                candidates_left = [
                    (idx, item)
                    for idx, item in enumerate(remaining)
                    if id(item) not in selected_ids
                ]
            if not candidates_left:
                break
            best_idx, _item = max(
                candidates_left,
                key=lambda pair: float(pair[1].get("score") or 0.0),
            )

        item = remaining.pop(best_idx)
        selected.append(item)
        selected_ids.add(id(item))
        source_counts[_candidate_source_bucket(item)] += 1
        branch_id = str(item.get("branch_id") or "").strip()
        if branch_id:
            branch_counts[branch_id] += 1
        for author in _candidate_author_keys(item):
            author_counts[author] += 1
        venue = _candidate_venue_key(item)
        if venue:
            venue_counts[venue] += 1
        for topic in _candidate_topic_keys(item)[:3]:
            topic_counts[topic] += 1

    preview = selected[:visible_limit]
    preview_sources = Counter(_candidate_source_bucket(item) for item in preview)
    preview_branches = Counter(str(item.get("branch_id") or "").strip() for item in preview if str(item.get("branch_id") or "").strip())
    preview_authors: Counter[str] = Counter()
    preview_venues: Counter[str] = Counter()
    preview_topics: Counter[str] = Counter()
    for item in preview:
        preview_authors.update(_candidate_author_keys(item))
        venue = _candidate_venue_key(item)
        if venue:
            preview_venues[venue] += 1
        preview_topics.update(_candidate_topic_keys(item)[:3])

    summary = {
        "candidate_pool": len(pool),
        "selected": len(selected),
        "visible_limit": visible_limit,
        "staging_limit": target,
        "top_score": round(top_score, 3),
        "min_selected_score": round(min((float(item.get("score") or 0.0) for item in selected), default=0.0), 3),
        "relevance_floor": round(relevance_floor, 3),
        "source_counts_preview": dict(preview_sources),
        "branch_counts_preview": dict(preview_branches),
        "max_author_count_preview": max(preview_authors.values(), default=0),
        "max_venue_count_preview": max(preview_venues.values(), default=0),
        "max_topic_count_preview": max(preview_topics.values(), default=0),
        "source_target": source_target,
        "branch_target": branch_target,
    }
    return selected, summary


def _recommendation_mix_summary(rec_rows: list[tuple], *, ranked_by_paper: list[dict]) -> dict[str, Any]:
    by_paper = {
        str(item.get("paper_id") or "").strip(): item
        for item in ranked_by_paper
        if str(item.get("paper_id") or "").strip()
    }
    source_counts: Counter[str] = Counter()
    branch_counts: Counter[str] = Counter()
    source_api_counts: Counter[str] = Counter()
    author_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    topic_counts: Counter[str] = Counter()
    for row in rec_rows:
        paper_id = str(row[3] or "").strip()
        source_counts[str(row[7] or "unknown")] += 1
        if row[8]:
            source_api_counts[str(row[8])] += 1
        if row[10]:
            branch_counts[str(row[10])] += 1
        candidate = by_paper.get(paper_id) or {}
        author_counts.update(_candidate_author_keys(candidate))
        venue = _candidate_venue_key(candidate)
        if venue:
            venue_counts[venue] += 1
        topic_counts.update(_candidate_topic_keys(candidate)[:3])
    return {
        "total": len(rec_rows),
        "source_type_counts": dict(source_counts),
        "source_api_counts": dict(source_api_counts),
        "branch_counts": dict(branch_counts),
        "branch_attributed": sum(branch_counts.values()),
        "max_author_count": max(author_counts.values(), default=0),
        "max_venue_count": max(venue_counts.values(), default=0),
        "max_topic_count": max(topic_counts.values(), default=0),
        "top_authors": dict(author_counts.most_common(5)),
        "top_venues": dict(venue_counts.most_common(5)),
        "top_topics": dict(topic_counts.most_common(5)),
    }


def _merge_channel_candidates(
    *,
    channel_weights: dict[str, float],
    channels: dict[str, list[dict]],
) -> dict[str, dict]:
    provenance_fields = (
        "source_type",
        "source_api",
        "source_key",
        "branch_id",
        "branch_label",
        "branch_mode",
        "taste_strength",
        "negative_pref_penalty",
    )
    metadata_fields = (
        "title",
        "authors",
        "abstract",
        "url",
        "doi",
        "openalex_id",
        "semantic_scholar_id",
        "semantic_scholar_corpus_id",
        "specter2_embedding",
        "specter2_model",
        "year",
        "journal",
        # T5 — S2-only fields. Kept in the back-fill list so a later
        # S2 lane can populate them on a candidate first found by
        # OpenAlex.
        "tldr",
        "influential_citation_count",
    )

    def _blank(value: object) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, (list, tuple, dict, set)):
            return len(value) == 0
        return False

    merged: dict[str, dict] = {}
    bucket_sets: dict[str, set[str]] = {}
    for channel_name, items in channels.items():
        channel_weight = float(channel_weights.get(channel_name, 0.0) or 0.0)
        if channel_weight <= 0:
            continue
        for item in items:
            key = _candidate_key(item)
            score = float(item.get("score", 0.0) or 0.0)
            weighted = score * channel_weight
            if key not in merged:
                merged[key] = {
                    "title": (item.get("title") or "").strip(),
                    "authors": (item.get("authors") or "").strip(),
                    "abstract": (item.get("abstract") or "").strip(),
                    "url": (item.get("url") or "").strip(),
                    "doi": (item.get("doi") or "").strip(),
                    "openalex_id": (item.get("openalex_id") or "").strip(),
                    "semantic_scholar_id": (item.get("semantic_scholar_id") or "").strip(),
                    "semantic_scholar_corpus_id": str(item.get("semantic_scholar_corpus_id") or "").strip(),
                    "specter2_embedding": item.get("specter2_embedding"),
                    "specter2_model": item.get("specter2_model"),
                    "year": item.get("year"),
                    "journal": item.get("journal"),
                    "cited_by_count": int(item.get("cited_by_count") or 0),
                    # T5: thread S2-origin tldr + influential count through
                    # the merge so downstream upsert_paper can persist them.
                    # Non-S2 lanes won't set these; we keep them as default.
                    "tldr": (item.get("tldr") or "").strip(),
                    "influential_citation_count": int(item.get("influential_citation_count") or 0),
                    "score": 0.0,
                    "score_breakdown": {},
                    "_primary_weighted": 0.0,
                    "_branch_match_weighted": -1.0,
                }
                bucket_sets[key] = set()
                for field in provenance_fields:
                    if field in item:
                        merged[key][field] = item.get(field)
            else:
                for field in metadata_fields:
                    if _blank(merged[key].get(field)) and not _blank(item.get(field)):
                        merged[key][field] = item.get(field)
                merged[key]["cited_by_count"] = max(
                    int(merged[key].get("cited_by_count") or 0),
                    int(item.get("cited_by_count") or 0),
                )
            merged[key]["score"] += weighted
            merged[key]["score_breakdown"][channel_name] = {
                "value": score,
                "weight": channel_weight,
                "weighted": weighted,
            }
            item_branch_id = str(item.get("branch_id") or "").strip()
            if item_branch_id and weighted >= float(merged[key].get("_branch_match_weighted", -1.0) or -1.0):
                merged[key]["_branch_match_weighted"] = weighted
                merged[key]["_branch_match"] = {
                    "branch_id": item_branch_id,
                    "branch_label": item.get("branch_label"),
                    "branch_mode": item.get("branch_mode"),
                    "branch_core_topics": item.get("branch_core_topics"),
                    "branch_explore_topics": item.get("branch_explore_topics"),
                    "matched_query": item.get("matched_query"),
                }
            # Consensus bucket: each non-external channel contributes one
            # bucket per channel name; the external channel contributes one
            # bucket per distinct `source_api` (openalex / semantic_scholar /
            # …) so the same paper surfaced by both OpenAlex *and* S2 inside
            # the external lane counts as 2 independent confirmations rather
            # than 1. The post-score consensus bonus reads `consensus_buckets`
            # and rewards multi-source agreement on a band-relative
            # diminishing-returns curve (see scoring._consensus_bonus).
            if channel_name == "external":
                source_api = str(item.get("source_api") or "").strip().lower()
                bucket_sets[key].add(f"external:{source_api or 'unknown'}")
            else:
                bucket_sets[key].add(f"channel:{channel_name}")
            if weighted >= float(merged[key].get("_primary_weighted", 0.0) or 0.0):
                merged[key]["_primary_weighted"] = weighted
                for field in provenance_fields:
                    if field in item:
                        merged[key][field] = item.get(field)
    for key, value in merged.items():
        value["score"] = round(value["score"] * 100.0, 4)
        value.pop("_primary_weighted", None)
        value.pop("_branch_match_weighted", None)
        branch_match = value.pop("_branch_match", None)
        if isinstance(branch_match, dict) and branch_match.get("branch_id"):
            for field in (
                "branch_id",
                "branch_label",
                "branch_mode",
                "branch_core_topics",
                "branch_explore_topics",
                "matched_query",
            ):
                if _blank(value.get(field)) and not _blank(branch_match.get(field)):
                    value[field] = branch_match.get(field)
            value["branch_attribution_source"] = "branch_lane"
        buckets = sorted(bucket_sets.get(key) or set())
        value["consensus_buckets"] = buckets
        value["consensus_count"] = len(buckets)
    return merged


def _candidate_key(item: dict) -> str:
    doi = normalize_doi((item.get("doi") or "").strip())
    if doi:
        return f"doi:{doi.lower()}"
    openalex_id = (item.get("openalex_id") or "").strip().lower()
    if openalex_id:
        return f"openalex:{openalex_id}"
    title = (item.get("title") or "").strip().lower()
    return f"title:{title}"


