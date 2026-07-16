"""Discovery use-cases (settings, recommendations, lenses, and signals)."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import uuid
from collections import defaultdict
from datetime import datetime
from time import perf_counter
from typing import Any

from alma.core.concurrency import bounded_thread_pool
from alma.core.db_retry import commit_with_retry
from alma.core.scoring_math import clamp
from alma.discovery import similarity as sim_module
from alma.discovery.scoring import (
    compute_preference_profile,
)
from alma.discovery.scoring import (
    load_settings as load_scoring_settings,
)
from alma.discovery.semantic_scholar import upsert_specter2_embedding

from .. import library as library_app
from ..feed import _commit_if_pending

# --- D-9: re-exported from .lens_crud (moved out of this god-module) ---
from .lens_crud import (
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
    DEFAULT_BRANCH_CONTROLS,
    DEFAULT_CHANNEL_WEIGHTS,
    VALID_CONTEXT_TYPES,
    VALID_RECOMMENDATION_ACTIONS,
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
    count_new_discovery_recommendations,
    create_lens,
    default_channel_weights,
    delete_lens,
    get_lens,
    get_recommendation,
    latest_discovery_refresh_window,
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

# --- D-9: re-exported from .retrieval (moved out of this god-module) ---
from .retrieval import (
    _GRAPH_FALLBACK_DEADLINE_S,
    _candidate_author_keys,
    _candidate_key,
    _candidate_source_bucket,
    _candidate_topic_keys,
    _candidate_venue_key,
    _drain_futures_within_deadline,
    _merge_channel_candidates,
    _recommendation_mix_summary,
    _retrieve_external_channel,
    _retrieve_graph_channel,
    _retrieve_lexical_channel,
    _retrieve_vector_channel,
    _select_diverse_recommendation_candidates,
)
from .scoring_loop import SIGNAL_NAMES, ScoringContext, score_candidates

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
    _resolve_branch_resolution,
    _resolve_branch_temperature,
    _seed_strength,
    _seed_token_set,
    _tokenize_for_keywords,
    _top_negative_terms,
    _top_preferred_authors,
    _top_profile_terms,
    preview_lens_branches,
    split_preference_pubs,
)

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


def _cache_get(db: sqlite3.Connection, cache_key: str, fingerprint: str) -> dict | None:
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
               fingerprint: str, *, value_json: str | None = None,
               value_blob: bytes | None = None) -> None:
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
) -> dict | None:
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
    # Every lens computes its taste (preference profile + the scoring
    # positive/negative documents) from its OWN context papers — exactly the way
    # the library lens is scoped to the Library. The seeds already ARE that
    # context set per type (collection / topic_keyword / tag / any future author
    # or monitor lens), so a non-library lens derives its taste from the seeds
    # and passes their ids as the profile scope. Without this the profile bleeds
    # in topics, authors, tags and monitored-corpus priors from the rest of the
    # Library, which is what makes an off-topic cluster look like it "leaked"
    # into a focused lens. The library_global lens keeps the full-Library inputs
    # (and the monitored-corpus prior) because the Library *is* its scope.
    if lens.get("context_type") == "library_global":
        _library_pubs, positive_pubs, negative_pubs = _load_library_preference_inputs(db)
        scope_paper_ids = None
    else:
        positive_pubs, negative_pubs = split_preference_pubs(seeds)
        scope_paper_ids = {str(s["id"]) for s in seeds if s.get("id")}
    profile = compute_preference_profile(
        db, positive_pubs, negative_pubs, scoring_settings, scope_paper_ids=scope_paper_ids
    )
    # A collection lens is *tied* to its collection: it excludes only papers
    # already in that collection, and still surfaces Library papers that live in
    # OTHER collections (so the user can pull them into this one). Non-collection
    # lenses keep the plain "hide everything already in the Library" rule.
    lens_collection_id = None
    if lens.get("context_type") == "collection":
        lens_collection_id = str((lens.get("context_config") or {}).get("collection_id") or "").strip() or None
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
        label: str | None = None,
    ):
        started = perf_counter()
        if not parent_job_id:
            return runner()
        from alma.api.scheduler import add_job_log as _add_job_log
        from alma.api.scheduler import set_job_status as _set_job_status
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
                # Defensive flush of the lane's own connection. The lane's real
                # writes (e.g. reference backfill) already self-gate + commit via
                # write_section in their helpers; this just retries the residual
                # commit on transient lock instead of a raw, un-retried one.
                commit_with_retry(conn, label=f"discovery lane {lane_name}")
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    lane_results: dict[str, Any] = {}
    lane_pool = bounded_thread_pool(4, thread_name_prefix="lens-lane-top")
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
    user_topic_embeddings: dict[str, Any] | None = None
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
    )
    from alma.services.signal_lab import (
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
    from alma.application.outcome_calibration import compute_outcome_calibration
    calibration_source = compute_outcome_calibration(db, dimension="source_api")
    calibration_branch_mode = compute_outcome_calibration(db, dimension="branch_mode")
    calibration_branch_id = compute_outcome_calibration(db, dimension="branch_id")

    # Score each candidate with the full 10-signal system. The per-candidate
    # pass is extracted into scoring_loop.score_candidates (D-9): it mutates
    # each candidate in place (score + breakdown + provenance) and returns the
    # scoring-profile aggregates consumed below. Read-only inputs are bundled in
    # ScoringContext instead of passed as ~19 loose arguments.
    phase_started = perf_counter()
    signal_names = SIGNAL_NAMES
    _scoring_aggregates = score_candidates(
        merged,
        ScoringContext(
            db=db,
            profile=profile,
            scoring_settings=scoring_settings,
            positive_centroid=positive_centroid,
            negative_centroid=negative_centroid,
            positive_texts=positive_texts,
            negative_texts=negative_texts,
            positive_example_embeddings=positive_example_embeddings,
            negative_example_embeddings=negative_example_embeddings,
            candidate_text_map=candidate_text_map,
            candidate_embedding_map=candidate_embedding_map,
            lexical_profile=lexical_profile,
            precomputed_lexical_map=precomputed_lexical_map,
            user_topic_embeddings=user_topic_embeddings,
            preloaded_preference_profile=preloaded_preference_profile,
            topic_provider=_topic_provider,
            calibration_source=calibration_source,
            calibration_branch_mode=calibration_branch_mode,
            calibration_branch_id=calibration_branch_id,
        ),
    )
    timings_ms["scoring"] = int(round((perf_counter() - phase_started) * 1000))
    signal_value_sums = _scoring_aggregates.signal_value_sums
    signal_weighted_sums = _scoring_aggregates.signal_weighted_sums
    text_mode_counts = _scoring_aggregates.text_mode_counts
    topic_mode_counts = _scoring_aggregates.topic_mode_counts
    raw_semantic_scores = _scoring_aggregates.raw_semantic_scores
    raw_semantic_exemplar_scores = _scoring_aggregates.raw_semantic_exemplar_scores
    raw_semantic_support_scores = _scoring_aggregates.raw_semantic_support_scores
    raw_lexical_scores = _scoring_aggregates.raw_lexical_scores
    raw_lexical_word_scores = _scoring_aggregates.raw_lexical_word_scores
    raw_lexical_char_scores = _scoring_aggregates.raw_lexical_char_scores
    raw_lexical_term_scores = _scoring_aggregates.raw_lexical_term_scores
    final_scores = _scoring_aggregates.final_scores
    embedding_ready_count = _scoring_aggregates.embedding_ready_count
    compressed_similarity_count = _scoring_aggregates.compressed_similarity_count
    low_similarity_count = _scoring_aggregates.low_similarity_count
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
    # For a collection lens: which candidates are already IN the linked
    # collection (the only Library papers that stay hidden for this lens).
    in_linked_collection: set[str] = set()
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
        if lens_collection_id:
            member_rows = db.execute(
                f"""SELECT paper_id FROM collection_items
                    WHERE collection_id = ? AND paper_id IN ({placeholders})""",
                (lens_collection_id, *chunk),
            ).fetchall()
            for row in member_rows:
                in_linked_collection.add(str(row["paper_id"]))
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
    skipped_low_score = 0
    # Relevance floor (0-100). Recommendations below it are dropped so the feed
    # doesn't pad with weak, off-topic matches once real neighbours run out.
    # 0 = keep everything (default). See limits.min_score in discovery settings.
    try:
        min_score = max(0.0, float(scoring_settings.get("limits.min_score", "0") or 0))
    except (TypeError, ValueError):
        min_score = 0.0
    for idx, candidate, paper_id in staged_candidates:
        paper_status = status_by_paper.get(paper_id, "tracked")
        if lens_collection_id:
            # Collection lens: hide dismissed/removed and papers already IN this
            # collection, but KEEP Library papers that live in other collections
            # so the user can add them here. (reading_status only hides papers
            # not already in the Library — a saved paper's reading queue state
            # shouldn't hide it from an "add to this collection" surface.)
            if (
                paper_id in in_linked_collection
                or paper_status in ("dismissed", "removed")
                or (paper_status != "library" and reading_status_by_paper.get(paper_id))
            ):
                skipped_library += 1
                continue
        elif paper_status in ("library", "dismissed", "removed") or reading_status_by_paper.get(paper_id):
            skipped_library += 1
            continue
        if paper_id in actioned_paper_ids:
            skipped_actioned += 1
            continue
        if min_score > 0 and float(candidate["score"]) < min_score:
            skipped_low_score += 1
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
        "skipped_low_score": skipped_low_score,
        "min_score": min_score,
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


# Backstop wall-clock cap per retrieval lane when the 4 lanes run
# concurrently (F3). Lanes are bounded internally (external: 8 s source-lane
# deadline; graph fallbacks: _GRAPH_FALLBACK_DEADLINE_S), so this only catches
# a pathological hang in a section not otherwise bounded (e.g. an OpenAlex
# batch-fetch helper). The lane pool is shut down wait=False so an abandoned
# lane can't block the refresh.
_LANE_HARD_CAP_S: float = 60.0
