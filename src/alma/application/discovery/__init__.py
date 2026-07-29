"""Discovery use-cases (settings, recommendations, lenses, and signals)."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import uuid
from collections import defaultdict
from concurrent.futures import wait
from datetime import datetime
from threading import Event, Lock
from time import perf_counter
from typing import Any

from alma.core.concurrency import bounded_thread_pool
from alma.core.db_retry import commit_with_retry
from alma.core.db_write import write_section
from alma.core.paper_groups import resolve_paper_root_id
from alma.core.settings_helpers import setting_float
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.utils import resolve_existing_paper_id
from alma.discovery import similarity as sim_module
from alma.discovery.scoring import (
    compute_preference_profile,
    resolve_candidate_topics,
)
from alma.discovery.scoring import (
    load_settings as load_scoring_settings,
)
from alma.discovery.semantic_scholar import upsert_specter2_embedding

from .. import library as library_app
from .citation_fabric import build_citation_fabric_maps
from .exploration import build_slate

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
    get_lens_last_seen,
    get_recommendation,
    home_discovery_snapshot,
    latest_discovery_refresh_window,
    list_lens_recommendations,
    list_lens_signals,
    list_lenses,
    list_recommendations,
    mark_lens_seen,
    mark_recommendation_action,
    read_settings,
    recommendation_stats,
    record_lens_signal,
    reorder_lenses,
    reset_settings_to_defaults,
    update_lens,
    upsert_setting,
)
from .observations import (
    insert_ranking_candidates,
    load_ltr_observations,
    ranking_candidate_rows,
    record_impressions,
)
from .ranker import RANKER_VERSION, apply_repaired_prior, fit_shadow_ranker

# --- D-9: re-exported from .retrieval (moved out of this god-module) ---
from .retrieval import (
    _candidate_author_keys,
    _candidate_key,
    _candidate_source_bucket,
    _candidate_topic_keys,
    _candidate_venue_key,
    _merge_channel_candidates,
    _recommendation_mix_summary,
    _retrieve_external_channel,
    _retrieve_graph_channel,
    _retrieve_lexical_channel,
    _retrieve_vector_channel,
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


logger = logging.getLogger(__name__)

# One lens refresh already fans out into four retrieval lanes plus its shared
# preference-profile worker. Running several refreshes concurrently multiplies
# that entire local workload and makes the per-lane deadline measure scheduler /
# CPU contention rather than pathological work in one lane.
_LENS_REFRESH_GATE = Lock()

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
    """Store a cached artifact in one short, writer-gated transaction.

    Lens refresh keeps ``db`` open across retrieval, measurement, Activity
    logging, and the final recommendation swap. A bare cache upsert on that
    connection leaves Python's implicit transaction open across all of that
    later work while the process writer gate remains free.
    """
    try:
        with write_section(db, label=f"discovery scoring cache: {cache_key}"):
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
    """Queue concurrent lens refreshes behind one local-retrieval work slot."""
    waiting_since = perf_counter()
    with _LENS_REFRESH_GATE:
        waited_ms = int(round((perf_counter() - waiting_since) * 1000))
        if waited_ms >= 250:
            logger.info(
                "lens refresh %s waited %dms for local retrieval capacity",
                lens_id,
                waited_ms,
            )
            if ctx is not None:
                ctx.log_step(
                    "retrieval_capacity",
                    f"Lens refresh waited {waited_ms}ms for local retrieval capacity",
                )
        return _refresh_lens_recommendations(
            db,
            lens_id,
            trigger_source=trigger_source,
            limit=limit,
            ctx=ctx,
        )


def _refresh_lens_recommendations(
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
    # Build the read-only preference projection alongside the local retrieval
    # lanes. On the dev corpus this substrate takes about as long as graph
    # retrieval; doing them serially doubled warm refresh latency.
    from alma.api.deps import open_db_connection as _open_lane_conn

    profile_pool = bounded_thread_pool(
        1,
        thread_name_prefix="lens-preference",
    )

    def _build_preference_profile():
        conn = _open_lane_conn()
        try:
            return compute_preference_profile(
                conn,
                positive_pubs,
                negative_pubs,
                scoring_settings,
                scope_paper_ids=scope_paper_ids,
            )
        finally:
            conn.close()

    profile_future = profile_pool.submit(_build_preference_profile)

    def _await_preference_profile():
        return profile_future.result()
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
                started_at=utcnow().isoformat(),
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
            if lane_abandoned[lane_name].is_set():
                logger.info(
                    "lens lane %s failed after it was abandoned; late result ignored",
                    lane_name,
                )
                raise
            try:
                _set_job_status(
                    subtask_id,
                    status="failed",
                    finished_at=utcnow().isoformat(),
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
        if lane_abandoned[lane_name].is_set():
            logger.info(
                "lens lane %s finished after it was abandoned; late result ignored",
                lane_name,
            )
            timings_ms[f"lane_{lane_name}_ms"] = duration_ms
            return result
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
                finished_at=utcnow().isoformat(),
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
    # Each local-read lane gets its own SQLite connection. Network population
    # and frontier writes belong to scheduled maintenance, never this refresh.
    lane_specs = (
        ("lexical", "Lexical (local corpus + frontier)",
         lambda c: _retrieve_lexical_channel(c, lens, seeds, limit=limit)),
        ("vector", "Vector (local SPECTER2 cosine)",
         lambda c: _retrieve_vector_channel(c, lens, seeds, limit=limit)),
        ("graph", "Graph (local references + PPR)",
         lambda c: _retrieve_graph_channel(c, lens, seeds, limit=limit)),
        ("external", "Taste/branch (offline frontier)",
         lambda c: _retrieve_external_channel(
             c, lens, seeds, limit=limit,
             preference_profile=_await_preference_profile(),
             positive_pubs=positive_pubs)),
    )
    # A running Future cannot be cancelled. Once the parent has abandoned a
    # lane, keep that terminal decision explicit so the late worker cannot
    # overwrite its failed Activity row as "completed".
    lane_abandoned = {name: Event() for name, _label, _fn in lane_specs}

    def _run_lane_with_conn(lane_name: str, label: str, fn):
        conn = _open_lane_conn()
        try:
            return _run_lane_subtask(lane_name, lambda: fn(conn), label=label)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    lane_label_by_name = {name: label for name, label, _ in lane_specs}
    # A backstop against pathological local computation, not a normal budget —
    # and a setting, because when it was a hardcoded 8.0 it silently became the
    # binding constraint on the external lane. Read through the canonical
    # settings reader; there is exactly one of those.
    lane_deadline_s = setting_float(
        scoring_settings, "limits.lane_deadline_seconds", 30.0, 5.0, 300.0
    )

    def _fail_lane_subtask(lane_name: str, reason: str) -> None:
        """Record a lane that produced nothing, in the log AND in Activity.

        Both destinations on purpose: the Python log is for whoever is tailing
        the server, the subtask status and the parent's Activity entry are for
        the person looking at the refresh and wondering why their deck is thin.
        """
        pretty = lane_label_by_name.get(lane_name, lane_name)
        logger.warning("lens lane %s produced nothing: %s", lane_name, reason)
        if not parent_job_id:
            return
        from alma.api.scheduler import add_job_log as _add_job_log
        from alma.api.scheduler import set_job_status as _set_job_status

        try:
            _set_job_status(
                f"{parent_job_id}_lane_{lane_name}",
                status="failed",
                finished_at=utcnow().isoformat(),
                error=reason,
                message=f"{pretty} retrieval failed: {reason}",
                parent_job_id=parent_job_id,
            )
            _add_job_log(
                parent_job_id,
                f"Lane '{pretty}' contributed no candidates: {reason}",
                level="ERROR",
                step=f"lane.{lane_name}.failed",
                data={"lane": lane_name, "reason": reason},
            )
        except Exception:
            logger.debug("lane failure bookkeeping failed for %s", lane_name, exc_info=True)

    lane_results: dict[str, Any] = {}
    lane_pool = bounded_thread_pool(4, thread_name_prefix="lens-lane-top")
    try:
        fut_to_name = {
            lane_pool.submit(_run_lane_with_conn, name, label, fn): name
            for name, label, fn in lane_specs
        }
        done, _pending = wait(
            fut_to_name,
            timeout=lane_deadline_s,
        )
        for fut, name in fut_to_name.items():
            if fut not in done:
                # A lane that misses the deadline costs the deck an entire
                # retrieval family. That MUST be visible: previously this was a
                # bare `logger.warning`, so the subtask row stayed "running"
                # forever, Activity showed no error, and the only symptom was a
                # thinner deck with no explanation (CLAUDE.md → no silent
                # failures). Note the thread is NOT stopped — a running future
                # cannot be cancelled — it is abandoned, and says so.
                lane_abandoned[name].set()
                _fail_lane_subtask(
                    name,
                    f"exceeded the {lane_deadline_s:.0f}s local-read deadline "
                    f"and was abandoned; this deck was built WITHOUT it",
                )
                lane_results[name] = (
                    ([], {}) if name in ("graph", "external") else []
                )
                continue
            try:
                lane_results[name] = fut.result()
            except Exception as exc:
                _fail_lane_subtask(name, f"{type(exc).__name__}: {exc}")
                lane_results[name] = (
                    ([], {}) if name in ("graph", "external") else []
                )
    finally:
        lane_pool.shutdown(wait=False)
    try:
        profile = _await_preference_profile()
    finally:
        profile_pool.shutdown(wait=False)

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
    if external_summary.get("lane_runs") or []:
        _log(
            "retrieval_detail",
            f"Lens '{lens_name}': retrieval plan used {len(external_summary.get('lane_runs') or [])} external lane runs",
            data={
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
        f"Lens '{lens_name}': measuring {len(merged)} candidates for the family ranker",
        data={
            "candidate_count": len(merged),
            "signals": 10,
            "positive_library_examples": len(positive_pubs),
            "negative_library_examples": len(negative_pubs),
            "cached_embeddings_available": cached_embeddings_available,
            "embeddings_available": cached_embeddings_available,
        },
    )

    # --- Measure every candidate, then rank with the one family prior ---
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
    in_memory_embedding_count = 0
    incompatible_embedding_count = 0
    # Citation-fabric scoring features (task 47 §7): candidate→paper coupling +
    # co-citation strengths vs the high-signal set, precomputed once (see below).
    citation_fabric_map: dict[str, Any] = {}
    if cached_embeddings_available and candidate_text_map:
        # First choice: vectors already returned by the retrieval transport.
        # They are usable only when their declared model exactly matches the
        # active local space; cosine across model spaces is meaningless even
        # when dimensions happen to match.
        for key, candidate in merged.items():
            # NOT `vector` — that name holds the vector LANE's results for the
            # rest of this function. Shadowing it here made
            # `retrieval_summary.channels.vector` report the last candidate's
            # embedding length instead of the lane's candidate count, and threw
            # `TypeError: object of type 'NoneType' has no len()` outright
            # whenever the last candidate had no embedding (2026-07-27).
            transported = candidate.get("specter2_embedding")
            model = str(candidate.get("specter2_model") or "").strip()
            if transported is None:
                continue
            if model != active_embedding_model:
                candidate["embedding_model_compatible"] = False
                incompatible_embedding_count += 1
                continue
            try:
                decoded = np.asarray(transported, dtype=np.float32)
            except (TypeError, ValueError):
                continue
            if decoded.ndim != 1 or decoded.size == 0:
                continue
            if positive_centroid is not None and decoded.shape != positive_centroid.shape:
                candidate["embedding_model_compatible"] = False
                incompatible_embedding_count += 1
                continue
            candidate["embedding_model_compatible"] = True
            candidate_embedding_map[key] = decoded
            in_memory_embedding_count += 1

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
                                candidate_paper_ids[matched_key] = resolve_paper_root_id(
                                    db, str(row["id"]), strict=False
                                )

        if candidate_paper_ids:
            pid_to_keys: dict[str, list[str]] = defaultdict(list)
            for key, pid in candidate_paper_ids.items():
                pid_to_keys[pid].append(key)
            for chunk in _chunked(list(pid_to_keys.keys()), 200):
                placeholders = ", ".join("?" for _ in chunk)
                rows = db.execute(
                    f"""
                    SELECT pe.paper_id, pe.embedding
                    FROM publication_embeddings pe
                    JOIN papers p ON p.id = pe.paper_id
                    WHERE pe.model = ?
                      AND pe.paper_id IN ({placeholders})
                      AND {standalone_paper_sql('p')}
                    """,
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
                        if matched_key in candidate_embedding_map:
                            continue
                        candidate_embedding_map[matched_key] = decoded
                        reused_embedding_count += 1

        # Batched, local-only citation-fabric strengths for every candidate that
        # resolved to a local paper — coupling (shared references) + co-citation
        # (shared citers) against the loved/saved set. One precompute; no
        # per-candidate DB access in the scoring loop.
        if candidate_paper_ids and positive_ids:
            pos_titles = {
                str(p.get("id")): str(p.get("title") or "")
                for p in positive_pubs
                if p.get("id")
            }
            citation_fabric_map = build_citation_fabric_maps(
                db, candidate_paper_ids, positive_ids, title_lookup=pos_titles
            )
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
            "candidate_embeddings_in_memory": in_memory_embedding_count,
            "candidate_embeddings_reused": reused_embedding_count,
            "candidate_embeddings_incompatible": incompatible_embedding_count,
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
    # Topic remains an explicit lexical/ontology family in online scoring.
    # Do not invoke an embedding provider here: a configured OpenAI provider
    # would put HTTP back on the refresh path, while cold local SPECTER2
    # inference took minutes and duplicated the candidate-level semantic
    # family already computed from stored exact-model vectors.
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
    #
    # The term list comes from `resolve_candidate_topics` — the same owner the
    # SCORER asks. This loop used to read `candidate["topics"]` directly, which
    # meant it never warmed the title-token fallback, and every candidate with
    # no curated topics paid ~80 single-term forwards after all (2026-07-27).
    phase_started = perf_counter()
    if user_topic_embeddings is not None and _topic_provider is not None:
        candidate_topic_terms: set[str] = set()
        for candidate in merged.values():
            for t in resolve_candidate_topics(candidate, db):
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
    from alma.services.feedback_substrate import (
        preload_candidate_authors as _preload_authors,
    )
    from alma.services.feedback_substrate import (
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

    # Measure each candidate; ranking follows in apply_repaired_prior. The per-candidate
    # pass is extracted into scoring_loop.score_candidates (D-9): it mutates
    # each candidate in place (score + breakdown + provenance) and returns the
    # scoring-profile aggregates consumed below. Read-only inputs are bundled in
    # ScoringContext instead of passed as ~19 loose arguments.
    phase_started = perf_counter()
    signal_names = SIGNAL_NAMES
    from alma.application.signal_lab.scoring_terms import load_lab_scoring_context

    lab_ctx = load_lab_scoring_context(db, scoring_settings)
    _scoring_aggregates = score_candidates(
        merged,
        ScoringContext(
            db=db,
            lab_ctx=lab_ctx,
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
            citation_fabric_map=citation_fabric_map,
            lexical_profile=lexical_profile,
            precomputed_lexical_map=precomputed_lexical_map,
            user_topic_embeddings=user_topic_embeddings,
            preloaded_preference_profile=preloaded_preference_profile,
            topic_provider=_topic_provider,
        ),
    )
    feature_timestamp = utcnow().isoformat()
    shadow_model, shadow_training_size = fit_shadow_ranker(
        load_ltr_observations(db),
        scoring_settings=scoring_settings,
    )
    apply_repaired_prior(
        merged,
        timestamp=feature_timestamp,
        scoring_settings=scoring_settings,
        shadow_model=shadow_model,
        shadow_training_size=shadow_training_size,
    )
    # MMR consumes the same verified, same-model vectors as semantic scoring.
    # Keep them private/in-memory: ranking snapshots record the semantic atoms,
    # not a duplicated 768-float payload.
    for key, candidate in merged.items():
        ranking_vector = candidate_embedding_map.get(key)
        if ranking_vector is not None:
            candidate["_ranking_embedding"] = ranking_vector
            candidate["_ranking_embedding_model"] = active_embedding_model
            candidate["embedding_model_compatible"] = True
    timings_ms["scoring"] = int(round((perf_counter() - phase_started) * 1000))
    signal_value_sums = _scoring_aggregates.signal_value_sums
    text_mode_counts = _scoring_aggregates.text_mode_counts
    topic_mode_counts = _scoring_aggregates.topic_mode_counts
    raw_semantic_scores = _scoring_aggregates.raw_semantic_scores
    raw_semantic_exemplar_scores = _scoring_aggregates.raw_semantic_exemplar_scores
    raw_semantic_support_scores = _scoring_aggregates.raw_semantic_support_scores
    raw_lexical_scores = _scoring_aggregates.raw_lexical_scores
    raw_lexical_word_scores = _scoring_aggregates.raw_lexical_word_scores
    raw_lexical_char_scores = _scoring_aggregates.raw_lexical_char_scores
    raw_lexical_term_scores = _scoring_aggregates.raw_lexical_term_scores
    final_scores = [float(candidate.get("score") or 0.0) for candidate in merged.values()]
    embedding_ready_count = _scoring_aggregates.embedding_ready_count
    compressed_similarity_count = _scoring_aggregates.compressed_similarity_count
    low_similarity_count = _scoring_aggregates.low_similarity_count
    avg_signal_values = {
        name: round(signal_value_sums[name] / max(1, len(merged)), 4)
        for name in signal_names
    }
    # The average points each RANKER FAMILY contributed. Read from the
    # explanation the ranker just wrote, so "top drivers" names the things that
    # actually moved the ranking rather than a parallel weighting of its own.
    family_point_sums: dict[str, float] = {}
    for candidate in merged.values():
        explanation = (candidate.get("score_breakdown") or {}).get("explanation") or {}
        for family in explanation.get("families") or []:
            key = str(family.get("key") or "")
            if key:
                family_point_sums[key] = family_point_sums.get(key, 0.0) + float(
                    family.get("points") or 0.0
                )
    avg_family_points = {
        name: round(total / max(1, len(merged)), 4)
        for name, total in family_point_sums.items()
    }
    top_driver_names = [
        name
        for name, _value in sorted(
            avg_family_points.items(),
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
            "avg_family_points": avg_family_points,
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
    # Freeze the complete counterfactual pool before lifecycle filtering.
    # Two hundred candidates is large enough for exploration/evaluation while
    # keeping the single batch write and paper staging bounded.
    staging_limit = min(len(full_ranked), 200)
    ranked = full_ranked[:staging_limit]
    diversity_summary: dict[str, Any] = {
        "candidate_pool": len(full_ranked),
        "staged_pool": len(ranked),
        "policy": "mmr-explore-v1",
    }
    _log(
        "scoring_result",
        f"Lens '{lens_name}': froze top {len(ranked)} candidates after scoring",
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
    now = utcnow().isoformat()
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
        "feature_schema_version": "discovery-features-v3",
        "feature_timestamp": feature_timestamp,
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

    _log(
        "insert",
        f"Lens '{lens_name}': resolving lifecycle for top {len(ranked)} candidates",
    )

    phase_started = perf_counter()
    staged_candidates: list[tuple[int, dict, str | None]] = []
    for idx, candidate in enumerate(ranked, start=1):
        paper_id = resolve_existing_paper_id(
            db,
            openalex_id=candidate.get("openalex_id"),
            doi=candidate.get("doi"),
            title=candidate.get("title"),
            year=candidate.get("year"),
        )
        if paper_id:
            paper_id = resolve_paper_root_id(db, paper_id, strict=False)
            candidate["paper_id"] = paper_id
        staged_candidates.append((idx, candidate, paper_id))
    timings_ms["paper_identity_resolution"] = int(
        round((perf_counter() - phase_started) * 1000)
    )

    phase_started = perf_counter()
    status_by_paper: dict[str, str] = {}
    reading_status_by_paper: dict[str, str] = {}
    actioned_paper_ids: set[str] = set()
    # For a collection lens: which candidates are already IN the linked
    # collection (the only Library papers that stay hidden for this lens).
    in_linked_collection: set[str] = set()
    unique_paper_ids = [
        paper_id
        for paper_id in dict.fromkeys(
            paper_id for _idx, _candidate, paper_id in staged_candidates
        )
        if paper_id and str(paper_id).strip()
    ]
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
        #
        # SCOPED TO THIS LENS. A dismiss means "not in THIS lens's suggestions
        # for a while" — a visibility verdict on one lens, never a global
        # opinion about the paper (D6 as amended 2026-07-26). Without the
        # `lens_id` filter one dismiss cooled the paper down in EVERY lens,
        # so passing on a paper in a methods lens silently suppressed it in an
        # unrelated topic lens. Global negative opinion is `dislike`, and it
        # travels as a rating — not through this window.
        action_rows = db.execute(
            f"""
            SELECT paper_id, user_action, action_at, created_at
            FROM recommendations
            WHERE lens_id = ?
              AND paper_id IN ({placeholders})
              AND user_action IN ('dismiss', 'dismissed', 'remove', 'removed')
            """,
            (lens_id, *chunk),
        ).fetchall()
        for paper_id, score in _paper_dismissal_scores(action_rows).items():
            if score <= _PAPER_DISMISS_SUPPRESSION_THRESHOLD:
                actioned_paper_ids.add(paper_id)

    eligible_candidates: list[dict] = []
    seen_candidate_identities: set[str] = set()
    skipped_library = 0
    skipped_actioned = 0
    skipped_duplicate_paper = 0
    skipped_low_score = 0
    for candidate in ranked:
        # Every frozen candidate gets an exact policy probability. Lifecycle,
        # score-floor, and identity exclusions are deterministically
        # ineligible—not unknown.
        candidate["_selection"] = {
            "exploration": False,
            "inclusion_probability": 0.0,
            "position_probability": None,
            "final_position": None,
        }
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
        dedup_identity = str(paper_id or candidate.get("candidate_key") or "")
        if dedup_identity in seen_candidate_identities:
            skipped_duplicate_paper += 1
            continue
        seen_candidate_identities.add(dedup_identity)
        if paper_id:
            candidate["paper_id"] = paper_id
        eligible_candidates.append(candidate)

    slate, slate_summary = build_slate(
        eligible_candidates,
        limit=max(1, limit),
    )
    diversity_summary.update(slate_summary)
    phase_started = perf_counter()
    # Candidate promotion + transported-vector persistence is DML too. It used
    # to rely on a bare helper-level commit after the loop, leaving this
    # long-lived refresh connection outside the centralized writer gate.
    with write_section(db, label=f"lens refresh materialize: {lens_id}"):
        materialized_slate, staged_paper_ids = _materialize_slate(db, slate)
    slate = materialized_slate
    timings_ms["paper_upsert"] = int(
        round((perf_counter() - phase_started) * 1000)
    )
    # The materialization write section committed promoted papers before this
    # target-scoped hydration worker opens its own connection. Scheduling first
    # would race SQLite visibility and make the worker see absent targets.
    if staged_paper_ids:
        try:
            from alma.services.corpus_rehydrate import (
                schedule_pending_hydration_sweep,
            )

            schedule_pending_hydration_sweep(
                reason="lens_refresh",
                target_paper_ids=staged_paper_ids,
            )
        except Exception as exc:
            logger.debug("Lens refresh hydration sweep skipped: %s", exc)

    rec_rows: list[tuple] = []
    inserted_paper_ids: list[str] = []
    for candidate in slate:
        paper_id = str(candidate["paper_id"])
        provenance = _derive_recommendation_provenance(candidate, lens_id)
        display_rank = int((candidate.get("_selection") or {}).get("final_position") or len(rec_rows) + 1)
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
    retrieval_summary["diversity"] = diversity_summary
    retrieval_summary["final_mix"] = _recommendation_mix_summary(rec_rows, ranked_by_paper=ranked)
    _log(
        "filter_result",
        f"Lens '{lens_name}': {len(rec_rows)} recommendations remained after library/action filters",
        data=retrieval_summary["filters"],
    )
    phase_started = perf_counter()
    # Atomic swap: delete old un-actioned recommendations and insert new ones
    # together, through the SHARED WRITER GATE.
    #
    # It used to run ungated on the runner's own connection, with the caller
    # doing a bare `conn.commit()`. Gated writers (title resolution, metadata
    # rehydration, the Activity ledger) therefore could not queue against it —
    # they collided at the SQLite level and fell back to `busy_timeout`. A
    # refresh that takes 31 s alone took 195 s with maintenance running, and the
    # Activity writer logged "another connection is holding the SQLite write
    # lock outside the gate" while dropping rows (measured 2026-07-27).
    #
    # Everything slow already happened: retrieval, scoring and enrichment are
    # above this line, so this window is short, bounded, and holds no network
    # I/O — which is what makes it eligible for the gate at all.
    with write_section(db, label=f"lens refresh swap: {lens_id}"):
        _persist_refresh(
            db,
            lens_id=lens_id,
            suggestion_set_id=suggestion_set_id,
            now=now,
            lens=lens,
            trigger_source=trigger_source,
            rec_rows=rec_rows,
            ranked=ranked,
            retrieval_summary=retrieval_summary,
            timings_ms=timings_ms,
            overall_start=overall_start,
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


def _materialize_slate(
    db: sqlite3.Connection,
    slate: list[dict],
) -> tuple[list[dict], list[str]]:
    """Persist selected candidates inside the caller's writer-gated section."""
    materialized_slate: list[dict] = []
    staged_paper_ids: list[str] = []
    for candidate in slate:
        paper_id = str(candidate.get("paper_id") or "").strip()
        if not paper_id:
            paper_id = library_app.upsert_paper(
                db,
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
                semantic_scholar_corpus_id=candidate.get(
                    "semantic_scholar_corpus_id"
                ),
                cited_by_count=int(candidate.get("cited_by_count") or 0),
                tldr=(candidate.get("tldr") or None),
                influential_citation_count=(
                    int(candidate["influential_citation_count"])
                    if candidate.get("influential_citation_count") is not None
                    else None
                ),
                status="tracked",
                added_from="discovery",
            )
        paper_id = resolve_paper_root_id(db, paper_id, strict=False)
        if paper_id in staged_paper_ids:
            raise RuntimeError(
                "Two ranked candidate keys resolved to one canonical paper; "
                "retrieval identity merge must deduplicate before slate selection"
            )
        candidate["paper_id"] = paper_id
        upsert_specter2_embedding(db, paper_id, candidate)
        staged_paper_ids.append(paper_id)
        materialized_slate.append(candidate)
    return materialized_slate, staged_paper_ids


def _persist_refresh(
    db: sqlite3.Connection,
    *,
    lens_id: str,
    suggestion_set_id: str,
    now: str,
    lens: dict,
    trigger_source: str,
    rec_rows: list,
    ranked: list,
    retrieval_summary: dict,
    timings_ms: dict,
    overall_start: float,
) -> None:
    """The refresh's ONE write window. Caller owns the gate; this owns the SQL.

    Extracted so the transaction boundary is a single visible `with` rather
    than sixty lines the reader has to hold in their head to know what is
    inside it.
    """
    phase_started = perf_counter()
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
            RANKER_VERSION,
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
    insert_ranking_candidates(
        db,
        ranking_candidate_rows(
            suggestion_set_id=suggestion_set_id,
            lens_id=lens_id,
            candidates=ranked,
            created_at=now,
        ),
    )
    timings_ms["recommendation_insert"] = int(round((perf_counter() - phase_started) * 1000))

    # 50-L NOTE (audited 2026-07-25): candidates already arrive enriched via
    # the ONE canonical route — the staging loop above persists EVERYTHING the
    # retrieval APIs returned (upsert_paper: abstract/tldr/ids, nothing
    # discarded; upsert_specter2_embedding: source vectors), upsert_paper
    # writes the durable enrichment ledger rows (auto_schedule_hydration=False
    # defers scheduling), and ONE bounded target-scoped
    # schedule_pending_hydration_sweep fires after the loop (S-4/S-9). The
    # ledger self-filters (enriched/terminal sources with the same lookup +
    # fields key are never re-fetched), the chain orders metadata → abstract
    # recovery → S2 vectors → local fill, and the M1 placement hooks put each
    # paper on the map as its vector lands. Do NOT add a second enqueue/sweep
    # here — a rec-subset sweep gets a different target-scoped operation key
    # and double-schedules the same work.

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
