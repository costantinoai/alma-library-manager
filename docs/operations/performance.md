---
title: Performance
description: The budgets ALMa is held to, the regression probes that pin them, and how to profile your own instance.
---

# Performance

ALMa is a single-user system on a single machine, so "performance"
isn't about scaling — it's about keeping per-action latencies in a
range that doesn't make the UI feel sluggish.

## Budget targets

| Operation | Budget | Notes |
|---|---|---|
| **Discovery lens refresh** (canonical lens, ~330 saved papers) | external retrieval lane (parallel, 12 workers) is the dominant floor (~30 s) | Multi-source retrieval, ranking, branch clustering. After the perf wave (wider lane pool, batched seed/author fetches, deferred hydration) the in-process work is small; the irreducible cost is the rate-limited external network lane. |
| **Page-mount reads** (`/library/saved`, `/feed`, `/authors`) | < 1 s P95 | Stays responsive even during a concurrent refresh. |
| **Detail-panel reads** (`/papers/{id}`, prior / derivative works) | < 500 ms P95 | Mostly cached. |
| **Activity poll** (`/api/v1/activity`) | < 200 ms | Used for the live status of background jobs. |
| **Frontend tsc check** | < 30 s | Useful as a build sanity check. |
| **Cold backend boot** | 2–4 s | Includes schema migration check. |

These are observed budgets on a typical Library (~300 saved
papers, 50 followed authors, with embeddings enabled). YMMV on
larger / smaller corpora.

## Where time goes in a lens refresh

A typical cold lens refresh breaks down roughly as:

| Phase | Time | What's happening |
|---|---|---|
| Setup + seed projection (citation/topic/author neighbours) | < 1 s | Reads your Library and prepares projected signals. Cold cost was ~58 s pre-2026-05-06; expression-index fixes brought it to sub-second. Seed reference / DOI resolution is now batched up front (one OpenAlex OR-filter call per batch) before the related / citation strategies fan out. |
| External retrieval (OpenAlex + S2 + Crossref) | dominant cost (~30 s) | The remaining floor. Lanes fan out in parallel across a **12-worker** pool (was 6) with per-lane deadlines, and the followed-author OpenAlex fetch is now one batched OR-filter call (was one request per author). Most of the wall-clock is the rate-limited sources: S2 is gated to ~1 rps process-wide and arXiv to 1 req/3 s, so more workers only overlap the fast sources (OpenAlex, Crossref) — they can't speed the slow ones. If S2 returns a 429, the lane is dropped process-wide for the rest of the pass (see below), which trims this further. |
| SPECTER2 cosine over candidates | 5–10 s | Vector-cache hits where possible. |
| Scoring | 1–3 s | Per-candidate signal computation. |
| Branch clustering + representative labels | 5–15 s | Label extraction and projection add the upper end. |
| Writes (recommendations + suggestion_set) | 1–3 s | Single transaction. |

Subsequent refreshes against the same lens use cached candidates
where possible and run much faster — typically under 10 seconds.

### Deferred one-sweep hydration

Lens-refresh staging and Feed ingest used to auto-schedule a metadata-
hydration job **per paper** they staged — staging ~110 papers fired
~110 `schedule_with_envelope` + `operation_status` scans (a classic
N+1). Both batch paths now pass `auto_schedule=False` when they write
the per-paper hydration-ledger rows and fire **one**
`schedule_pending_hydration_sweep` after the loop, scoped to the ids
staged that run. This took lens staging from ~84 s to ~1.4 s. (Single
inserts — a Library save, an importer row, an engine / OpenAlex-client
insert — still auto-schedule their own sweep; only the batch paths
defer.) Source: `src/alma/application/discovery/__init__.py`
(`auto_schedule_hydration` flag), `src/alma/application/feed.py`.

### S2 429 process-wide adaptive cooldown

The Semantic Scholar lane is the rate-limit-prone source. Once S2
returns a 429, an adaptive cooldown is armed **process-wide**, and for
the rest of that refresh discovery and feed **drop the S2 lane
entirely** instead of queuing every remaining lane behind the 30 s
adaptive floor and waiting out each lane deadline. The cooldown self-
clears after 60 s. This both speeds the affected refresh and is the
usual explanation for incomplete or slow S2 coverage in a given run.
Source: `src/alma/discovery/source_search.py` (`is_in_adaptive_cooldown`),
`src/alma/core/http_sources.py`.

## Profiling your own refresh

**Activity → Operations** shows per-source timing for each refresh.
Open the most recent lens refresh; the **Per-source timing** sub-
panel breaks down the external retrieval phase per channel. Slow
channels are the place to start.

For deeper profiling, run the slow operation again with the browser
DevTools Network panel open and read both the per-source timing in
**Activity → Operations** and the overall request waterfall. Most
slow refreshes resolve to a single misbehaving channel; the per-
source breakdown points at it directly.

## When something is slow

Step 1: **read the Activity logs.** A slow operation almost always
has an obvious culprit in the per-source timing.

Common patterns:

| Pattern | Cause | Fix |
|---|---|---|
| Lens refresh > 5 minutes | External retrieval lane is slow or timing out | Check per-source timings and tighten refresh limits. |
| Lens refresh stuck at "retrieving" | One source is timing out without raising | Per-lane deadline should kill it; if not, check `discovery.limits.*`. |
| Feed refresh slow | A single monitor is pulling thousands of works | Tighten the monitor's filters or cap `feed.monitor_defaults.daily_max`. Non-author monitor search is now parallelized (a 3-way `ThreadPoolExecutor`: a db-free network phase A, then a sequential db-write phase B), hydration is deferred to one sweep with one commit per monitor, and a single non-author monitor refresh skips the author-mirror sync + the corpus-wide orphan prune — so a slow run almost always traces to one fat monitor, not the loop. |
| Page mount reads slow | Embedding fetch running synchronously, or write contention from a burst of background jobs | Move embedding work to the AI compute background job (the default). If reads stall during a heavy refresh, it's SQLite write contention: lower the worker cap (`ALMA_SCHEDULER_WORKERS`, default 5) so fewer background jobs compete for the single writer. |
| S2 coverage thin / S2 lane skipped this run | Semantic Scholar returned a 429; the adaptive cooldown is armed process-wide | Expected, self-healing. The lane self-clears after 60 s and rejoins the next refresh. Set `SEMANTIC_SCHOLAR_API_KEY` for higher quotas if it recurs. |
| Backfill S2 vectors crawling | Public S2 rate limits | Set `SEMANTIC_SCHOLAR_API_KEY` for higher quotas. |
| Insights / graph page first load slow after a big import | Materialised view rebuilding | Wait — subsequent loads hit the cache. The page shows a "Refreshing…" pill while the rebuild runs. |

## Write contention is the foreground-responsiveness knob

SQLite has a single writer. The main lever for keeping the UI snappy
under load is therefore **how many background jobs are allowed to
write at once**: the scheduler worker pool defaults to **5**
(`ALMA_SCHEDULER_WORKERS`, clamped to `[1, 16]`). Lower it on a small
host so a burst of heavy jobs can't starve the app's writer; raise it
only if you have CPU and disk headroom to spare.

Foreground, user-facing commits (a dismiss, a follow, a save) retry on
a transient `database is locked` via `src/alma/core/db_retry.py`
(`run_with_lock_retry` — a few attempts, exponential backoff from
~50 ms) so a brief lock never silently drops a click. Background jobs
deliberately do **not** retry — they're idempotent and re-run on the
next sweep.

## Client-side read caching

The frontend avoids redundant refetches on the hot read paths.
Lens-recommendation queries use a `staleTime` of 60 s and the feed
inbox 30 s, so switching lenses, toggling a filter, or refocusing the
window serves the cached payload instead of hitting the backend again.
Source: `frontend/src/pages/DiscoveryPage.tsx`,
`frontend/src/pages/FeedPage.tsx`.

## Cached read aggregates (materialised views)

Insights and the three graph endpoints are served from a fingerprint-
keyed cache (`materialized_views` table; see
`src/alma/application/materialized_views.py`). On every GET, a tiny
SQL query computes a content fingerprint of the view's inputs (paper
counts, last-update timestamps, embedding count, active model). If
the fingerprint matches the cached row, the GET returns in <10 ms.
If it doesn't match, the cached payload is returned immediately with
`stale: true, rebuilding: true`, and a background rebuild is enqueued
under the view's `operation_key` (e.g.
`materialize.graph.paper_map.library`, deduped via the scheduler). The
frontend shows a "Refreshing…" pill and auto-refetches when the
rebuild completes (`useOperationToasts` invalidates the matching
React Query roots).

| View | Build cost (cold) | Triggers a rebuild |
|---|---|---|
| `insights:overview` | ~30 ms | Library paper add/edit, recommendations churn, follow change, embedding-model change |
| `graph:paper_map:library` | seconds-to-minutes | Library paper add/edit, embedding count change, embedding-model change |
| `graph:paper_map:corpus` | tens of seconds | Same, corpus-wide |
| `graph:author_network:library` | seconds | Library paper add/edit, follow change |
| `graph:author_network:corpus` | tens of seconds | Same, corpus-wide |
| `graph:topic_map` | ~hundreds of ms | Any paper change |

Explicit "Rebuild graphs" (`POST /graphs/rebuild`) and the cluster-
label refresh job bypass the fingerprint check and force a fresh
build.

## Database size

A typical install:

| Library size | `scholar.db` size | Notes |
|---|---|---|
| 100 papers, no embeddings | ~5 MB | Bare metadata. |
| 1k papers, no embeddings | ~30 MB | |
| 1k papers, with SPECTER2 vectors | ~80 MB | 768-dim float32 = ~3 KB / paper. |
| 10k papers, with SPECTER2 vectors | ~600 MB | Includes citation graph. |

WAL files can grow during heavy writes — they're auto-checkpointed
on every commit but a `PRAGMA wal_checkpoint(TRUNCATE)` will
collapse them if you want to reclaim space.

## Memory

The backend itself uses ~150–300 MB resident.

If you've enabled local SPECTER2, expect another ~1–2 GB while the
model is loaded for an embedding job. The model is unloaded when
the job completes; idle backend stays at the lower band.
