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
| **Discovery lens refresh** (canonical lens, ~330 saved papers) | ~76 s end-to-end | Multi-source retrieval, ranking, branch clustering. |
| **Page-mount reads** (`/library/saved`, `/feed`, `/authors`) | < 1 s P95 | Stays responsive even during a concurrent refresh. |
| **Detail-panel reads** (`/papers/{id}`, prior / derivative works) | < 500 ms P95 | Mostly cached. |
| **Activity poll** (`/api/v1/activity`) | < 200 ms | Used for the live status of background jobs. |
| **Frontend tsc check** | < 30 s | Useful as a build sanity check. |
| **Cold backend boot** | 2–4 s | Includes schema migration check. |

These are observed budgets on a typical Library (~300 saved
papers, 50 followed authors, with embeddings enabled). YMMV on
larger / smaller corpora.

## Where time goes in a lens refresh

A typical 76-second lens refresh breaks down roughly as:

| Phase | Time | What's happening |
|---|---|---|
| Setup + read seeds | 2–5 s | Read your Library, build the seed set. |
| External retrieval (OpenAlex + S2) | 30–45 s | Per-lane parallel fan-out, per-lane deadlines. |
| SPECTER2 cosine over candidates | 5–10 s | Vector-cache hits where possible. |
| Scoring | 5–10 s | Per-candidate signal computation. |
| Branch clustering + representative labels | 5–15 s | Label extraction and projection add the upper end. |
| Writes (recommendations + suggestion_set) | 1–3 s | Single transaction. |

Subsequent refreshes against the same lens use cached candidates
where possible and run much faster — typically under 10 seconds.

## Profiling your own refresh

**Activity → Operations** shows per-source timing for each refresh.
Open the most recent lens refresh; the **Per-source timing** sub-
panel breaks down the external retrieval phase per channel. Slow
channels are the place to start.

For deeper profiling, the `scripts/probe_*.py` directory has
in-process probes that exercise specific paths:

```bash
python scripts/probe_activity_overlap.py    # page-mount reads under refresh
python scripts/probe_lens_refresh.py        # full refresh timing
python scripts/probe_authors_deep_refresh.py
```

Each script writes timings to stdout and a JSON summary to
`/tmp/alma-probe-*.json`.

## When something is slow

Step 1: **read the Activity logs.** A slow operation almost always
has an obvious culprit in the per-source timing.

Common patterns:

| Pattern | Cause | Fix |
|---|---|---|
| Lens refresh > 5 minutes | External retrieval lane is slow or timing out | Check per-source timings and tighten refresh limits. |
| Lens refresh stuck at "retrieving" | One source is timing out without raising | Per-lane deadline should kill it; if not, check `discovery.limits.*`. |
| Feed refresh > 10 minutes | A single monitor is pulling thousands of works | Tighten the monitor's filters or cap `feed.monitor_defaults.daily_max`. |
| Page mount reads slow | Embedding fetch running synchronously | Move it to AI compute background job (the default). |
| Backfill S2 vectors crawling | Public S2 rate limits | Set `SEMANTIC_SCHOLAR_API_KEY` for higher quotas. |

## Regression probes

Critical perf paths are pinned by probes the codebase keeps in
`scripts/probe_*.py`. The two worth knowing:

1. **`probe_activity_overlap.py`** — measures page-mount read
   latency while a feed refresh is in progress. Catches regressions
   in the "feed refresh blocks reads" failure mode.
2. **`probe_lens_refresh.py`** — measures end-to-end lens refresh
   on a real `scholar.db`. Catches regressions in the multi-source
   retrieval phase.

Run them before and after any change that touches the recommender
or the scheduler. Compare timings and look at the per-source
breakdown for unexplained shifts.

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
