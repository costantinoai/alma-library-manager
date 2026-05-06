---
title: Configuring AI providers
description: Pick an embedding provider, point ALMa at the right Python environment, and verify that the embedding stack is ready.
---

# Configuring AI providers

ALMa's public AI surface is currently about **embeddings**. You can run
the app with no embedding provider at all, but enabling one improves:

* semantic ranking in Discovery
* graph projections in Insights
* tag-suggestion quality
* vector-backed similarity workflows

## Where to start

Open **Settings → AI & embeddings**.

That card controls three things:

1. the active embedding provider
2. the local model selection for the `local` provider
3. the dependency environment ALMa should probe for heavy AI packages

## Provider modes

| Provider | What it does | When to choose it |
|---|---|---|
| `none` | Disables live embedding generation. | You want the lightest possible install. |
| `local` | Uses local SPECTER2 compute. | Best quality without a cloud dependency. |
| `openai` | Uses OpenAI for embedding tasks. | You already have an OpenAI key and prefer cloud embedding. |

Separately from that selector, ALMa can also fetch
**Semantic Scholar's pre-computed `specter_v2` vectors** in bulk. That
is usually the fastest way to seed a library.

## Local SPECTER2

If you want the `local` provider, install the AI extras first:

```bash
pip install -e ".[ai]"
```

That pulls in the heavy stack:

* `torch`
* `transformers`
* `adapters`
* `numpy`
* `scikit-learn`
* `hdbscan`
* `umap-learn`

Then set the dependency environment in the Settings card so ALMa knows
which Python installation should be probed and used.

## OpenAI

If you want the `openai` provider:

1. add `OPENAI_API_KEY` to `.env`, or set it through the Settings UI
2. choose `openai` as the provider
3. use **Recheck environment** to refresh the status card

OpenAI is optional and currently used for embeddings only in the public
build.

## Dependency environment

The Settings card can point ALMa at a dedicated Python interpreter or
environment path. This is most useful on non-Docker installs where the
backend environment is intentionally slim.

The card persists:

* `ai.provider`
* `ai.local_model`
* `ai.python_env_path`

and probes the selected environment for the packages ALMa needs.

### How the card is laid out

The **Dependencies & Environment** section (collapsed by default; the
header shows a status badge — `Ready` / `Using fallback` / `Restart
needed` / `N missing` — so the verdict is visible without expanding it)
splits the question of "does AI work?" into two independent parts:

1. **Configured** — the path you typed in. Either validates (`Validated`),
   sits empty (`Not set`), or fails to resolve (`Unreachable`, e.g. when
   you set a host path while running ALMa in Docker).
2. **Active runtime** — the Python ALMa is actually importing from. When
   the configured path is unreachable, ALMa transparently falls back to
   the backend's own Python so AI features keep working; the runtime
   card shows the executable being used and how many packages it could
   import.

A single derived verdict at the top names the resulting state in plain
English (e.g. *"AI is working — but your configured environment is
unreachable"*). Below that, the path input + **Recheck** button, the
package chips for the active runtime, and a collapsible **Show
diagnostics** drawer with the raw executables / versions / detected
layout for troubleshooting.

The card never reports "all dependency checks passed" *and* "environment
invalid" at the same time — those used to be presented as parallel
chips but they answer different questions, so a fallback (config bad,
runtime fine) reads as one warning instead of a contradiction.

## Background actions

Heavy embedding work does not run inline. It goes through Activity:

* **Backfill S2 vectors** — also fills DOI, abstract, URL,
  publication date, year, and citation count from the same
  Semantic Scholar response, so a vector hit doubles as cheap
  metadata repair.
* **Compute missing embeddings** — local SPECTER2 only runs on
  papers that already have both a title and an abstract. The
  status card surfaces a separate **blocked missing
  title/abstract** count for rows that still need metadata
  repair before they are eligible.
* **Rebuild graph projections**

If the blocked count is large, run **Settings → Corpus maintenance
→ Rehydrate metadata** first. The job runs in three phases and is
**Activity-enveloped** (visible in the Activity panel with
queued → running → completed status):

1. **OpenAlex batched** (50 work IDs per call) fills DOI / abstract
   / URL / publication date / authorships / topics / references /
   biblio / OA flags / FWCI / keywords on already-stored papers.
2. **Semantic Scholar batched** (100 lookup IDs per call) adds
   `tldr` (rendered on every paper card) and
   `influential_citation_count` (drives Discovery's
   `citation_quality` ranker) plus an abstract fallback.
3. **Crossref per-paper** is a last-resort abstract fill for the
   residual papers OpenAlex and S2 both left blank.

Per-paper bookkeeping in `paper_enrichment_status` (one row per
source) makes reruns cheap; the job picks up automatically every
time a new paper is added (Library save / Feed candidate / Discovery
rec) so the corpus stays hydrated without recurring manual sweeps.
A single click handles up to 100,000 papers per run.

You can keep using the app while those jobs run.

## Verifying

After configuring:

1. **Settings → AI & embeddings → Status** should show the selected
   provider and environment as healthy.
2. **Backfill S2 vectors** should land vectors for the subset Semantic
   Scholar already knows.
3. **Compute missing embeddings** should fill part of the remaining gap
   if you selected `local`.
4. **Insights → Graph** should stop showing the cold-start / no-vector
   state once enough embeddings exist.

If something looks wrong, inspect the Activity logs for the failed job
or use **Recheck environment** in the AI card.
