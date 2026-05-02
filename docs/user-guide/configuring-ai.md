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
→ Rehydrate metadata** first. That job batches OpenAlex by work ID
to fill missing DOI / abstract / URL / publication date /
authorships / topics / references on already-stored papers, with
per-paper bookkeeping in `paper_enrichment_status` so reruns are
cheap.

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
