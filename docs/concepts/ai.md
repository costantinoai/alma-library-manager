title: AI capabilities
description: What ALMa can do with no embeddings, with Semantic Scholar vectors, and with local or OpenAI embedding providers.
---

# AI capabilities

ALMa's current public AI surface is about **embeddings**, not LLMs.
The base install works without any embedding provider selected. When
you enable embeddings, ALMa gets better semantic ranking, better graph
projections, and stronger tag-suggestion propagation.

## What runs without AI

Even with `provider = none`:

* Feed monitors run.
* Library curation works.
* Discovery retrieval still runs through lexical, topic, author,
  citation, and external-source channels.
* Citation-graph and co-author-graph channels work.
* Insights renders every non-graph chart.
* Alerts work for `author`, `keyword`, and `topic` rules.

What you lose:

* Semantic similarity in Discovery.
* Vector-backed Library search and similarity views.
* Embedding-backed graph projections.
* Highest-quality tag suggestions.
* `similarity` alert rules.

## Embeddings: the current model

ALMa's canonical paper embedding is **SPECTER2** —
`allenai/specter2_base`, 768-dim. Every embedding-aware feature
reads from the `publication_embeddings` table; every write to that
table is a SPECTER2 vector for a paper.

Three provider modes exist in the Settings UI:

| Provider | What it means | Best use |
|---|---|---|
| `none` | Disable live embedding generation. | Small installs or pure metadata workflows. |
| `local` | Compute SPECTER2 locally in a selected Python environment. | Best quality without a cloud dependency. |
| `openai` | Use OpenAI for query-time embedding tasks. | When you want a cloud fallback and already have a key. |

Independently of that selection, ALMa also knows how to fetch
**Semantic Scholar's pre-computed `specter_v2` vectors** in bulk. Those
fetched vectors are still the cheapest way to seed a large library.

### 1. Semantic Scholar pre-computed vectors

Semantic Scholar exposes pre-computed `specter_v2` vectors via
`/paper/batch`. ALMa fetches them in batches and stores them
unchanged. This is the **default and preferred path** — no local
compute, no GPU, no model download.

The **Backfill S2 vectors** action in Settings → AI runs across your
corpus and writes vectors for every paper S2 has one for. Coverage
is typically 70–95% on a normal Library.

### 2. Local SPECTER2 compute

Papers S2 doesn't have can be embedded locally. ALMa loads the
SPECTER2 model in-process (with the `adapters` library) and computes
vectors on demand.

This is the **fall-back path**. The **AI compute missing** action
runs SPECTER2 locally over papers that have no S2 vector. Slower
than the S2 fetch (seconds per paper instead of milliseconds), but
it covers the long tail.

Both paths write to the same column (`publication_embeddings.vector`)
with `source ∈ {'s2', 'local'}` so the provenance is preserved.

### 3. OpenAI embeddings

OpenAI is optional. In the current public build it is used only as an
embedding provider, not as a general LLM surface. If you select
`provider = openai`, ALMa expects `OPENAI_API_KEY` to exist in the
secret store or environment and uses that provider for live embedding
tasks that need it.

## Dependency environment

The Settings card can point ALMa at a dedicated Python environment for
AI dependencies. This matters mainly for non-Docker installs where you
want the backend in a lean env and the embedding stack in a heavier one.

The card persists:

* `ai.provider`
* `ai.local_model`
* `ai.python_env_path`

and probes the selected environment for `numpy`, `torch`,
`transformers`, `adapters`, `hdbscan`, `umap-learn`, and related
packages.

## Heavy work runs in the background

Any AI operation that takes more than a couple of seconds is
**Activity-backed** — it returns a job envelope, runs in the
scheduler worker, and reports per-batch progress in the Activity
panel. Synchronous in-request AI calls are forbidden by convention
(and by the runtime design).

Common background actions:

* **Backfill S2 vectors** (across the whole corpus or a scope)
* **AI compute missing** (local SPECTER2 fallback)
* **Graph rebuild / projection refresh**
* **Bulk tag suggestions / propagation**

## Why this rigour

A provider selection is only useful if the chosen runtime can actually
import the required packages and the corpus has enough vectors to make
embedding-backed surfaces meaningful.

The current setup makes that impossible:

* **Truthful UI** — if a provider's runtime can't import its
  package, the Settings card says so.
* **No silent cross-provider swaps** — a missing S2 vector stays
  missing until you explicitly compute or fetch another one.
* **Explicit local compute** — running SPECTER2 locally is an
  Activity-backed job you trigger, not a silent autoenrichment.

## API

```
GET  /api/v1/ai/dependencies         # what's installed
POST /api/v1/ai/configure            # set default providers
POST /api/v1/ai/compute-embeddings   # local SPECTER2 over missing
POST /api/v1/ai/backfill-s2-vectors  # fetch from S2 in bulk
DELETE /api/v1/ai/embeddings/inactive  # drop vectors not used by the active model
GET  /api/v1/ai/status               # current model + runtime status
```
