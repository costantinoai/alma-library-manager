---
title: Discovery compute and Raspberry Pi audit
description: Source-backed CPU, GPU, memory, I/O and portability inventory for the full Discovery lifecycle.
---

# Discovery compute and Raspberry Pi audit

**Status, 2026-09-24:** static code audit plus read-only local SQLite-copy
measurements. No Raspberry Pi run has been performed. “Can run” below means
the code has a CPU/no-local-model path; it does **not** claim acceptable Pi
latency or peak memory. This audit is maintained during the combined
Discovery task (`tasks/backlog/92_DISCOVERY_LENSES_BRANCHES_SURFACE_AUDIT_2026-09-22.md`)
and is the baseline for deferred portability task
(`tasks/backlog/94_DISCOVERY_COMPUTE_AND_PI_PORTABILITY_2026-09-24.md`).
See also the [scope/learning map](discovery-scope-and-learning-audit.md).
The revised [design and method decisions](discovery-component-architecture.md)
adds the proposed paths below. They are unimplemented and unmeasured; no
runtime or accuracy result follows from their inclusion in the inventory.

## Proposed paths added by the ground-up design review

| Operation | Compute and cache contract | Quality/cost comparison required |
|---|---|---|
| Typed phrase/aspect extraction | CPU phrase/span baseline; optional model extraction in a background job, cached by content hash and extractor version; preserve spans and use/compare/negation roles | Span/role precision, rare phrase and method-transfer coverage, missing-full-text cases, time per paper and peak RAM. |
| Aspect embeddings and late interaction | Optional encoder inference, potentially GPU accelerated; store separate compatible model/role vectors; match cached spans on CPU; never run encoder under writer gate | Document-only versus aspect matching on judged scientific facets and native acquisition yield; token/encoding time, index memory and rank latency. |
| Grounded query expansion or bounded cross-encoder reranking | Optional local/remote model; bounded ambiguous candidate/query sets, cached by intent/content/model revisions | Added qualified recall or ranking gain against deterministic queries/additive baseline; inference cost, network dependence, cold/warm RAM/latency and fallback loss. |
| Sparse neighbourhood graph and alternative structural partition | CPU blockwise neighbour search or measured ANN, sparse edges, optional Leiden community detection; version by corpus/model/structural settings | Compare to current UMAP/HDBSCAN and cosine baseline for coherence, coverage, identity stability, memory and rebuild/query time. No dense all-pairs matrix required. |
| Action-aware local additive model | CPU regularized pointwise/ordinal fitting plus event-linked aspect constraints; sparse entity features and full-vector residuals with separate regularization | Forward explicit-outcome prediction, confounding/support and branch isolation; fit time, feature storage and approximate uncertainty cost. Pairwise fitting is optional, not required before every update. |
| Kernel/diffusion residual challenger | CPU computation over fixed compatible views, branch-scoped and asynchronous; all training event revisions retained | Incremental nonlinear preference gain against the linear residual, over-smoothing/negative spread and peak memory; no blanket adoption of quadratic/cubic fitting. |

Representation arithmetic: 12,000 × 768 float32 coordinates occupy about
36.9 MB; eight sentence vectors per paper add about 295 MB; a dense 12,000²
similarity matrix occupies 576 MB, before runtime overhead. These are calculated
array sizes, not measured RSS. Share matrices across branches, stream exact
search and persist sparse graph edges. ANN, compression and quantization require
their own recall/quality gates, independently of CPU-vs-GPU acceleration.

## What actually uses a GPU

| Operation and owner | Current compute and trigger | GPU requirement | Pi 4 GB assessment |
|---|---|---|---|
| Local paper embeddings: `services/embeddings.py` → `ai/providers.py` → `discovery/similarity.py:SpecterEmbedder` | SPECTER2 base + adapter, 768 dimensions, 512-token maximum; maintenance/user job, batches of 16; PyTorch chooses CUDA if present, CPU otherwise | **Optional CUDA acceleration**, but local model needs Torch/Transformers/Adapters and roughly another 1–2 GB while loaded (project performance guide). | Lite image excludes the model. CPU inference on a Pi may be technically possible in a different image but is not an acceptable default until measured; use fetched vectors or an explicit remote provider. |
| Branch clustering: `seed_profile.py` → `ai/clustering.py` → `ai/accel.py` | Normalize stored vectors, UMAP to about five dimensions if enough seeds, then HDBSCAN; optional cuML UMAP when importable, CPU umap-learn fallback; lexical fallback without enough vectors | **Optional cuML/CUDA acceleration**. HDBSCAN, sampling and identity matching are CPU. | CPU path is available in lite. Rebuild cost and peak memory need an ARM64 benchmark; cache by structural revision and avoid clustering on each deck refresh. |
| Explicit ad hoc semantic search: `ai/semantic_search.py` | Loads local SPECTER2 query adapter and compares query to stored paper vectors; explicit search action, separate from ordinary lens refresh | **Optional CUDA**, local Torch stack required for query embedding | Unavailable in lite as implemented. Keep a lexical search fallback or make remote query embedding explicit; do not claim feature parity for this action. |

The repository has no other GPU dispatch in the Discovery call graph: the
actual CUDA choice is in `SpecterEmbedder._resolve_device`; the only cuML
dispatch is `ai.accel.umap_fit`, called by clustering. Candidate ranking
does **not** run a neural model at refresh time. An optional future trainable
embedding adapter would add a new compute dependency and must pass its own
quality and Pi gates; it is not part of the current branch-local value plan.

## Compute that needs no local GPU

| Stage | CPU / network / memory behavior | Pi design requirement |
|---|---|---|
| Semantic Scholar SPECTER2 vector backfill | Remote `/paper/batch` returns precomputed vectors; local CPU validates and stores them in bounded batches. No Torch or GPU. API latency/rate limits dominate. | Preferred Pi vector source; keep network work outside write units and report missing coverage. |
| Optional OpenAI embeddings | Remote API computes vectors; local request/storage only. Its model is 1536-dimensional and is **not** interchangeable with SPECTER2 768-dimensional vectors. | Optional cloud mode, not required for core; enforce exact model compatibility and account for privacy/cost/network dependence. |
| Lens membership and seed preparation | Indexed SQLite reads plus `paper_signal` batch; every member is a seed (`lens.max_seeds` removed 2026-10-30). | Represent every eligible member in a chunked/incremental structural pass. Bound query exemplars only after the full partition. Benchmark full-set coverage, memory, latency and approximation error at growing library sizes; no fixed 500 anchor cap. |
| Lexical and frontier retrieval | Python text preparation and repeated scans/query scoring of local corpus/frontier; no GPU. Frontier index normalizes text once per refresh. | Build/reuse a compact read-only search index by corpus revision; measure query count × pool size and cap work per branch. |
| Vector retrieval and semantic scoring | NumPy decode, normalize, centroid and matrix cosine over stored vectors; no GPU dispatch. Refresh reuses transported or stored vectors and reports zero candidate embeddings computed inline. | Keep vectors model-homogeneous; load/chunk once per job, avoid copies per branch, cache centroids by branch revision. |
| Citation retrieval and PPR | SQLite reference joins; SciPy/NumPy sparse graph walk for Library/Loved PPR; no GPU. Current global PPR is a D26 scope leak. | Build graph adjacency by corpus revision, seed the walk per branch and bound/cache it; never rebuild a full graph under a write lock. |
| Feature measurement and ranking | CPU NumPy/Python lexical/semantic/topic/author/venue/recency/citation/feedback features; ten-family prior, calibration and diversity/MMR; no GPU. | Compute each candidate × branch once, reuse one feature snapshot for score, persistence and explanation; avoid per-candidate DB lookups. |
| Calibration, outcome evaluation and branch learning | CPU random corpus sample, quantile tables, sparse/logistic shadow fit and planned pairwise exact-value fit; no GPU. Current calibration is potentially expensive and depends on a global profile. | Make calibration taste-neutral or branch-correct, versioned, incremental, asynchronous and separately budgeted; a small shrunk value model needs no neural training. |
| Proposed sparse semantic/citation neighbour graph and text-factor challengers | Revisioned kNN graph construction, sparse diffusion/traversal, ICA or TF-IDF/SVD/NMF factorization; CPU implementations exist, but build memory and update cost are unmeasured. | Keep these optional challengers until incremental unseen yield and held-out utility justify CPU/RAM cost. Build asynchronously in chunks; cache by model/corpus revision and measure on the real Pi before promotion. |
| Proposed multivariate fusion and personal contrastive fit | Normalized vector + sparse/exact/graph feature preparation; simple additive or weighted concatenated scoring is CPU work. A learned low-rank metric/adapter could add substantial fit, memory and possibly GPU cost; no adequate branch labels yet. | Benchmark separate-view fusion against concatenation before adding trainable parameters. Measure cold/warm fit and inference, feature memory, missing-vector behavior and quality on identical/native pools. Keep an additive CPU path as the main baseline. |
| Metadata enrichment, frontier maintenance, Activity and persistence | Mostly network, SQLite and disk I/O; no GPU. Several workers can compete for SQLite's one writer. | Coalesce jobs, bound worker concurrency, precompute outside the gate, and measure writer wait and foreground response. |

## Observed scale and limits of these numbers

Read-only SQLite copies taken 2026-09-23:

| Copy | Papers | Stored active SPECTER2 vectors | Vector BLOB total | DB file | Recent refresh timing evidence |
|---|---:|---:|---:|---:|---|
| Production | 4,175 | 2,306, all Semantic Scholar sourced | 3.4 MiB | 64 MiB | One stored refresh summary: 80.8 s total, 24.7 s channel retrieval; its Activity wall span was 115 s. |
| Dev | 12,148 | 10,181 (7,422 Semantic Scholar; 2,759 local) | 14.9 MiB | 1.5 GiB | Eight most recent stored summaries: about 39–45 s total; channel retrieval about 13–17 s, scoring about 0.6–1.1 s. |

These are **not Pi benchmarks**. They are different host/profile runs, and
the dev DB size includes data beyond Discovery. Historical Activity durations
include old code versions, waits, failures and other jobs; do not present them
as isolated CPU timings. They do show that retrieval/refresh latency, not
the final numeric score computation, is the dominant current workload.

Project deployment docs estimate idle backend 150–300 MB, about 1–2 GB extra
while local SPECTER2 is loaded, and about 1 GB peak for the lite flavor. Those
are existing estimates, not measurements on this branch or on a Pi. The lite
ARM64 image already omits Torch and supports fetched vectors plus CPU
clustering, so a functional no-GPU configuration exists. It has **not** been
proven responsive for this 12k-paper profile on a Pi 4 GB.

The [Pi 4 hardware specification](https://www.raspberrypi.com/products/raspberry-pi-4-model-b/specifications/)
lists four Cortex-A72 cores, a 4 GB model and USB 3.0. A 64-bit
[Raspberry Pi OS](https://www.raspberrypi.com/documentation/computers/os.html)
is available for Pi 4. Its graphics hardware is not a CUDA device, so the
two optional GPU paths above take CPU/fallback routes.

## Portable main-version target and proof

The main experience must remain complete without local Torch/CUDA: lenses,
branch structure, lexical/citation/frontier retrieval, stored-vector semantic
retrieval, exact-value taste, ranking, explanations, feedback and learning.
Embedding *generation* can be an optional local accelerator or remote
enrichment source. A feature whose model is unavailable must show its actual
coverage and fallback, not silently advertise an inert semantic signal.

Task 94 will make **paired, post-Discovery measurements** of CUDA versus CPU
for the same SPECTER2 model and UMAP input, and separately compare lighter CPU
or remote alternatives. Each arm must report cold/warm time, throughput,
RAM/VRAM, vector and branch output differences, retrieval coverage, and
held-out ranking/relevance quality with uncertainty. Run both a common
candidate universe (ranking effect) and each arm's native universe (retrieval
coverage effect). Current snapshots contain too few impressed branch verdict
pairs to estimate per-branch accuracy, so this audit makes no accuracy claim
about a Pi or a lighter mode today.

Before calling Pi 4 GB supported for this workload, run the ARM64 lite image
on a real Pi 4 GB with a copy of the production profile and a 10–12k-paper
stress profile. Instrument each stage for wall/CPU time, peak RSS, memory
pressure/swap, vector coverage, SQLite read/write time and writer wait, disk
I/O, temperature/throttling, queue backlog and foreground API latency. Run
create/preview/refresh/action and concurrent foreground writes, with and
without stored vectors, cold/warm caches and network unavailable. Establish
numerical acceptance budgets before measuring, including no OOM/swap thrash,
bounded refresh completion, and responsive foreground actions. Record exact
hardware, OS, image, worker cap, DB location (prefer USB SSD for large copies),
dataset, model coverage and result; laptop timings cannot substitute.

Engineering sequence: remove the current global scope/seed defects; cache
structural branches and graph/vector indexes by revision; batch/chunk work
and reduce worker concurrency on small hosts; move calibration/fitting to
background; then benchmark the lite Pi and cut or defer any optional measure
whose cost exceeds its demonstrated ranking value. GPU support remains an
acceleration path, not a dependency of the Discovery contract.
