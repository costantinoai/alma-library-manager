"""GPU/CPU acceleration dispatch for the graph build's heavy numerics (task #21).

The corpus paper-map build is dominated by UMAP. Profiling the 8.3k-paper corpus
showed two UMAP fits accounting for ~26s of the ~33s build: a 5-D *clustering
substrate* (HDBSCAN input) and a 2-D *display projection*. Both run on the SAME
L2-normalised SPECTER2 vectors over the SAME cosine neighbourhood, so the work is
highly shareable. This module centralises those numerics behind ONE dispatch so
no caller hand-rolls UMAP params or recomputes the neighbour graph.

Two execution paths, ONE call site (the "fallback on correctly optimised CPU"
contract — there is no half-working middle path):

* **GPU (cuML / RAPIDS)** — when an importable ``cuml`` is present, UMAP runs on
  the GPU. Any GPU import/runtime error is caught and transparently falls back to
  the CPU path (logged, never silent). cuML is NOT in the base ``.venv`` today
  (it forces numpy 2.x, which would break the compiled torch/hdbscan stack), so
  this path stays dormant until cuML is provided in an isolated way — see
  ``gpu_available``.

* **Optimised CPU (umap-learn)** — two output-affecting-vs-neutral knobs:
    1. ``cosine_knn`` computes the cosine k-NN graph ONCE so the two fits can
       share it via umap-learn's ``precomputed_knn`` instead of each running its
       own neighbour search (the dominant cost at large N). The shared graph is
       the SAME neighbour graph each fit would have built (we call the exact
       ``umap.umap_.nearest_neighbors`` umap uses internally with matching
       params). The final layout still differs by a random ORIENTATION versus
       letting each fit search on its own — umap draws from one RNG for both the
       search and the SGD, so skipping the search shifts the SGD's seed — but the
       cluster structure is unchanged. Immaterial for a cached viz; the version
       bump covers the one-time orientation shift.
    2. ``umap_fit`` bounds ``n_epochs`` for large N. umap-learn defaults to 500
       SGD epochs (<10k points); the layout is well-converged for a viz/cluster
       substrate by ~200. This DOES change the layout marginally, so the graph
       version constants are bumped when this module is wired in.

Determinism is preserved (``random_state=42``) so the cached corpus layout is
stable across rebuilds — the Procrustes-anchored variant work depends on it. We
deliberately do NOT trade reproducibility for numba multithreading of the SGD
(setting ``random_state`` already forces umap's layout loop single-threaded); the
kNN search itself is the parallel part and is shared rather than re-run.
"""

from __future__ import annotations

import importlib.util
import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Optional dependency — the CPU path. cuML (the GPU path) is probed lazily in
# `gpu_available()` so a missing GPU stack never costs an import at module load.
try:
    import umap as _umap

    _UMAP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only on a stripped env
    _umap = None
    _UMAP_AVAILABLE = False


# ── Tunables ──────────────────────────────────────────────────────────────────

# Below this N the neighbour search is already cheap and a single fit's
# n_neighbors may differ from the shared graph's width, so we don't bother
# sharing. Above it both fits use n_neighbors=SHARED_KNN_NEIGHBORS (a corpus-
# scale build), so one graph serves both.
SHARED_KNN_MIN_N = 600
# umap-learn caps n_neighbors at 15 for both fits at corpus scale; the shared
# graph is computed at this width and reused by both.
SHARED_KNN_NEIGHBORS = 15

# Bound the SGD epoch count for large builds. umap-learn's own default is 500
# below 10k points (200 above), so the 8k-paper corpus pays 500 by default. We
# bound it earlier — but the two fits have DIFFERENT convergence needs, measured
# on the corpus (8,333 papers, HDBSCAN coverage as the quality gauge):
#   * Clustering substrate (5-D, feeds HDBSCAN density): with a SHARED kNN the
#     SGD starts from a different random orientation than a per-fit search, and
#     at 200 epochs the density is under-settled → coverage drops 0.741→0.723.
#     By 300 epochs it recovers to 0.741 (== the own-kNN/500-epoch original), so
#     the substrate needs the larger budget to keep clustering honest.
#   * Display projection (2-D, viz only): coverage is irrelevant and the layout
#     is well-converged by 200 epochs (umap's own large-N default).
_BOUNDED_EPOCHS_MIN_N = 2000
CLUSTER_EPOCHS_LARGE_N = 300
LAYOUT_EPOCHS_LARGE_N = 200


def large_n_epochs(n: int, large_value: int) -> Optional[int]:
    """Epoch budget for a fit: ``large_value`` at corpus scale, else None (umap default).

    Small libraries keep umap-learn's default (500) — they're fast regardless and
    the default gives the cleanest layout; only large builds, where epochs are the
    dominant SGD cost, get the bounded value.
    """
    return large_value if n >= _BOUNDED_EPOCHS_MIN_N else None

KnnGraph = tuple[np.ndarray, np.ndarray]


# ── GPU capability probe ────────────────────────────────────────────────────

_GPU_PROBE: Optional[bool] = None


def gpu_available() -> bool:
    """True when an importable cuML (RAPIDS) GPU stack is present.

    Uses ``find_spec`` (instant, no import) rather than importing cuML — cuML's
    import + CUDA context init costs several seconds, which we must not pay on
    every build when the GPU path is absent (the common case). The actual import
    happens inside ``_gpu_umap_fit``; any failure there falls back to CPU. Probed
    once and cached; the outcome is logged a single time per process so the
    selected path is observable.
    """
    global _GPU_PROBE
    if _GPU_PROBE is None:
        _GPU_PROBE = importlib.util.find_spec("cuml") is not None
        logger.info(
            "accel: GPU(cuML) %s",
            "available — UMAP will run on the GPU"
            if _GPU_PROBE
            else "absent — using optimised CPU path (shared kNN + bounded epochs)",
        )
    return _GPU_PROBE


# ── Shared cosine k-NN graph ────────────────────────────────────────────────

def _l2_normalize(vectors: np.ndarray) -> np.ndarray:
    """Row-wise L2-normalise (so euclidean is rank-equivalent to cosine)."""
    out = vectors.astype(np.float32, copy=True)
    norms = np.linalg.norm(out, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return out / norms


def cosine_knn(
    vectors: np.ndarray,
    n_neighbors: int = SHARED_KNN_NEIGHBORS,
    *,
    random_state: int = 42,
) -> Optional[KnnGraph]:
    """Compute the cosine k-NN graph once for reuse across multiple UMAP fits.

    Returns ``(knn_indices, knn_dists)`` aligned to the row order of ``vectors``,
    suitable to pass straight to :func:`umap_fit` as ``precomputed_knn``; or
    ``None`` when umap-learn is unavailable or the search fails (callers then let
    each fit build its own — a correct, slightly slower fallback).

    We invoke the exact ``umap.umap_.nearest_neighbors`` that ``UMAP.fit`` calls
    internally for a cosine metric with a fixed ``random_state`` — ``angular=True``,
    ``n_jobs=1`` (umap forces single-thread when a seed is set), ``low_memory=True``
    — so the shared graph is the same neighbour graph each fit would otherwise have
    built. (The final 2-D layout still differs by a random orientation versus
    per-fit search, because umap shares one RNG between the search and the SGD;
    structure is unchanged.) Cosine is scale-invariant, so the same graph is valid
    for both the L2-normalised clustering input and the raw projection input.
    """
    if not _UMAP_AVAILABLE:
        return None
    try:
        from umap.umap_ import nearest_neighbors

        rng = np.random.RandomState(random_state)
        knn_indices, knn_dists, _ = nearest_neighbors(
            vectors,
            n_neighbors=int(n_neighbors),
            metric="cosine",
            metric_kwds={},
            angular=True,  # cosine is an angular metric (matches UMAP internals)
            random_state=rng,
            low_memory=True,
            n_jobs=1,  # deterministic; matches umap's forced single-thread under a seed
            verbose=False,
        )
        return np.asarray(knn_indices), np.asarray(knn_dists)
    except Exception as exc:  # pragma: no cover - defensive; falls back to per-fit kNN
        logger.warning("accel: shared cosine kNN failed (k=%d, n=%d): %s",
                       n_neighbors, getattr(vectors, "shape", ["?"])[0], exc)
        return None


# ── UMAP dispatch ────────────────────────────────────────────────────────────

def _gpu_umap_fit(
    vectors: np.ndarray,
    *,
    n_components: int,
    n_neighbors: int,
    min_dist: float,
    metric: str,
    random_state: int,
    n_epochs: Optional[int],
) -> np.ndarray:
    """Fit UMAP on the GPU via cuML. Raises on any GPU failure (caller falls back).

    cuML UMAP's cosine support varies across RAPIDS versions, so we route cosine
    as L2-normalise + euclidean (rank-equivalent and supported on every cuML).
    Output forced to a host numpy float32 array regardless of cuML's output type.
    """
    from cuml.manifold import UMAP as _CuUMAP

    if metric == "cosine":
        gpu_input = _l2_normalize(vectors)
        gpu_metric = "euclidean"
    else:
        gpu_input = np.ascontiguousarray(vectors, dtype=np.float32)
        gpu_metric = metric

    reducer = _CuUMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=gpu_metric,
        random_state=random_state,
        n_epochs=int(n_epochs) if n_epochs else 0,  # cuML: 0 → auto
        output_type="numpy",
    )
    embedding = reducer.fit_transform(gpu_input)
    return np.asarray(embedding, dtype=np.float32)


def umap_fit(
    vectors: np.ndarray,
    *,
    n_components: int,
    n_neighbors: int,
    min_dist: float,
    metric: str = "cosine",
    random_state: int = 42,
    n_epochs: Optional[int] = None,
    precomputed_knn: Optional[KnnGraph] = None,
) -> np.ndarray:
    """Fit UMAP and return the ``(n, n_components)`` embedding as float32.

    Dispatches to the GPU when available (falling back to CPU on any GPU error),
    otherwise runs umap-learn with the CPU optimisations: a bounded ``n_epochs``
    for large N and an optional shared ``precomputed_knn`` to skip a duplicate
    neighbour search. Raises if umap-learn is unavailable (callers guard with
    their own ``_UMAP_AVAILABLE`` check before calling).
    """
    if not _UMAP_AVAILABLE:
        raise RuntimeError("umap-learn is not available")

    n = int(vectors.shape[0])
    # Bounded epochs: the single biggest CPU SGD knob. A caller that knows its
    # convergence needs passes n_epochs explicitly (see CLUSTER/LAYOUT constants);
    # None falls back to the layout budget at corpus scale (umap default below it).
    if n_epochs is None and n >= _BOUNDED_EPOCHS_MIN_N:
        n_epochs = LAYOUT_EPOCHS_LARGE_N

    if gpu_available():
        try:
            return _gpu_umap_fit(
                vectors,
                n_components=n_components,
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                metric=metric,
                random_state=random_state,
                n_epochs=n_epochs,
            )
        except Exception as exc:
            logger.warning("accel: GPU UMAP failed (n=%d); falling back to CPU: %s", n, exc)

    kwargs = dict(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        random_state=random_state,
        n_epochs=n_epochs,
    )
    # umap-learn's default sentinel is (None, None, None); only override when a
    # real graph is supplied. A 2-tuple (indices, dists) is accepted — the search
    # index (3rd slot) is only needed for .transform(), which we never call.
    if precomputed_knn is not None:
        kwargs["precomputed_knn"] = precomputed_knn
    reducer = _umap.UMAP(**kwargs)
    return np.asarray(reducer.fit_transform(vectors), dtype=np.float32)
