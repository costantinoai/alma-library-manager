"""Float16 storage / float32 runtime for embedding vectors.

Embeddings used to be stored as 4-byte float32 blobs. SPECTER2's
768-dim vectors are 3072 bytes each at that precision, which adds up
fast: a 5,700-paper library has ~6.6 MB of vectors plus ~1.5 MB of
author centroids, all duplicated through index pages and caches.

Float16 halves on-disk storage with effectively zero recall loss for
cosine similarity over normalized SPECTER2 vectors (the worst-case
absolute error is ~3e-4, well below the noise floor of any downstream
ranking signal).

All call sites should encode/decode through this module rather than
inlining ``np.frombuffer`` so the storage dtype can be tuned in one
place if we ever migrate to int8 quantization.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np

# What the database stores. Changing this requires a one-shot migration
# in ``init_db_schema`` to rewrite every existing blob.
STORAGE_DTYPE = np.float16

# What downstream math operates on. Cosine similarity, centroid
# averaging, and clustering all expect float32; upcasting on read keeps
# every existing call site numerically equivalent to the float32 era.
RUNTIME_DTYPE = np.float32


def encode_vector(vec) -> bytes:
    """Cast a vector to the storage dtype and return its raw bytes.

    Accepts anything ``np.asarray`` can consume: a list, a tuple, or an
    existing numpy array of any float dtype.
    """
    return np.asarray(vec, dtype=STORAGE_DTYPE).tobytes()


def decode_vector(blob: bytes) -> np.ndarray:
    """Decode one stored blob into a 1-D float32 array.

    Returns a copy (not a view over the bytes object), so callers can
    mutate the result freely.
    """
    return np.frombuffer(blob, dtype=STORAGE_DTYPE).astype(RUNTIME_DTYPE, copy=False).copy()


def decode_vectors(blobs: Iterable[bytes]) -> np.ndarray:
    """Decode many blobs into a 2-D float32 array (rows = vectors).

    Returns an empty (0,) array when ``blobs`` is empty so callers can
    chain ``np.linalg.norm`` / ``np.dot`` without an explicit guard.
    """
    blobs_list = [b for b in blobs if b]
    if not blobs_list:
        return np.zeros((0,), dtype=RUNTIME_DTYPE)
    return np.stack([decode_vector(b) for b in blobs_list])


__all__ = [
    "STORAGE_DTYPE",
    "RUNTIME_DTYPE",
    "encode_vector",
    "decode_vector",
    "decode_vectors",
]
