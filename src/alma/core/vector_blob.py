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

import logging
import sqlite3
from typing import Iterable, Optional

import numpy as np

logger = logging.getLogger(__name__)

# What the database stores. Changing this requires a one-shot migration
# in ``init_db_schema`` to rewrite every existing blob.
STORAGE_DTYPE = np.float16

# What downstream math operates on. Cosine similarity, centroid
# averaging, and clustering all expect float32; upcasting on read keeps
# every existing call site numerically equivalent to the float32 era.
RUNTIME_DTYPE = np.float32

# Byte width of the storage dtype. Used by the migration helper to
# detect legacy float32 rows (exactly 2× the canonical float16 length
# and divisible by 4).
_STORAGE_BYTES = np.dtype(STORAGE_DTYPE).itemsize  # 2
_LEGACY_BYTES = 4  # float32


def encode_vector(vec) -> bytes:
    """Cast a vector to the storage dtype and return its raw bytes.

    Accepts anything ``np.asarray`` can consume: a list, a tuple, or an
    existing numpy array of any float dtype. **Always** stores as
    float16 — every writer must go through this helper so the storage
    dtype is invariant.
    """
    return np.asarray(vec, dtype=STORAGE_DTYPE).tobytes()


def decode_vector(blob: bytes, *, expected_dim: Optional[int] = None) -> np.ndarray:
    """Decode one stored blob into a 1-D float32 array.

    Two-level robustness:

    * **Default**: blob is interpreted as the canonical storage dtype
      (float16). This is the contract every writer respects — see
      :func:`encode_vector`.

    * **With** ``expected_dim``: the byte length of the blob disambig-
      uates float16 vs legacy float32 storage:

        - ``len(blob) == expected_dim * 2`` → float16 (canonical)
        - ``len(blob) == expected_dim * 4`` → float32 (pre-918e5fc
          legacy; reader auto-converts to runtime float32)
        - otherwise → fall back to the canonical decode (caller
          decides what to do with the wrong-shape result)

    Pass ``expected_dim`` whenever the caller already knows the
    canonical dim for this row (e.g. after computing a library centroid
    for the same model). That makes the read path resilient even
    before the init-time migration finishes rewriting legacy blobs.

    Returns a copy (not a view over the bytes object), so callers can
    mutate the result freely.
    """
    if expected_dim is not None:
        n = len(blob)
        if n == int(expected_dim) * _STORAGE_BYTES:
            return (
                np.frombuffer(blob, dtype=STORAGE_DTYPE)
                .astype(RUNTIME_DTYPE, copy=False)
                .copy()
            )
        if n == int(expected_dim) * _LEGACY_BYTES:
            # Legacy float32 row (e.g. an author_centroids blob
            # written before commit 918e5fc). Decode at the legacy
            # width and upcast to the runtime dtype — no data loss.
            return (
                np.frombuffer(blob, dtype=np.float32)
                .astype(RUNTIME_DTYPE, copy=False)
                .copy()
            )
        # Fall through to default decode; caller can shape-check the
        # result and skip the row.
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


def decode_vectors_uniform(
    blobs: Iterable[bytes],
    *,
    expected_dim: Optional[int] = None,
) -> tuple[np.ndarray, list[bool]]:
    """Decode blobs to a uniform-dim float32 matrix, rescuing legacy rows.

    Used everywhere we ``np.stack`` a pool of vectors. A single
    legacy/corrupt blob with a mismatched length would otherwise blow
    up the entire batch with ``shapes ... not aligned`` or ``all input
    arrays must have the same shape``.

    Two-pass algorithm:

    1. **Probe**: decode each blob with the canonical (float16) dtype
       to collect candidate dims. The modal dim wins (or
       ``expected_dim`` when caller supplies it).
    2. **Reconcile**: re-decode each blob using
       :func:`decode_vector` with ``expected_dim=target_dim``. That
       call interprets the blob as float16 if its byte length matches
       the float16 width, or float32 if it matches the legacy float32
       width — so a stray legacy row is correctly upcast instead of
       being dropped. Anything still wrong-shape is dropped with a
       warning so the operator can investigate.

    Returns ``(matrix, kept_mask)`` where ``matrix`` is float32 with
    rows = kept vectors (empty 1-D array if nothing survives), and
    ``kept_mask[i]`` indicates whether ``blobs[i]`` was kept. Empty /
    falsy blobs are always dropped.
    """
    blob_list = list(blobs)
    if not blob_list:
        return np.zeros((0,), dtype=RUNTIME_DTYPE), []

    if expected_dim is None:
        # Probe: canonical-dtype decode of each blob, then pick the
        # modal dim. Legacy-fp32 rows show up here at "wrong" dim but
        # in the minority case (most rows are already canonical), so
        # the modal still picks the right target.
        from collections import Counter

        probe_dims: list[int] = []
        for blob in blob_list:
            if not blob:
                continue
            try:
                probe_dims.append(
                    np.frombuffer(blob, dtype=STORAGE_DTYPE).shape[0]
                )
            except Exception:
                continue
        if not probe_dims:
            return np.zeros((0,), dtype=RUNTIME_DTYPE), [False] * len(blob_list)
        target_dim = Counter(probe_dims).most_common(1)[0][0]
    else:
        target_dim = int(expected_dim)

    kept: list[np.ndarray] = []
    kept_mask: list[bool] = []
    rescued_legacy = 0
    dropped = 0
    for blob in blob_list:
        if not blob:
            kept_mask.append(False)
            continue
        try:
            vec = decode_vector(blob, expected_dim=target_dim)
        except Exception:
            kept_mask.append(False)
            dropped += 1
            continue
        if vec.shape[0] != target_dim:
            kept_mask.append(False)
            dropped += 1
            continue
        # Heads-up when we just rescued a legacy fp32 row so the
        # operator knows the init-time migration hasn't run yet on
        # this table.
        if len(blob) == target_dim * _LEGACY_BYTES:
            rescued_legacy += 1
        kept.append(vec)
        kept_mask.append(True)

    if rescued_legacy:
        logger.warning(
            "decode_vectors_uniform rescued %d legacy float32 vector(s) "
            "(target dim=%d) — run the init-time migration to rewrite them",
            rescued_legacy, target_dim,
        )
    if dropped:
        logger.warning(
            "decode_vectors_uniform dropped %d vector(s) with mismatched "
            "dim (target=%d)",
            dropped, target_dim,
        )
    if not kept:
        return np.zeros((0,), dtype=RUNTIME_DTYPE), kept_mask
    return np.stack(kept), kept_mask


def migrate_blob_column_to_float16(
    conn: sqlite3.Connection,
    table: str,
    blob_col: str,
    *,
    key_cols: tuple[str, ...],
    model_col: Optional[str] = "model",
) -> int:
    """Re-encode any legacy float32 blobs in a vector column as float16.

    Generic version of the algorithm used by ``init_db_schema``: per
    ``model`` (or globally when ``model_col`` is None), the modal blob
    length is taken as canonical (float16). Rows with a blob exactly
    twice the modal length and divisible by 4 are decoded as float32
    and re-encoded through :func:`encode_vector`. Idempotent.

    ``key_cols`` is the tuple of primary-key columns used to scope the
    UPDATE — every (table, blob_col) pair has its own PK shape so the
    caller passes it explicitly rather than us guessing from
    ``PRAGMA table_info``.

    Returns the number of rows fixed.
    """
    try:
        if model_col:
            rows = conn.execute(
                f"SELECT {model_col} AS model, length({blob_col}) AS n, "
                f"COUNT(*) AS c FROM {table} "
                f"GROUP BY {model_col}, length({blob_col})"
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT '' AS model, length({blob_col}) AS n, "
                f"COUNT(*) AS c FROM {table} "
                f"GROUP BY length({blob_col})"
            ).fetchall()
    except sqlite3.OperationalError:
        return 0

    counts: dict[tuple[str, int], int] = {
        (str(r["model"] or ""), int(r["n"])): int(r["c"]) for r in rows
    }
    if not counts:
        return 0

    modal_len: dict[str, int] = {}
    for (model, n), c in counts.items():
        if model not in modal_len or c > counts[(model, modal_len[model])]:
            modal_len[model] = n

    fixed = 0
    where_keys = " AND ".join(f"{col} = ?" for col in key_cols)
    select_keys = ", ".join(key_cols)
    for model, mod_len in modal_len.items():
        target_len = mod_len * 2
        # Legacy float32 blobs are exactly 4 bytes per element ⇒ length
        # divisible by 4 AND twice the canonical float16 length.
        if target_len % _LEGACY_BYTES != 0 or counts.get((model, target_len), 0) == 0:
            continue
        if model_col:
            broken = conn.execute(
                f"SELECT {select_keys}, {blob_col} AS _blob FROM {table} "
                f"WHERE {model_col} = ? AND length({blob_col}) = ?",
                (model, target_len),
            ).fetchall()
        else:
            broken = conn.execute(
                f"SELECT {select_keys}, {blob_col} AS _blob FROM {table} "
                f"WHERE length({blob_col}) = ?",
                (target_len,),
            ).fetchall()
        for row in broken:
            try:
                vec = np.frombuffer(row["_blob"], dtype=np.float32)
                new_blob = encode_vector(vec)
            except Exception:
                continue
            params = (new_blob, *(row[col] for col in key_cols))
            conn.execute(
                f"UPDATE {table} SET {blob_col} = ? WHERE {where_keys}",
                params,
            )
            fixed += 1

    if fixed:
        logger.info(
            "Re-encoded %d %s.%s rows from float32 to float16",
            fixed, table, blob_col,
        )
    return fixed


__all__ = [
    "STORAGE_DTYPE",
    "RUNTIME_DTYPE",
    "encode_vector",
    "decode_vector",
    "decode_vectors",
    "decode_vectors_uniform",
    "migrate_blob_column_to_float16",
]
