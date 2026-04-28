"""V1 probe: compare S2-downloaded vs locally-computed SPECTER2 vectors.

Why: ALMa stores both `source='semantic_scholar'` and `source='local'`
vectors under the *same* canonical model key `allenai/specter2_base`,
on the assumption that they share the same vector space. That
assumption is load-bearing for every kNN / centroid / cos_sim read.

Findings from V2 research (2026-04-26):
- Both pipelines use SPECTER2 base + proximity adapter + [CLS] pooling
  + max_length=512, dim=768. So the *manifold* is the same.
- Input text differs:
    * Semantic Scholar:   `title + tokenizer.sep_token + abstract`
      (canonical, per the SPECTER2 HF README).
    * ALMa local:         `prepare_text(title, abstract, keywords, topics)`
      which produces `f"{title}. {title}. {abstract}\nKeywords: ...\nTopics: ..."`
      with title repetition + metadata enrichment, no [SEP] token.
- Result: vectors will be *close* but not identical. Expected cosine
  similarity for the same paper: ~0.95-0.99 (same model, different
  input string), NOT ≥ 0.999.

What this probe does:
1. Pick N papers with `source='semantic_scholar'` vectors AND a
   non-empty title + abstract.
2. For each, run the local SPECTER2 stack and store the result under
   a sibling model key `allenai/specter2_base__local_probe` so both
   coexist for comparison.
3. Compute cosine, L2, and per-dimension delta between the two vectors.
4. Print mean / median / min / max / histogram.

Run with:
    python scripts/probe_s2_vs_local_specter2.py [--limit 50] \
                                                 [--cleanup]

Requires: `torch`, `transformers`, `adapters` installed in the active
env. The probe will print a clear error if they're missing.

Decision rule:
- Mean cosine ≥ 0.999 → vectors are bit-equivalent. Keep the unified
  model key; no schema change.
- 0.95 ≤ mean cosine < 0.999 → same manifold but input divergence.
  Decide whether to canonicalise the input pipeline (use [SEP] tokens
  on the local side) or split the model key.
- Mean cosine < 0.95 → different model / adapter / pooling. Split the
  model key immediately and stop mixing them in any kNN read.
"""

from __future__ import annotations

import argparse
import struct
import sys
from datetime import datetime
from pathlib import Path

# Ensure src/ is importable when running from the repo root.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

PROBE_MODEL_KEY = "allenai/specter2_base__local_probe"
SOURCE_LABEL = "local_probe"


def _vector_blob(vector) -> bytes:
    return struct.pack(f"<{len(vector)}f", *(float(x) for x in vector))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=50, help="Sample size (default 50)")
    parser.add_argument("--cleanup", action="store_true", help="Delete probe vectors after running")
    args = parser.parse_args()

    try:
        import numpy as np
    except ImportError:
        print("ERROR: numpy not installed in this env.", file=sys.stderr)
        return 1

    try:
        from alma.api.deps import open_db_connection
        from alma.discovery.similarity import (
            SpecterEmbedder,
            prepare_text,
        )
    except ImportError as exc:
        print(f"ERROR: cannot import alma modules: {exc}", file=sys.stderr)
        return 1

    conn = open_db_connection()

    # 1. Sample papers with S2-downloaded vectors AND title+abstract.
    rows = conn.execute(
        """
        SELECT p.id, p.title, p.abstract, pe.embedding AS s2_blob
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = 'allenai/specter2_base'
          AND pe.source = 'semantic_scholar'
          AND COALESCE(TRIM(p.title), '') <> ''
          AND COALESCE(TRIM(p.abstract), '') <> ''
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (args.limit,),
    ).fetchall()

    if not rows:
        print("No papers with both S2 vectors and title+abstract — nothing to probe.")
        return 0

    print(f"Sampled {len(rows)} papers. Loading local SPECTER2 (this loads the model)...")

    # 2. Load local SPECTER2 once.
    try:
        embedder = SpecterEmbedder.get_instance()
        embedder._load_model()  # force eager load + dep check
    except RuntimeError as exc:
        print(f"ERROR: local SPECTER2 not runnable: {exc}", file=sys.stderr)
        print("Install `torch`, `transformers`, `adapters` and re-run.", file=sys.stderr)
        return 1

    print(f"Local SPECTER2 ready on {embedder.device}.")

    # 3. Prepare input texts using the SAME pipeline production uses for
    #    the `local` source. Match `services/embeddings.py` behavior:
    #    `prepare_text(title, abstract, topics=..., max_tokens=...)`.
    #    Topics are loaded per-paper; we skip them in the probe to make
    #    the input match what gets used when topics are unavailable
    #    (still the dominant code path for newly-imported papers).
    texts = [prepare_text(r["title"] or "", r["abstract"] or "", max_tokens=512, is_query=False) for r in rows]

    # 4. Encode in one batch.
    print(f"Encoding {len(texts)} papers locally...")
    local_vectors = embedder.encode(texts)  # shape (N, 768)

    # 5. Upsert under the probe model key so both coexist.
    now = datetime.utcnow().isoformat()
    for row, vec in zip(rows, local_vectors):
        conn.execute(
            """
            INSERT INTO publication_embeddings (paper_id, embedding, model, source, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(paper_id, model) DO UPDATE SET
                embedding = excluded.embedding,
                source = excluded.source,
                created_at = excluded.created_at
            """,
            (row["id"], _vector_blob(vec), PROBE_MODEL_KEY, SOURCE_LABEL, now),
        )
    conn.commit()
    print(f"Stored {len(local_vectors)} probe vectors under model={PROBE_MODEL_KEY}.")

    # 6. Compute cosine + L2 + per-dim delta.
    cosines = []
    l2s = []
    max_deltas = []
    for row, local_vec in zip(rows, local_vectors):
        s2_vec = np.frombuffer(row["s2_blob"], dtype=np.float32).copy()
        local_arr = np.asarray(local_vec, dtype=np.float32)
        if s2_vec.shape != local_arr.shape:
            print(f"  WARNING shape mismatch on {row['id']}: s2={s2_vec.shape} local={local_arr.shape}")
            continue
        cos = float(np.dot(s2_vec, local_arr) / (np.linalg.norm(s2_vec) * np.linalg.norm(local_arr) + 1e-12))
        l2 = float(np.linalg.norm(s2_vec - local_arr))
        max_delta = float(np.max(np.abs(s2_vec - local_arr)))
        cosines.append(cos)
        l2s.append(l2)
        max_deltas.append(max_delta)

    cos_arr = np.asarray(cosines)
    l2_arr = np.asarray(l2s)
    delta_arr = np.asarray(max_deltas)

    print()
    print("=" * 60)
    print(f"S2-downloaded vs locally-computed SPECTER2  (N={len(cosines)})")
    print("=" * 60)
    print("Cosine similarity:")
    print(f"  mean   = {cos_arr.mean():.4f}")
    print(f"  median = {np.median(cos_arr):.4f}")
    print(f"  min    = {cos_arr.min():.4f}")
    print(f"  max    = {cos_arr.max():.4f}")
    print()
    print("L2 distance:")
    print(f"  mean   = {l2_arr.mean():.4f}")
    print(f"  median = {np.median(l2_arr):.4f}")
    print(f"  max    = {l2_arr.max():.4f}")
    print()
    print("Per-dim max abs delta:")
    print(f"  mean   = {delta_arr.mean():.4f}")
    print(f"  max    = {delta_arr.max():.4f}")
    print()
    print("Cosine histogram:")
    bins = [0.0, 0.5, 0.8, 0.9, 0.95, 0.98, 0.99, 0.999, 1.001]
    counts, _ = np.histogram(cos_arr, bins=bins)
    for lo, hi, n in zip(bins[:-1], bins[1:], counts):
        bar = "#" * min(40, int(n))
        print(f"  [{lo:.3f}, {hi:.3f})  n={n:3d}  {bar}")
    print()
    print("Decision rule:")
    print("  cosine ≥ 0.999      → bit-equivalent, keep unified model key")
    print("  0.95 ≤ cos < 0.999  → same manifold, split source labels OR canonicalise input")
    print("  cosine < 0.95       → different vector space, split model key immediately")

    if args.cleanup:
        conn.execute("DELETE FROM publication_embeddings WHERE model = ?", (PROBE_MODEL_KEY,))
        conn.commit()
        print(f"\nCleaned up probe vectors.")
    else:
        print(f"\nProbe vectors retained under model={PROBE_MODEL_KEY!r}.")
        print("Re-run with --cleanup to remove.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
