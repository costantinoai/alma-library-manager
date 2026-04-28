---
title: Scoring formulas
description: The 10-weight Discovery scorer, in numbers.
---

# Scoring formulas

A Discovery candidate is a paper proposed by one of the
[retrieval channels](../concepts/discovery.md#retrieval-channels). The
scorer combines several signals into a single number used for
ranking. This page documents how.

## The hybrid scorer

For each candidate $c$ in lens $L$:

$$
\text{score}(c, L) = \sum_{i} w_i \cdot s_i(c, L)
$$

where $w_i$ is the weight from
[Discovery settings](../reference/configuration.md#where-each-settings-card-stores-its-values)
and $s_i$ is one of the signal functions below.

## Signals

### `source_relevance`

How strongly the channel that produced this candidate "votes" for
it. OpenAlex `related-works` votes hard (≈1.0); broad topic search
votes lower (≈0.3). SPECTER2 cosine votes proportionally to the
cosine value.

Range: 0…1.

### `text_similarity`

Two paths, blended:

1. **Semantic** — SPECTER2 cosine between the candidate's vector
   and the lens's preference centroid (mean of saved-paper
   vectors). Available when embeddings are enabled.
2. **Lexical** — TF-IDF + character n-gram + scholarly term overlap
   over a richer scholarly document (title + abstract + topic terms
   + venue), not just plain title/abstract.

Both are calibrated through piecewise curves so method-level
matches aren't compressed into near-zero values. The two paths
average; if only one is available, that one is used.

Range: 0…1.

### `author_affinity`

Has the candidate's author appeared in your Library, your follow
list, or as a co-author of your saved authors?

Computed as a weighted sum:

| Match | Weight |
|---|---|
| Followed author | 1.0 |
| Author of a saved Library paper | 0.7 |
| Co-author of a saved-Library author | 0.4 |
| Author cited by saved papers | 0.3 |

Range: 0…1 (clamped).

### `journal_affinity`

Does the candidate's venue (journal / conference) appear often in
your Library? Computed as $\log(1 + n) / \log(1 + N)$ where
$n$ is the count of saved papers in this venue and $N$ is the
count in your most-saved venue.

Range: 0…1.

### `recency_boost`

A small boost for newer papers, decaying linearly:

$$
\text{recency}(c) =
\max(0, 1 - \frac{\text{years\_since}(c)}{R})
$$

where $R$ is `discovery.limits.recency_window_years` (default 5).

Range: 0…1.

### `citation_quality`

Log-scaled citation count:

$$
\text{citation\_quality}(c) = \frac{\log(1 + \text{cited\_by})}{\log(1 + 10000)}
$$

A paper with 10k citations gets ≈1.0; a paper with 0 gets 0. The
denominator is fixed so the function is interpretable across
candidates.

Range: 0…1.

### `feedback_adj`

Adjusts the score based on prior feedback on the candidate's
attributes (topics, authors, venue). Each prior feedback event
contributes:

* **Positive** (rating ≥ 4) → small boost.
* **Negative** (rating ≤ 2) → small penalty.

Decayed over time using two windows:

| Window | Default | Weight |
|---|---|---|
| `feedback_decay_days_full` | 30 | 1.0 |
| `feedback_decay_days_half` | 90 | 0.5 |

Beyond `_half` days, weight tapers to 0.

Range: -1…+1.

### `preference_affinity`

Distance from the candidate's vector to the lens's
`preference_profiles` centroid (a learned projection of your
positive-rated saves). Lower distance → higher score.

Computed as $1 - \text{cosine\_distance}$, with optional
non-linear calibration to spread out the top.

Range: 0…1. Available only when embeddings are enabled.

### Two more weights

The `discovery_settings.weights` object has a couple more knobs that
the UI exposes:

* **`source_relevance` boost per channel** — per-channel multipliers
  used by the retrieval phase before the global scorer.
* **`usefulness_boost`** — a small explicit per-source bonus that
  lets you say "I trust S2 recs more than topic search" without
  changing the channel weights.

## Defaults

The default weights err on the side of "balanced":

| Signal | Weight |
|---|---|
| `source_relevance` | 1.0 |
| `text_similarity` | 1.0 |
| `feedback_adj` | 1.0 |
| `preference_affinity` | 0.8 |
| `author_affinity` | 0.7 |
| `citation_quality` | 0.5 |
| `journal_affinity` | 0.5 |
| `recency_boost` | 0.4 |

You can shift the balance per-lens (each lens overrides global
weights) and see the effect after the next refresh. The
`recommendations` table caches the last batch so you don't lose
results when re-tuning — only the next refresh applies the new
weights.

## Score breakdown

`GET /api/v1/discovery/recommendations/{id}/explain` returns the
per-signal contribution for one recommendation:

```json
{
  "id": "rec-abc",
  "paper_id": "p-xyz",
  "score": 0.71,
  "score_breakdown": {
    "source_relevance": 0.95,
    "text_similarity": 0.62,
    "author_affinity": 0.40,
    "journal_affinity": 0.10,
    "recency_boost": 0.80,
    "citation_quality": 0.55,
    "feedback_adj": 0.0,
    "preference_affinity": 0.71
  },
  "weights_used": { "...": 0 },
  "channel": "openalex_related"
}
```

The UI's "why this paper?" hover surfaces this breakdown so you can
see which signals pushed each recommendation up.
