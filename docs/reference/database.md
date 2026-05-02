---
title: Database schema
description: SQLite schema reference — every table in scholar.db, the columns that matter, and what writes to each one.
---

# Database schema

ALMa stores everything in **one SQLite file**: `data/scholar.db`.
WAL mode is enabled by default. The `scheme` is created and migrated
on startup; you do not run migrations manually.

## Inspecting the live schema

```bash
sqlite3 data/scholar.db .tables
sqlite3 data/scholar.db "SELECT sql FROM sqlite_master WHERE type='table';"
sqlite3 data/scholar.db "PRAGMA table_info(papers)"
```

For an exhaustive dump:

```bash
sqlite3 data/scholar.db .schema > docs/_internal/schema.sql
```

## Tables, by domain

### Papers and lifecycle

| Table | Purpose |
|---|---|
| `papers` | The central table. One row per work. Carries `status` (membership), `reading_status`, `rating`, `notes`, `added_from`, `added_at`, source IDs (`openalex_id`, `doi`, `semantic_scholar_id`), abstract, year, journal, citation count, and `canonical_paper_id` (for preprint↔journal twins). |
| `publication_authors` | Many-to-many between papers and authors. |
| `publication_topics` | Many-to-many between papers and topics, with score. |
| `publication_institutions` | Author-institution links per paper. |
| `publication_references` | Citation graph: who cites whom. PK is `(paper_id, referenced_work_id)` for paper-side lookups; `idx_publication_references_ref` on `referenced_work_id` accelerates the graph lane's corpus-overlap query (which would otherwise be O(N²) on dense reference graphs). |
| `paper_enrichment_status` | Per-paper, per-source, per-purpose ledger for the corpus metadata rehydration job. Records `status` (`enriched` / `unchanged` / `terminal_no_match` / `retryable_error`), `lookup_key`, `fields_key`, `attempts`, and `next_retry_at` so reruns skip what's already covered. |

### Curation

| Table | Purpose |
|---|---|
| `collections` | User-defined collections. |
| `collection_items` | Many-to-many `collection ↔ paper`. |
| `tags` | User-defined tags (max 5 per paper enforced in code). |
| `publication_tags` | Many-to-many `tag ↔ paper`. |
| `tag_suggestions` | LLM-suggested tags awaiting user accept / dismiss. |
| `topics` | Source-backed scholarly topics. |
| `topic_aliases` | User-defined aliases that collapse to a canonical topic. |

### Authors

| Table | Purpose |
|---|---|
| `authors` | Researcher profiles with OpenAlex / S2 / ORCID / Scholar IDs and `id_resolution_status`. |
| `followed_authors` | Authors the user is actively monitoring. |
| `author_centroids` | Per-author SPECTER2 centroid (mean of their papers' vectors). Materialised for the `semantic_similar` author suggestion source. |
| `author_suggestion_cache` | Per-source cache of OpenAlex / S2 author suggestions. |
| `missing_author_feedback` | "I rejected this suggestion" history. Carries `suggestion_bucket` (the rail bucket label that surfaced the rejected author) so per-bucket outcome calibration can attribute the negative event correctly. |
| `author_suggestion_follow_log` | Positive-side counterpart of `missing_author_feedback`: one row per rail-originated follow with the `suggestion_bucket` attribution. Fed by `POST /authors/suggestions/track-follow`. Read by `compute_author_bucket_calibration` to compute per-bucket quality multipliers. |

### Discovery

| Table | Purpose |
|---|---|
| `discovery_settings` | Mirror of the Discovery section of `settings.json` (used by some hot paths). |
| `discovery_lenses` | Saved lens definitions. |
| `lens_signals` | Per-lens positive / negative feedback counters. |
| `recommendations` | Materialised recommendations from the last lens refresh. |
| `suggestion_sets` | Each refresh produces a suggestion set; rows track which set produced which recommendation. |
| `feedback_events` | The append-only signal store. Every Save / Like / Dismiss / Signal-Lab event lands here with `context_json` (lens id, source bucket, surface, etc.). Paper actions are commonly stored as `event_type='paper_action'`, `entity_type='publication'`, `entity_id=<paper_id>`, with JSON `value` containing `action`, `rating`, and `signal_value`. Ranking code also tolerates older `entity_type='paper'` rows and direct event names like `like` / `dismiss`. |
| `preference_profiles` | Materialised preference centroids derived from `feedback_events`. |
| `scoring_cache` | Cached per-paper score breakdowns per lens. |
| `similarity_cache` | Cached cosine-similarity lookups. |

### Feed

| Table | Purpose |
|---|---|
| `feed_items` | One row per (monitor, paper) pair the monitor surfaced. |
| `feed_monitors` | Active author / topic / query monitors. |

### Embeddings

| Table | Purpose |
|---|---|
| `publication_embeddings` | SPECTER2 vectors per paper. `source` ∈ `{'s2', 'local'}` for provenance. |
| `publication_embedding_fetch_status` | Per-paper S2 fetch state (`unmatched`, `missing_vector`, `lookup_error`, etc.). |
| `publication_clusters` | HDBSCAN cluster assignment per paper. |
| `graph_cache` | 2D projection cache for the Insights graph. |
| `graph_cluster_labels` | LLM-generated cluster labels. |
| `paper_network_cache` | Cached citation / co-author graphs per paper. |

### Alerts

| Table | Purpose |
|---|---|
| `alerts` | Top-level alert definitions. |
| `alert_rules` | Individual rules (author / keyword / topic / similarity / discovery_lens). |
| `alert_rule_assignments` | Many-to-many `alert ↔ rule`. |
| `alert_history` | Every dispatch with the digest payload. |
| `alerted_publications` | Per-paper history so the same paper isn't sent twice. |

### Operations

| Table | Purpose |
|---|---|
| `operation_status` | Activity-envelope state for every background job. |
| `operation_logs` | Per-job log lines (used by the Activity panel's logs sub-tab). |

## Key invariants

These are pinned by code:

* **`papers.id`** is a UUID hex string. Generated by ALMa, not by
  upstream sources. Stable across upserts.
* **`papers.status`** is one of `tracked`, `library`, `dismissed`,
  `removed`. Library reads filter `status='library'`. Discovery
  reads filter `status='tracked' AND status NOT IN ('dismissed', 'removed')`.
* **`papers.canonical_paper_id`** is non-null on preprint rows that
  collapsed into a journal twin. Library / Discovery reads filter
  `canonical_paper_id IS NULL` to show one card per work.
* **`feedback_events`** is append-only. Negative signals are not
  deletes, they're new rows.
* **`publication_embeddings.source`** distinguishes S2-fetched from
  locally-computed vectors. Same vector dimension (768) and same
  model (`allenai/specter2_base`) regardless of source.

## What you can edit by hand

Pretty much anything via the UI. Direct SQL edits are technically
fine since ALMa is single-user, but be aware:

* The UI uses optimistic updates in some places — your hand-edit
  may not appear until the user reloads.
* The recommender consumes `feedback_events` as append-only; deleting
  rows there will not "un-train" anything cleanly.
* `recommendations` is regenerated on every lens refresh — editing
  it by hand has no lasting effect.

## Backups

The whole file is the backup. See [Backups](../operations/backups.md)
for the safe backup paths (online via the Settings UI, offline by
file copy).
