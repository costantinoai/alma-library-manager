---
title: REST API
description: Every endpoint ALMa exposes, grouped by domain, with live Swagger UI for try-it-yourself.
---

# REST API

ALMa exposes a single REST API rooted at `/api/v1/`. The same FastAPI
app serves the API and the SPA, so in dev you'll hit
`http://localhost:8000/api/v1/...` and in production whatever
hostname your reverse proxy is binding to.

## Authentication

By default there is **no authentication** — ALMa is a single-user
local tool.

You can require an API key by setting the `API_KEY` environment
variable. When set, every request must include a header:

```
X-API-Key: <your key>
```

Set `API_KEY` if you're exposing ALMa beyond `127.0.0.1` (for
example through a reverse proxy or Tailscale). It does **not**
create user accounts — it gates the entire API behind a shared
secret.

## Base URL

| Environment | Base |
|---|---|
| Local dev | `http://localhost:8000/api/v1` |
| Docker compose | `http://localhost:8000/api/v1` |
| Behind a reverse proxy | `https://your-host/api/v1` |

## Response envelope

Most endpoints return JSON directly — a list of objects, a single
object, or a status payload.

Long-running operations return an **Activity envelope** instead:

```json
{
  "job_id": "f3b2…",
  "status": "queued",
  "message": "Refresh started; track progress in Activity."
}
```

When you see this envelope, the work is happening in the scheduler
worker. Track it via `GET /api/v1/activity/{job_id}` or watch the
Activity panel in the UI. See [Background jobs](../operations/background-jobs.md).

## Errors

Errors use standard HTTP status codes with a JSON body:

```json
{ "detail": "Paper not found" }
```

| Code | Meaning |
|---|---|
| `200` / `201` / `204` | Success |
| `400` | Validation error (bad query, missing required body) |
| `401` | Missing / wrong `X-API-Key` (when `API_KEY` is set) |
| `404` | Resource not found |
| `409` | Conflict (already exists, concurrent edit) |
| `500` | Internal error — check `/api/v1/logs` for stack trace |

Long traces and unexpected errors are written to the application log
and surfaced in **Activity → Logs**.

## Live API explorer

The full OpenAPI spec is rendered below. Expand any operation to see
its parameters, request body, and example responses, then **Try it
out** to send a real request against the documented base URL.

<swagger-ui src="../openapi.json"/>

!!! tip "Run a backend first"

    The Swagger explorer above can read the bundled
    `openapi.json`. To **send live requests** from the explorer, point
    the "Servers" dropdown at a running ALMa instance.

## Endpoints by domain

A curated overview. The Swagger explorer above is authoritative for
parameters and response shapes.

### Library — saved papers and curation

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/library/saved` | List saved papers (search, sort, paginate) |
| `POST` | `/library/saved` | Save a paper to Library |
| `PUT` | `/library/saved/{id}` | Update notes / rating |
| `DELETE` | `/library/saved/{id}` | Soft-remove from Library |
| `GET` | `/library/reading-queue` | Reading-list view |
| `POST` | `/library/papers/{id}/reading-status` | Set reading status |
| `GET` | `/library/workflow-summary` | Landing-card counters |
| `POST` | `/library/bulk/clear-rating` | Bulk: set rating to 0 |
| `POST` | `/library/bulk/remove` | Bulk: soft-remove |
| `POST` | `/library/bulk/add-to-collection` | Bulk: add to collection |
| `GET` `POST` `PUT` `DELETE` | `/library/collections[/…]` | Collection CRUD |
| `GET` `POST` `DELETE` | `/library/tags[/…]` | Tag CRUD + assign |
| `GET` `POST` `PUT` `DELETE` | `/library/topics[/…]` | Topics + aliases |
| `GET` `DELETE` | `/library/followed-authors[/…]` | Followed-author management |

### Library imports

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/library/import/bibtex` | Upload a `.bib` file |
| `POST` | `/library/import/bibtex/text` | Paste BibTeX text |
| `POST` | `/library/import/zotero` | Pull a Zotero library |
| `POST` | `/library/import/zotero/rdf` | Upload a Zotero RDF export |
| `GET` | `/library/import/zotero/collections` | List Zotero collections (preview) |
| `POST` | `/library/import/search` | Online OpenAlex search |
| `POST` | `/library/import/search/save` | Save an online search result |
| `POST` | `/library/import/resolve-openalex` | Re-resolve unresolved imports |
| `POST` | `/library/import/enrich` | Enrich resolved imports |
| `GET` | `/library/import/unresolved` | Imports staging panel data |

### Library management

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/library-mgmt/info` | DB size, paper count, last backup |
| `GET` `POST` `DELETE` | `/library-mgmt/backup[/…]` | Backups |
| `POST` | `/library-mgmt/restore/{name}` | Restore from a backup |
| `POST` | `/library-mgmt/deduplicate` | Run preprint↔journal dedup |
| `POST` | `/library-mgmt/embeddings/reset` | Delete only embedding artifacts (`publication_embeddings`, `author_centroids`, embedding fetch markers) so vectors can be re-fetched/recomputed |
| `POST` | `/library-mgmt/reset` | Wipe DB (dangerous; confirms) |

### Feed

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/feed` | Inbox items |
| `GET` | `/feed/status` | Refresh status plus latest-fetch `new_count` |
| `POST` | `/feed/refresh` | Trigger a refresh (Activity) |
| `POST` | `/feed/bulk-action` | Bulk save / dislike |
| `POST` | `/feed/{id}/add` | Save a Feed paper |
| `POST` | `/feed/{id}/like` | Save with rating 4 |
| `POST` | `/feed/{id}/love` | Save with rating 5 |
| `POST` | `/feed/{id}/dislike` | Negative signal (paper stays visible) |
| `GET` `POST` `PUT` `DELETE` | `/feed/monitors[/…]` | Monitor CRUD |
| `POST` | `/feed/monitors/{id}/refresh` | Refresh one monitor |

### Discovery

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/discovery/recommendations` | List recommendations |
| `DELETE` | `/discovery/recommendations` | Clear all |
| `POST` | `/discovery/refresh` | Refresh recs (Activity) |
| `GET` | `/discovery/status` | Refresh status |
| `GET` | `/discovery/stats` | Engagement counters |
| `GET` `PUT` `POST` | `/discovery/settings[/…]` | Weight + behaviour config |
| `POST` | `/discovery/recommendations/{id}/save` | Save → Library |
| `POST` | `/discovery/recommendations/{id}/read` | Add to Reading list |
| `POST` | `/discovery/recommendations/{id}/like` | Rate positively (`rating=4` like, `rating=5` love); stays visible |
| `POST` | `/discovery/recommendations/{id}/dislike` | Rate 1 + negative signal; stays visible |
| `POST` | `/discovery/recommendations/{id}/dismiss` | Hide suggestion + long-cooldown negative signal |
| `POST` | `/discovery/recommendations/{id}/seen` | Mark seen |
| `GET` | `/discovery/recommendations/{id}/explain` | Score breakdown |
| `POST` | `/discovery/similar` | "Find papers like these" |
| `POST` | `/discovery/manual-search` | Cross-source paper search |
| `POST` | `/discovery/manual-search/add` | Save a manual-search result |

### Discovery lenses

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/lenses` | List / create |
| `GET` `PUT` `DELETE` | `/lenses/{id}` | Get / update / delete |
| `POST` | `/lenses/{id}/refresh` | Refresh this lens |
| `GET` | `/lenses/{id}/recommendations` | Cached recs for this lens |
| `GET` | `/lenses/{id}/branches` | Branch map preview |
| `GET` | `/lenses/{id}/signals` | Lens-scoped signal counters |

### Authors

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/authors[/…]` | List / add |
| `GET` `DELETE` | `/authors/{id}` | Detail / remove |
| `GET` | `/authors/{id}/detail` | Light-weight popup data |
| `GET` | `/authors/{id}/dossier` | Full dossier (works, topics, co-authors) |
| `GET` | `/authors/{id}/publications` | Author's papers in our corpus |
| `GET` | `/authors/{id}/openalex-works` | Page through OpenAlex bibliography |
| `POST` | `/authors/{id}/refresh-cache` | Incremental refresh |
| `POST` | `/authors/{id}/deep-refresh` | Full re-fetch |
| `POST` | `/authors/deep-refresh-all` | Bulk deep refresh; `scope=needs_metadata` targets identity/profile gaps for Settings maintenance |
| `POST` | `/authors/backfill-works` | Pull works + S2 vectors |
| `POST` | `/authors/{id}/history-backfill` | Historical corpus backfill |
| `POST` | `/authors/{id}/empty-cache` | Clear cached works |
| `POST` | `/authors/{id}/repair` | Repair identifiers |
| `GET` | `/authors/{id}/id-candidates` | Identifier candidates |
| `POST` | `/authors/resolve-identifiers` | Resolve OpenAlex / Scholar IDs |
| `POST` | `/authors/{id}/confirm-identifiers` | Manually confirm IDs |
| `POST` | `/authors/{id}/confirm-openalex` | Confirm OpenAlex on Scholar author |
| `POST` | `/authors/resolve-openalex` | Resolve from Scholar ID |
| `POST` | `/authors/{id}/search-scholar` | Manual Google Scholar search |
| `POST` | `/authors/follow-from-paper` | Follow author seen on a paper card |
| `GET` | `/authors/needs-attention` | Authors needing manual triage |
| `GET` | `/authors/lookup` | Look up by display name |
| `GET` | `/authors/suggestions` | Multi-source author suggestions |
| `POST` | `/authors/suggestions/refresh-network` | Refresh OA / S2 caches |
| `POST` | `/authors/suggestions/reject` | Reject a suggestion (optional `suggestion_bucket` for bucket-quality calibration) |
| `POST` | `/authors/suggestions/track-follow` | Log a rail-originated follow with `suggestion_bucket` for bucket-quality calibration |
| `POST` | `/authors/{id}/fetch-and-send` | Fetch + dispatch (Activity) |
| `POST` | `/authors/{id}/fetch-preview` | Fetch preview (Activity) |
| `POST` | `/authors/{id}/preview/save` | Save previewed publications |
| `PATCH` | `/authors/{id}/type` | Set author_type |

### Feedback learning

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/feedback/track` | Record passive interaction events |
| `POST` | `/feedback/reset` | Reset learned feedback state |

### Insights & Reports

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/insights` | Overview (charts + summary). Served from a fingerprint-keyed cache; response carries `stale` / `rebuilding` / `computed_at` flags. |
| `GET` | `/insights/diagnostics` | Composed payload — assembles all eight diagnostic sections from cache. Backwards-compatible with pre-split clients. |
| `GET` | `/insights/diagnostics/sections/{section}` | One of the eight diagnostics sections (`feed`, `discovery`, `ai`, `authors`, `alerts`, `feedback`, `operational`, `evaluation`). Each section is a fingerprint-keyed materialised view; response carries `stale` / `rebuilding` / `computed_at`. The frontend uses these to stream cards in independently with per-card skeletons. |
| `GET` | `/insights/discovery/branch-action` | Branch-level engagement |
| `GET` | `/graphs/paper-map` | 2D SPECTER2 projection + clusters. Default options served from cache; custom options bypass cache. SWR flags ride inside `metadata`. |
| `GET` | `/graphs/author-network` | Co-authorship clusters. Cached. |
| `GET` | `/graphs/topic-map` | Topic co-occurrence graph. Cached. |
| `POST` | `/graphs/rebuild` | Force a full rebuild of all graph caches (re-cluster + re-project). |
| `GET` | `/reports/weekly-brief` | Weekly research brief |
| `GET` | `/reports/collection-intelligence` | Collection-level report |
| `GET` | `/reports/topic-drift` | Topic drift report |
| `GET` | `/reports/signal-impact` | Ranking signal impact report |

### Alerts

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/alerts` | List / create |
| `GET` `PUT` `DELETE` | `/alerts/{id}` | Get / update / delete |
| `POST` | `/alerts/{id}/evaluate` | Evaluate + send |
| `POST` | `/alerts/{id}/dry-run` | Evaluate without sending |
| `GET` `POST` `PUT` `DELETE` | `/alerts/rules[/…]` | Rule CRUD |
| `POST` | `/alerts/rules/{id}/toggle` | Enable / disable |
| `POST` | `/alerts/test/{id}` | Test-fire a rule |
| `POST` | `/alerts/{id}/rules` | Assign rules to alert |
| `DELETE` | `/alerts/{id}/rules/{rid}` | Unassign rule |
| `GET` | `/alerts/history` | Past dispatches |
| `GET` | `/alerts/templates` | Suggested rule templates |

### AI

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/ai/status` | Selected providers + runtime state |
| `GET` | `/ai/dependencies` | Installed package matrix |
| `POST` | `/ai/configure` | Choose providers / models |
| `POST` | `/ai/recheck-environment` | Re-introspect runtime |
| `POST` | `/ai/backfill-s2-vectors` | Bulk fetch SPECTER2 from S2 |
| `POST` | `/ai/compute-embeddings` | Local SPECTER2 fallback |
| `GET` | `/ai/embeddings/inactive` | Papers missing vectors |
| `DELETE` | `/ai/embeddings/inactive` | Drop unused vectors |

### Activity & operations

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/activity` | Active + recent operations |
| `GET` | `/activity/{job_id}` | One operation status |
| `GET` | `/activity/{job_id}/logs` | Per-job logs |
| `POST` | `/activity/{job_id}/cancel` | Cancel a running job |
| `DELETE` | `/activity/{job_id}` | Dismiss from history |
| `GET` | `/scheduler` | Scheduler health + next runs |
| `GET` | `/logs` | Application log ring buffer |

### Settings

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/settings` | Full settings document |
| `PUT` | `/settings` | Update a section |
| `GET` | `/settings/openalex/usage` | Live OpenAlex quota state |
| `GET` | `/settings/runtimes` | Detected Python runtimes for AI |

### Other

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/papers/{id}` | Paper detail |
| `GET` | `/papers/stats` | Top topics / journals / institutions |
| `GET` | `/papers/{id}/prior-works` | Papers this paper cites |
| `GET` | `/papers/{id}/derivative-works` | Papers that cite this one |
| `GET` | `/papers/enrichment-status` | Pure-read corpus metadata rehydration ledger summary (per-source counts: OpenAlex / Semantic Scholar / Crossref) + per-paper rows |
| `POST` | `/papers/rehydrate-metadata` | Queue a 3-phase metadata repair job (Activity envelope): Phase 1 OpenAlex batched, Phase 1.5 Semantic Scholar batched (fills `tldr` + `influential_citation_count` + abstract fallback), Phase 2 Crossref per-paper for residual abstract misses. `limit` accepts up to 100,000 papers per call. |
| `GET` | `/search` | Global search (papers + authors + collections) |
| `GET` | `/backup/export` | Export DB / JSON / BibTeX |
| `GET` | `/bootstrap` | Frontend boot payload |
| `GET` | `/plugins` | Plugin inventory (Slack, etc.) |
| `GET` `POST` `DELETE` | `/operations[/…]` | Bulk operation endpoints |

## OpenAPI artefact

The raw OpenAPI 3 document is bundled with these docs at
[`/openapi.json`](../openapi.json). Generate a fresh copy from a
running instance with:

```bash
curl -s http://localhost:8000/openapi.json > docs/openapi.json
```

Or offline, from inside the Python environment:

```python
from alma.api.app import app
import json
print(json.dumps(app.openapi(), indent=2))
```
