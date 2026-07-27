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
| `PATCH` | `/library/papers/{id}/reading-status` | Set reading status |
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
| `POST` | `/library/import/zotero/collections` | List Zotero collections (preview) |
| `POST` | `/library/import/search` | Online OpenAlex search |
| `POST` | `/library/import/search/save` | Save an online search result |
| `POST` | `/library/import/resolve-openalex` | Re-resolve unresolved imports |
| `POST` | `/library/import/enrich` | Enrich resolved imports |
| `GET` | `/library/import/unresolved` | Imports staging panel data |

### Library management

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/library-mgmt/info` | DB size, paper count, backup list |
| `POST` `DELETE` | `/library-mgmt/backup[/…]` | Create / delete a backup (list is in `/library-mgmt/info`) |
| `POST` | `/library-mgmt/restore/{name}` | Restore from a backup |
| `POST` | `/library-mgmt/deduplicate` | Run preprint↔journal dedup |
| `POST` | `/library-mgmt/embeddings/reset` | Delete only embedding artifacts (`publication_embeddings`, `author_centroids`, embedding fetch markers) so vectors can be re-fetched/recomputed |
| `POST` | `/library-mgmt/reset` | Wipe DB (dangerous; confirms) |

### Inbox capture

Papers you send yourself from another device. The sweep normally runs on a
timer; these exist so the loop is observable and forceable. See
[Inbox](../concepts/inbox.md).

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/inbox/status` | Is a channel configured, what is waiting, what failed (pure read) |
| `POST` | `/inbox/sweep` | Poll the capture channels now. Idempotent — messages are keyed `(channel, external_id)`, so pressing twice cannot duplicate a paper. 400 when no channel is configured. |

Triage uses the canonical paper-action route: `POST /papers/{id}/action` with
`{"action": "defer", "surface": "inbox"}` for the ✕ (leaves the Inbox writing no
rating and no feedback event), or `add` / `like` / `love` / `dislike` for the
rest.

### Feed

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/feed` | Inbox items |
| `GET` | `/feed/status` | Refresh status plus latest-fetch `new_count` |
| `POST` | `/feed/refresh` | Trigger a refresh (Activity) |
| `POST` | `/feed/bulk-action` | Bulk save / dislike |
| — | *(single-item actions)* | Use `POST /papers/{id}/action` with `surface=feed`, `scope_ref=<feed item id>`. |
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
| `POST` | `/discovery/recommendations/{id}/dismiss` | Hide this lens suggestion; no preference signal |
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
| `GET` | `/authors/enrichment-status` | Pure-read author hydration ledger summary (OpenAlex / ORCID / Semantic Scholar / Crossref) + per-author rows |
| `POST` | `/authors/rehydrate-metadata` | Queue author profile/affiliation/alias hydration through the Activity envelope. Omit `limit` to process all eligible authors; explicit `limit` accepts up to 100,000 authors for bounded probes. |
| `GET` | `/authors/{id}/affiliations` | Read affiliation evidence and the current display-affiliation decision |
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
| `POST` | `/authors/{id}/merge-profiles` | Merge duplicate author rows into this canonical author. Body requires `alt_author_ids`; optional `field_choices` maps each alt author id to per-field winners (`primary` / `alt`) for metadata discrepancies. |
| `POST` | `/authors/{id}/discover-aliases` | Read-only ORCID lookup for additional OpenAlex profiles that may represent the same person |
| `POST` | `/authors/conflicts/{id}/resolve` | Resolve a hard-identifier conflict recorded during merge |
| `GET` | `/authors/lookup` | Look up by display name |
| `GET` | `/authors/suggestions` | Multi-source author suggestions |
| `POST` | `/authors/suggestions/refresh-network` | Refresh OA / S2 caches |
| `POST` | `/authors/suggestions/reject` | Reject a suggestion (optional `suggestion_bucket` for bucket-quality calibration) |
| `POST` | `/authors/suggestions/track-follow` | Log a rail-originated follow with `suggestion_bucket` for bucket-quality calibration |
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
| `GET` | `/graphs/paper-map` | 2D SPECTER2 projection + clusters. Default options are pure reads of the stored layout; custom options use a durable bounded variant cache and queue process-isolated computation on a miss. `prefetch=true` makes the read speculative: a cache miss reports `building` **without** enqueuing a layout build, so a sidebar hover can inspect cache state but never start work. Papers that gained a vector since the last full fit are placed by interpolation from their nearest already-placed neighbours; each node carries `metadata.placement` (`layout` / `interpolated` / `null`) and the payload counts them in `metadata.approximate_positions` and `metadata.unknown_placement`. |
| `GET` | `/graphs/author-network` | The Author Map: each eligible author is placed at the centroid of at least two of their papers on the corpus substrate, then the 2D centroids are density-clustered into research communities. Cached and process-built; ships **no edges**. Authors without two placed papers are omitted and counted in `metadata.omitted_unplaced`. Accepts `prefetch=true` with the same read-only meaning as `/graphs/paper-map`. |
| `GET` | `/graphs/signal-field` | Space-owned preference field over the corpus substrate: one valence per paper at its layout coordinates, plus its live 0–100 score. Feeds every paper map's Terrain overlay and Score colouring. Pure read. |
| `GET` | `/graphs/author-field` | The Author Map's live field, keyed by author id: mean `paper_valence` over the papers of theirs you have a signal on (`v: null` when none), plus their mean live score. Same `signal_valence` weights as `/graphs/signal-field`. Pure read. |
| `POST` | `/graphs/selection/lens` | Atomically create a collection, save a visible paper/author selection into it under the declared Library/Corpus scope, and create a collection-backed Discovery lens. |
| `POST` | `/graphs/cluster-labels/refresh` | Re-label paper-map clusters. |
| `POST` | `/graphs/rebuild` | Queue a process-isolated local rebuild for one scope. Keeps the last-good layout readable until replacement; does not perform remote reference enrichment. |
| `POST` | `/graphs/reference-backfill` | Queue OpenAlex reference enrichment independently of layout recomputation. |
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
| `GET` | `/alerts/history` | Past dispatches (per channel; pruned past 180 d) |
| `GET` | `/alerts/templates` | Suggested automations (empty until a delivery channel is configured) |
| `POST` | `/alerts/templates/{key}/apply` | Materialize a suggestion: creates rule + digest atomically; 404 once applied |

Alert responses carry `last_outcome` (worst status of the latest
evaluation batch) and `next_due_at` (next sweep-eligible slot; `null`
for manual or disabled digests). Rule configs are validated per type at
create/update — a config that would match nothing returns `422`.
Delivery dedup (`alerted_publications`) is keyed per
`(alert, paper, channel)`.

### AI

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/ai/status` | Selected providers + runtime state |
| `GET` | `/ai/dependencies` | Installed package matrix |
| `POST` | `/ai/configure` | Choose providers / models |
| `POST` | `/ai/recheck-environment` | Re-introspect runtime |
| `POST` | `/ai/backfill-s2-vectors` | Bulk fetch SPECTER2 from S2 |
| `POST` | `/ai/compute-embeddings` | Local SPECTER2 fallback |
| `DELETE` | `/ai/embeddings/inactive` | Drop unused vectors |

### Activity & operations

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/activity` | Active + recent operations |
| `GET` | `/activity/{job_id}` | One operation status |
| `GET` | `/activity/{job_id}/logs` | Per-job logs |
| `POST` | `/activity/{job_id}/stop` | Graceful stop — finish the current batch, save, exit at the next checkpoint |
| `POST` | `/activity/{job_id}/cancel` | Hard kill — interrupt the worker thread; the in-flight batch may be lost |
| `DELETE` | `/activity/{job_id}` | Dismiss from history (terminal rows only) |

Both stop verbs close the row outright when its worker is already gone
(process restart or crash), and both put a **24-hour cooldown** on the
operation when the run was a *background* one — so the app's own
schedulers don't undo your stop. The response then carries an
`automation_paused` block, and
`POST /health/operations/{key}/resume` lifts the hold. Stopping a run you
launched yourself changes no policy. Full rules:
[Background jobs → Who may restart a job](../operations/background-jobs.md#who-may-restart-a-job).

Operation rows that participate in a chained workflow (e.g. Library
save → metadata hydrate → S2 vector fetch → local SPECTER2 fill)
carry two grouping fields:

- `chain_id` — uuid hex shared by every member of the chain.
- `chain_step` — one of `hydrate`, `s2_fetch`, `local_specter2_fill`.

The Activity panel groups all rows sharing a `chain_id` under the
member with the lowest `chain_step` rank (the starter), with the
remaining steps rendered as children inside one envelope.
| `GET` | `/scheduler/status` | Scheduler health + next runs |
| `POST` | `/scheduler/trigger/{job_id}` | Manually trigger a scheduled job |
| `GET` | `/logs` | Application log ring buffer |

### Settings

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/settings` | Full settings document |
| `PUT` | `/settings` | Update a section |
| `GET` | `/settings/openalex/usage` | Live OpenAlex quota state |
| `GET` | `/settings/openalex/status` | OpenAlex connection status |
| `GET` | `/settings/semantic-scholar/status` | Semantic Scholar connection status |
| `GET` | `/settings/export` | Export the settings document |

### Signal Lab

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/signal-lab/games` | Available calibration game specs |
| `GET` | `/signal-lab/{game}/queue?count=12` | At least ten signed, zero-write rounds for Home's game deck |
| `POST` | `/signal-lab/{game}/round/answer` | Validate signature and persist exactly one answered round |
| `GET` | `/signal-lab/summary` | Unique/duplicate ledger evidence, current-fit observations and constraints, freshness, structural region/edge coverage, and active effects |
| `GET` `PUT` | `/signal-lab/settings` | Native feature activation, sampler/refit, Terrain tint, and bounded promotion weights |
| `GET` | `/signal-lab/model` | Current wholesale fit |
| `GET` | `/signal-lab/eval` | Held-out metrics and promotion evidence |
| `POST` | `/signal-lab/purge` | Delete round history and invalidate the model; does not change activation/config |

### Other

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/papers/{id}/details` | Paper detail |
| `POST` | `/papers/{id}/action` | **THE paper-action route.** Every surface — Feed, Discovery, Inbox, Map, Library, onboarding — applies `add`/`save`, `like`, `love`, `dislike`, `dismiss`, `defer`, `read`, `undo` here, so "what does Like mean" has one answer. `surface` records where the user acted. `surface=feed\|discovery` additionally require `scope_ref` (the feed item / recommendation id): they settle THAT row, so a dismiss in one lens never mutes the paper in another. Response carries the shared `status`/`rating` plus the adapter's own echo under `surface_result`. |
| `GET` | `/papers/stats` | Top topics / journals / institutions |
| `GET` | `/papers/{id}/prior-works` | Papers this paper cites |
| `GET` | `/papers/{id}/derivative-works` | Papers that cite this one |
| `GET` | `/papers/enrichment-status` | Pure-read corpus metadata rehydration ledger summary (per-source counts: OpenAlex / Semantic Scholar / Crossref) + per-paper rows |
| `POST` | `/papers/rehydrate-metadata` | Queue a 3-phase metadata repair job (Activity envelope): Phase 1 OpenAlex batched, Phase 1.5 Semantic Scholar batched (fills `tldr` + `influential_citation_count` + abstract fallback), Phase 2 Crossref per-paper for residual abstract misses. Omit `limit` to process all eligible papers; explicit `limit` accepts up to 100,000 papers per call for bounded probes. |
| `GET` | `/search` | Global search (papers + authors + collections) |
| `GET` | `/backup/export` | Export DB / JSON / BibTeX |
| `GET` | `/bootstrap` | Frontend boot payload |
| `GET` | `/plugins` | Integration manifests: capabilities, activation, generated config schema, and direction status |
| `GET` | `/plugins/{id}/config` | Read validated configuration with masked secrets |
| `PUT` | `/plugins/{id}/config` | Strictly validate and replace integration configuration |
| `PUT` | `/plugins/{id}/enabled` | Activate/deactivate while retaining config |
| `POST` | `/plugins/{id}/test` | Run the manifest's production-transport test through Activity |
| `GET` `POST` | `/fetch[/…]` | Fetch / bulk operation endpoints |

### Browser connector (extension)

Endpoints the [Firefox connector](../user-guide/browser-connector.md)
talks to. `GET /extension/ping` is a pure-read handshake that also
returns an **instance identity** payload — `{profile, db_fingerprint}` —
so the connector can bind an offline-queued capture to a specific
database and deliver it only when the same instance answers (it never
slips into the wrong DB). Captures are ingested through the single
canonical path, `POST /extension/save`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/extension/ping` | Handshake + instance identity (`{profile, db_fingerprint}`); pure read |
| `POST` | `/extension/lookup` | Membership check + metadata preview (read-only) |
| `POST` | `/extension/save` | Save the open paper (add/like/love → 3/4/5 stars) |
| `POST` | `/extension/undo` | Reverse a connector save |

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
