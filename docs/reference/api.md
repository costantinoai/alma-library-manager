---
title: REST API
description: Every endpoint ALMa exposes, grouped by domain, with live Swagger UI for try-it-yourself.
---

# REST API

ALMa exposes a single REST API rooted at `/api/v1/`. The same FastAPI
app serves the API and the SPA, so in dev you'll hit
`http://localhost:8000/api/v1/...`, and on a private network the
address you opened ALMa at.

## Authentication

By default there is **no authentication** — ALMa is a single-user
local tool.

You can require an API key by setting the `API_KEY` environment
variable. All write routes and most read routes then answer `401`
unless the request includes one of:

```
X-API-Key: <your key>
Authorization: Bearer <your key>
```

This is for API and connector use. The web app sends no key, so
**setting `API_KEY` breaks the browser UI**. It is a shared secret, not
a login, and it does not make ALMa safe to put on the internet. See
[Deployment](../operations/deployment.md#api-key).

## Cross-site requests

With or without an API key, every request must name a known address: a
`Host` other than `localhost`, an IP address or an address listed in
[`ALMA_ALLOWED_ORIGINS`](../operations/deployment.md#browser-origins)
gets `403` with `"error": "UnknownHostRefused"`, on every method.

A page on another website cannot change ALMa's data. `POST`, `PUT`,
`PATCH` and `DELETE` get `403` unless the `Origin` header is ALMa's own
address, a browser extension, or listed. `Origin: null` is refused.

Clients without an `Origin` header, such as curl and scripts, are not
affected by that second check, because it stops browsers and only
browsers set it.

## Base URL

| Environment | Base |
|---|---|
| Local dev | `http://localhost:8000/api/v1` |
| Docker compose | `http://localhost:8000/api/v1` |
| Private network | `https://alma.your-tailnet.ts.net/api/v1` |

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
| `403` | Cross-site request refused: the page's origin may not change data |
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
| `POST` | `/library/import/pdf?filename=` | Import a PDF (body = the file, `application/pdf`): match an existing paper first, else resolve online and save to Library, then keep the file. Activity envelope (`pdf.import:<upload_id>`) |
| `POST` | `/library/import/pdf/uploads/{upload_id}` | Retry an unidentified PDF with `{doi}` or `{title}` (upload kept for a day) |

### Paper PDFs

Every paper can carry one kept PDF. Fetching, attaching and importing are
Activity jobs (the response is the envelope; follow `/activity/{job_id}`);
the PDF's state is the `pdf` block of `GET /papers/{id}/details`
(`stored`, `file_missing`, `url`, `attempts`). Sources come from
`pdf_source` plugins — see [Building an integration](../development/integrations.md).

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/papers/{id}/pdf` | The kept PDF, inline, with `ETag` (its sha256), conditional GETs and `Range` support. 404 when none is kept |
| `POST` | `/papers/{id}/pdf/fetch` | Try every enabled PDF source in order (`pdf.fetch:<paper>`); 409 when no source is switched on |
| `POST` | `/papers/{id}/pdf?filename=` | Attach a PDF (body = the file, `application/pdf`; 413 over 100 MB, 415 if not a PDF) — `pdf.attach:<paper>` |
| `DELETE` | `/papers/{id}/pdf?reject=` | Remove the kept PDF; `reject=true` also remembers the file as wrong for this paper |

### Library management

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/library-mgmt/info` | DB size, paper count, backup list |
| `POST` `DELETE` | `/library-mgmt/backup[/…]` | Create / delete a backup (list is in `/library-mgmt/info`) |
| `POST` | `/library-mgmt/deduplicate` | Run preprint↔journal dedup |
| `POST` | `/library-mgmt/embeddings/reset` | Delete only embedding artifacts (`publication_embeddings`, `author_centroids`, embedding fetch markers) so vectors can be re-fetched/recomputed |
| `POST` | `/library-mgmt/reset` | Wipe DB (dangerous; confirms) |

### Inbox capture

Papers you send yourself from another device. The sweep normally runs on a
timer; these exist so the loop is observable and forceable. See
[Inbox](../concepts/inbox.md).

| Method | Path | Purpose |
|---|---|---|
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
| `POST` | `/discovery/refresh` | Refresh recs (Activity) |
| `GET` | `/discovery/status` | Refresh status |
| `GET` `PUT` `POST` | `/discovery/settings[/…]` | Weight + behaviour config; `GET` also serves `effective_weights` (what the ranker uses after rescaling + mode) and `reference_score` (the all-average paper's score) |
| `GET` | `/discovery/outcome-evaluation` | Stored answer to "does the ranker's order predict what you kept?": `state` (`not_built` / `not_ready` / `ready`), AUC + 95% interval + verdict for kept-vs-rejected and kept-vs-random-corpus, `current` (false when weights / Library / calibration moved since), and `rebuilding`. Pure read; never computes |
| `POST` | `/discovery/outcome-evaluation/refresh[?force=true]` | 202 + `job_id`; queues the background evaluation when its inputs moved (always with `force`). Also requested by a weights save / reset and by the periodic scoring tick |
| `POST` | `/discovery/recommendations/{id}/save` | Save → Library |
| `POST` | `/discovery/recommendations/{id}/read` | Add to Reading list |
| `POST` | `/discovery/recommendations/{id}/like` | Rate positively (`rating=4` like, `rating=5` love); stays visible |
| `POST` | `/discovery/recommendations/{id}/dislike` | Rate 1 + negative signal; stays visible |
| `POST` | `/discovery/recommendations/{id}/dismiss` | Hide this lens suggestion; no preference signal |
| `GET` | `/discovery/recommendations/{id}/explain` | Score breakdown |
| `POST` | `/discovery/similar` | "Find papers like these" |
| `POST` | `/discovery/manual-search` | Cross-source paper search |
| `POST` | `/discovery/manual-search/add` | Save a manual-search result |

### Discovery lenses

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/lenses` | List / create |
| `PUT` `DELETE` | `/lenses/{id}` | Update / delete |
| `POST` | `/lenses/{id}/refresh` | Refresh this lens |
| `GET` | `/lenses/{id}/recommendations` | Cached recs for this lens |
| `GET` | `/lenses/{id}/branches` | Branch map preview |

### Authors

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/authors[/…]` | List / add |
| `DELETE` | `/authors/{id}` | Remove |
| `GET` | `/authors/{id}/detail` | Light-weight popup data |
| `GET` | `/authors/{id}/publications` | Author's papers in our corpus |
| `GET` | `/authors/{id}/openalex-works` | Page through OpenAlex bibliography |
| `POST` | `/authors/{id}/deep-refresh` | Full re-fetch |
| `POST` | `/authors/deep-refresh-all` | Bulk deep refresh; `scope=needs_metadata` targets identity/profile gaps for Settings maintenance |
| `GET` | `/authors/{id}/affiliations` | Read affiliation evidence and the current display-affiliation decision |
| `POST` | `/authors/{id}/history-backfill` | Historical corpus backfill |
| `POST` | `/authors/{id}/repair` | Repair identifiers |
| `GET` | `/authors/{id}/id-candidates` | Identifier candidates |
| `POST` | `/authors/resolve-identifiers` | Resolve OpenAlex / Scholar IDs |
| `POST` | `/authors/{id}/confirm-identifiers` | Manually confirm IDs |
| `POST` | `/authors/{id}/search-scholar` | Manual Google Scholar search |
| `POST` | `/authors/follow-from-paper` | Follow author seen on a paper card |
| `GET` | `/authors/needs-attention` | Authors needing manual triage |
| `POST` | `/authors/{id}/merge-profiles` | Merge duplicate author rows into this canonical author. Body requires `alt_author_ids`; optional `field_choices` maps each alt author id to per-field winners (`primary` / `alt`) for metadata discrepancies. |
| `POST` | `/authors/{id}/not-same-person` | Permanently record that authors sharing a name are different people |
| `DELETE` | `/authors/{id}/accept-unidentified` | Undo "can't identify": the identity gap is open again |
| `POST` | `/authors/{id}/discover-aliases` | Read-only ORCID lookup for additional OpenAlex profiles that may represent the same person |
| `POST` | `/authors/conflicts/{id}/resolve` | Resolve a hard-identifier conflict recorded during merge |
| `GET` | `/authors/lookup` | Look up by display name |
| `GET` | `/authors/suggestions` | Multi-source author suggestions |
| `POST` | `/authors/suggestions/reject` | Reject a suggestion (optional `suggestion_bucket` for bucket-quality calibration) |
| `POST` | `/authors/suggestions/track-follow` | Log a rail-originated follow with `suggestion_bucket` for bucket-quality calibration |

### Feedback learning

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/feedback/track` | Record passive interaction events |
| `POST` | `/feedback/reset` | Reset learned feedback state |

### Insights & Reports

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/insights` | Overview (charts + summary). Served from a fingerprint-keyed cache; response carries `stale` / `rebuilding` / `computed_at` flags. |
| `GET` | `/insights/diagnostics` | Composed payload — assembles all seven diagnostic sections from cache. Backwards-compatible with pre-split clients. |
| `GET` | `/insights/diagnostics/sections/{section}` | One of the seven diagnostics sections (`feed`, `discovery`, `ai`, `authors`, `feedback`, `operational`, `evaluation`). Each section is a fingerprint-keyed materialised view; response carries `stale` / `rebuilding` / `computed_at`. The frontend uses these to stream cards in independently with per-card skeletons. |
| `GET` | `/reports/weekly-brief` | Weekly research brief |
| `GET` | `/reports/collection-intelligence` | Collection-level report |
| `GET` | `/reports/topic-drift` | Topic drift report |
| `GET` | `/reports/signal-impact` | Ranking signal impact report |

### Add-ons

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/addons` | Every add-on in this build, on or off |
| `GET` | `/addons/{id}` | One add-on and what Remove would delete |
| `PUT` | `/addons/{id}/enabled` | Switch on or off. Keeps data; lists jobs still finishing |
| `POST` | `/addons/{id}/remove` | Delete its data for good. Needs `{"confirm": "<id>"}` and the add-on off |

### Alerts

An add-on: every path here answers 404 while it is off.

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/alerts` | List / create |
| `PUT` `DELETE` | `/alerts/{id}` | Update / delete |
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

### Onboarding and walkthroughs

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/onboarding/walkthroughs` | State of the wizard (`welcome`), every page tour and any feature walkthrough: `pending`, `outcome`, `recorded_version`, `current_version`, `at`, `source`. The UI shows an overlay only from this. Pure read. |
| `POST` | `/onboarding/walkthroughs/{id}/outcome` | Record `{"outcome": "done"\|"skipped"}`. `404` for an unknown id, `409` for `welcome`, which closes through `/onboarding/complete`. |
| `POST` | `/onboarding/walkthroughs/{id}/replay` | Ask for one walkthrough again. It stays pending until closed. |
| `POST` | `/onboarding/walkthroughs/import` | One-way import of a browser's old page-tour flags, `{"ids": [...]}`. Returns `imported` and `ignored`. Never overrides a server record the user wrote. |
| `POST` | `/onboarding/complete` | Close the wizard, optional `{"outcome": "done"\|"skipped"}` (default `done`), and start the convergence chain. |
| `POST` | `/onboarding/reset` | Settings → Restart onboarding: mark the wizard and every walkthrough as requested. |
| `GET` | `/onboarding/status` | First-run state and counts. `completed` is true when `welcome` is not pending. |

### Other

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/papers/{id}/details` | Paper detail |
| `POST` | `/papers/{id}/action` | **THE paper-action route.** Every surface — Feed, Discovery, Inbox, Library, onboarding — applies `add`/`save`, `like`, `love`, `dislike`, `dismiss`, `defer`, `read`, `undo` here, so "what does Like mean" has one answer. `surface` records where the user acted. `surface=feed\|discovery` additionally require `scope_ref` (the feed item / recommendation id): they settle THAT row, so a dismiss in one lens never mutes the paper in another. `undo` takes `undo_aspect`: `membership`, `rating`, `reading`, `all`, `last` (reverse the newest action, what every toast's Undo sends) or `inbox` (undo of `defer`, no valence); with `surface=feed\|discovery` it reverses that row's Dismiss. Response carries the shared `status`/`rating` plus the adapter's own echo under `surface_result`. |
| `GET` | `/papers/stats` | Top topics / journals / institutions |
| `GET` | `/papers/{id}/prior-works` | Papers this paper cites |
| `GET` | `/papers/{id}/derivative-works` | Papers that cite this one |
| `GET` | `/search` | Global search (papers + authors + collections) |
| `GET` | `/backup/export` | Export DB / JSON / BibTeX |
| `GET` | `/bootstrap` | Frontend boot payload |
| `GET` | `/plugins` | Integration manifests: capabilities, activation, generated config schema, and direction status |
| `GET` | `/plugins/{id}/config` | Read validated configuration with masked secrets |
| `PUT` | `/plugins/{id}/config` | Strictly validate and replace integration configuration |
| `PUT` | `/plugins/{id}/enabled` | Activate/deactivate while retaining config |
| `POST` | `/plugins/{id}/test` | Run the manifest's production-transport test through Activity |

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
