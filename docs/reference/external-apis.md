---
title: External APIs
description: How ALMa talks to OpenAlex, Semantic Scholar, Crossref, arXiv, bioRxiv, and Google Scholar — with rate-limit and identifier rules.
---

# External APIs

ALMa pulls scholarly data from several public sources. Each has its
own quotas, identifier shapes, and quirks. This page documents what
ALMa fetches, how it batches, and what to expect when a source goes
sideways.

## Global network control

The same application-wide **External network access** switch is available in
**Health → System status** and **Settings → Connections → External APIs**. It
controls outbound scholarly APIs, Slack, email, Google Scholar, and hosted AI.
Turning it off leaves local Library, maps, and local search usable, shows a
persistent warning, and stops requests at the shared transport before any
socket is opened. `ALMA_DISABLE_NETWORK=1` is the operations hard override: it
can force access off but cannot force a user's stored off choice on.

Find & Add remains graceful when OpenAlex's paid search pool is drained:
paper search omits only the OpenAlex lane and continues Semantic Scholar,
Crossref, arXiv, bioRxiv, and Europe PMC; author search falls back to matching
local OpenAlex-linked identities. Import review and Health repair cards use the
same credit forecast and disable work that the known remaining pool cannot
cover.

## OpenAlex (primary)

[OpenAlex](https://docs.openalex.org/) is ALMa's primary source for
metadata, citations, topics, institutions, and the works graph.

* **Endpoints used**: `/works`, `/works/{id}`, `/authors`,
  `/authors/{id}`. Topics, sources, and institutions arrive **embedded**
  via `select=` on `/works` and `/authors` — they are not separate calls.
  Related works come from `/works/{id}` with `select=related_works`,
  then a bulk-by-ID fetch of those IDs.
* **No semantic `/find/works`**: the natural-language `/find/works`
  semantic call was removed — it 404s and is a separate paid product
  (~1000 credits/call). Hybrid search is now purely the lexical
  `/works?search=` path.
* **API key — REQUIRED (since 2026-02-13)**: every request needs
  `OPENALEX_API_KEY`. OpenAlex retired the email "polite pool"; without a
  key you get 100 free credits/day and then **HTTP 409**. A free key
  (openalex.org/settings/api) gives standard limits — 100,000 credits/day
  (singleton GETs cost 0 credits, list requests 1 each), at a typical
  ~10 req/s. Set it in `.env` or via
  **Settings → Connections → OpenAlex**.
* **Contact email (optional)**: `OPENALEX_EMAIL` no longer affects rate
  limits (the polite pool is gone) but still sets a courteous User-Agent
  and feeds the Crossref polite pool.
* **Field projection**: ALMa always sends a `select=` parameter so we
  only fetch the fields we use. The select list lives in
  `alma/openalex/client.py::_WORKS_SELECT_FIELDS`. Adding a
  downstream consumer of a new field requires updating the select.
* **Pricing is per request *class*, and singletons are FREE.** This is the
  fact that decides when to batch:

  | operation | cost / 1 000 calls |
  |---|---|
  | Get singleton (one entity by ID or DOI) | **free** |
  | List + filter | $0.10 |
  | Search (full-text) | $1.00 |
  | Content download | $10.00 |

  A free key is **$1.00/day** — unlimited singletons, ~10 000 list+filter,
  ~1 000 search. Source of truth in-repo:
  `alma/openalex/http.py::_CLASS_COSTS_USD` (+ `cost_class()`), which mirrors
  <https://developers.openalex.org/api-reference/authentication>. Read those
  constants rather than copying prices, which drift.
* **Batch on latency, not by rule.** Because singleton GETs cost nothing,
  mechanically replacing them with pipe-filter calls makes things *more*
  expensive, not less. The trade only pays on a hot path: 100 singletons is
  ~10 s of round trips at $0, whereas one pipe-filter is ~0.3 s at $0.0001
  (0.01% of the daily budget). So ALMa batches the **interactive per-candidate
  author-enrichment loop** in `openalex/client.py` and deliberately leaves the
  one-off singleton lookups alone.
* **Bulk-by-ID batching**: the corpus rehydrator fetches metadata for
  many papers in one call via `filter=openalex_id:W1|W2|…` — up to
  **100 IDs per filter** (the documented OR ceiling) with `per-page=100`,
  splitting on HTTP 400/414 if a URL ever runs long. Worth 1 list credit
  instead of N free singletons because the backlog is latency-bound. The
  client uses **thread-local sessions** so the rehydration pipeline can fan
  these out concurrently.
* **Rate limits**: ALMa tracks every response's `x-ratelimit-*`
  headers and exposes them at **Settings → External APIs → OpenAlex
  usage**. If you see "no calls yet", you haven't hit the API yet —
  it does not mean we failed to record.

## Semantic Scholar

[Semantic Scholar](https://api.semanticscholar.org/) covers what
OpenAlex doesn't: pre-computed SPECTER2 vectors, paper-level
recommendations, and the reference/citation graph.

* **Endpoints used**:
  * `/paper/batch` for bulk metadata + `specter_v2` vectors.
  * `/paper/search/bulk` for the recent Feed/monitor ingestion lane;
    `/paper/search` for interactive/general Discovery.
  * `/paper/search/match` for title→paper identity (the corpus
    title-resolution sweep and author-identity triangulation).
  * `/paper/{id}/references` and `/paper/{id}/citations` for the
    graph lane.
  * `/recommendations/v1/papers/forpaper/{id}` (single seed, all-time) and
    `POST /recommendations/v1/papers` (positive + negative seeds, new work
    only) for the `s2_related` recommendation channel — a different host path
    than the rest of the graph API.
  * `/author/batch`, `/author/search`, and `/author/{id}/papers` for
    author identity resolution.

#### Documented limits (from the live `graph/v1/swagger.json`)

| endpoint | limits |
|---|---|
| `POST /paper/batch` | **500 lookup ids**, **10 MB response**, 9 999 citations |
| `POST /author/batch` | **1 000 author ids**, 10 MB |
| `GET /paper/search` | `limit` ≤ 100, **≤ 1 000 relevance-ranked results total**, 10 MB |
| `GET /paper/search/bulk` | **1 000 rows/call**, **no `limit`/`offset` parameter**, `token` pagination, 10 M total, no nested data |
| `GET /paper/search/match` | one closest-title row; **404** on a miss |
| `GET /paper/{id}/citations` · `/references` | `limit` ≤ **1 000** |
| `GET /author/{id}/papers` · `/author/search` | `limit` ≤ **1 000** |

`/paper/{paper_id}` and its sub-resources accept `DOI:<doi>`,
`CorpusId:<id>`, `ARXIV:<id>`, `PMID:`, `PMCID:` — never pre-resolve an
identifier to a `paperId` with an extra request.

* **Field projection is per-endpoint, not global** (`ENDPOINT_FIELDS` +
  `project_fields()` in `discovery/semantic_scholar.py`). S2 does **not**
  accept the same `fields` everywhere, and an unsupported field is a hard
  **HTTP 400**, not a silent drop. Verified live 2026-07-27:

  | endpoint | `tldr` + `embedding.specter_v2` |
  |---|---|
  | `POST /paper/batch` | accepted |
  | `GET /paper/search` · `/paper/search/match` | accepted |
  | `GET /paper/search/bulk` | **rejected (400)** |
  | `POST /recommendations/v1/papers` | **rejected (400)** |
  | `GET /recommendations/v1/papers/forpaper/{id}` | **rejected (400)** |

  The registry is **additive** — an endpoint declares what it allows, and an
  unregistered endpoint raises rather than inheriting the global list. The
  previous subtractive patch (one `_BULK_UNSUPPORTED_FIELDS` constant) failed
  open: `POST /recommendations` inherited the two rejected fields and returned
  HTTP 400 on **every** call for the life of the lane, hidden behind
  `if resp.status_code != 200: return []`. Non-429 4xx now logs at WARNING.

  **The trap that makes a registry necessary rather than a probe**: the field
  error only fires when the query actually matches something. `GET /forpaper`
  with the full field set returns a clean `200 {"recommendedPapers": []}` on
  `from=recent` (empty for older seeds) and a hard **400** on `from=all-cs`.
  An endpoint can look field-compatible right up until it starts returning
  results.
* **Bulk search is two-step (bulk → batch)**: bulk cannot return `tldr` or the
  SPECTER2 vector, so ALMa requests the bulk-supported subset for breadth,
  reranks locally, then issues a single `/paper/batch` to hydrate only the
  slice it keeps.
* **Bulk has no relevance ordering.** Its `sort` accepts only `paperId`,
  `publicationDate` and `citationCount`, and **defaults to `paperId:asc`** — a
  sha hash. Left at the default this lane returned the 1 000 lowest-paperId
  matches out of millions, deterministically, so a keyword monitor surfaced the
  same papers forever. ALMa now sends `sort=publicationDate:desc` (the pool
  means "newest matching work") and reconstructs relevance locally with
  `core.scoring_math.query_match_score`. Note the spec's caveat: records with an
  undefined sort value sort **last**, so date-less papers fall outside the
  1 000-row window — accepted for this recent-ingestion lane only.
* **`year` is pushed server-side** (`year=2020-`). It used to be filtered in
  Python after downloading all 1 000 rows.
* **API key — strongly recommended**: set `SEMANTIC_SCHOLAR_API_KEY`
  (free at semanticscholar.org/product/api), in `.env` or via
  **Settings → Connections → Semantic Scholar**. Without one you share
  the **anonymous worldwide pool** (~5,000 requests / 5 min across *all*
  anonymous clients), so 429s are frequent — they're the root cause of
  the multi-minute Discovery graph-lane stalls. With a key you get a
  dedicated ~1 RPS.
* **`/paper/batch` has TWO independent budgets**, and conflating them is a bug:
  * **ID budget** — 500 *lookup ids* per call. One paper contributes up to two
    (S2 ID + DOI), so a "500 papers" setting emits **1 000 ids**.
  * **Payload budget** — **10 MB per response**. Measured 2026-07-27 with the
    full field set: **18 899 bytes/row**, dominated by the 768-d SPECTER2
    vector. 500 vector-bearing rows ≈ 9.4 MB, i.e. 94% of the hard cap.
  * `plan_paper_batch()` sizes each request from the *requested field
    projection* (a narrow projection legitimately earns a bigger batch), keeps
    a 75% safety margin, and recursively splits on a size refusal. The batch
    knob surfaced in Settings reads that same plan via `services/eta.py`, so
    the estimate and the run can no longer disagree.
  * Results preserve the **request order** by lookup id (DOI / S2
    ID / OpenAlex ID).
  * Compacting the response shifts good papers onto bad IDs and
    corrupts state. ALMa preserves the original index.
  * `null` rows in the middle of a response are real — the lookup
    didn't match.
* **One batched-vector primitive**: `fetch_vectors_for_identifiers()` returns
  an `IdentifierFetchOutcome` (rows keyed by *requested* id, plus disjoint
  `terminal_ids` / `retryable_ids`). `fetch_papers_batch` and
  `services/s2_vectors` are both thin callers — a second resilient-split
  implementation is exactly what `CLAUDE.md` → "fix the primitive" forbids.
* **Recommendation pools**: `GET /forpaper/{id}` defaults to `from=recent`,
  which is **empty for older seeds** (measured: 0/20 results on a 2017 seed vs
  20/20 with `from=all-cs`). ALMa always sends `from=all-cs` — that lane exists
  for foundational/older related work. The POST endpoint has **no `from`
  parameter at all** and only returns 2025–26 work, so it is a *frontier*
  source, not a general related-papers source.
* **Author hydration batches.** `/author/batch` takes 1 000 ids; the sweep
  gathers a whole phase's S2 ids into one request instead of one request per
  author (at 1 RPS that was one second per author).
* **Failure classification**:
  * Retryable failures (`429`, `5xx`, network) **stay retryable** —
    they don't become terminal "no match".
  * Only validation failures (4xx other than 429) split down to
    singleton lookups and mark only those papers as
    `lookup_error`.
  * `search_papers(raise_on_rate_limit=True)` surfaces a 429 to its
    caller as `SemanticScholarBatchError(status_code=429)` so the
    title-search rescue can defer instead of stamping
    `terminal_no_match`. The legacy default (silent empty list) is
    preserved for non-critical callers like interactive search.
* **Adaptive throttle**: any 429 observed by the shared HTTP client
  (`core/http_sources.SourceHttpClient`) engages a 10-second floor
  on the per-request interval for the next 30 seconds. Retries: 5
  attempts, jittered exponential backoff capped at 60 seconds. Fresh
  429s within the cooldown re-arm the floor. While the cooldown is
  armed, Discovery and Feed **drop the S2 source for the rest of that
  refresh pass** (via `is_in_adaptive_cooldown()`) instead of having
  each lane queue behind the floor and wait out its deadline; the
  window self-clears after 30s (`discovery/source_search.py`).
* **Terminal statuses** for vector fetch: `unmatched`,
  `missing_vector`, `lookup_error`, `bad_local_doi`.
  `bad_local_doi` is set before any HTTP call when the local DOI
  fails the registry-shape regex `^10\.\d{4,9}/.+`; it never reaches
  S2. Terminally-missed papers stay eligible for explicit local
  SPECTER2 compute. The trigger
  `papers_clear_fetch_status_on_id_change` (see
  `api/deps.py:init_db_schema`) drops these terminal rows whenever
  `papers.doi` or `papers.semantic_scholar_id` actually changes — so
  a paper hydration step that finds a better DOI re-enters the
  fetch pool automatically.
* **Title-search rescue**: papers that miss `/paper/batch` get one
  **`/paper/search/match`** call each — the purpose-built closest-title
  endpoint, which returns one row instead of N ranked ones and still carries
  `abstract` + the SPECTER2 vector. This lives in
  `services/title_resolution.py` (not `s2_vectors.py`, which delegates
  to it), capped at `TITLE_RESOLUTION_PER_RUN_BUDGET` = 500 papers per
  run. The first 429 short-circuits the rest of the batch's rescue.

  Three contract details that make it **not** a drop-in for `/paper/search`:
  the envelope is `{"data": [row]}`; a miss is **HTTP 404**
  (`Title match not found`), which is a normal outcome, not an error; and
  `matchScore` is **unbounded** (131.8 on an exact title) and cannot be
  requested in `fields`. Acceptance therefore stays with ALMa's local
  Jaccard 0.92 / |Δyear| ≤ 1 contract — never `matchScore`.

  `match_paper_by_title()` returns the **raw** S2 row so `externalIds` and
  `authors[].authorId` survive. `application/author_identity` needs both, and
  ALMa's normalized candidate shape drops them: it flattens `authors` to a
  comma-joined string, so the previous `search_papers`-based code raised
  `AttributeError` on `a.get("authorId")` and never extracted a preprint id.
* **Identity resolution (OpenAlex-first)**: the *Resolve missing
  identity* sweep (`services/title_resolution.py`) handles title-only
  papers with no usable identifier. It tries OpenAlex `/works?search`
  first (the larger, cheaper, higher-throughput pool) and only falls
  back to S2 `/paper/search` (1 RPS) when OpenAlex misses — so an
  OpenAlex-cold corpus can't blow the S2 quota. An accepted OpenAlex
  match fills the **full work from that same search response** (no
  second fetch), while the S2 fallback captures the SPECTER2 vector it
  carries. Same Jaccard 0.92 / |Δyear|≤1 contract as the rescue above.
* **DOI hygiene** (`core.utils.canonical_lookup_doi`): DOIs sent to
  S2 are lowercased, URL-decoded, and stripped of trailing publisher
  fragments (`/pdf`, `/full`, `/abstract`, `/epdf`, `/meta`). The
  match-side bookkeeping uses the same canonical form so case-only
  differences round-trip cleanly.

## Crossref

[Crossref](https://api.crossref.org/) is the DOI authority and a
metadata fallback when OpenAlex doesn't have a paper.

* **Endpoints used**:
  * `/works/{doi}` — singleton DOI lookup; used by per-paper
    `_hydrate_via_crossref` and other one-off paths.
  * `/works?filter=doi:DOI1,doi:DOI2,...&rows=50` — batched DOI
    lookup via `discovery.crossref.fetch_works_by_dois`. Phase 2 of
    the bulk corpus rehydrator uses this to resolve up to 50 DOIs
    per HTTP call (~50× round-trip reduction at full backlog vs the
    singleton path).
* **Polite pool**: Set `CROSSREF_MAILTO` to identify yourself. They
  ask for it; honour the request. Crossref retuned its REST limits on
  2025-12-01: the list/search path — what ALMa hits via `/works?query` —
  is now the stricter ceiling at **3 req/s polite / 1 req/s anonymous**.
  Single-record `/works/{doi}` is looser (10 req/s polite), but one
  client serves both paths, so ALMa paces to the search ceiling:
  **0.34s interval polite / 1.05s anonymous**, with concurrency
  **3 polite / 1 anonymous** (`core/http_sources.py`).
* **Used as a fallback**, not the primary path. Most papers resolve
  through OpenAlex first.

## Europe PMC

[Europe PMC](https://europepmc.org/RestfulWebService) indexes PubMed/MEDLINE,
PMC, preprint servers and Agricola — the life-sciences counterpart to S2's
CS-leaning corpus. Free, no key, abstracts inline.

* **Endpoint used**: `GET /search` with `format=json` and
  **`resultType=core`**. `core` is the only tier that returns `abstractText`;
  `lite` is cheaper but every candidate would arrive unscoreable.
* **Filters are pushed server-side** into the query string —
  `FIRST_PDATE:[YYYY-01-01 TO 3000-12-31]` for a year floor and
  `OPEN_ACCESS:y` for OA-only.
* **Response shape gotcha**: the journal title is nested at
  `journalInfo.journal.title`, *not* a top-level `journalTitle`.
  `authorString` is already display-ready.
* **Not a metadata authority.** OpenAlex and Crossref remain the
  identifier/metadata sources of truth; this adapter contributes discovery
  candidates plus the DOI / PMID / PMCID needed to reconcile them.
* Paced at ~5 req/s, 2 concurrent (`core/http_sources.py`). EBI publishes no
  hard per-second figure and asks only that clients be reasonable.

## arXiv and bioRxiv

* **arXiv**: ALMa uses arXiv's metadata API to resolve preprints
  not yet indexed by OpenAlex.
* **bioRxiv** (also covers medRxiv): same fall-through pattern, but
  bioRxiv has **no keyword-search endpoint** (date-range / DOI /
  category only). ALMa pulls a shared recent date window, caches it
  per `(server, interval)` for 300s so every keyword monitor/lane in
  one refresh shares a single network pull, then filters and re-ranks
  locally per query (`discovery/biorxiv.py`).
* **Abstract recovery** (task 05): both servers' **structured APIs**
  also back-fill a *missing* abstract on a paywalled paper that has an
  arXiv/bioRxiv preprint twin — arXiv's Atom `summary` (by id, or a
  title-field `ti:"…"` search) and bioRxiv's
  `/details/{server}/{doi}` `abstract`. This runs ahead of any
  landing-page HTML scrape (cleaner, paywall-free), is fill-only, and
  stamps a per-source ledger reason
  (`services/preprint_abstract.py`). bioRxiv is direct-DOI/URL only —
  it has no keyword endpoint, so the title-search fallback is
  arXiv-specific.

These are read-only fall-through paths. Each has its own DOI
prefix that triggers the
[preprint↔journal dedup engine](../concepts/authors.md#preprint-journal-twin-engine):

| DOI prefix | Source |
|---|---|
| `10.48550/arXiv.*` | arXiv |
| `10.1101/*` | bioRxiv / medRxiv |
| `10.31234/*` | psyArxiv (OSF) |
| `10.31219/*` | OSF |
| `10.26434/chemrxiv*` | chemRxiv |
| `10.20944/preprints*` | MDPI Preprints |

## Google Scholar (`scholarly`)

The `scholarly` package is **opt-in** and used only for author
identity resolution as a tiebreaker — never for primary metadata
fetches.

* **When it's used**: when OpenAlex / S2 don't disambiguate an
  author and you've added them by name.
* **When it's not used**: anything that can be answered by OpenAlex.
  You can leave `scholarly` uninstalled and most flows still work.
* **Stability warning**: Scholar has no public API. The library
  scrapes; expect occasional rate-limiting / breakage when Google
  changes the page. Set `SCHOLAR_RETRY_DELAYS` to tune backoff.

## OpenAI

OpenAI is optional and currently used as an embedding provider. Configure
it in **Settings → Intelligence → AI provider** and store the key in
`.env` or the secret store. See [AI capabilities](../concepts/ai.md).

## Slack

[Slack Web API](https://api.slack.com/web), bot-token based.

* **Token**: `SLACK_TOKEN`, or the secret store (`slack.bot_token`).
* **Outbound channel**: `SLACK_CHANNEL`, or `slack_channel` in settings.
* **Inbound (capture) channel**: `slack_inbox_channel`, Settings only.

ALMa uses `slack-sdk` through ONE client, `alma.slack.client.SlackNotifier`:
alerts post with `chat.postMessage` (Block Kit), capture reads with
`conversations.history` and acknowledges with `reactions.add`. No webhooks, and
no second client — see [External integrations](../concepts/channels.md).

## Identifier reference

Common ID shapes you'll see across the API:

| Shape | Source | Example |
|---|---|---|
| OpenAlex Work ID | OpenAlex | `W2123456789` |
| OpenAlex Author ID | OpenAlex | `A2123456789` |
| Semantic Scholar paper ID | S2 | `649def34f8be52c8b66281af98ae884c09aef38b` |
| Semantic Scholar corpus ID | S2 | `12345678` |
| DOI | Crossref / authority | `10.1038/nature12373` |
| ORCID | ORCID | `0000-0002-1825-0097` |
| arXiv ID | arXiv | `2401.12345` |

All five are stored on the relevant rows when known. Identity
resolution (authors and papers) tries to fill in as many as
possible.
