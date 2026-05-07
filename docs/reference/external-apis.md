---
title: External APIs
description: How ALMa talks to OpenAlex, Semantic Scholar, Crossref, arXiv, bioRxiv, and Google Scholar — with rate-limit and identifier rules.
---

# External APIs

ALMa pulls scholarly data from several public sources. Each has its
own quotas, identifier shapes, and quirks. This page documents what
ALMa fetches, how it batches, and what to expect when a source goes
sideways.

## OpenAlex (primary)

[OpenAlex](https://docs.openalex.org/) is ALMa's primary source for
metadata, citations, topics, institutions, and the works graph.

* **Endpoints used**: `/works`, `/works/{id}`, `/works/{id}/related-works`,
  `/authors`, `/authors/{id}`, `/topics`, `/sources`, `/institutions`.
* **Polite pool**: ALMa adds your `OPENALEX_EMAIL` (or
  `mailto={email}`) to every request. Strongly recommended — it gives
  you 100k requests/day with a higher per-second burst.
* **API key**: Set `OPENALEX_API_KEY` for premium quotas if you have
  one.
* **Field projection**: ALMa always sends a `select=` parameter so we
  only fetch the fields we use. The select list lives in
  `alma/openalex/client.py::_WORKS_SELECT_FIELDS`. Adding a
  downstream consumer of a new field requires updating the select.
* **Rate limits**: ALMa tracks every response's `x-ratelimit-*`
  headers and exposes them at **Settings → External APIs → OpenAlex
  usage**. If you see "no calls yet", you haven't hit the API yet —
  it does not mean we failed to record.

## Semantic Scholar

[Semantic Scholar](https://api.semanticscholar.org/) covers what
OpenAlex doesn't: pre-computed SPECTER2 vectors, paper-level
recommendations, author recommendations, related papers.

* **Endpoints used**:
  * `/paper/batch` for bulk metadata + `specter_v2` vectors.
  * `/paper/{id}/related` and `/paper/{id}/citations`.
  * `/recommendations/v1/papers/forpaper/{id}` for the
    `s2_related` recommendation channel.
  * `/author/{id}/papers` and `/author/{id}/recommendations`.
* **API key**: Set `SEMANTIC_SCHOLAR_API_KEY` for higher quotas.
  Without one, you'll hit the public rate limit (1 RPS, 100/day for
  some endpoints) faster than you'd like for backfills.
* **`/paper/batch` contract**:
  * Results preserve the **request order** by lookup id (DOI / S2
    ID / OpenAlex ID).
  * Compacting the response shifts good papers onto bad IDs and
    corrupts state. ALMa preserves the original index.
  * `null` rows in the middle of a response are real — the lookup
    didn't match.
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
  (`core/http_sources.SourceHttpClient`) engages a 30-second floor
  on the per-request interval for the next 60 seconds. Retries: 5
  attempts, jittered exponential backoff capped at 60 seconds. Fresh
  429s within the cooldown re-arm the floor.
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
  `/paper/search` call each (Jaccard 0.92 + |Δyear|≤1). Per-run
  budget cap: 50 calls (`TITLE_RESCUE_PER_RUN_BUDGET` in
  `services/s2_vectors.py`). The first 429 short-circuits the rest of
  the batch's rescue.
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
  ask for it; honour the request. Polite pool gives 10 RPS for
  singletons / 3 RPS for list queries; public pool is 5 RPS / 1 RPS.
* **Used as a fallback**, not the primary path. Most papers resolve
  through OpenAlex first.

## arXiv and bioRxiv

* **arXiv**: ALMa uses arXiv's metadata API to resolve preprints
  not yet indexed by OpenAlex.
* **bioRxiv** (also covers medRxiv): same pattern — preprint
  metadata fall-through.

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
it in **Settings → AI & embeddings** and store the key in `.env` or the
secret store. See [AI capabilities](../concepts/ai.md).

## Slack

[Slack Web API](https://api.slack.com/web), bot-token based.

* **Token**: `SLACK_TOKEN` (the bot token from your Slack app).
* **Default channel**: `SLACK_CHANNEL`.
* **Per-rule overrides**: `config/slack.config`.

ALMa uses `slack-sdk` and posts via `chat.postMessage`. No webhooks.

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
