# External API Audit — Providers, Call Sites, Costs, and Findings

**Date:** 2026-07-04
**Scope:** Every external API ALMa talks to — OpenAlex, Semantic Scholar (S2),
Crossref, Unpaywall, arXiv, bioRxiv/medRxiv, ORCID, Google Scholar, OpenAI
embeddings, Slack/SMTP — plus the health/maintenance system that drives most
of the traffic. Includes 2026 provider pricing/limits research and a full
call-site inventory with batching verdicts.

---

## 1. Provider landscape (verified July 2026)

### 1.1 OpenAlex — THE ONLY PROVIDER THAT COSTS MONEY

The pricing model changed in **February 2026** (usage-based pricing announced
2026-02-24; polite pool / `mailto` discontinued 2026-02-13). Sources:
[Authentication & Pricing](https://developers.openalex.org/api-reference/authentication),
[pricing blog post](https://blog.openalex.org/openalex-api-new-features-and-usage-based-pricing/).

| Fact | Value |
|---|---|
| API key | **Required for all requests** (free, from openalex.org/settings/api) |
| Daily budget, no key | $0.10 |
| Daily budget, free key | **$1.00**, resets midnight UTC |
| Singleton (`/works/W…`, `/works/doi:…`, `/authors/A…`) | **$0 — unlimited** |
| List + filter (`/works?filter=…`) | $0.0001/call ($0.10 / 1k) |
| Search (`?search=…`) | **$0.001/call ($1.00 / 1k) — 10× list** |
| Content (PDF) download | $0.01/call |
| Rate limit | 100 req/s |
| Filter OR-values | max 100 per filter |
| `per_page` | **max 100** (was 200 pre-2026) |
| Budget headers | `X-RateLimit-Limit/-Remaining/-Credits-Used/-Reset` |
| Paid options | Prepaid credits (any amount, kick in after daily budget, don't expire daily); org plans $5k / $20k / yr (Member / Member+) |
| Bulk alternative | Free quarterly snapshot (~330 GB gz / ~1.6 TB raw); paid plans get daily snapshots |
| Deprecations | `mailto` polite pool gone; old `*.search:` filter syntax deprecated (we don't use it — verified) |

**Key economic inversion:** under the old model, batched list calls were the
optimization target. Under the new model, **known-ID lookups are free as
singletons**, list+filter batches are nearly free, and **searches are the
only expensive class**. 1,000 searches = the entire daily budget.

### 1.2 Semantic Scholar — free, throughput-limited

- Free. API key recommended (`x-api-key`); keyed tier is **1 req/s**
  introductory; unauthenticated traffic shares a global throttled pool.
- `POST /paper/batch`: **max 500 IDs per call**, feature parity with the
  singleton endpoint (fields incl. `embedding.specter_v2`).
- `/paper/search`: limit ≤ 100 per page. Recommendations API
  (`/recommendations/v1/*`) is a separate upstream service.
- No paid tier. Cost dimension: **time, not money** (1 rps serialization).

### 1.3 Crossref — free, polite pool

- Free. Polite pool = send `mailto` (param or User-Agent) → better service.
- `rows` up to 1000; cursor pagination (`cursor=*`, cursors expire in 5 min).
- No fixed published rps; dynamic. Paid "Metadata Plus" = monthly snapshots,
  not needed at our scale.

### 1.4 Unpaywall — free, 100k/day

- Free REST API, **100,000 calls/day**, `email=` param required.
- No batch endpoint (one DOI per call is the API's own shape).

### 1.5 arXiv — free, 1 request / 3 seconds

- Terms of use: "no more than one request every three seconds, single
  connection at a time." Free, metadata CC0.

### 1.6 bioRxiv/medRxiv, ORCID — free

- bioRxiv: no published hard caps; windowed `/details` + `/pubs` endpoints;
  no keyword search endpoint.
- ORCID public API: free, no key needed for public reads.

### 1.7 OpenAI embeddings (opt-in AI provider)

- `text-embedding-3-small`, paid per token (~$0.02 / 1M tokens list price);
  negligible at our volumes. Only active when the user selects the OpenAI
  provider; default is local SPECTER2 (no network).

---

## 2. Our scale (dev DB snapshot, 2026-07-04)

| Metric | Value |
|---|---|
| Papers | 3,921 (18 library / 3,903 tracked) |
| Papers with `openalex_id` | 3,914 |
| Papers without DOI | 738 |
| Papers without abstract | 1,115 |
| Papers with no identity at all | **0** (title-resolution pool empty: 772 resolved, 109 sticky terminal) |
| Papers missing reference edges | 1,600 (= 64 batched calls) |
| OpenAlex metadata pending | 1,326 (= 14 batched calls) |
| Followed authors | 29 |
| Feed monitors | 31 (29 author — batched; 2 query — search-class) |

**Conclusion:** volume is tiny. Cap exhaustion is a **call-class problem**
(search-type calls), not a volume problem.

---

## 3. Client architecture

Three transport layers:

1. **`src/alma/openalex/http.py`** — `OpenAlexClient` singleton. Thread-local
   sessions, UA `ALMa/2.0`, api_key injected per request (env
   `OPENALEX_API_KEY` → secret store), `mailto` correctly dropped
   (post-2026-02-13). Retries {429,500,502,503} ×5, exp backoff cap 60 s,
   honors Retry-After; **fail-fast on 429 with `X-RateLimit-Remaining ≤ 0`**
   (drained-key case). In-process response cache: TTL 300 s, 1024 entries,
   caches 200 and 404. `operation_cache()` context (unbounded + negative
   cache) exists but has exactly one caller. **No global rate
   limiter/semaphore** — concurrency bounded only per-caller (pools of 3–6).
   `get_singleton()` / `get_list()` helpers are dead code.
2. **`src/alma/core/http_sources.py`** — `SourceHttpClient` per provider via
   frozen `SourcePolicy`; one process-wide client per source. UA
   `ALMa/3.0 (<contact email>)`. Per-source min-interval gate + concurrency
   semaphore + retries {429,5xx} + Retry-After (cap 60 s). Policies:

   | Source | min interval | concurrency | retries | auth |
   |---|---|---|---|---|
   | semantic_scholar | 1.05 s (+ adaptive 30 s floor / 60 s cooldown after 429) | 1 | 5 | `x-api-key` if set |
   | crossref | 0.34 s polite / 1.05 s anon | 3 / 1 | 3 | `mailto` param |
   | arxiv | 3.1 s | 1 | 2 | — |
   | biorxiv | 0.35 s | 1 | 2 | — |
   | unpaywall | 0.12 s | 1 | 2 | `email` param (calls skipped if unset) |
   | publisher (landing pages) | 0.5 s | 1 | 1 | — |
   | orcid | 0.04 s | 1 | 2 | — |

3. **`src/alma/core/fetcher.py`** — Google Scholar via `scholarly` (HTML
   scraping). Opt-in, OFF by default (D14). Not routed through shared
   clients; blocking 20/40/60 s retry sleeps; no proxy → block risk.

**Cross-cutting dedup/idempotency:** DB ledgers `paper_enrichment_status`
(per source × purpose, sticky `terminal_no_match`, `unchanged` + 30-day
cooldown, `retryable_error` + 6 h) and `publication_embedding_fetch_status`
(sticky `unmatched` / `missing_vector` / `lookup_error` / `bad_local_doi`)
guard corpus rehydrate, title resolution, S2 vectors, abstract recovery,
author hydration, and reference backfill. **Retry-storm protection is solid**
on all sweep paths. There is no persistent HTTP cache for any provider.

---

## 4. Call-site inventory and batching verdicts

### 4.1 OpenAlex

**Well-batched (keep):**

| Path | Shape | Cost class |
|---|---|---|
| Corpus rehydrate phase 1 (`corpus_rehydrate.py`) | `filter=openalex_id:` pipe, 100 ids/call, bisect on 400/414, ledger-guarded | list |
| Reference-edge backfill (`openalex/client.py:1591`) | `select=id,referenced_works`, 25 ids/call, 4 workers, zero-ref terminal stamp | list |
| Author profile/details batches (`client.py:431/1149`) | 50 ids/call, 4 workers | list |
| Feed bulk author monitors (`client.py:1230`) | `author.id:` pipe 50/call, cursor pages, `from_publication_date` incremental | list |
| DOI batches (`client.py:1416`) | 100 DOIs/call | list |
| Related/citing per seed (`openalex_related.py`) | `filter=related_to:` / `cites:`, 1/seed; S-5 DOI pre-resolve batches the id resolution | list |

**Problems:**

| # | Where | Problem |
|---|---|---|
| O1 | budget gate (`maintenance.py::_provider_daily_cap_block`, `http.py` counters) | treats all calls as equal; search is 10× list, singletons are free. No per-class accounting; credits spend not persisted (in-process counters lost on restart) |
| O2 | `title_resolution.py` | OpenAlex `/works?search` **first** (1 search/paper, budget 500/run, self-rescheduling) with S2 fallback capped at 50. One full sweep ≈ $0.50–0.88. Should be S2-first |
| O3 | `library/enrichment.py:667`, `client.py:2312` | 1 OpenAlex search per DOI-less title in loops (738 no-DOI papers in corpus) |
| O4 | `author_hydrate.py:305` | `batch_get_author_details([oid], batch_size=1)` = one **paid** list call per author; free `/authors/{id}` singleton or batch-50 both better |
| O5 | `feed.py:1922` | single-monitor refresh does a full cursor sweep per author monitor; bulk path already batches 50/call |
| O6 | `client.py:306` | per-page clamped at 200; new max is 100 |
| O7 | scoring paths (`core/resolution.py:834`, `enrichment.py:235,289`, `openalex_related.search_works`) | full 25-field `_WORKS_SELECT_FIELDS` (incl. `abstract_inverted_index`, `referenced_works`, `counts_by_year`) fetched for scoring that reads title/doi/year/authors — payload waste |
| O8 | `client.py:48` | `get_author_name_by_id` fetches full author object, no `select` |
| O9 | `http.py:288/308` | `get_singleton()` / `get_list()` dead code; when the daily budget is drained, known-ID work could continue on free singletons instead of `skipped_daily_cap` |
| O10 | fan-out sites | no global OpenAlex concurrency ceiling (per-caller pools only); fine at 100 req/s but uncoordinated |

### 4.2 Semantic Scholar

**Well-shaped:** vector backfill and rehydrate phase 1.5 batch 250 papers/call
(2 lookup ids each, deliberate robustness), resilient bisection on batch
errors, ledger-guarded, **cross-sweep vector reuse** (metadata batches carry
`embedding.specter_v2`, saving the second `/paper/batch`), self-reschedule
caps (1500/run, depth 50). `search_papers_bulk` = 2 calls/query.
`author_backfill` uses a narrow 3-field projection (model to copy).

**Problems:**

| # | Where | Problem |
|---|---|---|
| S1 | `search_papers` callers (`author_identity.py:149,365`, `engine.py:888`, `resolution.py:992`) | full `FIELDS` incl. 768-dim SPECTER2 vector + tldr + abstract fetched, then discarded — pass slim `fields=` |
| S2 | `identifier_resolution.py:202-219` | `/author/search` with full fields **then** `/author/batch` re-fetching same IDs with same fields — redundant double call; plus up to 3 × `/author/{id}/papers` = up to 5 calls per author lookup |
| S3 | `graph.py:282` | 4-worker fan-out over `fetch_related_papers` (3–4 calls per DOI) but client concurrency = 1 → threads serialize; pool only buys deadline-abandonment |
| S4 | discovery/interactive paths | no cross-run result caching (only `author_network` has a TTL cache); repeated sweeps re-issue identical searches/recommendations |
| S5 | `api/routes/settings.py:676` | raw `requests.get` probe bypasses the shared limiter — an ungated call that can arm the 30 s cooldown for everyone |
| S6 | recommendations vs graph API | both share one limiter/cooldown; a 429 on one starves the other (conservative-safe, but blunt) |
| S7 | dead code | `_fetch_edge_graph`, `fetch_references_for_paper`, `fetch_citations_for_paper`, `fetch_author_papers` have no callers |

### 4.3 Others

| Provider | Verdict | Notes |
|---|---|---|
| Crossref | **Good.** 50 DOIs/call ×3 workers, polite pool honored, ledger-guarded | N single-row `SELECT doi` loops in phase 2 (DB waste, not network); polite pool requires contact email set |
| arXiv | Compliant (3.1 s ≥ ToU 3 s) | free-text lane fires on every multi-source query, often exceeds the 8 s lane deadline and gets dropped — wasted round trip |
| bioRxiv | **Good** — 300 s window cache shares one pull across monitors/lanes | 3 interval buckets can triple pulls; up to 5 `/pubs` reconcile calls per search; unknown-server abstract lookup doubles calls |
| Unpaywall | Fine — per-DOI is the API's shape; skipped when no contact email; ledger-gated | none significant |
| Publisher HTML scrape | **Weak** | arbitrary-URL GET, up to 3 pages/paper, no response size cap, no robots.txt, concurrency 1 → one slow host stalls the whole recovery phase |
| ORCID | Fine (0.04 s gate) | `/person` + `/researcher-urls` often redundant (person payload already carries researcher-urls) |
| Google Scholar | Risky but OFF by default, opt-in gated (D14) | scraping, no proxy, blocking 20/40/60 s sleeps in workers |
| OpenAI embeddings | Good batching (100/req + 0.5 s spacing) | no in-provider 429 retry; some per-term single-vector calls |
| Slack (sdk path) | Fine — resolution cached, 15 papers/msg | — |
| Slack (legacy plugin) | **Wasteful** — enumerates ALL channels + users (cursor loops, limit 1000) on every send, no cache | up to 3 full workspace scans per message |
| SMTP | Fine — stdlib, off-thread, 50 papers/email cap, no-raise | no retry (alert stays un-acked, refires) |

---

## 5. Health & maintenance system assessment

- **Checks** (`services/health.py`): all pure local SQL; zero network on GET;
  `_safe_assess` turns assessor failures into typed error dimensions. ✅
- **Repairs** (`services/maintenance.py`, 15-task registry): idempotent
  scheduling (`find_active_job`), prerequisite DAG
  (`title_resolution → corpus_metadata → s2_vector → embedding → centroids`;
  author lane `author_metadata → dedup_orcid → author_works`), destructive
  ops behind plan-fingerprint confirmation tokens, per-task manual/auto
  limits. ✅
- **Chains** (`embedding_chain.py`): manual runs never auto-chain
  (`trigger_source` check); background chains re-arm via durable KV after
  yields; 15-min hydration drain reconciles ledgers. ✅
- **Idle healer**: opt-in per task, 1 task/tick, ≤50 items/tick, worst-first,
  yields to user activity, honors OpenAlex reserve. ✅
- **Frontend HealthPage**: read-only on mount; only poller is job status at
  1.5 s while a job runs; run-sequence auto-advance is bounded and stops at
  manual gates / no-progress. ✅
- **Uncommitted fixes (working tree, correct — ship them):** 429 fail-fast on
  drained key (`openalex/http.py`); `_provider_daily_cap_block` +
  `_blocking_prerequisites` gates in `maintenance.py`.
- Gaps: budget gate is not cost-class-aware (O1); healer could spend
  ~$1.20/day on a regrown search-class pool (24 ticks × 50 searches) without
  per-class budgets; MV first-build runs inline on GET (accepted design).

---

## 6. Cost analysis

### Steady state (everything enabled), per day

| Job | Calls | $ |
|---|---|---|
| Author refresh (daily, 29 authors, works pagination) | ~100–200 list | ~$0.015 |
| Citation-graph backfill (24 h, limit 500, batch 25) | ≤20 list | $0.002 |
| Feed sweeps ×4 (29 authors batched + 2 query monitors) | ~8 list + 8 search | ~$0.009 |
| Discovery lenses ×4 (per-seed related/citing + topic/S2 searches) | ~150 list + ~10 search | ~$0.025 |
| Hydration drain / rehydrate residuals | ~10 list | $0.001 |
| **Total** | | **≈ $0.05/day** (5% of free budget) |

### Spike scenarios

| Scenario | Calls | $ |
|---|---|---|
| Full corpus re-hydrate (3,914 papers) | 40 list | $0.004 |
| Re-resolve 1,000 DOI-less titles — current (OpenAlex-search-first) | 1,000 search | **$1.00 → drains budget** |
| Same, S2-first with OpenAlex fallback | ~50 search | $0.05 |
| Onboard new followed author (500 works + refs + vectors) | ~30 list (+ S2 free) | $0.003 |
| Bulk BibTeX import, 500 DOI-less rows (title pre-resolve) | 500 search | $0.50 |

### If we paid

- **Prepaid credits** are the only sensible paid option: with current code the
  worst realistic year costs ~$20; after the fixes below, the free tier
  covers everything with 20× headroom. A one-time **$5–10 prepaid balance**
  is adequate spike insurance.
- Member plans ($5k/$20k/yr) and the 330 GB snapshot are for institutions —
  not for a 4k-paper personal corpus.
- Every other provider is free; S2's constraint is throughput (1 req/s), not
  money.

---

## 7. Recommendations (ranked) — status as of 2026-07-04 PM

**P0 — ship what exists**
1. Commit the working-tree fixes: 429 fail-fast on drained key, daily-cap
   gate, prerequisite gates. *(pending commit)*

**P1 — cost-class awareness (biggest resilience win)**
2. **[done]** `http.py::classify_request` — ONE canonical
   singleton/list/search classifier; per-class call counters +
   `estimated_spend_usd` on the client, exposed via
   `openalex_usage_snapshot()` (`calls_by_class`, `estimated_spend_usd`).
3. **[done]** `_provider_daily_cap_block` is cost-class aware: search-heavy
   tasks (`title_resolution`, `corpus_metadata` Phase 0) must leave
   `RESERVED_USER_CALLS + OPENALEX_FALLBACK_PER_RUN_BUDGET` headroom before
   a background run.
4. **[done]** `batch_fetch_works_by_dois` / `batch_fetch_works_by_openalex_ids`
   flip to the FREE singleton path (`client.budget_drained()` →
   `_iter_singleton_works`) when the daily budget is drained — and also for
   tiny inputs (≤2 ids), where a batch call spends a credit for nothing.
   `get_list()` (dead) deleted; `get_singleton()` wired.

**P2 — search-class demand reduction (biggest $ win)**
5. **[done → refined to ADAPTIVE, same day]** Title resolution order is now
   picked per run. **Benchmark on the real corpus (20 resolvable titles,
   live APIs, 2026-07-04):** OpenAlex-first = **5.7 s total (0.29 s/paper)**
   at $0.001/paper; S2-first = **125.8 s (6.29 s/paper)** at ~$0.
   **Why S2 is slow (verified with raw compliant requests, keyed, exact
   1.05 s pacing — we are NOT violating their limit):** the search endpoint
   itself takes 2–4.4 s per successful response, ~25% of fully-compliant
   calls still get transient server-side 429s (no Retry-After header), and
   each 429 costs retry/backoff time. Our adaptive cooldown was softened
   30 s/60 s → 10 s/30 s (re-bench: 5.14 s/paper) — the remaining slowness
   is S2 server latency, not client policy. Policy: **user-triggered runs
   with remaining credits > run size + user reserve → OpenAlex-first**
   (ungated primary, miss advances to free S2); **background / low-budget
   runs → S2-first** with the paid fallback capped at
   `OPENALEX_FALLBACK_PER_RUN_BUDGET` (100/run); budget-exhausted and
   rate-limited outcomes stamp retryable (never terminal). Same engine
   serves the sweep and rehydrate Phase 0.
6. **[done → adaptive]** `core/resolution.py::resolve_paper_openalex_work`
   (backs enrich-all + importer, both interactive): OpenAlex-first while
   `budget_drained(reserve=100)` is False, else free-S2-first — same
   speed/cost trade, decided per call.

**P3 — mechanical fixes**
7. **[done]** `author_hydrate.py` uses new free
   `get_author_details_singleton()` instead of a paid batch-of-1 list call.
8. **[done]** Single-monitor refresh verified one-shot (no loop callers);
   bulk feed sweep now wrapped in `operation_cache("feed_refresh")` for
   in-run dedup + negative caching.
9. **[done]** per-page clamped to 100 centrally in `http.py::get()` (every
   caller fixed at once) + the stale 200 clamp in `client.py` corrected.
10. **[partly done]** `get_author_name_by_id` now selects `id,display_name`.
    Scoring-path lean select NOT applied to `core/resolution.py` — verified
    the winning candidate's full record is persisted downstream, so the
    25-field select is load-bearing there (`_WORKS_SCORING_SELECT` exists in
    `client.py` for future genuinely score-only paths).
11. **[done]** S2 author double-fetch removed (`identifier_resolution.py`
    uses the search rows directly); redundant ORCID `/researcher-urls` call
    dropped (parsed from `/person`).
12. **[done]** Settings S2 probe routed through the shared gated client
    (`max_retries=0`).
13. **[done 2026-07-04]** S2 title-resolution hit now merges the FULL search
    response and stamps the `semantic_scholar/metadata` ledger `enriched`
    (`title_resolution.py::_apply_s2_title_match`) — Phase 1.5 no longer
    re-fetches a paper the title search already returned (B1 twin of the
    OpenAlex-side inline merge).
14. **[done 2026-07-04]** `backfill_all_resolved_authors` pre-batches author
    profiles (50/call via `batch_get_author_profiles`) and passes
    `profile_cache` down — the maintenance `author_works` task no longer
    issues one profile fetch per author (mirrors the deep-refresh route's
    pre-flight).

**P4 — hygiene / robustness**
13. Widen `operation_cache()` use to feed sweeps and discovery runs (in-run
    dedup + negative cache).
14. Publisher scrape: response size cap + per-host politeness.
15. Slack legacy plugin: cache channel/user resolution (or fold into the sdk
    client).
16. OpenAI provider: add 429/5xx retry with backoff.
17. Delete dead code: `get_singleton`/`get_list` (after #4 wires the former),
    S2 edge-graph functions, `fetch_author_papers`.

---

## 7b. Fresh-DB onboarding e2e run — 2026-07-04 night (findings folded back)

A full onboarding was driven live against an isolated fresh backend (owner
ingest 14 works, 3 co-author follows totalling ~550 works, monitor, lens,
complete). Fetch-side verdicts:

- **Wizard cost ≈ 115 credits (≈$0.01)**: 1 author search + singleton
  profile resolves + list-class works backfills. No redundant re-fetch: works
  land complete at upsert (abstract/topics/authorships/pub-date decoded from
  the works projection); re-follow and re-ingest are idempotent (0 new /
  438 total on the second pass).
- **Four convergence defects found by the run, all fixed same night** (detail
  in `tasks/STATUS.md` 2026-07-04 NIGHT): the kick-vs-backfill coalescing
  race (sweep now self-continues); the missing title-resolution hop on the
  onboarding chain; the paper-count vs credit-unit mismatch in the adaptive
  gates (`SEARCH_COST_CREDITS = 10`); and the drained-pool 429 wedge — the
  `remaining <= 0` fail-fast never fired at remaining 1–9, so paid-search
  fetchers ground full backoff ladders for 40+ min (now class-relative via
  `_CLASS_COST_CREDITS`, plus a live-budget pre-check in the fallback stage
  that skips the socket entirely).
- **Steady-state expectation on a drained pool**: S2-first resolves what it
  can free (~3–6 s/paper, 429 tail); paid-fallback misses stamp retryable
  `fallback_budget_exhausted` at zero network cost and clear after the daily
  reset (00:00 UTC). Health reads truthful: `queued` while enqueued,
  `exhausted` floors (abstracts/DOIs upstream doesn't have) count as ok.

## 8. Sources

- OpenAlex: [API introduction](https://developers.openalex.org/api-reference/introduction) ·
  [Authentication & Pricing](https://developers.openalex.org/api-reference/authentication) ·
  [Download overview](https://developers.openalex.org/download/overview) ·
  [Usage-based pricing announcement](https://blog.openalex.org/openalex-api-new-features-and-usage-based-pricing/) ·
  [Membership blog](https://blog.openalex.org/a-new-way-to-support-openalex-become-a-member/)
- Semantic Scholar: [API product page](https://www.semanticscholar.org/product/api) ·
  [API release notes](https://github.com/allenai/s2-folks/blob/main/API_RELEASE_NOTES.md)
- Crossref: [REST API tips](https://www.crossref.org/documentation/retrieve-metadata/rest-api/tips-for-using-the-crossref-rest-api/)
- Unpaywall: [REST API](https://unpaywall.org/products/api)
- arXiv: [API terms of use](https://info.arxiv.org/help/api/tou.html)
