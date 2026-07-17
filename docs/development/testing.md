---
title: Testing
description: pytest layout, the cassette-isolation pattern, and what each kind of test is for.
---

# Testing

Everything runs from the single repo `.venv` — the one interpreter that
carries the full stack (FastAPI + the `[ai]` extras). There is no
"AI tests skip here" environment; every test runs the real stack.

## Layout

```
tests/
├── conftest.py                 # Shared fixtures (db_path, db, client) + autouse isolation
├── _cassette.py                # HTTP replay layer
├── _helpers.py                 # Test helpers
├── test_*.py                   # Unit + integration tests, one file per concern
├── cassettes/                  # Recorded external-HTTP responses (replayed offline)
└── fixtures/                   # JSON / SQL fixture files
```

Around 100 backend test files spanning unit and integration tiers.
Frontend tests live next to the code under `frontend/src/` as
`*.test.ts(x)` files (Vitest).

## Fixtures

The three fixtures most backend tests use (all in `conftest.py`,
scoped per test — fresh DB each time, no state leakage):

* `db_path` — a throwaway SQLite path exported via `DB_PATH`, with
  auth disabled. Use when you only need the path.
* `db` — a real, schema-initialised `sqlite3.Connection` (Row
  factory). Use for tests that build their own state directly.
* `client` — a FastAPI `TestClient` against a fresh schema-initialised
  DB. Use for route + DB integration tests.

Two autouse fixtures run for every test:

* **HTTP isolation** — every external source (OpenAlex, Semantic
  Scholar, Crossref, arXiv, bioRxiv, Unpaywall, ORCID) funnels through
  `requests.Session.request`, which is patched to replay recorded
  cassettes and block live network. Opt a test into real outbound HTTP
  with the `@pytest.mark.network` marker. `time.sleep` is no-op'd off
  the network so rate-limit/retry logic runs without the wall-clock
  waits.
* **Scheduler reset** — clears the scheduler's in-process job dicts
  between tests so a queued job can't leak into the next test.

## Running tests

```bash
# everything
.venv/bin/python -m pytest

# one file
.venv/bin/python -m pytest tests/test_author_suggestions_scoring.py

# one test
.venv/bin/python -m pytest tests/test_author_suggestions_scoring.py::test_cited_by_high_signal_weights_lead_over_consortium_middle

# with coverage
.venv/bin/python -m pytest --cov=alma --cov-report=term-missing
```

`addopts = -v --strict-markers` and `testpaths = tests` are set in
`pyproject.toml`, so bare invocations already target the suite.

Some files are heavy (full integration runs against multi-second
fixtures). For day-to-day development, prefer running the test for the
function you're changing plus the integration test that exercises the
route you're touching; skip the full suite until you push.

## Real-load behaviour

For perf and behaviour-under-load that pytest doesn't capture well,
exercise the path against a running instance and read **Activity →
Operations**. The per-source timing sub-panel breaks down each
external retrieval lane and is the single best signal for refresh
regressions.

Use unit tests for pure-function logic and the integration suite for
route + DB interactions; reach for live load only when you need real
timings, multi-step interaction (e.g. refresh during read), or
end-to-end shape verification.

## Frontend tests

```bash
cd frontend
npm run test        # Vitest, single run
npm run test:watch  # Vitest, watch mode
```

Vitest drives the unit/component tests that live beside the code
(`src/lib/*.test.ts`, `src/components/**/*.test.tsx`, plus guards like
`src/test/surface-guard.test.ts`). No backend is involved — pure logic
and rendering.

### Frontend type check

```bash
cd frontend
npx tsc --noEmit
```

Strict mode, no errors expected. Run after any change to
`api/client.ts` or any cross-component prop change.

### Frontend build

```bash
cd frontend
npm run build
```

Catches Vite-level errors (missing imports, broken aliases) that
`tsc` doesn't see. Fast.

## What each tier is for

| Tier | Tool | When to add |
|---|---|---|
| **Unit** | pytest / Vitest | Pure-function logic. Fast, isolated. |
| **Integration** | pytest + `client`/`db` fixture | Route + DB + side effects. Most ALMa tests live here. |
| **Live load** | Activity → Operations UI | Perf budgets, interaction patterns, shape verification — driven against a running instance. |
| **Type check** | `npx tsc --noEmit` | After any frontend change. |

## Conventions

* **No mocked DB.** Tests run against a real SQLite (file-backed via
  the fixture). Mocking the DB hides schema-mismatch bugs that
  production absolutely will hit.
* **No live network.** External HTTP replays from cassettes; block is
  the default and `@pytest.mark.network` is the deliberate opt-out.
* **Test the contract, not the implementation.** Assert on the
  HTTP response, not on internal call paths.
* **Test the failure modes too.** Every test for a happy path
  should have a sibling that exercises the obvious failure
  (missing field, wrong status, conflict).

## What gets pinned by tests

These invariants live in tests and will fail loudly if you break
them:

* **No new raw `conn.commit()`** outside the infra allowlist — the AST
  guard `tests/test_no_raw_commit_guard.py`.
* **Write gating** for foreground Activity rows —
  `tests/test_activity_write_gating.py`,
  `tests/test_db_write_guardrails.py`.
* **`Author suggestions cited_by_high_signal`** filters negative-signal
  authors (`tests/test_author_suggestions_scoring.py`).
* **No raw surface / semantic colour** in components — the frontend
  guard `frontend/src/test/surface-guard.test.ts`.

When you change one of these contracts, change the test in the
same commit.
