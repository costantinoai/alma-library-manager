---
title: Testing
description: pytest layout, the in-process probe pattern, and what each kind of test is for.
---

# Testing

## Layout

```
tests/
├── conftest.py                 # Shared fixtures (lib_db, base_db)
├── test_*.py                   # Unit + integration tests, one file per concern
├── e2e/                        # Playwright UI smoke tests
└── fixtures/                   # JSON / SQL fixture files
```

A few hundred test files spanning unit, integration, and end-to-end
tiers.

## Fixtures

The two fixtures most tests use:

* `base_db` — empty schema, freshly migrated. Use for tests that
  build their own state.
* `lib_db` — `base_db` plus a small saved Library, a few authors,
  some recommendations. Use for tests that exercise paths over
  pre-existing state.

Both are scoped per test (fresh DB per test) so you don't have to
worry about state leakage.

## Running tests

```bash
# everything
pytest

# one file
pytest tests/test_library.py

# one test
pytest tests/test_library.py::test_save_and_list

# with coverage
pytest --cov=alma --cov-report=term-missing

# parallel (across cores)
pytest -n auto
```

Some files are heavy (full integration runs against multi-second
fixtures). For day-to-day development, prefer running:

* The test for the function you're changing.
* The integration test that exercises the route you're touching.
* `tests/test_models.py` (Pydantic models — fast and broad).

Skip the full suite until you push.

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

## Frontend type check

```bash
cd frontend
npx tsc --noEmit
```

Strict mode, no errors expected. Run after any change to
`api/client.ts` or any cross-component prop change.

## Frontend build

```bash
cd frontend
npm run build
```

Catches Vite-level errors (missing imports, broken aliases) that
`tsc` doesn't see. Fast.

## E2E (Playwright)

```bash
cd tests/e2e
npx playwright test
```

The e2e tests mock the backend at the `fetch` boundary, drive the
SPA in a real browser, and capture screenshots in
`tests/e2e/_screenshots/`. Useful for catching genuine UI defects
that static review misses.

The CI runs them in headless Chromium. Locally you can `--ui` for
interactive debugging.

## What each tier is for

| Tier | Tool | When to add |
|---|---|---|
| **Unit** | pytest | Pure-function logic. Fast, isolated. |
| **Integration** | pytest + `lib_db` fixture | Route + DB + side effects. Most ALMa tests live here. |
| **Live load** | Activity → Operations UI | Perf budgets, interaction patterns, shape verification — driven against a running instance. |
| **E2E** | Playwright | Visual regressions, complex flows that span multiple surfaces. |
| **Type check** | `npx tsc --noEmit` | After any frontend change. |

## Conventions

* **No mocked DB.** Tests run against a real SQLite (in-memory or
  file-backed via the fixture). Mocking the DB hides
  schema-mismatch bugs that production absolutely will hit.
* **Test the contract, not the implementation.** Assert on the
  HTTP response, not on internal call paths.
* **Test the failure modes too.** Every test for a happy path
  should have a sibling that exercises the obvious failure
  (missing field, wrong status, conflict).
* **Prefer probes over pytest for slow paths.** Some test files
  take minutes to run and obscure the signal. Use a probe instead
  when you can.

## What gets pinned by tests

These invariants live in tests and will fail loudly if you break
them:

* Feedback-learning routes never mutate papers / rating /
  `reading_status`.
* `add_to_library` is monotonic over rating
  (`test_library.py::test_save_does_not_downgrade`).
* `Recommendations.liked` counts only positive actions, distinct
  from `saved` counts (`test_insights*.py`).
* `Author suggestions cited_by_high_signal` filters negative-signal
  authors (`test_authors_d12.py`).
* `S2 batch preserves request order` (`test_s2_vectors.py`).

When you change one of these contracts, change the test in the
same commit.
