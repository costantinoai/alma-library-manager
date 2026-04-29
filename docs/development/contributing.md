---
title: Contributing
description: Branch model, commit conventions, code style, and how to land a change.
---

# Contributing

ALMa is currently a single-author project, but the conventions
below apply to anyone landing a change — including future-you.

## Branch model

* **`main`** — stable. Never push directly.
* **`dev`** — active work. PRs target `dev`; `dev` merges into
  `main` periodically.
* Feature branches are usually short-lived; rebase onto `dev`
  before merging.

## Commits

Conventional commit prefixes:

* `feat:` — new feature
* `fix:` — bug fix
* `refactor:` — restructure without behaviour change
* `chore:` — tooling, dependencies, build
* `docs:` — documentation only
* `test:` — tests only
* `perf:` — performance improvement

Subject lines are short (≤ 72 chars). Body explains the **why**;
the diff explains the **what**.

Sign your commits as yourself. Don't squash-merge feature branches
that contain meaningful intermediate commits — let them carry
their history.

## Code style

### Python

* Black-formatted. `ruff` lints and catches the obvious.
* Type-hinted on public functions and module boundaries.
* Docstrings on classes and on functions whose intent isn't
  obvious from the signature. Brief, factual.
* Comments explain **why**, not **what**. Identifiers should make
  the **what** obvious.

### TypeScript

* Strict mode, no implicit `any`.
* Function components only; no class components.
* `const` for everything; `let` only when actually reassigning.
* Avoid default exports — named exports stay greppable.
* Imports ordered: external libs → internal aliases → relative.

## DRY without overengineering

Three similar lines is better than a premature abstraction. But
when you've copied a meaningful pattern three times, lift it.

Where the lifted pieces live:

* **Backend helpers**: `src/alma/api/helpers.py`. Don't define
  `raise_internal`, `row_to_paper_response`, `safe_div`,
  `table_exists`, `json_loads`, `normalize_topic_term` locally.
* **Frontend utilities**: `frontend/src/lib/utils.ts`. Don't
  define `formatTimestamp`, `formatNumber`, `truncate`, badge
  class functions locally.
* **Query invalidation**: `frontend/src/lib/queryHelpers.ts`. Use
  `invalidateQueries(qc, ...keys)` instead of repeated
  `qc.invalidateQueries(...)` calls.

## Forward-looking changes

Renames, schema changes, and contract changes are **one-shot
migrations**, not multi-version compatibility shims. If
`/library/likes` becomes `/library/saved`, the old endpoint is
removed in the same change.

The reasoning: ALMa is single-user, single-version. There's no
client out there running an old build that needs the old contract.
Carrying both adds complexity nobody benefits from.

## Testing your change

Before opening a PR:

```bash
# python
pytest tests/test_<the_thing_you_changed>.py
ruff check .
black --check .

# frontend (if you touched it)
cd frontend
npx tsc --noEmit
npm run build
cd ..
```

For changes to the recommender or scheduler, exercise a real
refresh and inspect **Activity → Operations** for per-source timing.
Compare against the previous run on the same lens — unexplained
shifts are the regression signal.

For UI changes, capture screenshots at three viewports
(1440×900, 1024×768, 390×844) and look at them. Static review
misses real defects.

## Pull requests

* Title: `<type>(<scope>): <imperative summary>`. Example:
  `feat(discovery): add S2 snippet retrieval lane`.
* Description: what changed, why, what's tested. Include screenshots
  for UI changes.
* Keep diffs small. If you find unrelated work that needs doing,
  open a separate PR.
* Self-review the diff before requesting review.

## Things to avoid

* **Adding fallbacks for impossible scenarios.** Trust internal
  code and framework guarantees; only validate at system
  boundaries (user input, external APIs).
* **Hidden auto-compute / auto-enrichment hooks.** Heavy work is
  Activity-backed and explicit, not silently triggered on read
  paths.
* **Half-finished implementations.** Either ship it or don't.
* **`window.confirm` / `window.alert`.** Use the AlertDialog
  primitive.
* **Provider-inferred labels.** AI provider names come from the
  backend contract, not from frontend ternaries.
* **Mocking the database in integration tests.** Real SQLite,
  every time.

## Releasing

ALMa doesn't ship versioned releases — it's a self-hosted personal
tool. Updates roll forward via `git pull` + restart.

When a change requires user action on existing installs (a config
key rename, a new env var), document it in the relevant
[Reference](../reference/index.md) page and add a brief note to
the README.
