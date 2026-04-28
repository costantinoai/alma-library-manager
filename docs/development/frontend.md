---
title: Frontend
description: React 19 + Vite + TypeScript + Tailwind 4 + shadcn primitives. The component conventions that keep the UI honest.
---

# Frontend

## Stack

* **React 19** — including the new `use()` hook, suspense
  boundaries, and improved error boundaries.
* **Vite 6** — dev server + prod build.
* **TypeScript 5** — strict mode.
* **TailwindCSS 4** — `@theme` driven; `alma-*` and `gold-*` token
  ramps.
* **shadcn/ui** components — installed individually rather than via
  `shadcn init`. Pulled into `components/ui/`.
* **Radix UI** primitives under shadcn.
* **TanStack Query 5** — server state.
* **`@tanstack/react-table`** — DataTable primitive.
* **`@dnd-kit/sortable`** — drag-reorder for table headers.
* **`recharts`** — Insights charts.
* **`reaviz`** — clustered embedding graph.
* **`lucide-react`** — icons.

## Routing

Hash-routed via `lib/hashRoute.ts`. No React Router. The SPA's URL
shape is `#/feed`, `#/library?tab=saved`, `#/discovery?lens=…`.

This was chosen for two reasons:

1. The backend's catch-all route serves `index.html` for any
   non-API path. Hash routing keeps the server contract trivial.
2. Deep-linking from the CommandPalette uses the same shape as the
   sidebar — both go through `navigateTo(...)` from
   `lib/hashRoute.ts`.

## Pages

One file per top-level surface, in `frontend/src/pages/`:

| File | Path |
|---|---|
| `FeedPage.tsx` | `#/feed` |
| `DiscoveryPage.tsx` | `#/discovery` |
| `AuthorsPage.tsx` | `#/authors` |
| `LibraryPage.tsx` | `#/library` |
| `InsightsPage.tsx` | `#/insights` |
| `AlertsPage.tsx` | `#/alerts` |
| `SettingsPage.tsx` | `#/settings` |

Each page composes feature components from `components/<feature>/`
plus shared primitives from `components/shared/` and
`components/ui/`.

## Primitives

The Settings page introduced a small palette of primitives that
the rest of the app re-uses:

* `SettingsCard` — titled card.
* `SettingsSection` — collapsible disclosure.
* `AsyncButton` — debounced + loading-state.
* `ToggleRow` — labelled switch row.
* `OptionCard` — selectable card.
* `SettingsNumberField` — spinner-input.
* `StatTile`, `KeyValueRow`, `PackageChip`.

For paper rows: `PaperCard` (compact / default / detailed variants),
`PaperActionBar` (the rating verbs), `StatusBadge` (the only badge
path).

## DataTable

`components/ui/data-table/DataTable.tsx` is the shared table
primitive. Built on `@tanstack/react-table` + `@dnd-kit`. Used by:

* Library Saved compact view.
* Settings → Corpus Explorer modal.
* Insights Reports tab.
* Authors followed-list table.
* Feed compact view.

Features: column visibility toggle, drag-reorder, resize, sort,
optional row selection, persistence to `localStorage` per
`storageKey`.

## State

Server state — TanStack Query. Each page declares its queries with
keys like `['library-saved']`, `['library-workflow-summary']`,
`['feed-inbox']`. Mutations invalidate the matching keys via
`invalidateQueries(qc, ...keys)` from `lib/queryHelpers.ts`.

Local state — `useState`, `useReducer`. Forms use
`react-hook-form` + `zod` schemas (Settings, Authors resolve dialog,
Alerts).

## Toasts and dialogs

* **Toasts** — `useToast()` / `errorToast()` from `hooks/useToast`.
  Sonner-backed. Use for success / failure feedback after a
  mutation.
* **Dialogs** — `Dialog` from `components/ui/dialog` (Radix-backed).
  Use for forms / large modals (Import dialog, paper detail panel).
* **Confirms** — `AlertDialog` for destructive actions
  (`window.confirm` is forbidden by convention).

## Avoid

* `window.confirm` / `window.alert` — use `AlertDialog`.
* Per-surface badge implementations — use `StatusBadge`.
* Per-surface paper row markup — use `PaperCard`.
* Inline `useState` forms for anything stateful — use
  `react-hook-form`.
* Redundant query helpers — use `invalidateQueries(qc, ...)` from
  `lib/queryHelpers`.

## Design language

Distinctive over generic. ALMa is a research tool, not a SaaS
landing page. Rules of thumb:

* No glassy gradients, no marketing-style hero sections.
* Restrained colour — `slate` / `alma-*` / `gold-*` ramps.
* Tabular numerics where it matters (counts, scores, citations).
* Tooltips and HoverCards over modals when surfacing detail.
* Empty states are explicit ("No suggestions — refresh this
  lens") not generic ("Nothing here yet").

## Tests

Frontend tests are sparse — most behaviour is exercised by Python
integration tests against the backend. The two checks worth running:

```bash
cd frontend
npx tsc --noEmit             # type check
npm run build                # full Vite production build
```

Both are fast (under a minute on a typical machine) and catch the
overwhelming majority of regressions.

For UI smoke tests, the project uses Playwright in
`tests/e2e/`. They mock the backend, drive the SPA, and capture
screenshots per viewport.
