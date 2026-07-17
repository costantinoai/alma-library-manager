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
* **`react-force-graph-2d` / `react-force-graph-3d`** — clustered
  embedding graph (paper map).
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
| `HealthPage.tsx` | `#/health` |
| `AlertsPage.tsx` | `#/alerts` |
| `SettingsPage.tsx` | `#/settings` |

Each page composes feature components from `components/<feature>/`
plus shared primitives from `components/shared/` and
`components/ui/`.

## Primitives

Canonical, app-wide primitives — reach for these before hand-rolling:

* `MetricTile` (`components/shared/MetricTile.tsx`) — the one number
  tile. Bordered + icon-led variants; tones `neutral / success /
  warning / critical / info / accent`. Settings' `StatTile` is a thin
  shim that delegates here.
* `ConceptCallout` (`components/ui/concept-callout.tsx`) — the in-page
  "What is this?" explainer for a complex feature. Once per surface,
  near the top. Never nested.
* `JargonHint` (`components/shared`) — per-term info popover for a
  single word of jargon inside a paragraph or label.
* `Card` / `SubPanel` / `Surface` (`components/ui/`) — the surface
  primitives that drive the elevation ladder (`Card` lifts one level,
  `SubPanel` lifts + recesses). Never hand-write a surface background.

Settings-scoped helpers the rest of the app also re-uses:

* `SettingsCard` — titled card.
* `SettingsSection` — collapsible disclosure.
* `AsyncButton` — debounced + loading-state.
* `ToggleRow` — labelled switch row.
* `OptionCard` — selectable card.
* `SettingsNumberField` — spinner-input.
* `KeyValueRow`, `PackageChip`.

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
* One neutral elevation ladder decides every surface colour
  (`bg-surface-N` / `border-edge-N`) — never a hand-picked
  `bg-white` / `bg-slate-*`. Colour meaning routes through semantic
  tokens (`accent` = the single interactive identity, `primary` = the
  one heavy navy fill, `success / warning / critical / info`,
  `gold` = trim only); `slate-*` is the text ramp, never a surface.
  The `surface-guard` test fails CI on raw surface/semantic classes.
* Tabular numerics where it matters (counts, scores, citations).
* Tooltips and HoverCards over modals when surfacing detail.
* Empty states are explicit ("No suggestions — refresh this
  lens") not generic ("Nothing here yet").

## Tests

Frontend logic and rendering are covered by **Vitest** — `*.test.ts(x)`
files that live beside the code (`src/lib/*.test.ts`,
`src/components/**/*.test.tsx`, guards like
`src/test/surface-guard.test.ts`). Broader behaviour is exercised by
the Python integration suite against the backend.

```bash
cd frontend
npm run test                 # Vitest, single run
npx tsc --noEmit             # type check (strict mode)
npm run build                # full Vite production build
```

All three are fast and catch the overwhelming majority of regressions.
See [Testing](testing.md) for the full picture.
