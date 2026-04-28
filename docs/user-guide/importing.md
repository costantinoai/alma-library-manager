---
title: Importing from Zotero / BibTeX
description: Walk an existing Zotero or BibTeX library into ALMa, with automatic OpenAlex enrichment afterwards.
---

# Importing from Zotero / BibTeX

If you already maintain a personal library in Zotero or as `.bib`
files, the Imports tab is the right way to bring it into ALMa.
Imports land **directly in your saved Library** (`status='library'`)
— they're treated as explicit save intents, not as candidates that
need re-saving.

## Import sources

The Import dialog has four tabs:

| Tab | Source |
|---|---|
| **BibTeX** | Upload a `.bib` file or paste BibTeX text. |
| **Zotero** | Connect to the Zotero Web API (Library ID + API key). |
| **Zotero RDF** | Upload a Zotero RDF export file. |
| **Online** | Search OpenAlex live and save results into Library. |

All four flow through the same backend resolver and write the same
provenance (`added_from='import'` for the first three;
`added_from='online_search'` for the fourth).

## BibTeX flow

1. Library → Imports → **Import Papers**.
2. **BibTeX** tab.
3. Drop a `.bib` file or switch to "Paste text" mode.
4. (Optional) Set a **Collection name** — every paper in this
   import will be added to a new collection of that name.
5. Click **Import**.

BibTeX parsing handles the standard `@article`, `@book`,
`@inproceedings`, etc. types. Each entry tries to resolve to an
OpenAlex work via DOI first, then by canonical title.

* **Resolved entries** land in Library with full OpenAlex
  metadata (topics, institutions, citation count).
* **Unresolved entries** still land in Library, with a
  `not_openalex_resolved` status badge in the Imports staging
  panel. Use the **Resolve OpenAlex** action later to retry.

## Zotero (web API) flow

1. In Zotero, generate an API key at
   [zotero.org/settings/keys](https://www.zotero.org/settings/keys).
2. Note your Library ID (visible on your profile URL) and library
   type (`user` or `group`).
3. In ALMa: Import dialog → **Zotero** → enter Library ID, API
   key, and library type.
4. Click **Connect**. ALMa fetches your collection list.
5. Pick a specific collection or "All items".
6. (Optional) Local collection name.
7. Click **Import from Zotero**.

The import runs as an Activity-backed job — for large libraries
(thousands of items), the dialog returns a "queued" envelope
immediately and the import continues in the background. Watch
progress in the Activity panel.

## Zotero RDF flow

If you exported your Zotero library as RDF (Zotero → File → Export
Library → RDF), upload the `.rdf` file in the **Zotero RDF** tab.
Same resolution flow as the API import.

## Online search flow

The **Online** tab is a different beast — it searches OpenAlex live
and lets you save results one at a time:

1. Type a query.
2. Press Enter.
3. (Optional) Expand year filters.
4. For each result, use the standard Save / Like / Love / Dislike
   actions.

Results decorated with `in_library` show their existing state so
you don't accidentally re-save. Already-saved papers show their
rating; papers you've previously dismissed appear with a dismissed
badge.

## After import: resolve & enrich

The Imports tab in Library shows two action buttons:

* **Resolve OpenAlex** — runs the OpenAlex resolver against
  unresolved imports (no DOI, no title match). Activity-backed.
* **Enrich Metadata** — for already-resolved imports, fills in
  topics, institutions, keywords, and citation counts. Activity-backed.

Both are safe to re-run. The unresolved-imports panel below shows
each pending paper with its resolution status badge and reason
(`no_doi_no_title_match`, `ambiguous_match`, etc.) so you can see
why something didn't resolve.

## Promotion of existing tracked papers

If your import contains a paper that ALMa already knows about
(e.g. it was pulled in by a monitor as `tracked`), the import
**promotes** that row to `library` rather than silently skipping
it. Same intent: "I want this in my Library." The promotion
respects the [monotonic rating rule](saving-papers.md#monotonic-upgrade) —
re-importing a Loved paper does not downgrade it.

## Background vs inline

* Default: BibTeX / Zotero / Zotero RDF imports run as
  **background** Activity-envelope jobs. The dialog returns
  immediately with a job id; results land later.
* Override: append `?background=false` to the API call to force
  inline execution. Useful for debugging or for scripts that need
  the result synchronously. Not exposed in the UI.
