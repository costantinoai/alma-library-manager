---
title: Browser connector
description: Install the Firefox connector and save the paper open in your browser straight into ALMa — like the Zotero connector.
---

# Browser connector (Firefox)

The ALMa connector is a small Firefox extension that saves the paper
open in your browser straight into ALMa — the same idea as the Zotero
connector. Click the toolbar button on any paper page and a popup shows
what was detected (title, DOI / arXiv, authors); choose a destination
and a rating, and the paper lands in your Library.

It talks **only** to your own running ALMa (by default a local install at
`http://localhost:8000`). Nothing is sent anywhere else.

## What you need

* **ALMa running and reachable.** A normal local or Docker install
  listens on `http://localhost:8000`. If you use the dev server it
  listens on `http://localhost:8001` — you'll set that in the
  connector's Settings (below).
* **Firefox 115 or newer.**
* **The signed connector `.xpi`** from your ALMa release (see Install
  below) — nothing to build or clone.

## Install

The connector ships **with each ALMa release** as a signed add-on — there's
nothing to build.

1. Make sure **ALMa is running** (open `http://localhost:8000` — or your
   address — and confirm the app loads).
2. On the [ALMa release](https://github.com/costantinoai/alma-library-manager/releases)
   you're running, download **`alma-connector-<version>.xpi`** (its version
   matches your ALMa version).
3. Open it in Firefox — drag it onto `about:addons`, or `about:addons` →
   ⚙ → **Install Add-on From File…**. It's signed by Mozilla, so it
   installs **permanently**, survives restarts, and **auto-updates** with
   future ALMa releases.

??? note "Running from source? (contributors)"
    To load the unpackaged extension during development: **`about:debugging`**
    → **This Firefox** → **Load Temporary Add-on…** → pick
    `extension/manifest.json`. It's removed when Firefox restarts.

1. Make sure **ALMa is running** (open `http://localhost:8000` — or your
   address — and confirm the app loads).
2. In Firefox, type **`about:debugging`** in the address bar and press
   ++enter++.
3. In the left sidebar, click **This Firefox**.
4. Click **Load Temporary Add-on…**.
5. Browse to the `extension/` folder from the repo and select the
   **`manifest.json`** file, then click **Open**.
6. The **ALMa** icon (an open book) appears in your toolbar. If you don't
   see it, click the **puzzle-piece** / extensions icon in the toolbar
   and **pin** ALMa Connector so it's always visible.

!!! tip "Pin it for one-click saving"
    Pinning the icon to the toolbar turns saving into a single click on
    any paper page.

That's it — open a paper page and click the **ALMa** button.

!!! tip "Green dot = paper here"
    The toolbar icon shows a small **green dot** when the current page
    looks like a savable paper (arXiv, a DOI in the URL, a publisher
    `/doi/…` page, a preprint, a DOI-bearing PDF), so you can tell at a
    glance before clicking. It's a URL-only hint — a page that hides its
    DOI only in `<meta>` tags won't light up, but clicking still detects
    it.

## First use

1. Open a paper page — a publisher article (Nature, Science, ACM, IEEE,
   Springer, Wiley, PLOS, …), an **arXiv** abstract, or a **PDF**.
2. Click the **ALMa** toolbar button. The popup reads the page and shows
   the detected paper, with a small "Detected via…" line so you can see
   how it was identified.
3. Choose a **Rating** — **Add** (3★) · **Like** (4★) · **Love** (5★) —
   and a **destination**:
      * **Library** — saved, untriaged.
      * **Reading list** — saved **and** added to your reading list
        (reading status `reading`).

    These are selectors — **nothing is saved yet**. (The ratings are the
    same [save verbs](saving-papers.md) used everywhere else in ALMa.)
4. Click **Save to ALMa** to commit, or **Cancel** to close without doing
   anything.

The connector sends the DOI (preferred) to ALMa, which resolves full
canonical metadata via OpenAlex and saves the paper with
`added_from='browser_extension'`. Re-saving never downgrades a rating
(the [monotonic rule](saving-papers.md#monotonic-upgrade) applies here
too).

!!! tip "Already in your library?"
    If the paper is already saved, the card shows a calm **"In your
    Library ✓"** (or **"On your Reading list"**) ribbon with its current
    rating, the rating/destination are pre-selected to match, and the
    button reads **Update** instead of Save.

!!! note "Undo"
    Right after a save, the popup shows an **Undo** button that reverses
    it — removing a just-created paper from your Library (back to a
    tracked row), or restoring the previous rating/placement of a paper
    that was already there. It also clears the feedback signal the save
    recorded, so an undone save doesn't nudge your recommendations.

!!! note "Why DOI matters"
    The connector's job is to find the **identifier**, not to scrape a
    perfect record. When a page has no readable metadata — a **PDF**, say —
    but a DOI is in the URL or the file, the popup **resolves the title
    from ALMa and shows it right there** (briefly "Resolving title…", then
    the real title) so you see exactly what you're saving. When there's no
    DOI at all, it falls back to the title/authors/year it could scrape.

## Choosing the ALMa server

The connection pill at the top-right of the popup shows which instance a
save will go to (e.g. **`:8000`** or **`:8001`**) with a live status dot.
**Click it** to open a dropdown of servers and pick one — no typing.

The connector automatically probes the standard local ports
(**`:8000`** Docker, **`:8001`** dev) and offers any that are running,
even if you never configured them (tagged **DETECTED**). If your saved
default is down but another instance is up, the popup connects to the
running one for you.

### Servers panel (gear → Servers)

The gear opens an in-popup **Servers** panel (no separate tab) to manage
instances:

* **Status** — a sage dot for online, brick for offline; **Recheck**
  re-probes.
* **Make active** — click a row; the active one is marked **Active**.
* **Add detected** — a running instance found on a standard port shows
  **Add** to save it to your list.
* **Add a server** — for a custom port, a LAN address, or a tunnel: enter
  the URL and an optional **API key** (only needed if your ALMa runs with
  `API_KEY` — sent as the `X-API-Key` header). A non-local address prompts
  Firefox once for permission to reach that host.
* **Remove** — the × on a saved server.

Everything is stored locally in your browser. The same panel is available
full-window via `about:addons` → **ALMa Connector** → **Preferences**.

## Running more than one ALMa (Docker + dev)

It's common to have two instances up at once — a Docker/production ALMa
on `:8000` and the dev server on `:8001`. They have **separate, isolated
databases**, so it matters which one you save into.

The connector targets **one** instance at a time (it does **not** fan out
to both, nor pick based on the tab you're viewing). But you rarely have to
think about it:

* Both standard ports are **auto-detected** and appear in the server
  dropdown with live status.
* If your saved default is offline, the popup **auto-connects to the
  running instance** for that session (without changing your saved
  default).
* The pill always shows the **active target** (e.g. `:8001`), so you can
  see at a glance which database a save lands in — switch with one click.
* A save only ever writes to one database. If a paper "doesn't show up",
  check the pill — you were probably pointed at the other instance.

## How identification works

The connector finds a paper the way Zotero's translators do, in priority
order:

1. **Embedded citation metadata** — Highwire (`citation_*`), Dublin Core
   (`dc.*`), PRISM (`prism.*`), bepress, EPrints, Open Graph. This covers
   most publisher landing pages.
2. **Identifier in the page URL** — `doi.org/10.x`, `/doi/10.x`,
   `arxiv.org/abs/…` and `…/pdf/…`, and preprint paths like
   bioRxiv/medRxiv `…/content/10.x/…v1.full.pdf` (the version/format
   suffix is stripped). This works even on the PDF itself, where there's
   no embedded metadata to read.
3. **DOI in `doi.org` links or visible page text.**
4. **PDFs** — arXiv and publisher PDF URLs resolve from the URL; for
   other PDFs the connector scans the PDF for a DOI in its XMP metadata
   or text.

## How releases work (maintainer)

The connector ships **with each ALMa release** — no separate version or tag.
When a `v<version>` tag is pushed, the **Release browser connector** workflow
(`.github/workflows/release-connector.yml`) reads the version from
`pyproject.toml` (so the connector version always equals the ALMa version),
signs the add-on as an *unlisted* add-on via Mozilla (automated, no review),
attaches **`alma-connector-<version>.xpi`** to that GitHub Release, and
refreshes `extension/updates.json` on `main` so installed copies
**auto-update**. One-time setup (AMO API secrets) and the local-signing
command are documented in `extension/README.md`.

## Troubleshooting

??? failure "The connection pill is red / says \"offline\""
    The active server isn't reachable.

    * **Click the pill** — a running instance on a standard port appears
      in the dropdown (tagged DETECTED); pick it.
    * Confirm ALMa is running and that opening its address in a normal tab
      loads the app (the dev server is on `:8001`, Docker on `:8000`).
    * For a custom port / LAN address, add it in the **Servers** panel
      (gear → Servers); if it uses an `API_KEY`, set the key there.

??? failure "\"No paper detected here\""
    The page has no citation metadata, DOI, or recognizable identifier.

    * Open the paper's **article page**, not a search-results or table-
      of-contents listing.
    * Some pages expose metadata only on the abstract/landing page, not
      on the full-text or print view.

??? failure "Nothing happens on a local PDF (`file://`)"
    Firefox blocks add-ons from reading local files unless you allow it.
    Open `about:addons` → **ALMa Connector** → **Permissions** and enable
    **Access your data for all websites / files**. PDFs served over
    `http(s)` work without this.

??? failure "The button is greyed out on `about:`, `view-source:`, add-on pages"
    These are privileged Firefox pages the connector can't read by
    design. Navigate to a real paper page and try again.

## Limitations

* PDF text inside **compressed** streams isn't scanned — only XMP
  metadata and uncompressed text. (Bundling `pdf.js` would cover the
  rest; it's a planned enhancement.)
* The connector is verified on **Firefox**. The code uses MV3 and the
  `browser`/`chrome` shim, so a Chrome/Edge port is a small step but
  isn't officially supported yet.

## Related

* [Saving papers](saving-papers.md) — the save / like / love vocabulary.
* [Reading workflow](reading-workflow.md) — what "Reading list" means.
* [Importing from Zotero / BibTeX](importing.md) — bulk import instead of
  one-at-a-time.
