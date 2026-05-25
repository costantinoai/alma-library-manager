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
* **A copy of the `extension/` folder** from the ALMa repository (it
  ships in the source tree, not in the Docker image):

    ```bash
    git clone https://github.com/costantinoai/alma-library-manager.git
    # the connector lives in:  alma-library-manager/extension/
    ```

    (Or download the repo as a ZIP from GitHub and unzip it — you only
    need the `extension/` directory.)

## Install (temporary add-on)

This is the quickest way and needs no signing. The trade-off: Firefox
removes temporary add-ons when it restarts, so you redo these steps
after a restart. For a permanent install, see [below](#install-permanent).

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

## First use

1. Open a paper page — a publisher article (Nature, Science, ACM, IEEE,
   Springer, Wiley, PLOS, …), an **arXiv** abstract, or a **PDF**.
2. Click the **ALMa** toolbar button. The popup reads the page and shows
   the detected paper, with a small "Detected via…" line so you can see
   how it was identified.
3. Pick a destination:
      * **Library** — saved, untriaged.
      * **Reading list** — saved **and** added to your reading list
        (reading status `reading`).
4. Pick a rating: **Save** (3★) · **Like** (4★) · **Love** (5★). These
   are the same [save verbs](saving-papers.md) used everywhere else in
   ALMa.

The connector sends the DOI (preferred) to ALMa, which resolves full
canonical metadata via OpenAlex and saves the paper with
`added_from='browser_extension'`. Re-saving never downgrades a rating
(the [monotonic rule](saving-papers.md#monotonic-upgrade) applies here
too).

!!! note "Why DOI matters"
    The connector's job is to find the **identifier**, not to scrape a
    perfect record. With a DOI, ALMa fetches authoritative metadata
    itself. When a page has no DOI (some preprints, working papers), the
    connector falls back to the title/authors/year it could read.

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
   `arxiv.org/abs/…` and `…/pdf/…`.
3. **DOI in `doi.org` links or visible page text.**
4. **PDFs** — arXiv and publisher PDF URLs resolve from the URL; for
   other PDFs the connector scans the PDF for a DOI in its XMP metadata
   or text.

## Install (permanent) {#install-permanent}

To avoid reinstalling after each Firefox restart, build a packaged
add-on with Mozilla's [`web-ext`](https://github.com/mozilla/web-ext):

```bash
cd alma-library-manager/extension
npx web-ext build          # produces a .zip under web-ext-artifacts/
```

Then either install the zip in a Firefox build that allows unsigned
add-ons (Developer Edition / Nightly with
`xpinstall.signatures.required = false`), or
[self-distribute a signed build](https://extensionworkshop.com/documentation/publish/signing-and-distribution-overview/)
via `web-ext sign` with an AMO API key.

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
