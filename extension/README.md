# ALMa Connector (Firefox)

Save the paper open in your browser straight into ALMa — like the Zotero
connector, for your personal ALMa library.

Click the toolbar button on any paper page and a popup shows what was
detected (title, DOI/arXiv, authors). Choose a destination
(**Library** or **Reading list**) and a rating (**Save** 3★ / **Like** 4★
/ **Love** 5★). The connector sends the identifier to your local ALMa,
which resolves full metadata via OpenAlex and saves the paper.

## How paper identification works

Mirrors how Zotero finds a paper, in priority order:

1. **Embedded citation metadata** — Highwire (`citation_*`), Dublin Core
   (`dc.*`), PRISM (`prism.*`), bepress, EPrints, Open Graph. Covers most
   publisher landing pages (Nature, Science, ACM, IEEE, Springer, Wiley,
   PLOS, …).
2. **Identifier in the page URL** — `doi.org/10.x`, `/doi/10.x`,
   `arxiv.org/abs/…` and `…/pdf/…`.
3. **DOI in `doi.org` links or visible page text**.
4. **PDFs** — arXiv/publisher PDF URLs resolve from the URL; for other
   PDFs the connector scans the PDF bytes for a DOI in XMP metadata or
   text.

The DOI is the high-value output: ALMa resolves it to canonical metadata,
so a perfect scrape isn't needed — just the identifier. Scraped
title/authors/year are the fallback for DOI-less pages.

## Install (temporary, for development)

The connector talks to ALMa's `/api/v1/extension/*` endpoints (ALMa
≥ the build that added them).

1. Start ALMa so its API is reachable (default `http://localhost:8000`;
   the dev server uses `:8001` — set that in the connector's Settings).
2. In Firefox open **`about:debugging`** → **This Firefox** →
   **Load Temporary Add-on…** and pick `extension/manifest.json`.
3. Open a paper page and click the **ALMa** toolbar button.

Temporary add-ons are removed when Firefox restarts. To package a
permanent build: `cd extension && web-ext build` (requires
[`web-ext`](https://github.com/mozilla/web-ext)), then load/sign the zip.

## Choosing a server

The connection pill (top-right of the popup) shows the active instance
with a live status dot. **Click it** to pick a server — no typing.

The connector auto-probes the standard local ports (`:8000` Docker,
`:8001` dev) and offers any that are running (tagged **DETECTED**). If
the saved default is offline but another instance is up, the popup
connects to the running one for that session.

The gear opens an in-popup **Servers** panel (also at `about:addons` →
Preferences) to make a server active, **Add** a detected one, **Add a
server** by URL (with an optional `API_KEY` sent as `X-API-Key`; non-local
hosts prompt for permission once), **Remove**, or **Recheck**. Stored
locally in your browser.

## Tests

- **Identification logic** (no browser needed):
  `node test/extract.test.js`
- **Offline UI render** (needs a browser engine): open
  `test/popup-render.html` in any browser to see the popup with mocked
  data and exercise the states — no extension APIs required.
- **Backend endpoint**: `tests/test_extension_save.py` in the repo root
  (run with the project's test env).

## Known limitations / future work

- **PDF text in compressed streams** isn't scanned — only XMP metadata
  and uncompressed text. Bundling `pdf.js` would cover the rest (future).
- **Local `file://` PDFs** need Firefox's "Access your data for files"
  toggle for the add-on; http(s) PDFs work out of the box.
- **Chrome/Edge**: the code uses the `browser`/`chrome` shim and MV3, so
  it should port with minor manifest tweaks, but it's only verified on
  Firefox.

## Files

```
extension/
  manifest.json      MV3 manifest (action popup, activeTab + scripting + storage)
  popup.html/.css/.js  the toolbar popup (detect → choose → save + Servers panel)
  options.html/.css/.js  full-window server manager (about:addons → Preferences)
  lib/extract.js     paper-identification logic (page + PDF) — shared, testable
  lib/settings.js    persisted server list + /ping probing + permissions (shared)
  lib/servers-ui.js  server-manager component used by popup panel + options (shared)
  icons/alma.svg     toolbar / add-on icon
  test/extract.test.js     Node unit tests for extract.js
  test/popup-render.html   offline popup render harness (mocked APIs)
```
