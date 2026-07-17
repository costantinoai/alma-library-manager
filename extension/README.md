# ALMa Connector (Firefox)

Save the paper open in your browser straight into ALMa — like the Zotero
connector, for your personal ALMa library.

Click the toolbar button on any paper page and a popup shows what was
detected (title, DOI/arXiv, authors). Pick a **rating** (Add 3★ / Like 4★
/ Love 5★) and a **destination** (Library or Reading list) — these are
selectors; **nothing is saved until you press Save** (Cancel closes). The
connector sends the identifier to your local ALMa, which resolves full
metadata via OpenAlex and saves the paper.

An optional **Collection** picker files the paper into one of your ALMa
collections at save time (or creates a new one by name). The picker only
appears when the connected ALMa exposes `/extension/collections`; options
the paper is already filed in are marked. Undo reverses the filing too —
but only memberships that save created.

If the paper is **already in your Library / Reading list**, the card shows
a clear ribbon with its current rating (plus an **Open** link straight to
the paper in ALMa) and the button becomes **Update**. Right after saving,
an **Undo** button reverses it — on the server that performed the save,
even if you switch the connection pill afterwards.

Small conveniences: the DOI / arXiv badges are **click-to-copy**;
**Enter** triggers Save; the popup **mirrors the ALMa app's theme**
(the light/dark/system toggle at the bottom of ALMa's sidebar — synced
whenever an ALMa tab is open; light by default).

The toolbar icon shows a **green dot** when the current page looks like a
savable paper (URL-level detection — arXiv, DOI-in-URL, `/doi/…`,
preprints, DOI-bearing PDFs), so you can tell before clicking.

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

The connector talks to ALMa's `/api/v1/extension/*` endpoints (ALMa
≥ the build that added them).

## Install (for users)

1. On the ALMa [release](https://github.com/costantinoai/alma-library-manager/releases)
   you're running, download **`alma-connector-<version>.xpi`** (its version
   matches your ALMa version).
2. Open it in Firefox — drag onto `about:addons`, or `about:addons` → ⚙ →
   **Install Add-on From File…**. It's signed by Mozilla, so it installs
   **permanently**.
3. Start ALMa, open a paper page, click the **ALMa** toolbar button.

To update later, download the newer `.xpi` from a later release and install
it the same way. (When you open ALMa it shows a toast **only if the connector
and ALMa no longer speak the same save format** — i.e. one of them needs
updating; an installed, compatible connector stays silent.) That's all most
people need — there's nothing to build.

## Develop it (contributors)

Load the source as a temporary add-on: **`about:debugging`** → **This
Firefox** → **Load Temporary Add-on…** → pick `extension/manifest.json`.
It's removed on restart; use the signed release for a permanent install.

## How releases work (maintainer)

The connector ships **with each ALMa release** at the **same version**,
signed **locally** and attached to the GitHub Release as a plain `.xpi`
(no auto-update — users download the new `.xpi` to update).

One-time setup (kept out of the repo):

- A free **AMO API key** (addons.mozilla.org → Developer Hub → Manage API
  Keys), stored in **`~/.config/alma/amo.env`** (`chmod 600`):
  ```bash
  export AMO_JWT_ISSUER=...
  export AMO_JWT_SECRET=...
  ```
- **`gh`** (GitHub CLI), authenticated once: `gh auth login`.
- Install the pre-push hook once, from the repo root:
  ```bash
  ln -sf ../../extension/hooks/pre-push .git/hooks/pre-push
  ```

Then a release is just the normal ALMa tag push — **bump the version in
`pyproject.toml`, tag `v<version>`, and push the tag**. The hook builds +
signs the connector locally, asks for a `y/N` confirmation, and attaches
`alma-connector-<version>.xpi` to that release. (AMO signs each version
once, so the version must be new — it always matches the ALMa version.)

For a **connector-only** rebuild between ALMa releases (e.g. a fix in the
add-on itself), AMO still needs a *new* version, so sign with a fourth
component off the current ALMa version — it stays anchored to that release
and doesn't imply a product bump:

```bash
extension/release.sh --local --version 0.14.0.1   # connector patch on 0.14.0
```

To run it by hand instead of via the hook:

```bash
extension/release.sh            # build, sign, then (after confirm) upload
extension/release.sh --local    # build + sign only; no upload, no git writes
```

Signing is always local; the **only** write (creating the release/tag,
uploading) is gated behind the confirmation.

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

The same panel shows the **Offline queue**: captures saved while their
target ALMa was down, each bound to the intended instance. You can
**Sync now**, discard individual captures, and see held conflicts
("different DB") with the reason — queued work is never an invisible
background state.

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
  background.js      toolbar green-dot badge (URL-level paper detection per tab)
  content.js         announces the connector to the ALMa web app at startup
                       (stamps version + save-contract on the page for detection)
  lib/extract.js     paper-identification logic (page + PDF) — shared, testable
  lib/settings.js    persisted server list + /ping probing + permissions (shared)
  lib/servers-ui.js  server-manager component used by popup panel + options (shared)
  icons/alma.svg     toolbar / add-on icon
  test/extract.test.js     Node unit tests for extract.js
  test/popup-render.html   offline popup render harness (mocked APIs)
```
