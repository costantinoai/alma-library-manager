# ALMa — **A**nother **L**ibrary **Ma**nager

A personal academic literature monitor that runs on your own machine.
ALMa watches [OpenAlex](https://openalex.org/) and Semantic Scholar for
new work from the authors and topics you follow, builds a local SQLite
library of the papers you save, and uses SPECTER2 embeddings to surface
related work you haven't seen yet. Nothing about your reading list
leaves the box you put it on.

> **Early preview.** The three core jobs — Library, Discovery, and Feed
> — work end-to-end. A polished first-run onboarding is in progress.
> Public testing welcome. See the
> [latest release](https://github.com/costantinoai/alma-library-manager/releases/latest)
> for the current version.

**Full documentation:** <https://costantinoai.github.io/alma-library-manager/>

## The five views

- **Feed** — a chronological inbox of new publications from the authors
  and topics you follow.
- **Library** — every paper you've saved, with notes, ratings, tags,
  collections, and a reading list.
- **Authors** — the researchers you track, plus suggested authors whose
  work overlaps with what you read.
- **Discovery** — papers related to your library that haven't shown up
  in the Feed yet, ranked by topical and citation similarity.
- **Insights** — charts and a clustered map of your library: how it's
  spread across years, topics, and journals, and which papers cluster
  together by content.

<p align="center">
  <img src="docs/screenshots/desktop-library.png" alt="Library" width="49%">
  <img src="docs/screenshots/desktop-discovery.png" alt="Discovery" width="49%">
</p>
<p align="center">
  <img src="docs/screenshots/desktop-insights.png" alt="Insights" width="49%">
  <img src="docs/screenshots/desktop-feed.png" alt="Feed" width="49%">
</p>
<p align="center">
  <img src="docs/screenshots/desktop-authors.png" alt="Authors" width="49%">
  <img src="docs/screenshots/desktop-settings.png" alt="Settings" width="49%">
</p>

## Why ALMa

Staying current with the literature is really three jobs: **keeping** the
papers that matter, **watching** for new work from the people and topics
you care about, and **finding** adjacent papers you don't yet know
exist. Those jobs usually live in separate tools — Zotero for the first,
Connected Papers or Semantic Scholar for the third, scattered journal
alerts for the second — and that separation is where most of the friction
comes from.

The jobs also need each other. A recommender ranks better when it knows
which papers you kept and which you dismissed. A watcher is more useful
when it can up-weight authors that keep showing up in your saved work.
ALMa puts all three on one local database so they inform each other —
the library trains discovery, the monitors fill the feed, and discovery
suggests what to monitor next.

Read the full [vision & philosophy](docs/vision.md) for the design
principles and the lifecycle model the whole UI is built around.

## Browser connector (Firefox)

Save the paper open in your browser straight into ALMa — like the Zotero
connector. Grab the signed `.xpi` from the
[latest release](https://github.com/costantinoai/alma-library-manager/releases/latest)
and install it via `about:addons` → **Install Add-on From File…**. Full
guide: [browser connector](docs/user-guide/browser-connector.md).

## Quick start

**One line, any OS.** The installer checks Docker, auto-detects your
hardware (NVIDIA GPU / Raspberry Pi / generic CPU) to pick the right
image, and starts ALMa with named Docker volumes so your library
survives upgrades. **Prerequisite:**
[Docker](https://docs.docker.com/get-docker/) installed and running.

**Linux / macOS**

```bash
curl -sSL https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.sh | bash
```

**Windows (PowerShell)**

```powershell
irm https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.ps1 | iex
```

Then open <http://localhost:8000>. To update later, re-run the same
command — the installer detects an existing install and pulls the latest
image; your data lives in named volumes, so nothing is lost.

> **Other ways to install:** manual `docker run`, the three image
> flavors (`:latest` / `:latest-gpu` / `:latest-lite`), Docker Compose,
> auto-updates with Watchtower, and exposing ALMa on your LAN are all
> covered in the [Docker guide](docs/getting-started/docker.md). For a
> [bare-metal Python install](docs/getting-started/installation.md)
> (development only — not recommended for everyday use), see the
> installation docs.

## First use

ALMa is empty on first launch. Three steps make it useful:

1. **Add your OpenAlex API key.** OpenAlex requires a free key (no
   signup beyond an email; ~30s at
   [openalex.org/settings/api](https://openalex.org/settings/api)).
   Paste it into **Settings → Connections** and save. A
   [Semantic Scholar key](https://www.semanticscholar.org/product/api)
   is recommended too — it keeps Discovery off the shared rate-limit
   pool.
2. **Follow a few authors.** Open **Authors** and add three to five
   researchers by name (an ORCID or OpenAlex ID also works). Each follow
   kicks off a background backfill of their recent papers — watch it run
   under **Activity**.
3. **Wait one refresh.** Once the backfills finish, the **Feed** surfaces
   new papers and **Discovery** recommends related work. Save, like, or
   dismiss as you go — every action teaches the ranker what you care
   about.

Optional: import a BibTeX file or Zotero library from **Library →
Imports** for much better Discovery seed material from day one.

Full walkthrough: [first-run checklist](docs/getting-started/first-run.md).

## Configuration

Most settings — Discovery weights, AI provider, clustering — live in the
database and are tuned from the **Settings** page. The `.env` file holds
secrets (API keys, Slack tokens) and a few deployment knobs. The full
reference is in
[docs/reference/configuration.md](docs/reference/configuration.md), and
[backups](docs/operations/backups.md) covers keeping your library safe.

## License

Licensed under [PolyForm Noncommercial 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/) —
a source-available license. Personal use, academic research, hobby
projects, and use by nonprofits or educational institutions are all
permitted; commercial use is not. Attribution (the `LICENSE` file and
copyright notice) must be preserved in copies and derivative works. See
[LICENSE](LICENSE) for the full text.

## Author

**Andrea Ivan Costantino** · [github.com/costantinoai](https://github.com/costantinoai)
