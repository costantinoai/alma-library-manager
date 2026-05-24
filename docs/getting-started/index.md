---
title: Getting started
description: Start ALMa with Docker, then complete the first-run checklist in the app.
---

# Getting started

**For normal use, run ALMa with Docker.** The published image pulls
from GitHub Container Registry in seconds and includes the FastAPI
backend, the built React frontend, the SPECTER2 encoder, and every
native dependency already pinned and tested. You provide a port, an
OpenAlex email, and a place to store your library.

Bare-metal Python is **not recommended** unless you're actively
developing on ALMa itself — the AI stack (torch, transformers, hdbscan,
umap-learn) has heavy native dependencies that are easy to mismatch in
unmanaged Python environments. Almost every "ALMa won't start" report
on the bare-metal path is a dependency build mismatch.

<div class="grid cards" markdown>

-   :material-docker:{ .lg .middle } **Docker (recommended)**

    ---

    One-line installer for Linux, macOS, and Windows — auto-detects
    your hardware (GPU / CPU / Pi), pulls the prebuilt image from
    GHCR, and starts ALMa with named volumes that survive upgrades.
    Manual `docker run` and Docker Compose paths are documented too.

    [:octicons-arrow-right-24: Docker](docker.md)

-   :material-language-python:{ .lg .middle } **Bare metal (advanced — not recommended)**

    ---

    Python virtualenv / conda / uv plus a Vite frontend build. Use
    this **only** when you're developing ALMa or are comfortable
    managing a heavy native Python AI stack by hand. Use Docker
    otherwise.

    [:octicons-arrow-right-24: Installation](installation.md)

</div>

After the app is running, do the [first-run pass](first-run.md): set
OpenAlex email, follow authors, import papers if you have them, and
refresh Feed / Discovery.

## Docker requirements

| Component | Minimum | Comfortable |
|---|---|---|
| **Docker** | Docker Engine + Compose plugin | Recent Docker Desktop / Engine |
| **Disk** | 1 GB free | 5 GB+ if you use embeddings |
| **RAM** | 2 GB | 4 GB+ for the normal image |

Docker users do **not** need local Python, Node, a virtualenv, or
`npm`. Those are already inside the image.

## Bare-metal requirements

Only follow these if you use [Installation](installation.md) instead
of Docker:

| Component | Minimum | Comfortable |
|---|---|---|
| **Python** | 3.10 | 3.11+ |
| **Node** | 20 | 22 |
| **Disk** | 1 GB free | 5 GB+ if you enable embeddings |
| **RAM** | 2 GB | 4 GB+ if you run local SPECTER2 |

ALMa uses one SQLite file (`data/scholar.db`) — no separate database
server, no Redis, no message broker.

## What you'll need before installing

* An email address for [OpenAlex's polite pool](https://docs.openalex.org/how-to-use-the-api/api-overview#the-polite-pool).
  Strongly recommended; un-throttled access in exchange for being
  identified.
* (Optional) A [Semantic Scholar API key](https://www.semanticscholar.org/product/api) —
  improves rate limits on related-papers / batch / vector lookups.
* (Optional) A [Slack bot token](https://api.slack.com/apps) if you
  want digest alerts.
* (Optional) An `OPENAI_API_KEY` if you want OpenAI as an embedding
  provider.

None of these are blocking. ALMa runs without keys; missing
capabilities are hidden in the UI rather than producing errors.
