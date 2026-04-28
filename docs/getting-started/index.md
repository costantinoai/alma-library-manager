---
title: Getting started
description: Start ALMa with Docker, then complete the first-run checklist in the app.
---

# Getting started

For normal use, start with Docker. It is the release path and it
includes the backend, frontend, and native dependencies in one image.
Bare metal is for development or for users who intentionally want to
manage Python, Node, and AI packages themselves.

<div class="grid cards" markdown>

-   :material-docker:{ .lg .middle } **Docker quick start**

    ---

    Pull or build the container, mount your local data, and open the
    app at `localhost:8000`.

    [:octicons-arrow-right-24: Docker](docker.md)

-   :material-language-python:{ .lg .middle } **Bare metal**

    ---

    Python virtualenv / conda / uv plus a Vite frontend build. Use this
    only when you are developing ALMa or deliberately avoiding Docker.

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
