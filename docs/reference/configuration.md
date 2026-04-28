---
title: Configuration
description: Runtime files, environment variables, and where Settings stores user choices.
---

# Configuration

ALMa has two local configuration layers:

1. **Environment variables / `.env`** — secrets and deployment knobs.
2. **`settings.json`** — small bootstrap file written next to the
   repo / mount.

Most UI-tuned product settings (Discovery weights, AI provider
selection, Library housekeeping) are stored in the SQLite database
by the relevant Settings cards, not in `settings.json`.

## Docker vs bare metal

Docker users normally edit only host-mounted files:

* `.env`
* `settings.json`
* `data/`
* `config/`

Bare-metal users use the same files but also manage their own Python
and Node environments.

## Environment variables

### Core

| Variable | Default | Purpose |
|---|---|---|
| `API_HOST` | `127.0.0.1` | Host the backend binds to. |
| `API_PORT` | `8000` | Port the backend binds to. |
| `API_KEY` | *(unset)* | If set, every request must include `X-API-Key: <value>`. Use it when exposing ALMa beyond localhost. |
| `DB_PATH` | `./data/scholar.db` | SQLite file path. |
| `DATA_DIR` | `./data` | Where caches, logs, and `secrets.json` go. |
| `DEBUG` | `false` | Verbose logging + tracebacks in API responses. |

### External APIs

| Variable | Purpose |
|---|---|
| `OPENALEX_EMAIL` | Identifies you to OpenAlex's polite pool. **Strongly recommended.** |
| `OPENALEX_API_KEY` | Optional, for higher quotas. |
| `SEMANTIC_SCHOLAR_API_KEY` | Optional, for higher S2 batch / related rate limits. |
| `CROSSREF_MAILTO` | Identifies you to Crossref's polite pool. |
| `ALMA_USER_AGENT` | Override the User-Agent ALMa sends to all sources. |

### AI

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY` | Optional OpenAI embedding provider key. |

Local SPECTER2 is configured through **Settings → AI & embeddings**,
including the dependency environment path.

### Slack

| Variable | Purpose |
|---|---|
| `SLACK_TOKEN` | Slack bot OAuth token. |
| `SLACK_CHANNEL` | Default channel for digests. |

### Scheduler

| Variable | Default | Purpose |
|---|---|---|
| `SCHEDULER_ENABLED` | `true` | Set to `false` to disable background jobs. Useful in tests. |
| `AUTHOR_REFRESH_HOUR` | `3` | Hour-of-day (UTC) for nightly author refresh. |
| `ALERT_CHECK_INTERVAL_HOURS` | `6` | How often the alert dispatcher runs. |
| `SCHOLAR_RETRY_DELAYS` | `1,2,4,8` | Comma-separated retry backoff for `scholarly`. |

### Secrets file

`ALMA_SECRETS_PATH` — path to a JSON file with secrets that
shouldn't be in `.env` (default `data/secrets.json`). Currently only
used by select cleanup paths; most users can ignore it.

## `.env.example`

A starter file is committed at the repo root. Copy and edit:

```bash
cp .env.example .env
chmod 600 .env
```

## `settings.json`

Auto-created on first run. The committed example is
`settings.example.json`:

```json
{
  "database": "./data/scholar.db",
  "slack_config_path": "./config/slack.config",
  "api_call_delay": "1.0",
  "backend": "scholar",
  "openalex_email": null,
  "fetch_full_history": false,
  "from_year": null,
  "slack_channel": null,
  "id_resolution_semantic_scholar_enabled": true,
  "id_resolution_orcid_enabled": true,
  "id_resolution_scholar_scrape_auto_enabled": false,
  "id_resolution_scholar_scrape_manual_enabled": true
}
```

`settings.json` only holds bootstrap values. Discovery weights, AI
provider selection, and other UI-tuned product settings live in the
database (the `discovery_settings` table) and are written by the
Settings page.

## Where each Settings card stores its values

| UI surface | Storage |
|---|---|
| Settings → External APIs | `.env`, `settings.json` (`openalex_email`), and the secret store |
| Settings → AI & embeddings | `discovery_settings` keys (`ai.provider`, `ai.local_model`, `ai.python_env_path`, …) |
| Settings → Discovery weights | `discovery_settings` (`discovery.weights.*`, `discovery.strategies.*`, `discovery.limits.*`) |
| Settings → Discovery weights → Branch behaviour | `discovery_settings` (`discovery.branches.*`) |
| Settings → Discovery weights → Feed monitor defaults | `discovery_settings` (`feed.*`) |
| Settings → Integrations → Slack | `.env`, `settings.json`, and `config/slack.config` |
| Settings → Data & system → Corpus Explorer | (no setting; opens modal) |
| Settings → Data & system → Backup / restore | (no setting; runs operations) |

## Reading the live config

```bash
curl http://localhost:8000/api/v1/settings
```

Or, from the UI: **Settings → Status → Show raw config**.
