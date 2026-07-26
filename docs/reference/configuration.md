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
| `API_HOST` | `0.0.0.0` | Host the backend binds to. |
| `API_PORT` | `8000` | Port the backend binds to. |
| `API_KEY` | *(unset)* | If set, every request must include `X-API-Key: <value>`. Use it when exposing ALMa beyond localhost. |
| `DB_PATH` | *(platform data dir)* | SQLite file path. Docker pins `./data/scholar.db`; bare-metal defaults to the OS data dir (`~/.local/share/alma/scholar.db` on Linux). |
| `DATA_DIR` | *(platform data dir)* | Where caches, logs, and `secrets.json` go. Docker pins `./data`; bare-metal uses the OS data dir. |
| `DEBUG` | `false` | Verbose logging + tracebacks in API responses. |

### External APIs

| Variable | Purpose |
|---|---|
| `OPENALEX_API_KEY` | **Required** since 2026-02-13 (keyless → 100 credits/day then HTTP 409). Free at [openalex.org/settings/api](https://openalex.org/settings/api). |
| `OPENALEX_EMAIL` | Optional contact email. The OpenAlex polite pool is retired, but this still sets a courteous User-Agent. |
| `SEMANTIC_SCHOLAR_API_KEY` | **Strongly recommended.** Without it S2 uses the shared anonymous pool and 429s often (stalls Discovery). Free at [semanticscholar.org/product/api](https://www.semanticscholar.org/product/api). |
| `CROSSREF_MAILTO` | Identifies you to Crossref's polite pool (still active). |
| `ALMA_USER_AGENT` | Override the User-Agent ALMa sends to all sources. |

### AI

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY` | Optional OpenAI embedding provider key. |

Local SPECTER2 is configured through **Settings → Intelligence → AI
provider**, including the dependency environment path.

### Slack

| Variable | Purpose |
|---|---|
| `SLACK_TOKEN` | Slack bot OAuth token. |
| `SLACK_CHANNEL` | Default channel for digests. |

### Email / SMTP

The email digest channel (sibling of Slack). Normally configured from
**Settings → Email digests**; these env vars override the stored
settings for headless setups.

| Variable | Default | Purpose |
|---|---|---|
| `SMTP_HOST` | *(unset)* | SMTP server host. Email delivery is off until this, the From address, and at least one recipient are set. |
| `SMTP_PORT` | `587` | SMTP port. `587` = STARTTLS, `465` = implicit TLS (chosen automatically from the port). |
| `SMTP_USERNAME` | *(unset)* | SMTP auth username. Leave unset for an unauthenticated relay. |
| `SMTP_PASSWORD` | *(unset)* | SMTP auth password. Overrides the `smtp.password` secret store entry. |
| `SMTP_FROM` | *(falls back to `SMTP_USERNAME`)* | From address on digest emails. |
| `SMTP_TO` | *(unset)* | Recipient list, separated by commas, semicolons, or newlines. |

`SMTP_USE_TLS` has no env var — the STARTTLS toggle lives only in
`settings.json` (`smtp_use_tls`, default `true`) and is ignored on
port 465.

### Scheduler

| Variable | Default | Purpose |
|---|---|---|
| `SCHEDULER_ENABLED` | `true` | Set to `false` to disable background jobs. Useful in tests. |
| `ALMA_SCHEDULER_WORKERS` | `5` | Max background jobs running at once (1–16). Lower it on a small host (a Raspberry Pi is happy at `1`–`2`) if the app feels sluggish or logs `database is locked`; raise it only if you have spare CPU/GPU. |
| `AUTHOR_REFRESH_HOUR` | `3` | Hour-of-day (UTC) for nightly author refresh. |
| `ALERT_CHECK_INTERVAL_HOURS` | `1` | How often the alert dispatcher runs. |
| `INBOX_SWEEP_INTERVAL_MINUTES` | `5` | How often ALMa polls your [Inbox](../concepts/inbox.md) capture channels. Minutes, not hours — this is the phone→ALMa loop, so latency is the experience. `0` disables the sweep. Costs one API call per configured channel per sweep, and returns immediately when no channel is set up. |
| `ALMA_DEEP_REFRESH_WORKERS` | `4` | Concurrency for the per-author deep-refresh fan-out (clamped 1–16). |
| `SCHOLAR_RETRY_DELAYS` | `20,40,60` | Comma-separated retry backoff (seconds) for external fetches. |
| `ALMA_AUTHOR_SUGGESTION_REFRESH_INTERVAL_HOURS` | `6` | How often suggested authors are recomputed in the background. |
| `ALMA_HYDRATION_DRAIN_INTERVAL_MINUTES` | `15` | How often the pending-hydration ledger is drained (the sweep that fills missing metadata, abstracts and identifiers). |
| `ALMA_IDLE_MAINTENANCE_INTERVAL_HOURS` | `1` | How often idle-time maintenance (vacuum, checkpoint, reconciliation) may run. |
| `ALMA_OPERATION_LOG_RETENTION_DAYS` | unset | Trim `operation_logs` older than N days during maintenance. Unset keeps everything. |
| `ALMA_BACKUP_RETAIN` | `5` | How many automatic database backups to keep before the oldest is pruned. |

### Paths and profile

Set before start; they decide **where ALMa keeps your data**, so changing one on
an existing install points it at a different (probably empty) profile.

| Variable | Default | Purpose |
|---|---|---|
| `ALMA_ENV` | `prod` | Profile namespace. `dev` gives an isolated copy so development never touches your real library — `scripts/start-dev.sh` sets it. |
| `ALMA_CONFIG_DIR` | OS config dir | Directory holding `.env` and `settings.json`. Docker sets it to `/app`. |
| `ALMA_SETTINGS_PATH` | `<config dir>/settings.json` | Exact settings file. Docker pins it into the data volume so settings survive image upgrades. |
| `ALMA_DEFAULT_AI_PROVIDER` | auto | Forces the embedding compute provider (`none` / `local` / `openai`) instead of auto-detecting. Defaults to `local` when torch is importable (D19). |

### Slack (legacy env aliases)

Prefer **Settings → Channels**, which writes the secret store. These are read at
startup for existing installs and Docker `env_file` setups.

| Variable | Purpose |
|---|---|
| `SLACK_API_TOKEN` / `SLACK_TOKEN` | Bot User OAuth token (`xoxb-…`). |
| `SLACK_DEFAULT_CHANNEL` | Channel for outgoing alerts. |
| `SLACK_CONFIG_PATH` | Path to a plugin config file (`config/slack.config`). |

The **inbound** capture channel has no env alias on purpose — it is set in
Settings only, so capture cannot be switched on by an environment a user did not
review. See [Capturing from your phone](../user-guide/capturing-from-your-phone.md).

### Secrets file

`ALMA_SECRETS_PATH` — path to a JSON file with secrets that
shouldn't be in `.env` (default `data/secrets.json`). Currently only
used by select cleanup paths; most users can ignore it.

The store holds namespaced runtime credentials written by the
Settings cards (so they never land in `settings.json`):

| Key | Set from | Holds |
|---|---|---|
| `slack.bot_token` | Settings → Delivery channels | Slack bot OAuth token. |
| `smtp.password` | Settings → Email digests | SMTP auth password (overridable by `SMTP_PASSWORD`). |
| `semantic_scholar.api_key` | Settings → External APIs | Semantic Scholar API key. |
| `openalex.api_key` | Settings → External APIs | OpenAlex API key. |
| `openai.api_key` | Settings → Intelligence → AI provider | OpenAI embedding key. |
| `zotero.api_key` | Settings → External APIs | Zotero API key. |

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
  "slack_config_path": "./config/slack.config",
  "api_call_delay": "1.0",
  "backend": "openalex",
  "openalex_email": null,
  "fetch_full_history": false,
  "from_year": null,
  "slack_channel": null,
  "id_resolution_semantic_scholar_enabled": true,
  "id_resolution_orcid_enabled": true,
  "id_resolution_scholar_scrape_auto_enabled": false,
  "id_resolution_scholar_scrape_manual_enabled": false
}
```

The `database` key is deliberately omitted — the DB path is computed
(`DB_PATH` env → explicit settings value → OS data dir), so a fresh
install resolves to the platform data dir rather than a CWD-relative
`./data`.

`settings.json` only holds bootstrap values. Discovery weights, AI
provider selection, and other UI-tuned product settings live in the
database (the `discovery_settings` table) and are written by the
Settings page.

## Where each Settings card stores its values

| UI surface | Storage |
|---|---|
| Settings → External APIs | `.env`, `settings.json` (`openalex_email`), and the secret store |
| Settings → Intelligence → AI provider | `discovery_settings` keys (`ai.provider`, `ai.local_model`, `ai.python_env_path`, …) |
| Settings → Discovery weights | `discovery_settings` (`discovery.weights.*`, `discovery.strategies.*`, `discovery.limits.*`) |
| Settings → Discovery weights → Branch behaviour | `discovery_settings` (`discovery.branches.*`) |
| Settings → Discovery weights → Feed monitor defaults | `discovery_settings` (`feed.*`) |
| Settings → Delivery channels | `data/secrets.json` (Slack bot token, key `slack.bot_token`) and `settings.json` (`slack_channel`, `check_interval_hours`) |
| Settings → Email digests | `settings.json` (`smtp_host`, `smtp_port`, `smtp_username`, `smtp_from`, `smtp_to`, `smtp_use_tls`) and `data/secrets.json` (SMTP password, key `smtp.password`) |
| Settings → Data & system → Corpus Explorer | (no setting; opens modal) |
| Settings → Data & system → Backup / restore | (no setting; runs operations) |

## Reading the live config

```bash
curl http://localhost:8000/api/v1/settings
```
