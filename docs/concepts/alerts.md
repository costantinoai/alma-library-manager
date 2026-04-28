---
title: Alerts
description: Composable rules that turn Library / Feed / Discovery state into Slack digests on manual, daily, or weekly schedules.
---

# Alerts

**Alerts** are scheduled digests delivered to Slack. They turn rule
sets like "every Monday morning, send me the new papers in topics X,
Y, and Z that I haven't seen yet" into automatic posts.

## Rule types

A rule is one of:

| Type | What it matches |
|---|---|
| `author` | Recent works from a specific followed author. |
| `keyword` | Free-text search across recent papers. |
| `topic` | OpenAlex topic / concept ID. |
| `similarity` | Papers with high SPECTER2 cosine to a seed paper. |
| `discovery_lens` | Top-N recommendations from a saved [Lens](lenses.md). |

A single Alert can compose multiple rules — for example, "any new
work matching topic `interpretability` OR cited by my saved paper
`xyz`".

## Schedules

| Schedule | When it runs |
|---|---|
| **Manual** | Only when you trigger it. Useful for one-shot digests. |
| **Daily** | Every day at a configurable hour. |
| **Weekly** | Once per week on a configurable day. |

Each Alert has its own schedule. Daily and weekly Alerts are
dispatched by the APScheduler background loop.

## Delivery

Currently: **Slack** via a Slack bot token. The plugin layer is
designed to grow more channels (email, RSS, webhook), but Slack is
the only first-class delivery target today.

Configuration lives in:

* `.env` — `SLACK_TOKEN`, default `SLACK_CHANNEL`.
* `config/slack.config` — per-channel overrides.
* **Settings → Integrations → Slack** — UI configuration.

## Anatomy of a Slack digest

```
📚 ALMa daily digest · 2026-04-25

3 new papers matching "interpretability":
  • Paper A — Authors et al. (Nature, 2026) ★★★★
  • Paper B — Authors et al. (NeurIPS, 2026)
  • Paper C — Authors et al. (arXiv, 2026)

Top recommendations from your global lens:
  • Paper D — Authors et al. — score 0.87
  • Paper E — Authors et al. — score 0.81

Tap a paper to open it in ALMa.
```

Each paper line links back to the ALMa instance running on your
machine (or behind your reverse proxy).

## Rule evaluation

When an Alert fires:

1. Each rule runs against the database in the order defined.
2. Results are deduplicated across rules (a single paper matching
   two rules appears once).
3. The combined list is filtered against `alerted_publications` so
   you don't get the same paper twice across runs.
4. The digest is rendered with the configured template and posted.

The `alert_history` table records every dispatch with the digest
content for audit.

## Usefulness scoring

Each Alert tracks four counters:

* `total_runs` / `failed_runs`
* `empty_runs` (digest had zero papers)
* `papers_sent` / `sent_runs`

These feed a per-alert **usefulness score** (0–100) that surfaces in
the Alerts page so you can see which rules are noise and which are
producing signal.

## API

```
GET    /api/v1/alerts                   # list rules
POST   /api/v1/alerts                   # create
PUT    /api/v1/alerts/{id}              # update
DELETE /api/v1/alerts/{id}              # delete

POST   /api/v1/alerts/{id}/run          # trigger manually
GET    /api/v1/alerts/history
```
