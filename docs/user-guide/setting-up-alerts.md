---
title: Setting up alerts
description: Build a Slack digest from composable rules — author, keyword, topic, similarity, lens.
---

# Setting up alerts

## Prerequisites

* A Slack workspace where you can install bots.
* A bot token with `chat:write` scope.
* The bot invited to the channels you want to post to.

Set the token and default channel in `.env`:

```bash
SLACK_TOKEN=replace-with-real-slack-token
SLACK_CHANNEL=#research
```

Restart the backend (or use **Settings → Integrations → Slack →
Reload**) so ALMa picks up the new token.

Verify with **Settings → Integrations → Slack → Test message**
before going further.

## Building an Alert

**Alerts → New alert**:

1. **Name** — what shows up in the digest header and history.
2. **Schedule** — Manual / Daily / Weekly. For Daily / Weekly,
   pick the time and (for Weekly) the day.
3. **Channel** — defaults to `SLACK_CHANNEL`, override per-alert.
4. **Rules** — one or more rules.

Save. The alert appears in the list with its next-run time.

## Rule types

Each rule has its own form:

### `author`

Recent works from a specific followed author.

* **Author** — pick from your follow list.
* **Window days** — how far back to look (default: matches schedule
  cadence).

Useful for "Send me anything new from author X."

### `keyword`

Free-text search across recent papers.

* **Query** — the search string. Matches title + abstract + topic
  terms.
* **Window days**.

Useful for "Catch any new paper that mentions phrase Y."

### `topic`

OpenAlex topic / concept ID.

* **Topic ID** — pick from a dropdown sourced from
  `/api/v1/library/topics`.
* **Window days**.

Useful for "Send me anything new in this scholarly topic."

### `similarity`

Papers with high SPECTER2 cosine to a seed paper.

* **Seed paper** — pick a paper from your Library.
* **Threshold** — cosine threshold (default 0.85).
* **Window days**.

Requires embeddings to be configured — without vectors, this rule
type is greyed out.

### `discovery_lens`

Top-N recommendations from a saved Lens.

* **Lens** — pick from your lens list.
* **Top N** (default 10).

Useful for "Email me my Discovery top 10 every Monday."

## Composing rules

A single Alert can carry multiple rules. They run in declaration
order; results are deduplicated across rules so a paper matching
two rules appears once.

For example: "Daily digest of new papers in topic X **OR** matching
keyword Y **OR** by author Z" is three rules attached to one alert.

## Anti-spam

ALMa keeps an `alerted_publications` table that records every
paper sent in every digest. The dispatcher filters new candidates
against this table — you'll never get the same paper twice from
the same alert across runs.

If you want to re-send (e.g. after fixing a rule), the **Reset
history** action on the alert clears its sent-papers history.

## Dry-run

Before scheduling, run the alert manually:

* **Test fire a rule** — runs one rule and shows what it would
  send.
* **Dry-run alert** — runs all rules + dedup + filter, shows the
  digest payload, but does **not** post to Slack.

Both are exposed on the Alert detail page.

## Usefulness score

After a few runs, each alert shows a **usefulness score** (0–100)
derived from:

* Reliability (failed runs vs total runs).
* Non-empty rate (digests with zero papers are noise).
* Volume score (papers per dispatch — saturates above ~4 per
  digest to avoid biasing toward firehose alerts).

Alerts trending toward zero usefulness are worth reviewing —
either the rule is too narrow, or the topic genuinely has no new
work.

## Manual digest

Even with no schedule, you can keep an Alert and fire it on demand
via the **Send now** button. Useful for "I'm about to start a
literature review on X — send me a one-shot digest of recent
work."
