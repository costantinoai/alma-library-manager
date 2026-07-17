---
title: Alerts
description: Scheduled (or manual) Slack digests of new papers from a chosen subset of feed monitors. Two-layer cold-start filter, per-alert dedup, async-enveloped delivery.
---

# Alerts

**Alerts** turn the Feed into a push channel. You pick a subset of your
feed monitors (or all of them), pick a schedule (daily / weekly /
manual), and ALMa drops a Slack DM with the new papers each time it
fires.

The model is intentionally close to the older `scholar-slack-bot`
script — small, opinionated, and bounded. Alerts are not a generic
notifier; they are "the new-papers digest for the things I told ALMa
to watch."

## Mental model

```
   feed_monitors ─┐
                  ├─► Alert (digest) ─► schedule fires ─► Slack DM
   alert_rules  ──┘
```

Three primitives:

| Concept | Table | What it is |
|---|---|---|
| **Monitor** | `feed_monitors` | A "thing ALMa watches." Today this means a followed author, but the schema is open to keyword / topic monitors as the Feed grows. |
| **Rule** | `alert_rules` | The matching predicate that selects papers from one monitor (or other source). For v1 the only UI-supported rule type is `feed_monitor`. |
| **Alert (digest)** | `alerts` | The delivery config: name, schedule, channels, plus a list of assigned rules. One alert can compose many rules. |

A paper is delivered when **all** of these hold:

1. It matches at least one rule assigned to the alert.
2. Its `publication_date` is within the rolling 30-day window
   (`max_age_days`, configurable per rule, default 30).
3. Its `feed_items.fetched_at` is **after** `alert.created_at` —
   the cold-start watermark (see below).
4. It has not already been sent by **this same alert** in a previous
   fire (`alerted_publications` per-alert dedup).

## Why these filters?

Two specific failure modes the filters prevent:

### 1. Backfill spam (Layer 1: 30-day publication-date window)

When you add a new monitor, ALMa backfills the author's recent
publications into `feed_items`. Without a publication-date filter,
the first alert fire would dump every recent backfill into Slack —
papers that are new TO THE MONITOR but not new TO THE WORLD.

The 30-day window means: "only papers published in the last 30 days
are eligible." Older papers stay in the Feed where you can browse
them; they do not become Slack notifications.

Papers with no `publication_date` (rare on OpenAlex; common on
imports) are **dropped**, not back-filled with `fetched_at`. Per the
project's "don't fabricate timestamps" rule, a missing pub date is
not the same as "today."

### 2. Cold-start floods (Layer 2: alert.created_at watermark)

If you create an alert covering a monitor that already has a few
weeks of papers in the Feed, Layer 1 alone would still send the
recent-but-already-seen ones on the first fire.

Layer 2 says: "the alert starts caring from the moment it was
created." Anything fetched into the Feed *before* `alert.created_at`
is treated as historical and skipped, even if its `publication_date`
is recent.

This mirrors `scholar-slack-bot`'s cache semantics — a brand-new
author starts with everything already in the cache, so nothing
fires until truly new papers arrive.

## Per-alert dedup, NOT global

`alerted_publications` is keyed on `(alert_id, paper_id)`. The same
paper can deliver through two distinct alerts — once each. This is
deliberate: each alert is its own deliberate subscription with its
own scope, and collapsing across alerts would let a noisy
"follow-this-author" alert silence a more curated topic alert for
the same paper.

Inside a single alert, the same paper is sent at most once.

## Slack message format

Each fire posts one Slack message (or several, if there are more
than 15 papers — see *Chunking* below). Per paper:

```
*<https://doi.org/…|Title of paper>*
Authors: First, [+N], Last
2026-04-26 | Nature Machine Intelligence
Match: Alice Smith, Bob Jones
The first ~280 chars of the abstract, truncated …
```

- **Title** is bold and links out (DOI > url > pub_url).
- **Authors** abbreviate to `First, [+N], Last` past four authors.
- The metadata line is `publication_date | journal`. Citations are
  intentionally omitted — these are new papers by construction, so
  the count is always 0 / near-0 and adds noise.
- **Match** lists the entities inside the rule(s) that triggered
  this paper — author names, topic labels, keywords. When one paper
  matches multiple rules in the same alert, the entries are joined
  with `, ` so you see all the reasons it surfaced.
- **Abstract** is truncated to 280 chars; missing abstracts produce
  no line.

The footer reads `Sent by ALMa | YYYY-MM-DD HH:MM UTC`.

## Chunking past 15 papers

A single Slack Block-Kit message has a 50-block limit, which works
out to ~15 papers. When a fire produces more than that, ALMa splits
into multiple messages with headers like:

```
Alert: Weekly digest -- papers 1-15 of 23
…
Alert: Weekly digest -- papers 16-23 of 23
…
```

The dispatch is **all-or-nothing**: papers are only marked
delivered (`alerted_publications`) after every chunk has succeeded.
A partial Slack outage leaves the un-acked papers eligible for the
next fire.

## Schedules

| Schedule | When it fires |
|---|---|
| **Manual** | Only when you click "Evaluate" on the Alerts page (or POST `/alerts/{id}/evaluate`). |
| **Daily** | Each day at a configurable hour, evaluated by the in-process scheduler sweep. |
| **Weekly** | Once per week on a configurable day + hour. |

The scheduler sweep checks every hour by default
(`ALERT_CHECK_INTERVAL_HOURS`) and fires every alert whose
`_is_due()` predicate returns true. Schedule times are stored as
naive UTC; users in non-UTC time zones will see the local fire
time shift by their offset.

## Async + Activity envelope

Every Slack-touching call runs through the canonical
[activity envelope](../operations/background-jobs.md):

| Endpoint | Operation key |
|---|---|
| `POST /alerts/{id}/evaluate` | `alerts.evaluate:<alert_id>` |
| `POST /plugins/slack/test` | `alerts.slack.test` |
| `POST /plugins/email/test` | `alerts.email.test` |
| Periodic sweep | `alerts.evaluate_scheduled` |

The HTTP request returns in ~100 ms with a `JobEnvelope`
(`{ job_id, status: "queued", operation_key, … }`). The actual
evaluation runs on the scheduler thread pool; progress lands in
`operation_status` so the **Activity** tab shows the job moving
from queued → running → completed (or failed) with a punch-line
terminal message like *"Sent 7 new paper(s) for 'Weekly digest'"*.

Concurrent re-fires of the same alert dedupe via `find_active_job`:
clicking "Evaluate" twice returns the same `job_id` on the second
call, with `status: "already_running"`.

## Delivery channels

Two working channels: **Slack** and **Email**. An alert delivers to
whichever channels are checked on its `channels` list (`slack`,
`email`, or both); each channel sends the same matched-paper set
independently. The `MessagingPlugin` interface in `alma.plugins.base`
leaves room for Discord / webhook follow-ons, but Slack and Email are
the implemented paths today. Each has a "Send test" button in
Settings that runs through the same notifier as real delivery — a
green test proves the production path works.

### Slack

Delivery via a Slack Bot User OAuth Token through `SlackNotifier`.
The bot token is stored in the unified secret store
(`data/secrets.json`, key `slack.bot_token`). The DM target lives
in `data/settings.json` under `slack_channel`. Both are editable
from **Settings → Delivery channels**; no environment variable hand-edits
needed.

### Email

Delivery via `EmailNotifier` (`alma.mailer.client`), a stdlib
`smtplib` digest sender that mirrors `SlackNotifier`: an
`is_configured` gate (host + from + recipients), an async
`send_paper_alert`, a `send_test_message`, and a `test_connection`
handshake. It renders the same paper-dict shape Slack does into a
combined HTML + plaintext email, capped at **50 papers per email**.
Transport is STARTTLS on port 587 (the default) or implicit TLS on
port 465.

SMTP host, port, username, from, recipient list, and the STARTTLS
toggle are stored in `data/settings.json` (keys `smtp_host`,
`smtp_port`, `smtp_username`, `smtp_from`, `smtp_to`,
`smtp_use_tls`). The SMTP **password** is held in the unified secret
store (`data/secrets.json`, key `smtp.password`) — never in
`settings.json`. All are editable from **Settings → Email digests**;
each also has an env-var override (`SMTP_HOST`, `SMTP_PORT`,
`SMTP_USERNAME`, `SMTP_FROM`, `SMTP_TO`, `SMTP_PASSWORD`). See the
[Configuration reference](../reference/configuration.md#email--smtp).

`slack_channel` accepts:

- a public/private channel name (`general`, `#general`),
- a user display name (`Andrea Costantino`),
- a Slack ID (`C0123…`, `U0123…`).

Resolution to a channel ID happens at send time and the result is
cached for the lifetime of the process. A wrong name produces a
precise `channel_not_found` error in the Activity row, not a
generic "API failed."

## Current limits

- Single global Slack DM target. Per-alert channel override is on
  the roadmap — today every alert delivers to whatever
  `slack_channel` is set in Settings.
- Schedule times are timezone-naive (UTC). A daily 09:00 alert
  fires at 09:00 UTC, which is 11:00 in CET / 10:00 in CEST.
- Only `feed_monitor` rules are exposed in the v1 dialog. The other
  rule types (`author`, `collection`, `keyword`, `topic`,
  `similarity`, `discovery_lens`, `branch`, `library_workflow`) exist
  in code (`VALID_RULE_TYPES`) and accept API calls, but the
  new-alert form centres on monitors.

## Schema

```sql
alert_rules               (id, name, rule_type, rule_config, channels,
                           enabled, created_at)
alerts                    (id, name, channels, schedule, schedule_config,
                           format, enabled, created_at, last_evaluated_at)
alert_rule_assignments    (alert_id, rule_id)               -- M:N
alert_history             (id, alert_id, channel, sent_at, status,
                           publications, publication_count,
                           message_preview, error_message)
alerted_publications      (id, alert_id, paper_id, alerted_at)
                          -- UNIQUE(alert_id, paper_id)
```

`channels` is a JSON array of channel names; the implemented values
are `slack` and `email` (an alert may list either or both).

`rule_config` is a JSON blob; for `feed_monitor` rules it must
include `monitor_id` (or `monitor_name`). `max_age_days` (default
`30`) is the only other key that affects matching.

## API surface

```
# Rules (the matching predicates)
GET    /api/v1/alerts/rules
POST   /api/v1/alerts/rules
PUT    /api/v1/alerts/rules/{rule_id}
DELETE /api/v1/alerts/rules/{rule_id}
POST   /api/v1/alerts/rules/{rule_id}/toggle
POST   /api/v1/alerts/test/{rule_id}        # dry-match, no Slack send

# Alerts (the delivery configs)
GET    /api/v1/alerts/
POST   /api/v1/alerts/
GET    /api/v1/alerts/{alert_id}
PUT    /api/v1/alerts/{alert_id}
DELETE /api/v1/alerts/{alert_id}
POST   /api/v1/alerts/{alert_id}/rules      # assign rules
DELETE /api/v1/alerts/{alert_id}/rules/{rule_id}

# Evaluation (async-enveloped)
POST   /api/v1/alerts/{alert_id}/evaluate   # returns JobEnvelope
POST   /api/v1/alerts/{alert_id}/dry-run    # sync; returns matched papers

# History
GET    /api/v1/alerts/history
GET    /api/v1/alerts/templates             # one-click suggestions

# Channel tests
POST   /api/v1/plugins/slack/test           # returns JobEnvelope
POST   /api/v1/plugins/email/test           # sends a test email (SMTP)
```

For the request/response shapes see the [API reference](../reference/api.md).
