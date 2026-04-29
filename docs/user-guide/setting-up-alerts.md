---
title: Setting up alerts
description: Configure Slack credentials in Settings, build a digest from your feed monitors, and verify it end to end.
---

# Setting up alerts

Alerts deliver new papers from your monitors to Slack on a schedule
(or on demand). The whole flow lives inside ALMa — there's no `.env`
hand-editing.

## 1. Create a Slack bot and grab a token

1. Visit <https://api.slack.com/apps> and create a new app from
   scratch.
2. Add a Bot User to the app.
3. In **OAuth & Permissions**, add scopes:
   - `chat:write` — required, lets the bot post messages.
   - `channels:read` and `groups:read` — needed if you want to
     resolve a channel name (`general`, `#general`).
   - `users:read` and `im:write` — needed if you want to resolve a
     user display name (`Andrea Costantino`) and DM yourself.
4. Install the app to your workspace; copy the **Bot User OAuth
   Token** (starts with `xoxb-`).
5. Either invite the bot to the channel you want to post to
   (`/invite @your-bot` in Slack), or DM yourself — the bot can DM
   any workspace user it can resolve.

## 2. Configure Slack in ALMa

Go to **Settings → Channels** and fill:

- **Slack Bot Token** — paste the `xoxb-…` token. ALMa stores it in
  the unified secret store at `data/secrets.json` (gitignored,
  permission `0o600`). It is masked everywhere except inside that
  file. The token is **never** written to `.env`.
- **Default Slack Channel** — accepts any of:
  - a channel name: `general` or `#general`
  - a user display name: `Andrea Costantino` (resolves to a DM)
  - a Slack ID: `C0123…` (channel) or `U0123…` (user, resolves to
    DM via `conversations.open`)

  Resolution happens at send time and is cached for the lifetime
  of the backend process.
- **Check Interval (hours)** — how often the scheduler sweeps for
  due alerts. Default 24; set to 1 if you have any daily-schedule
  alerts that need to fire close to their configured time.

Save the form, then click **Test Slack Connection**. ALMa queues an
async job (visible in the **Activity** tab as
`alerts.slack.test`) that sends a "ALMa — Connection Test" message
through the same code path real alerts use. The toast reports the
resolved target on success or a precise `channel_not_found` on
failure.

## 3. Make sure you have feed monitors

Alerts read from the **Feed**. If you have no feed monitors yet,
nothing will match. Two ways to create monitors:

- **Authors page → Follow author**. Each followed author becomes a
  monitor of `monitor_type='author'`.
- (Other monitor types — keyword, topic — are wired in the data
  model but not yet exposed in the v1 UI.)

After following an author, wait for the next feed refresh (or
trigger one manually) so `feed_items` rows exist for that monitor.

## 4. Create a rule

A **rule** is the matching predicate for one source.

**Alerts → Rules tab → + Create Rule**:

- **Name** — anything human-readable; shown in history.
- **Type** — pick `feed_monitor`.
- **Monitor** — pick one of your feed monitors.
- **Channels** — leave as `["slack"]` (today the only option).
- **Enabled** — on.

Save. Use **Test fire** on the rule row to dry-match: ALMa runs
the rule against the database (without the cold-start watermark)
and shows you the first 20 paper titles that would be eligible
under Layer 1 alone (publication-date window). This is the right
button for "is this rule scoped sensibly?" before you wire it
into a digest.

You can create as many rules as you want — typically one per
monitor you care about.

## 5. Create the digest

A **digest** (UI label) / **alert** (data-model label) is the
delivery config. **Alerts → Digests tab → + Create Digest**:

- **Name** — what shows up in the Slack message header and history.
- **Channels** — `["slack"]`.
- **Schedule** — `manual`, `daily`, or `weekly`.
  - For daily: set the time of day (UTC).
  - For weekly: set the day and time of day (UTC).
- **Rules** — assign one or more of the rules from step 4.
- **Enabled** — on.

Save. The digest appears in the list with its next-fire time (if
scheduled).

## 6. Fire it

Two ways:

- **Evaluate** button — fires the digest now. ALMa returns an
  Activity envelope immediately (`operation_key:
  "alerts.evaluate:<digest_id>"`) and runs the work in the
  background. Watch progress in the **Activity** tab; the
  terminal row reads e.g. *"Sent 7 new paper(s) for 'Weekly
  digest'"* on success.
- **Wait for the schedule** — daily / weekly digests fire on the
  next due tick. The scheduler sweep records itself as
  `alerts.evaluate_scheduled` in Activity.

Either way, every paper that ends up in `alerted_publications`
for this digest is marked sent and will not appear in a future
fire of the same digest.

## What lands in Slack

Each fire is one Slack message (or several, if there are >15
papers — see *Chunking* below).

```
*<https://doi.org/…|Title of paper>*
Authors: First, [+N], Last
2026-04-26 | Nature Machine Intelligence
Match: Alice Smith, Bob Jones
The first ~280 chars of the abstract, truncated …
```

- Title is bold and links to the paper (DOI > url > pub_url).
- The metadata line is `publication_date | journal`. Citations are
  not shown — these are new papers, the count is always near zero.
- **Match** lists the entities inside your rule(s) that selected
  this paper — author names, topic labels, keywords. When the same
  paper matches multiple rules in the same digest, the entries are
  joined with `, `.
- The abstract is truncated to 280 chars; if the paper has no
  abstract that line is omitted.

The footer reads `Sent by ALMa | YYYY-MM-DD HH:MM UTC`.

### Chunking past 15

If a fire produces more than 15 papers, ALMa splits into multiple
Slack messages with headers like
`Alert: Weekly digest -- papers 1-15 of 23`. Papers are only
marked delivered after **every** chunk has succeeded — a partial
Slack outage during chunk 2 of 3 leaves all 23 papers eligible
for the next fire.

## Two cold-start filters

ALMa applies two independent filters before sending, to prevent
the most common failure modes:

1. **30-day publication-date window.** Only papers whose
   `publication_date` falls within the last 30 days are eligible.
   Papers with no publication date are dropped (not back-filled
   with the fetch timestamp). Adjustable per rule via
   `rule_config.max_age_days` (the field is in the data model;
   the v1 UI defaults to 30 and does not yet surface a slider).

2. **`fetched_at >= alert.created_at` watermark.** A digest only
   fires for papers that arrived in the Feed *after* the digest
   was created. So adding a digest today over a monitor with a
   month of backfill does not produce a 200-paper opening fire —
   only papers fetched from now onward count.

Combined with the per-digest dedup
(`alerted_publications(alert_id, paper_id)` UNIQUE), this is
robust against the two classic foot-guns: backfill spam and
"sent the same paper twice."

## Cross-digest delivery is independent

The same paper *can* arrive through two different digests if both
happen to match it. `alerted_publications` is keyed on the
`(alert_id, paper_id)` pair, not on `paper_id` alone. This is
deliberate: if you set up "topic: ML" and "follow: Alice", and
Alice publishes in ML, you legitimately get the paper through
both subscriptions.

Inside a single digest, each paper is sent at most once.

## Anatomy of an evaluate

When a digest fires (manually or on schedule), ALMa runs:

```
1. Look up the digest's assigned rules.
2. For each rule: SQL match against feed_items + papers, applying
   Layer 1 (publication_date >= now-30d) and -- if called from a
   digest, not from Test Fire -- Layer 2 (fetched_at >= alert.created_at).
3. Deduplicate the merged paper list by paper_id, joining each
   paper's "alert_source" strings with ", ".
4. Filter against alerted_publications for this digest.
5. For each channel (currently always "slack"):
     a. Resolve the channel string -> Slack ID.
     b. Render Block-Kit blocks; chunk into messages of <=15
        papers each.
     c. POST chat.postMessage; require ok=true on every chunk.
6. On full success: INSERT alerted_publications rows for every
   sent paper, INSERT an alert_history row, UPDATE
   alerts.last_evaluated_at.
```

The whole thing runs on the scheduler thread pool. Concurrent
re-fires of the same digest dedupe via `find_active_job` — the
second click returns the same `job_id` with status
`already_running`.

## Troubleshooting

**"Slack test failed" toast on the test button.**

Check the Activity row for the precise error:

- `Slack token not configured` — go back to Settings → Channels
  and save the token.
- `channel_not_found: '…'` — the resolver tried `conversations.list`
  then `users.list` and didn't find a match. Verify (a) the bot is
  in the channel for channel-name targets, and (b) the
  `channels:read` / `users:read` scopes are granted.
- `Slack API rejected the test message` — token is valid and
  resolution succeeded, but the bot can't post to the resolved
  target. Most often the bot isn't a member of the channel; for
  user DMs check `im:write` scope.

**Evaluate completes but `papers_new = 0` and you expected papers.**

Walk the funnel:

1. Open a SQL prompt against `data/scholar.db` and check
   `feed_items` for the monitor — if it's empty, the Feed hasn't
   pulled yet.
2. Among rows that exist, check `publication_date`. Anything
   older than 30 days is filtered by Layer 1; anything NULL is
   filtered.
3. Compare `feed_items.fetched_at` against
   `alerts.created_at` — anything fetched *before* the digest
   was created is filtered by Layer 2. (This is by design; it's
   the cold-start guard.)
4. Check `alerted_publications` — if the paper is already there
   for this digest, it was sent in a previous fire and will not
   be re-sent.

**Daily / weekly digest didn't fire.**

The scheduler sweep checks `alerts.last_evaluated_at` against
the configured schedule time on each tick. If the backend was
down at the scheduled time, the sweep catches up on the next
tick (i.e. the digest fires late, not skipped). Configure
`ALERT_CHECK_INTERVAL_HOURS` lower than your schedule cadence —
default 1 hour is appropriate for daily digests; raise it only
if you only run weekly digests.

## Reset history

If you want to re-send a paper through a digest (e.g. you fixed
a wrong rule and want to backfill), delete the relevant
`alerted_publications` rows for that digest:

```sql
DELETE FROM alerted_publications WHERE alert_id = '<digest_id>';
```

The next fire will treat every eligible paper as new again.
There is no UI button for this yet — file an issue if you want
one.
