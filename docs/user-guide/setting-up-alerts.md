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

Go to **Settings → Delivery channels** and fill:

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

## Configure Email in ALMa (optional)

Email is a second working delivery channel — pick it instead of, or
alongside, Slack. Go to **Settings → Email digests** and fill:

- **SMTP host** — your provider's mail server (e.g.
  `smtp.gmail.com`, `smtp.fastmail.com`).
- **Port** — `587` for STARTTLS (the default) or `465` for implicit
  TLS. ALMa picks the right transport from the port: port 465 opens
  an SSL connection directly; any other port issues `STARTTLS` when
  the **Use STARTTLS** switch is on.
- **Username** — the SMTP auth user (often your full email address).
  Leave blank for an unauthenticated relay.
- **Password** — your app password or SMTP key. ALMa stores it in the
  unified secret store at `data/secrets.json` (gitignored, permission
  `0o600`, key `smtp.password`) — **never** in `settings.json`. It is
  masked everywhere except inside that file; leave the masked value
  in place to keep the existing password.
- **From address** — the sender address. Defaults to the username
  when blank.
- **Send digests to** — one or more recipient addresses, separated by
  commas, semicolons, or newlines.
- **Use STARTTLS** — recommended for port 587; ignored on port 465.

Everything except the password is written to `data/settings.json`
(`smtp_host`, `smtp_port`, `smtp_username`, `smtp_from`, `smtp_to`,
`smtp_use_tls`). Each field also accepts an environment-variable
override (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_FROM`,
`SMTP_TO`, `SMTP_PASSWORD`) for headless setups.

Save the form, then click **Send test email**. ALMa runs the test on
the scheduler pool (Activity op key `alerts.email.test`) using the
same `EmailNotifier` real digests use; the toast reports the
recipients on success or the SMTP error on failure.

Email digests are capped at 50 papers per message; a larger fire is
truncated with an "…and N more" line rather than split into multiple
emails.

## 3. Make sure you have feed monitors

Alerts read from the **Feed**. If you have no feed monitors yet,
nothing will match. Two ways to create monitors:

- **Authors page → Follow author**. Each followed author becomes a
  monitor of `monitor_type='author'`.
- (Other monitor types — keyword, topic — are wired in the data
  model but not yet exposed in the v1 UI.)

After following an author, wait for the next feed refresh (or
trigger one manually) so `feed_items` rows exist for that monitor.

## Shortcut: suggested automations

The card at the top of the Alerts page proposes **one-click
automations** derived from what you already use: productive feed
monitors, monitored authors, curated collections, and engaged
Discovery branches. Clicking **Create Automation** creates the rule
*and* its digest in a single transaction (a failure can never leave
an orphan rule), after which the suggestion disappears — it can't
become a duplicate factory. Suggestions are delivery-aware: they
propose exactly the channels you have configured, and the card is
empty until at least one channel (Slack or email) is set up.

## 4. Create a rule

A **rule** is the matching predicate for one source. Delivery
(channels, schedule) belongs entirely to the **digest** — rules
carry no channel setting.

**Alerts → Rules tab → + Create Rule**:

- **Name** — anything human-readable; shown in history.
- **Type** — pick `feed_monitor` (or any of the other 8 types).
- **Monitor** — pick one of your feed monitors. (Author rules offer
  a picker over your followed authors, with a "Custom ID…" escape
  hatch for anyone you don't follow.)
- **Enabled** — on.

A config that could never match anything (e.g. a monitor rule
without a monitor) is rejected at save time with a precise error —
never stored as a silently-dead rule.

Each rule card shows **what it watches** (monitor name, keywords,
score threshold…), and a flask **Test** button dry-runs just that
rule and lists the matching titles — nothing is sent, nothing is
recorded. For feed-monitor rules the test shows the broader
pre-watermark match set; a digest may deliver fewer (see the
cold-start filters below).

A rule that isn't assigned to any digest **never runs** — its card
carries a "Not in any digest" warning that jumps you to the Digests
tab to assign it.

You can create as many rules as you want — typically one per
monitor you care about.

## 5. Create the digest

A **digest** (UI label) / **alert** (data-model label) is the
delivery config. **Alerts → Digests tab → + Create Digest**:

- **Name** — what shows up in the message header and history.
- **Channels** — tick **Slack**, **Email**, or both. Each ticked
  channel gets the same matched-paper set; an unconfigured channel is
  skipped (recorded as `skipped` in the digest's channel results).
- **Schedule** — `manual`, `daily`, or `weekly`.
  - For daily: set the time of day (UTC).
  - For weekly: set the day and time of day (UTC).
- **Rules** — assign one or more of the rules from step 4.
- **Enabled** — on.

Leaving both channels unticked is allowed but warned about, in the
dialog and on the card: such a digest evaluates and delivers
nothing.

Save. The digest card shows its schedule, a **next-run** line for
scheduled digests (when the hourly sweep can next fire it — hidden
while the digest is disabled), and after the first evaluation a
**last-outcome chip** (the *worst* channel outcome of the latest
run, so a failed email is never hidden behind a successful Slack
send). Clicking the chip opens the History tab pre-filtered to that
digest.

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

Either way, every paper that a channel actually receives is
recorded in `alerted_publications` **for that digest and that
channel**, and will not be re-sent there. Dedup is per-channel: if
Slack delivered but email failed, the papers stay eligible for
email and go out on the next fire — the Slack success doesn't
consume them.

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

Combined with the per-digest, per-channel dedup
(`alerted_publications(alert_id, paper_id, channel)` UNIQUE), this
is robust against the two classic foot-guns: backfill spam and
"sent the same paper twice."

## Cross-digest delivery is independent

The same paper *can* arrive through two different digests if both
happen to match it. `alerted_publications` is keyed on the
`(alert_id, paper_id, channel)` triple, not on `paper_id` alone.
This is deliberate: if you set up "topic: ML" and "follow: Alice",
and Alice publishes in ML, you legitimately get the paper through
both subscriptions.

Inside a single digest, each paper reaches each channel at most
once.

## Anatomy of an evaluate

When a digest fires (manually or on schedule), ALMa runs:

```
1. Look up the digest's assigned rules.
2. For each rule: SQL match against feed_items + papers, applying
   Layer 1 (publication_date >= now-30d) and -- if called from a
   digest, not from Test Fire -- Layer 2 (fetched_at >= alert.created_at).
3. Deduplicate the merged paper list by paper_id, joining each
   paper's "alert_source" strings with ", ".
4. Compute each channel's NEW set: papers not yet in
   alerted_publications for this (digest, channel).
5. For each channel on the digest ("slack" and/or "email"):
     - slack: resolve the channel string -> Slack ID, render
       Block-Kit blocks, chunk into messages of <=15 papers each,
       and POST chat.postMessage requiring ok=true on every chunk.
     - email: render one HTML+text digest (capped at 50 papers) and
       send it over SMTP via EmailNotifier.
     An unconfigured channel is recorded as "skipped" and does not
     block the others.
6. AFTER all sends (writes never straddle network I/O): for every
   channel that succeeded, INSERT its alerted_publications rows;
   INSERT one alert_history row per channel; UPDATE
   alerts.last_evaluated_at.
```

The whole thing runs on the scheduler thread pool. Concurrent
re-fires of the same digest dedupe via `find_active_job` — a
second click gets the running job's envelope flagged
`already_running` and the UI tells you to watch Activity instead
of starting a duplicate.

## Troubleshooting

**"Slack test failed" toast on the test button.**

Check the Activity row for the precise error:

- `Slack token not configured` — go back to Settings → Delivery
  channels and save the token.
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

## History retention

`alert_history` (the per-channel outcome log behind the History
tab) is pruned automatically: the hourly sweep deletes entries
older than 180 days. The floor is 90 days so the Insights weekly
trend always has its full window.

## Reset history

If you want to re-send a paper through a digest (e.g. you fixed
a wrong rule and want to backfill), delete the relevant
`alerted_publications` rows for that digest:

```sql
-- everything for the digest:
DELETE FROM alerted_publications WHERE alert_id = '<digest_id>';
-- or one channel only (dedup is per-channel):
DELETE FROM alerted_publications
 WHERE alert_id = '<digest_id>' AND channel = 'email';
```

The next fire will treat every eligible paper as new again.
There is no UI button for this yet — file an issue if you want
one.
