---
title: Capturing from your phone
description: Send a paper to yourself from anywhere and have it waiting in ALMa, already enriched.
---

# Capturing from your phone

You find a paper on your phone. ALMa isn't installed there, and by the time
you're back at your desk you've forgotten it.

The Inbox fixes that: send the link to a Slack channel, and the paper is waiting
in ALMa — already in your corpus, already enriched, needing only a decision.

!!! info "What this is not"
    Capturing is **not** saving. A captured paper sits in the Inbox until you
    triage it, and it does **not** influence your recommendations while it
    waits. A link you flick at 8am and discard at noon leaves no trace.

## Before you start

You need the ALMa Slack app installed in your workspace, with the capture
scopes, and its token saved in ALMa. That is one page, done once:

**→ [Connecting Slack](connecting-slack.md)**

Make sure you granted the capture scopes there — `channels:read`, `groups:read`,
`groups:history`, `reactions:write` (plus `channels:history` if your channel is
public). If you set Slack up earlier for alerts only, add those scopes now and
**Reinstall to Workspace**; a scope does nothing until you reinstall.

Two rules that cause most failures, repeated here because they bite hardest in
this flow:

1. **Scopes take effect only after (re)installing the app.**
2. **A bot reads only channels it has been added to** — installing it in the
   workspace grants nothing on its own.

---

## Setting up capture

### Step 1 — Create the capture channel and add the bot **to that channel**

Now in the **Slack app itself** (not the developer site):

1. Click **+** beside **Channels** in the sidebar → **Create a channel**.
2. **Name**: `alma-inbox`. **Visibility**: **Private**. → **Create**.
3. Open `#alma-inbox` and send this message in the channel:

        /invite @ALMa

    Use your bot's actual name if you named the app something else — Slack
    autocompletes it as you type. If nothing autocompletes, the app is not
    installed — see [Connecting Slack](connecting-slack.md) step 3.

4. Slack confirms: *"@ALMa was added to #alma-inbox"*.

**That invite is the membership that matters.** The bot can now read
`#alma-inbox` — and still nothing else: not your other channels, not your DMs.

!!! warning "Use a channel for nothing else"
    Every message here is treated as a capture attempt. Don't reuse a channel
    you chat in.

---

### Step 2 — Tell ALMa which channel to read

**Settings → Plugins → Slack → Capture papers from** → type `alma-inbox`
(bare name, **no `#`**) → **Save plugin settings**.

Empty field = capture off.

!!! note "Why this is separate from 'Default Slack Channel'"
    **Default Slack Channel** is where ALMa **posts** alerts *to*.
    **Capture channel** is what ALMa **reads** *from*.
    They can be the same channel — the poller skips any message carrying a
    `bot_id`, so ALMa never re-reads its own alerts. A dedicated capture channel
    is still better: digests are long and frequent, and they bury the links you
    send.

---

### Step 3 — Prove it works

1. In Slack, post a paper link into `#alma-inbox`:

        https://doi.org/10.1038/s41586-019-1666-5

2. In ALMa: **Settings → Plugins → Slack → Check capture now** — runs the check
   immediately instead of waiting up to 5 minutes.
3. Expect the toast **"Captured 1 paper"**. (With nothing new to capture you
   get **"Connected to #alma-inbox"** instead — that is also a success, and it
   confirms the token, scopes, channel and bot membership are all correct.)
4. In Slack, your message now shows a 📥 reaction.
5. Open **Home** — the paper is in the **Inbox** section.

**If something else happened:**

| What you saw | Cause | Fix |
|---|---|---|
| *"No capture channel is configured"* | Step 2 didn't save | Re-enter the name and Save |
| Error mentioning `not_in_channel` | Bot isn't in the channel | Redo step 1.3 |
| Error mentioning `missing_scope` | A scope is absent, or the app wasn't reinstalled after you added one | Recheck the scopes in [Connecting Slack](connecting-slack.md), then **Reinstall to Workspace** |
| Error mentioning `invalid_auth` | Token wrong or regenerated | Re-copy it from [Connecting Slack](connecting-slack.md) step 4 |
| *"Connected to #alma-inbox"* | Success — the connection works, there was just nothing new to capture | Nothing to fix; post a link and re-check |
| ❓ instead of 📥 | The link held no identifiable paper | Read ALMa's thread reply |

---

### From then on

Just post links. ALMa checks every 5 minutes on its own — tune with
`INBOX_SWEEP_INTERVAL_MINUTES` (see
[Configuration](../reference/configuration.md#scheduler)); `0` turns the
automatic check off and leaves only the manual button.

## Using it

Post a link. That's the whole workflow.

ALMa checks every five minutes and reacts on your message so you know what
happened without opening the app:

| Reaction | Meaning |
|---|---|
| 📥 | Captured — it's in your Inbox |
| 📚 | You already have this in your Library |
| ❓ | Couldn't identify a paper — ALMa replies in-thread with why |
| ⚠️ | Something failed upstream; it'll retry |

A successful capture is otherwise silent. ALMa only speaks up when something
needs you.

### What it recognises

- DOIs in any form — bare, `doi:10.…`, `https://doi.org/…`
- arXiv links and ids (`arxiv.org/abs/…`, `arXiv:2401.12345`)
- bioRxiv links
- OpenAlex work ids
- A plain publisher URL, as a last resort

Type whatever you like around the link — prose is ignored. A message with no
usable identifier is kept, not silently dropped, and Home tells you about it.

## Triaging

Captured papers appear in the **Inbox** section on Home, newest first. The
section disappears entirely when it's empty — it notifies, it doesn't nag.

Each card carries the normal actions:

| Action | What it does |
|---|---|
| **Save / Like / Love** | Moves it into your Library (3★ / 4★ / 5★) |
| **Dislike** | Records a negative preference |
| **Not now** (the ✕) | Removes it from the Inbox, keeps it in your corpus, and records **no opinion at all** |

That last one matters. "Not now" is not a judgement — it means *"I looked, and
there's nothing to do here."* It teaches ALMa nothing. If you want ALMa to learn
that a paper is wrong for you, use **Dislike**.

The Inbox is a buffer, not a shelf. Papers aren't meant to accumulate there.

## While a paper waits

It's a full member of your corpus from the moment it arrives:

- its abstract and citation data are filled in automatically;
- it gets a semantic embedding and a position on your maps;
- if the same paper later arrives through your Feed, ALMa recognises it rather
  than showing it twice.

The one thing it does *not* do is affect Discovery. Your recommendations are
built only from your **Library**, so an untriaged capture can't skew them.

## Troubleshooting

**Nothing happens when I post.**
Check the bot is actually *in* the channel (`/invite @ALMa`), that the channel
name in Settings matches, and that you reinstalled the Slack app after adding
scopes. Then press **Check capture channel now** — it reports what it found.

**ALMa reacted ❓.**
It couldn't find a paper in the message. The in-thread reply says why. News
articles, blog posts and Google Docs links have no DOI and won't resolve.

**I posted the same link twice.**
Only one paper is created. Every message is keyed by its Slack timestamp, so
re-posting — or ALMa re-checking — can't duplicate it.

**Can I use email / Telegram instead?**
Not yet, but the capture layer is channel-agnostic by design: a new channel is a
small adapter, not a new pipeline. See
[Building an integration](../development/integrations.md).

**Why not a self-DM?**
A Slack bot can't read the DM you have with yourself — only a user token can,
and that would let ALMa read your entire Slack. A private channel keeps the
permissions narrow.

## Related

* [Inbox](../concepts/inbox.md) — how it works, and adding a channel
* [Paper lifecycle](../concepts/paper-lifecycle.md) — where `inbox` sits
* [Browser connector](browser-connector.md) — the desktop equivalent
* [Setting up alerts](setting-up-alerts.md) — the outbound direction
