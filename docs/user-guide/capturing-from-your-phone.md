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

## Setting it up

Five minutes, once. Do these in order — step 3 fails if you skip the reinstall
in step 2.

### 0. Do you already have an ALMa Slack bot?

If Slack alerts already work for you, yes — skip to step 1.

If not, create one first:

1. Go to **[api.slack.com/apps](https://api.slack.com/apps)** → **Create New
   App** → **From scratch**.
2. Name it `ALMa`, pick your workspace, **Create App**.
3. Left sidebar → **OAuth & Permissions**. Under **Scopes → Bot Token Scopes**,
   add `chat:write`.
4. Top of that page → **Install to Workspace** → **Allow**.
5. Copy the **Bot User OAuth Token** (starts `xoxb-`).
6. In ALMa: **Settings → Channels → Slack Bot Token**, paste it, **Save**.

### 1. Create the capture channel

In Slack:

1. **+** next to Channels → **Create a channel**.
2. Name it `alma-inbox`. Set visibility to **Private**.
3. Open the channel and type `/invite @ALMa` (use whatever you named the bot),
   then Enter. **The bot must be a member — it cannot read a channel it hasn't
   joined.**

Use a channel you use for nothing else: everything posted there is read as a
capture attempt.

### 2. Add the read scopes and REINSTALL

1. **[api.slack.com/apps](https://api.slack.com/apps)** → your ALMa app →
   **OAuth & Permissions**.
2. Under **Scopes → Bot Token Scopes**, **Add an OAuth Scope** three times:

    | Scope | Why it's needed |
    |---|---|
    | `groups:history` | Read the messages you post in the private channel |
    | `groups:read` | Turn the channel name into the id the API needs |
    | `reactions:write` | React on your message so you can see it landed |

    Made the channel **public** instead? Use `channels:history` +
    `channels:read`.

3. Scroll up → a yellow banner appears → **Reinstall to Workspace** → **Allow**.

    !!! warning "This step is not optional"
        Scopes do nothing until the app is reinstalled. Skipping it is the most
        common reason capture "silently does nothing" — the token is valid, it
        just isn't allowed to read.

4. The token does **not** change on reinstall, so there is nothing to re-paste
   in ALMa.

### 3. Point ALMa at the channel

**Settings → Channels → Capture channel (Inbox)** → type `alma-inbox` (no `#`)
→ **Save**.

Leave it empty to turn capture off.

!!! note "Why it's a separate field from 'Default Slack Channel'"
    That one is where ALMa **posts** alerts. If ALMa also *read* it, it would
    ingest its own notifications as captures. Keep them different channels.

### 4. Prove it works, now

1. In Slack, post a paper link into `#alma-inbox`. For example:

        https://doi.org/10.1038/s41586-019-1666-5

2. In ALMa: **Settings → Channels → Check capture channel now** (don't wait for
   the 5-minute timer).
3. You should see a toast: *"Captured 1 paper"*.
4. In Slack, your message now has a 📥 reaction.
5. Go to **Home** — the paper is in the **Inbox** section.

If instead you get *"No capture channel is configured"*, step 3 didn't save. If
you get a permissions error, step 2's reinstall was skipped.

### From then on

Just post links. ALMa checks every 5 minutes on its own — tune with
`INBOX_SWEEP_INTERVAL_MINUTES` (see
[Configuration](../reference/configuration.md#scheduler)).

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
[Inbox](../concepts/inbox.md#adding-a-channel).

**Why not a self-DM?**
A Slack bot can't read the DM you have with yourself — only a user token can,
and that would let ALMa read your entire Slack. A private channel keeps the
permissions narrow.

## Related

* [Inbox](../concepts/inbox.md) — how it works, and adding a channel
* [Paper lifecycle](../concepts/paper-lifecycle.md) — where `inbox` sits
* [Browser connector](browser-connector.md) — the desktop equivalent
* [Setting up alerts](setting-up-alerts.md) — the outbound direction
