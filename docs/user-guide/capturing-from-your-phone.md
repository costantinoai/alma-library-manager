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

Capture needs the Slack plugin: the ALMa app installed in your workspace with
the capture scopes, a private channel the bot has been invited to, and that
channel named in Settings. That is one page, done once:

**→ [Slack](../plugins/slack.md)**

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

Tune the five minutes with `INBOX_SWEEP_INTERVAL_MINUTES` (see
[Configuration](../reference/configuration.md#scheduler)); `0` turns the
automatic check off and leaves only the **Check capture now** button on the
Slack plugin card.

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
- it gets a semantic embedding, so it counts in search and in the structure
  Discovery reads;
- if the same paper later arrives through your Feed, ALMa recognises it rather
  than showing it twice.

The one thing it does *not* do is affect Discovery. Your recommendations are
built only from your **Library**, so an untriaged capture can't skew them.

## Troubleshooting

**Nothing happens when I post.**
Check the bot is actually *in* the channel (`/invite @ALMa`), that the channel
name in Settings matches, and that you reinstalled the Slack app after adding
scopes. Then press **Check capture now** — it reports what it found. The
[Slack page](../plugins/slack.md#troubleshooting) has the error-by-error table.

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
