---
title: Inbox
description: Send yourself a paper from anywhere — Slack, and any channel that honours the same schema — and triage it in ALMa.
---

# Inbox

The Inbox exists for one situation: you find a paper on your phone, where ALMa
isn't installed. You send it to yourself, and it is waiting in ALMa when you sit
down — already in the corpus, already enriched, needing only a decision.

It is a **buffer**, not a storage place. Papers are not supposed to accumulate
here. Everything that lands is a full corpus citizen from the moment it arrives;
the Inbox only marks that you haven't decided about it yet.

## Where it sits in the lifecycle

The Inbox is a value on the **membership axis** (see
[Paper lifecycle](paper-lifecycle.md)), between `tracked` and `library`:

```
tracked ──capture──▶ inbox ──┬─ Save / Like / Love ──▶ library
                             ├─ Dislike ─────────────▶ rating 1
                             └─ X (defer) ──────────▶ tracked
```

The reading axis is untouched by all of this. Capturing a paper says nothing
about whether you intend to read it.

### What "in the corpus in every sense" means

An Inbox paper is an ordinary `papers` row. Concretely:

- **It gets enriched.** `corpus_rehydrate`, the S2 vector fetch and the local
  SPECTER2 fill don't filter on status, so abstract recovery and embeddings
  happen while it sits there. By the time you triage it, it's complete.
- **It appears on the semantic map** and in search, and participates in
  deduplication — so if the same paper later arrives via Feed, it is recognised
  as the one you already have.
- **It does not affect Discovery.** Every preference query is scoped to
  `status='library'` (`paper_signal.py`, `discovery/engine.py`,
  `discovery/scoring.py`). A paper you capture at 8am and X out at noon leaves
  **no trace** on your recommendations. That is what makes a two-second phone
  flick a safe gesture.

### The X button

The X on an Inbox card means *"I looked at this and have no action for it."* It
returns the paper to `tracked` and writes **nothing else** — no rating, no
feedback event.

It is deliberately *not* the same verb as Discovery's dismiss. Under the amended
D6 (2026-07-26):

| Verb | Axis | Scope | Writes |
|---|---|---|---|
| `save` / `like` / `love` / `dislike` | valence | global — one opinion per paper | rating + feedback event |
| `dismiss` | resolution | **the surface that raised it** | that surface's row only |
| `hide` | visibility | global | `status='dismissed'` |
| `remove` | membership | global | `status='removed'` (a negative, per D3) |

So "I've dealt with this here" and "this is bad" are different statements.
Negative opinion is `dislike`; *bad and gone* is dislike **plus** dismiss — two
verbs you compose deliberately.

## Channels

The Inbox is **channel-agnostic**. Slack is the first delivery channel, but the
capture pipeline knows nothing about it. Any transport becomes a valid source by
honouring the schema in `alma.application.inbox_schema`.

```
┌── channel adapter ──┐   ┌──── application/inbound_capture ─────┐
│  Slack   fetch()    │   │  extract → resolve → land → promote  │
│  Email   acknowledge│──▶│                                       │
│  …                  │   │  (knows no transport)                 │
└─────────────────────┘   └───────────────────────────────────────┘
```

### The contract

A channel adapter implements `InboundChannel` — three methods, no database
access, no knowledge of papers:

| Member | Purpose |
|---|---|
| `name` | Channel id (`'slack'`). Also the `added_from` stamped on brand-new papers. |
| `is_configured()` | Is this channel set up enough to poll? An unconfigured channel is simply off, never an error. |
| `fetch(since_cursor)` | New messages, oldest first. Over-fetching is safe; under-fetching loses captures. |
| `acknowledge(message, result)` | Tell the user, in the channel, what happened. Best-effort. |

and hands in `InboundMessage`:

| Field | Meaning |
|---|---|
| `channel` | Which adapter produced this. |
| `external_id` | **Channel-unique, stable id.** Slack's message `ts`; email's `Message-ID`. Half the idempotency key. |
| `received_at` | When the channel says it arrived, ISO-8601. Never fabricated. |
| `text` | Raw body, verbatim. |
| `urls` | Links the channel already parsed. Slack wraps links as `<url\|label>`, so its own parse beats a regex over display text. |
| `metadata` | Channel-specific extras, opaque to the pipeline (Slack's `channel_id`, `thread_ts`). |

and gets back `CaptureResult`:

| Outcome | Meaning |
|---|---|
| `resolved` | Identified; in the corpus at `status='inbox'`. |
| `duplicate` | Already in your Library. Left completely alone — a save is never demoted — and not parked, because it needs no triage. |
| `unresolved` | No usable identifier, or nothing upstream recognised it. Recorded, never dropped. |
| `error` | The pipeline failed (upstream down). Distinct from `unresolved` because this one is worth **retrying**. |

### Idempotency

Channel delivery is **at-least-once**: polling twice, or crashing mid-batch,
re-delivers the same message. Every capture therefore starts with a lookup on
`(channel, external_id)`, which is `UNIQUE` on `inbox_messages`. A redelivered
message replays its recorded outcome instead of producing a second copy of the
paper.

The poll cursor is `MAX(external_id)` per channel, derived from that same table —
so the cursor can never claim progress the database doesn't actually hold.

### What `inbox_messages` stores

**Messages, never papers.** A resolved capture is an ordinary `papers` row; this
table records that a message arrived and what became of it. It exists for four
jobs nothing else can do: idempotency, a durable home for messages that resolved
to no paper, the cursor, and **provenance**.

Provenance is what lets a triage tile on Home say *"Slack · captured 2h ago"*,
and it is also the Inbox's sort key. Ordering captures by `papers.added_at`
would answer the wrong question — that is when a paper entered your *corpus* —
so a paper your Feed collected two years ago, re-sent from your phone this
morning, would sink to the bottom of the queue it had just joined. Home reads
the **newest** message per paper: re-sending a link you already sent is the same
capture, not two.

## Slack

### Why polling, not the Events API

Slack's normal design HTTP-POSTs to your server, which needs a public HTTPS
endpoint. ALMa binds `127.0.0.1` with no auth by default, so there is nothing
for Slack to reach — and exposing it would mean putting an unauthenticated app
on the public internet. Polling is outbound-only: no open port, no tunnel, no
public surface.

Socket Mode (an outbound websocket) would also work and would be instant. It
needs a second app-level token and a supervised connection, so it's a sensible
later upgrade — and because it is just another `InboundChannel`, adopting it
would not touch the capture pipeline at all.

### Why a channel, not your self-DM

A bot token (`xoxb`) can only read conversations the bot belongs to, and Slack
gives it no route into the DM you have with yourself. Reading a self-DM requires
a **user token** (`xoxp` + `im:history`) which can read your entire Slack — a
much broader credential for a personal tool. A private channel with the bot
invited costs one habit change and keeps the token narrow.

### Setup

1. Create a private channel, e.g. `#alma-inbox`, and invite the ALMa bot.
2. Add these bot scopes alongside the `chat:write` alerts already use:
     - `groups:history` + `groups:read` (private channel), or
       `channels:history` + `channels:read` (public)
     - `reactions:write` — the receipt on your message
3. Set the channel in Settings → Channels.

Capture stays **off** until a channel is nominated, and it is deliberately a
separate setting from the alert channel: polling the channel ALMa *posts* to
would bury your captures under digests. It is not a technical requirement —
the poller skips messages carrying a `bot_id`, so ALMa cannot re-read its own
alerts.

### What you see on your phone

| Reaction | Meaning |
|---|---|
| 📥 `:inbox_tray:` | Captured — it's in your Inbox. |
| 📚 `:books:` | You already have this in your Library. |
| ❓ `:question:` | Couldn't identify a paper. A thread reply says why. |
| ⚠️ `:warning:` | Something failed upstream; it will be retried. |

A successful capture is **silent** apart from the reaction. ALMa only speaks up
in-thread when something needs you.

### What gets recognised

DOIs in any form (bare, `doi:`, `https://doi.org/…`), arXiv links and ids
(converted to their registered `10.48550/arXiv.*` DOI, which OpenAlex indexes),
bioRxiv DOIs, OpenAlex work ids, and a bare publisher URL as a last resort.
Prose around the link is ignored.

## Adding a channel

1. Implement `InboundChannel` in `src/alma/services/inbox_channels/`.
2. Add a `ChannelDescriptor` to `alma.channels.CHANNELS` with the `receive`
   capability — registration is explicit, so that one list tells you exactly
   what can put a paper in your Inbox (and what ALMa can post to).

That's all. Resolution, corpus landing, Inbox membership, idempotency and the
cursor are already handled.

## Related

* [Paper lifecycle](paper-lifecycle.md) — the two state axes
* [Home](home.md) — where Inbox items surface
* [Library](library.md) — where triaged papers land
* [Delivery channels](channels.md) — the registry both directions share
