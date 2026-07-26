# Delivery channels

A **channel** is a service ALMa exchanges messages with. Each one declares what
it can do:

| Capability | Meaning | Who uses it |
|---|---|---|
| `send` | ALMa posts to it | [Alerts](alerts.md) — digests of matched papers |
| `receive` | it feeds your [Inbox](inbox.md) | capture — papers you send yourself from your phone |

Today: **Slack** does both, **email** sends.

One service, two directions, and they are configured separately on purpose. A
Slack token with a posting channel and no capture channel can send and not
receive — so the UI says exactly that, per direction, rather than one
all-or-nothing "connected" light.

## One registry

`alma.channels.CHANNELS` is the single list of every channel and its
capabilities. Registration is **explicit**: a channel that is not on that list
cannot post to you and cannot put a paper in your Inbox, and reading the list is
how you know.

There used to be two lists — one for senders, one for receivers — and Slack was
in both without either knowing. That is how you get a channel that is polled but
invisible in Settings, or listed in Settings and never actually polled.

The capability flag lives on the **registry entry**, not on a merged base class,
so the two halves stay separate protocols:

* outbound → `alma.plugins.base.MessagingPlugin` — send an already-rendered message
* inbound → `alma.application.inbox_schema.InboundChannel` — fetch messages, acknowledge outcomes

Neither grows a speculative half it has no implementation for. When email capture
arrives, it implements `InboundChannel` and flips one flag.

## One transport per service

Everything Slack goes through `alma.slack.client.SlackNotifier` — the token, the
`slack_sdk` client, the name→ID cache, posting, reading history, reacting. The
alerts engine, the Slack messaging plugin, and the Inbox capture adapter are all
callers of that one object.

This is enforced, not just intended: `tests/test_channel_registry.py` fails if a
second `WebClient` appears anywhere, or if the Slack plugin imports an HTTP
library of its own. It did both, for a year — a `requests` implementation of the
same four Slack endpoints, with its own cache, running in the same process.

## One credential

The Slack bot token lives in the secret store (`data/secrets.json`, key
`slack.bot_token`), settable from **Settings → Channels** or `SLACK_TOKEN`.
There is no second copy. A `config/slack.json` from an older install is imported
once at startup by the migrator and never read again.

## Where rendering lives

> **How a paper LOOKS in a channel is transport-scoped. What a paper IS is
> application-scoped.**

Rendering belongs with the transport that knows the medium's markup — Block Kit
in `SlackNotifier`, MIME in `EmailNotifier`. Identity belongs to the application:
`application/inbound_capture.py` extracts identifiers, and it has never heard of
Slack.

That line is why capture is channel-agnostic. Any transport that honours
`inbox_schema` feeds the same pipeline; consolidation happens on the transport
side of the boundary, never across it.

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/plugins` | Every channel, its capabilities, and whether each direction is configured |
| `GET` | `/api/v1/plugins/{name}` | One channel |
| `POST` | `/api/v1/plugins/{name}/test` | Send a real test message through the production notifier (Activity envelope) |
| `GET` | `/api/v1/inbox/status` | Capture-side view: configured channels, what is waiting, what needs a human |
| `POST` | `/api/v1/inbox/sweep` | Poll the receive-capable channels now |

Configuration is **not** written through these routes. Settings owns it, because
Settings owns the secret store; a second config writer on the plugins router is
how the bot token ended up mirrored to disk in plaintext.

## Health

The Health page reports a channel as **half set up** when it has credentials but
a direction it supports cannot run — a Slack token with no channel to post into,
or with no capture channel nominated. An entirely unconfigured channel is not a
fault: every channel is opt-in.
