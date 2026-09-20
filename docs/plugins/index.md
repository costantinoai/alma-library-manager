---
title: Plugins
description: The four optional adapters ALMa ships — what each one does, and the rules that apply to all of them.
---

# Plugins

ALMa's features are core. Alerts, the Inbox and a paper's PDF work as part of
the app; a plugin is only the optional adapter that connects one of them to an
outside service. Nothing here is required to use ALMa.

Four ship with it:

| Plugin | What it adds | Used by |
|---|---|---|
| **[Slack](slack.md)** | Posts alert digests to a channel or a DM, and captures papers you send from your phone | [Alerts](../concepts/alerts.md), [Inbox](../concepts/inbox.md) |
| **[Email](email.md)** | Sends alert digests over SMTP | [Alerts](../concepts/alerts.md) |
| **[Open-access PDFs](open-access-pdfs.md)** | Finds a paper's PDF from arXiv, PubMed Central, OpenAlex, Unpaywall, Crossref, the publisher's page and Semantic Scholar | [Paper PDFs](../user-guide/reading-pdfs.md) |
| **[Shadow libraries](shadow-libraries.md)** | Tries the Sci-Hub and Anna's Archive mirrors you add, after every open-access source has failed | [Paper PDFs](../user-guide/reading-pdfs.md) |

They all live in one place: **Settings → Plugins**.

## What is true of every plugin

**It ships switched off.** A fresh install has no plugin on, and nothing in a
plugin runs until you turn it on yourself.

**Switching it on is what reveals its setup.** An off plugin shows one line
saying what it would do and a switch. Turn the switch on and its fields appear,
along with its **Test connection** button where it has one.

**Switching it off keeps its configuration.** Your token, addresses and
recipients stay where they were, secrets included. An off plugin is excluded
from new delivery choices, from automated alert sends, from Inbox capture
sweeps, and from PDF fetches — and its status pills disappear from Home. Turn it
back on and it picks up where it was. Switching off never deletes an Inbox
paper or a line of alert history.

**An off plugin reaches nothing.** This is enforced in the manifest, not by
each plugin remembering to check: while a plugin is off, it hands out no inbound
channel, and sending an alert or running its connection test raises
`PluginDisabledError` — the API answers **409** rather than quietly making the
call. Only self-description, configuration and a local status read stay
available, which is why listing plugins never opens a connection to anything.

**Secrets never touch `settings.json`.** Tokens, passwords and keys go to the
secret store (`data/secrets.json`, gitignored, mode `0600`) and come back
masked. Leaving a masked value in place keeps the stored one; clearing the field
deletes it.

## Related

* [External integrations](../concepts/channels.md) — where the core/adapter
  line is drawn, and the API behind these pages
* [Building an integration](../development/integrations.md) — the contract for
  writing a new one
