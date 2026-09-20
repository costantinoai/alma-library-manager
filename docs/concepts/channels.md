---
title: External integrations
description: How optional plugins connect ALMa's core Alerts, Inbox and paper PDFs to outside services.
---

# External integrations

**Alerts, the Inbox and a paper's PDF are core ALMa features.** They do not
become optional merely because no external service is connected. Integration
plugins are the adapters that let those core features reach outside — to Slack,
to an SMTP server, to a PDF host.

Each plugin declares which of three capabilities it implements:

| Capability | Core owner | Integration responsibility |
|---|---|---|
| `send` | [Alerts](alerts.md) selects papers, schedules, deduplicates, and records outcomes | Render and deliver the finished digest |
| `receive` | [Inbox](inbox.md) resolves papers, lands corpus rows, deduplicates, and owns triage | Fetch external messages and acknowledge outcomes |
| `pdf_source` | [Paper PDFs](../user-guide/reading-pdfs.md) runs the sources in order, verifies the file against the paper, and stores it | Contribute candidate sources and fetch a file from each |

Four ship: [Slack](../plugins/slack.md) (`send` + `receive`),
[Email](../plugins/email.md) (`send`), and two PDF sources,
[Open-access PDFs](../plugins/open-access-pdfs.md) and
[Shadow libraries](../plugins/shadow-libraries.md). One Slack activation
controls both of its directions because they share one integration and one
credential; status still reports each direction separately.

Every plugin's own setup lives on its page. [Plugins](../plugins/index.md) is
the index, and states the rules that hold for all of them.

## One explicit manifest registry

`alma.plugins.registry.PLUGINS` is the only integration catalogue. Every entry is
a `PluginManifest` with:

- stable identity and version;
- its declared capabilities;
- one explicit activation flag;
- one strict Pydantic configuration model;
- generated JSON Schema, storage mapping, masked secret reads, and status;
- optional Alert sender, Inbox adapter, PDF sources, and connectivity-test
  action.

Registration is explicit. An unregistered id cannot enter an Alert row, deliver
a digest, put a message into Inbox, or be asked for a PDF. Not in this
registry: anything that is a native intelligence feature.

The capability protocols remain separate. Outbound integrations implement the
manifest's `AlertSender` callback; inbound integrations implement
`alma.application.inbox_schema.InboundChannel`; PDF integrations return
`PdfSource` values from `pdf_source_factory`. A send-only integration does not
grow fake receive methods.

## Activation is not deletion

Every plugin ships switched off, and `PluginManifest` enforces that itself
rather than trusting each package to check: while a plugin is off,
`inbound_channel()` yields nothing and `send_alert()` / `test_connection()`
raise `PluginDisabledError`, which the API returns as **409**. Only
self-description, configuration and `status()` stay available — and
`status_factory` must be local and side-effect-free, so listing plugins never
loads a transport.

Turning one off again in **Settings → Plugins** retains its configuration and
secrets. ALMa then:

- excludes it from new delivery choices and automated Alert sends;
- excludes its inbound adapter from Inbox capture sweeps;
- excludes its sources from PDF fetches;
- hides its direction-status pills on Home.

Switch it back on and the connectivity test is available again. Activation
never purges Inbox papers or Alert history.

## One transport and one credential

Everything Slack goes through `alma.slack.client.SlackNotifier`: token,
`slack_sdk` client, channel resolution, posting, history reads, and reactions.
The Slack manifest and Inbox adapter call that transport; neither owns another
HTTP client.

Secrets live only in `alma.core.secrets`. Settings shows masked values and the
server-generated schema marks secret fields with `x-alma-secret`. A legacy
`config/slack.*` file is imported once by storage migration and never read by
runtime code.

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/plugins` | All manifests, capabilities, schema, activation, and status |
| `GET` | `/api/v1/plugins/{id}` | One manifest |
| `PUT` | `/api/v1/plugins/{id}/enabled` | Activate/deactivate without deleting config |
| `GET` | `/api/v1/plugins/{id}/config` | Read validated config with masked secrets |
| `PUT` | `/api/v1/plugins/{id}/config` | Strictly validate and replace config |
| `POST` | `/api/v1/plugins/{id}/test` | Run the manifest test through Activity |
| `GET` | `/api/v1/inbox/status` | Core Inbox capture status |
| `POST` | `/api/v1/inbox/sweep` | Core Inbox action: poll active receive adapters |

The same Pydantic model validates writes and produces `config_schema`; the
frontend does not maintain a duplicate field list.

For each plugin's setup, see the [plugin pages](../plugins/index.md). For the
package contract and extension checklist, see
[Building an integration](../development/integrations.md).
