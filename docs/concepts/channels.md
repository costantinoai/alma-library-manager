---
title: External integrations
description: How optional Slack, SMTP, and future adapters connect ALMa's core Inbox and Alerts to other services.
---

# External integrations

**Inbox and Alerts are core ALMa features.** They do not become optional merely
because no external service is connected. Integration plugins are the adapters
that let those core features exchange information with Slack, SMTP, or a future
channel.

| Capability | Core owner | Integration responsibility |
|---|---|---|
| `send` | [Alerts](alerts.md) selects papers, schedules, deduplicates, and records outcomes | Render and deliver the finished digest |
| `receive` | [Inbox](inbox.md) resolves papers, lands corpus rows, deduplicates, and owns triage | Fetch external messages and acknowledge outcomes |

Slack implements both directions. SMTP currently implements `send` only. One
Slack activation controls both adapters because they share one external
integration and credential; status still reports each direction separately.

## One explicit manifest registry

`alma.plugins.registry.PLUGINS` is the only integration catalogue. Every entry is
a `PluginManifest` with:

- stable identity and version;
- `send` / `receive` capabilities;
- one explicit activation flag;
- one strict Pydantic configuration model;
- generated JSON Schema, storage mapping, masked secret reads, and status;
- optional Alert sender, Inbox adapter, and connectivity-test action.

Registration is explicit. An unregistered id cannot enter an Alert row, deliver
a digest, or put a message into Inbox. Signal Lab is not in this registry: it is
a native intelligence feature.

The capability protocols remain separate. Outbound integrations implement the
manifest's `AlertSender` callback; inbound integrations implement
`alma.application.inbox_schema.InboundChannel`. A send-only integration does not
grow fake receive methods.

## Activation is not deletion

Turning an integration off in **Settings → Plugins** retains its
configuration and secrets. ALMa then:

- excludes it from new delivery choices and automated Alert sends;
- excludes its inbound adapter from Inbox capture sweeps;
- hides its direction-status pills on Home.

A manual connectivity test remains available after reactivation. Activation
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

For the package contract and extension checklist, see
[Building an integration](../development/integrations.md).
