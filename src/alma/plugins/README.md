# External integration plugins

Inbox and Alerts are core features. Packages in this directory adapt external
services to those core protocols.

```
plugins/
├── manifest.py       # identity, capability, schema, activation, actions
├── configs.py        # strict shared config base
├── registry.py       # explicit catalogue
├── slack/            # send + receive; one Slack transport
└── email/            # SMTP send
```

Each integration package owns its Pydantic config model, masked read/write
mapping, direction status, transport callbacks, and exported `PluginManifest`.
`registry.PLUGINS` is the only list. There is no auto-discovery.

The core seams are intentionally different:

- `send`: the manifest's async `AlertSender` receives the paper payload and
  Alert name;
- `receive`: `application.inbox_schema.InboundChannel` fetches normalized
  messages and acknowledges `CaptureResult`.

An integration may implement either or both. It must use one transport per
external service and store secrets only through `alma.core.secrets`.

Everything here ships **switched off**. `PluginManifest` enforces that itself:
while `plugins.<id>.enabled` is false, `inbound_channel()` yields nothing and
`send_alert()` / `test_connection()` raise `PluginDisabledError`. Only
self-description, configuration and `status()` stay available — and
`status_factory` must be local, cheap and side-effect-free, so that listing
plugins never loads a transport.

See [the MkDocs integration guide](../../../docs/development/integrations.md)
for the complete skeleton, schema extensions, registration, and tests.
