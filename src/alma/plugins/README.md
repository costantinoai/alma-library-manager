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

See [the MkDocs integration guide](../../../docs/development/integrations.md)
for the complete skeleton, schema extensions, registration, and tests.
