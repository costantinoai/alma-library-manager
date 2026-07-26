# Messaging plugins — the outbound half of a delivery channel

```
plugins/
├── base.py     # MessagingPlugin: send an already-rendered message
└── slack/      # Slack implementation (delegates to alma.slack.client)
```

A plugin answers one question: **can ALMa deliver a finished message to this
service, and how is it configured?** Its mirror image is
`alma.application.inbox_schema.InboundChannel` — *what can this service deliver
to ALMa?*

Neither is a registry. Both directions are listed once in **`alma.channels`**,
per channel, with a capability flag. Read
[`docs/concepts/channels.md`](../../../docs/concepts/channels.md) first — it
owns the contract; this file only covers how to write the send half.

## Scope line

> How a paper **looks** in a channel is transport-scoped.
> What a paper **is** is application-scoped.

So a plugin does *not* render papers. It takes a string. Rendering lives with
the transport that knows the medium's markup (Block Kit in `SlackNotifier`, MIME
in `EmailNotifier`); identity lives in `application/inbound_capture.py`.

This is why `format_publications` / `format_authors` / `format_test_message` are
no longer on the base class. They used to be, with a FIXME asking whether they
should be shared. The answer turned out to be neither: they rendered the old
plain-text digest that the Block Kit alerts pipeline replaced, and once that
pipeline was deleted, nothing called either copy.

## A plugin must not own a transport

`SlackPlugin` is ~120 lines and makes no network calls. Every byte on the wire
goes through `alma.slack.client.SlackNotifier`, which owns the token, the
`slack_sdk` client and the name→ID cache.

It was not always so: this package used to carry a full second Slack client
built on `requests`, with its own caches, running alongside the notifier in the
same process. Two transports meant two error vocabularies and a credential
mirrored to a plaintext file so both could read it. `tests/test_channel_registry.py`
now fails if either grows back.

## Writing a new plugin

```python
from typing import Any

from alma.plugins.base import MessagingPlugin, PluginConfigError


class DiscordPlugin(MessagingPlugin):
    @property
    def name(self) -> str: return "discord"

    @property
    def display_name(self) -> str: return "Discord"

    @property
    def version(self) -> str: return "1.0.0"

    @property
    def description(self) -> str: return "Post digests to a Discord channel"

    def _validate_config(self) -> None:
        if "webhook_url" not in self.config:
            raise PluginConfigError("Missing webhook_url")

    def send_message(self, message: str, target: str) -> bool:
        return self._client().post(target, message)   # your transport, one owner

    def test_connection(self) -> bool:
        return self._client().check_auth()

    def get_config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "required": ["webhook_url"],
            "properties": {
                "webhook_url": {"type": "string", "secret": True},
            },
        }
```

Then add a `ChannelDescriptor` to `alma.channels.CHANNELS` with
`capabilities=(SEND,)` and an `outbound_factory` that builds it from the current
credentials. Registration is explicit — there is no auto-discovery, deliberately:
the list of things that can message you should be something you can read.

Credentials go in the secret store (`alma.core.secrets`), set from Settings.
Never write a config file holding a token.
