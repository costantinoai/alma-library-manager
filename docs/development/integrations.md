---
title: Building an integration
description: The manifest, schema, activation, transport, and test contract for adding an external ALMa integration.
---

# Building an integration

An integration connects an external service to core [Alerts](../concepts/alerts.md),
core [Inbox](../concepts/inbox.md), or both. It is not a place to implement
matching rules, paper resolution, triage, scheduling, or Signal Lab.

## Package shape

Create `src/alma/plugins/<id>/__init__.py`. The package exports one
`PluginManifest` and owns:

1. a strict Pydantic config model;
2. masked `read_config` and validated `write_config`;
3. direction-specific status;
4. transport callbacks for its declared capabilities;
5. an optional connectivity test.

```python
from pydantic import BaseModel, Field

from alma.plugins.configs import StrictPluginConfig
from alma.plugins.manifest import SEND, PluginManifest


class WebhookConfig(StrictPluginConfig):
    url: str = Field(
        "",
        title="Webhook URL",
        description="Destination for Alert digests.",
        json_schema_extra={
            "x-alma-secret": True,
            "x-alma-order": 10,
        },
    )


def read_config(_db) -> WebhookConfig:
    # Read the current secret, return only a masked value.
    ...


def write_config(_db, config: BaseModel) -> None:
    parsed = WebhookConfig.model_validate(config)
    # Persist secrets through alma.core.secrets; never plaintext settings.
    ...


def status(_db) -> dict:
    return {
        "configured": ...,
        "can_send": ...,
        "can_receive": False,
    }


async def send_alert(papers: list[dict], alert_name: str) -> bool:
    # Delegate to this service's one transport.
    ...


async def test_connection() -> dict:
    return {"ok": True, "target": "...", "message": "Test delivered"}


WEBHOOK_PLUGIN = PluginManifest(
    id="webhook",
    display_name="Webhook",
    version="1.0.0",
    description="Deliver Alert digests to an HTTPS webhook.",
    kind="integration",
    capabilities=(SEND,),
    config_model=WebhookConfig,
    read_config=read_config,
    write_config=write_config,
    status_factory=status,
    docs_path="/development/integrations/",
    alert_sender=send_alert,
    connection_tester=test_connection,
    action_ids=("test",),
)
```

`StrictPluginConfig` rejects unknown fields. The API serves
`WebhookConfig.model_json_schema()` and validates writes with the same class.
Supported UI extensions are:

| JSON Schema key | Meaning |
|---|---|
| `title`, `description`, `default` | Field copy and initial value |
| `minimum`, `maximum` | Numeric limits |
| `x-alma-secret` | Password input; value must be masked on reads |
| `x-alma-order` | Stable field order |
| `x-alma-advanced` | Put behind advanced disclosure |
| `x-alma-step` | Numeric input step |

## Add capabilities

For `receive`, implement `InboundChannel` from
`alma.application.inbox_schema` and set `inbound_factory`. The adapter only
fetches `InboundMessage` values and acknowledges `CaptureResult`; the core
capture pipeline owns identifiers, network resolution, corpus landing,
idempotency, and Inbox state.

For `send`, use `alert_sender`. The core Alert engine owns rules, matches,
schedules, per-integration deduplication, activation checks, history, and
Activity. The integration renders/delivers the supplied payload.

If both directions use the same external API, they must share one transport and
credential owner.

## Register and migrate

Import the package's manifest in `alma.plugins.registry` and add it once to
`PLUGINS`. Add `plugins.<id>.enabled` to `DEFAULT_SETTINGS`, advance the settings
schema, and write a forward migration. Existing configured installs may be
activated by migration; fresh unconfigured integrations default off. Runtime
code must never infer activation from credentials.

Add the integration's Settings icon only if a specific icon exists; the generic
icon already works. Alert delivery choices and schema fields are discovered
from `/api/v1/plugins`, so no service-specific form or route is added.

## Required tests

- registry identity and declared capabilities;
- generated schema equals the validating model and forbids extra fields;
- activate → deactivate retains config;
- inactive adapters neither send nor poll;
- secret reads are masked and plaintext never enters settings;
- transport callback uses the service's one client;
- inbound idempotency and acknowledgement, when `receive` is declared;
- connectivity action dispatches through the generic manifest route.

Run the backend suite, frontend typecheck/tests/build, and strict MkDocs build.
