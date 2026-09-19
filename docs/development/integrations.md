---
title: Building an integration
description: The manifest, schema, activation, transport, and test contract for adding an external ALMa integration.
---

# Building an integration

An integration connects an external service to core [Alerts](../concepts/alerts.md),
core [Inbox](../concepts/inbox.md), core [paper PDFs](../user-guide/reading-pdfs.md),
or several of them. It is not a place to implement matching rules, paper
resolution, triage, scheduling, file storage, or Signal Lab.

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
| `x-alma-links` | Help links under the field, `[{"label", "url"}]`. A `url` starting with `/` is a path on the documentation site (e.g. `/user-guide/reading-pdfs/#shadow-libraries`); anything else opens as given. Use it wherever a value has to be looked up elsewhere (addresses, keys) |

The manifest's `docs_path` becomes the card's **Guide** link, on the same
documentation site.

## Add capabilities

For `receive`, implement `InboundChannel` from
`alma.application.inbox_schema` and set `inbound_factory`. The adapter only
fetches `InboundMessage` values and acknowledges `CaptureResult`; the core
capture pipeline owns identifiers, network resolution, corpus landing,
idempotency, and Inbox state.

For `send`, use `alert_sender`. The core Alert engine owns rules, matches,
schedules, per-integration deduplication, activation checks, history, and
Activity. The integration renders/delivers the supplied payload.

For `pdf_source`, set `pdf_source_factory` to a callable returning the plugin's
`PdfSource` objects (`alma.application.pdf_schema`) in run order, and report
`can_fetch` in `status`. A source has an `id`, a `label`, a `tier` (`open` or
`shadow`) and one method, `candidates(ref: PaperRef) -> list[PdfCandidate]`:
network only, never the database. It returns the URLs it believes serve the
paper's PDF, best first, or raises `SourceSkippedError` when it cannot run at
all (no key, email or mirror address), so the attempt reads as *skipped*, not
as a miss, and `SourceBlockedError` when its own lookup page is behind a bot
wall, so it reads as *blocked*. The core owns everything after that: the
download (every redirect hop checked against private and tailnet addresses,
one HTML hop to the PDF a page advertises — `citation_pdf_url`, a `#pdf`
viewer element, a pdf.js `viewer.html?file=`), the byte cap, bot-wall
detection, pypdf verification against the paper's DOI or title, the attempts
ledger (one row per source; when a source returned several candidates, its
detail says what each host answered), storage and serving. The registry runs
every `open` source of every enabled plugin before any `shadow` source, and a
fetch runs only when the user asks for one paper's PDF.

A source that knows how its site says "not here" declares it instead of
parsing pages itself: `PdfCandidate.absent_titles` lists lower-case phrases
that, in a landing page's `<title>`, turn "no PDF on the page" into "does not
have this paper" (Sci-Hub: `"not available through sci-hub"`). A redirect to
the site's home page reads the same way without any declaration.

A source that needs its own transport (a browser TLS fingerprint, for example)
wraps it in a `SourcePolicy` with a `session_factory` and passes the client from
`client_for_policy()` as `PdfCandidate.transport`, so it keeps ALMa's pacing,
retries, diagnostics and network switch. The client's session is per thread,
so a login done in `candidates()` (a form `post(..., data=…)`) carries its
cookie into the core's download of the same fetch.

Everything a PDF source touches is untrusted input, and the rules are enforced
by the core, not by each plugin:

- **Guarded clients only.** The download step refuses a client whose policy
  lacks `guard_addresses=True`. A guarded client passes every request and every
  redirect hop through `core.url_safety.vet_url` (http(s), ports 80/443, no
  credentials or disguised hosts, and a host that resolves to public addresses
  only — never loopback, private, link-local or the tailnet), pins the
  connection to the checked addresses (`GuardedAdapter` for `requests`,
  `CURLOPT_RESOLVE` plus a peer check for curl), follows redirects itself
  (never re-sending a POST body on 307/308), and caps a non-streamed body
  (`max_body_bytes`). Give a curl session `trust_env=False` and
  `PROTOCOLS_STR` / `REDIR_PROTOCOLS_STR` = `"http,https"`, and keep one session
  per trust domain (a member login must not ride along to another site).
- **Never parse a PDF yourself.** The core opens every file in
  `pdfs.sandbox` — a child process with memory, CPU, file-size and time limits
  — and rewrites a fetched file without its active content before keeping it.
- **Mirror pages:** set `PdfCandidate.follow_anchors=False` so the landing hop
  follows only the page's viewer element, not its advertising links.
- **Remote text:** anything a server said that you put in an exception message
  or a result passes through `core.url_safety.clean_remote_text` (control and
  bidi characters out, bounded). The attempts ledger applies it again.

A `pdf_source` plugin's connectivity test should run the real path: build a
`PaperRef` for a well-known paper, take each address's candidate from the
plugin's own sources and pass it to `alma.application.pdfs.download.download_candidate`,
discarding the file. Return `results` rows `{source, address, state, severity}`
(`severity` from `lib/severity`: `ok` / `warning` / `critical`); Settings lists
them under the test button. A home page that loads proves nothing about a
walled paper page.

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
- a replayed PDF fetch per source (the `tests/_cassette.py` layer, with a real
  PDF from `tests/_pdf_fixtures.py`), when `pdf_source` is declared;
- connectivity action dispatches through the generic manifest route.

Run the backend suite, frontend typecheck/tests/build, and strict MkDocs build.
