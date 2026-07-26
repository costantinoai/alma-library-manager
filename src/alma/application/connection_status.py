"""What ALMa depends on from the outside world, and whether it last worked.

Home asks one question this module answers: *is anything that has to work
silently not working?* A capture channel, the two metadata providers and the
embedding stack are what the whole product sits on — when one breaks, every
downstream surface degrades quietly (an empty Feed, a stalled vector backfill,
an Inbox that never fills) and nothing on any page says why.

**Everything is named by its real name.** The capture entries come from the
channel registry (`alma.channels`), so they read "Slack", not an abstract
"Capture"; the AI entry names the stack that would actually run (SPECTER2 or
OpenAI). A status rail that invents its own vocabulary makes the user translate
before they can act.

**Derived from local state, never from a live probe.** Home is a pure read on a
hot path; probing three providers would make it slow AND would make a GET
perform I/O against third parties. `operation_status` already records the
outcome of every job that talked to each provider, so "did it work" is a local
question with a durable answer, and support for the local AI stack is a
`find_spec` away. The live probes stay where the user asks for them explicitly —
Settings → Connections.

The trade-off is stated in the payload rather than hidden: every connection
carries `checked_at`, so "OpenAlex was fine" is always qualified by *when*.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Literal

logger = logging.getLogger(__name__)

#: Job outcomes that PROVE something about a connection.
#:
#: `cancelled` is deliberately absent and is the whole reason this set exists:
#: a cancelled run says the user (or a shutdown) stopped the job, not that the
#: provider answered. Reading it as either success or failure would be a lie,
#: and cancellations are the single most common row in the ledger.
CONCLUSIVE_STATUSES = ("completed", "noop", "failed")

#: Modules the local SPECTER2 encoder needs. Checked with `find_spec`, never
#: imported — importing torch on a page load would cost seconds.
LOCAL_AI_MODULES = ("torch", "transformers", "adapters", "numpy")

ConnectionState = Literal["ok", "failed", "running", "unknown", "not_configured"]

SETTINGS_CHANNELS = "#/settings?anchor=channels"
SETTINGS_CONNECTIONS = "#/settings?anchor=connections"
SETTINGS_AI = "#/settings?anchor=ai"


@dataclass(frozen=True)
class Dependency:
    """One external dependency, and how to tell whether it last worked.

    `operation_prefixes` are matched with `LIKE prefix || '%'` because the
    ledger's keys are parameterized (`papers.rehydrate_metadata:openalex:
    metadata:target:<id>`). Every listed prefix must be a job that actually
    talks to this provider — a job that merely *reads the DB afterwards* would
    report a green light for a dead connection.
    """

    key: str
    label: str
    #: What the user loses when this breaks. Shown when the state is not ok.
    stake: str
    operation_prefixes: tuple[str, ...]
    #: Where the user goes to fix or re-probe it.
    href: str


#: The metadata providers. Both work without an API key (slower, rate-limited),
#: so neither has a "not configured" state — they are always a dependency.
PROVIDERS: tuple[Dependency, ...] = (
    Dependency(
        key="openalex",
        label="OpenAlex",
        stake="Feed monitors, author refreshes and paper metadata stall.",
        operation_prefixes=(
            "papers.rehydrate_metadata:openalex",
            "authors.deep_refresh",
            "authors.rehydrate_metadata",
        ),
        href=SETTINGS_CONNECTIONS,
    ),
    Dependency(
        key="semantic_scholar",
        label="Semantic Scholar",
        stake="New papers never get vectors, so Discovery stops seeing them.",
        operation_prefixes=(
            "ai.backfill_s2_vectors",
            "ai.title_resolution_sweep",
        ),
        href=SETTINGS_CONNECTIONS,
    ),
)

#: Jobs that prove the embedding compute path ran. Deliberately NOT the S2
#: vector backfill, which proves Semantic Scholar answered — a different fact,
#: already reported by its own entry.
AI_OPERATION_PREFIXES = ("ai.compute_embeddings",)


def _last_conclusive_run(
    db: sqlite3.Connection, prefixes: tuple[str, ...]
) -> dict[str, Any] | None:
    """The most recent run of any of `prefixes` that reached a verdict."""
    if not prefixes:
        return None
    like_clause = " OR ".join("operation_key LIKE ? || '%'" for _ in prefixes)
    status_clause = ",".join("?" for _ in CONCLUSIVE_STATUSES)
    row = db.execute(
        f"""
        SELECT operation_key, status, message, error,
               COALESCE(finished_at, updated_at) AS at
        FROM operation_status
        WHERE ({like_clause})
          AND status IN ({status_clause})
        ORDER BY at DESC
        LIMIT 1
        """,
        (*prefixes, *CONCLUSIVE_STATUSES),
    ).fetchone()
    return dict(row) if row else None


def _running(db: sqlite3.Connection, prefixes: tuple[str, ...]) -> bool:
    """Whether one of these jobs is in flight right now."""
    if not prefixes:
        return False
    like_clause = " OR ".join("operation_key LIKE ? || '%'" for _ in prefixes)
    row = db.execute(
        f"SELECT 1 FROM operation_status WHERE ({like_clause}) "
        "AND status = 'running' LIMIT 1",
        prefixes,
    ).fetchone()
    return row is not None


def _record(
    *,
    key: str,
    label: str,
    stake: str,
    href: str,
    configured: bool,
    unconfigured_detail: str,
    running: bool,
    last: dict[str, Any] | None,
) -> dict[str, Any]:
    """One connection's reportable state. Ordered most-decisive first."""
    if not configured:
        state: ConnectionState = "not_configured"
        detail = unconfigured_detail
    elif last and str(last.get("status")) == "failed":
        state = "failed"
        detail = str(last.get("error") or last.get("message") or "").strip() or (
            "The last attempt failed."
        )
    elif running:
        state = "running"
        detail = "Checking now."
    elif last:
        state = "ok"
        detail = str(last.get("message") or "").strip() or "Last attempt succeeded."
    else:
        # Configured but never exercised. Honest "unknown" rather than a green
        # dot for a connection that has literally never been used.
        state = "unknown"
        detail = "Not used yet, so there is nothing to report."

    return {
        "key": key,
        "label": label,
        "state": state,
        "detail": detail,
        # Only meaningful when the state is not ok — the UI decides whether to
        # spend the words, but the fact travels either way.
        "stake": stake,
        "checked_at": (last or {}).get("at"),
        "href": href,
    }


def _capture_connections(db: sqlite3.Connection) -> list[dict[str, Any]]:
    """One entry per receive-capable channel, under its own display name.

    All capture channels are swept by the same job, so they share one ledger
    outcome. That is honest today (Slack is the only receiver) and stays honest
    when a second one lands: the sweep really does succeed or fail as a batch,
    and a per-channel verdict would have to be invented.
    """
    try:
        from alma.channels import RECEIVE, get_channel_registry
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Channel registry unavailable: %s", exc)
        return []

    prefixes = ("inbox.capture_sweep",)
    last = _last_conclusive_run(db, prefixes)
    running = _running(db, prefixes)

    records: list[dict[str, Any]] = []
    for descriptor in get_channel_registry().with_capability(RECEIVE):
        # "Configured enough to poll" is exactly what `inbound_channel()`
        # answers — the same gate the sweep itself uses, so the rail cannot
        # claim a channel is set up that the sweep would skip.
        try:
            configured = descriptor.inbound_channel() is not None
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Channel %s could not report: %s", descriptor.name, exc)
            configured = False
        records.append(
            _record(
                key=f"capture:{descriptor.name}",
                label=descriptor.display_name,
                stake=(
                    f"Papers you send yourself from {descriptor.display_name} "
                    "stop arriving in your Inbox."
                ),
                href=SETTINGS_CHANNELS,
                configured=configured,
                unconfigured_detail=(
                    f"{descriptor.display_name} capture is not set up yet."
                ),
                running=running,
                last=last if configured else None,
            )
        )
    return records


def _ai_connection(db: sqlite3.Connection) -> dict[str, Any]:
    """Whether this machine can actually compute embeddings, and with what.

    Named for the stack that would really run — "SPECTER2" when the local
    torch/adapters stack is importable, "OpenAI embeddings" when only the
    hosted provider is configured. Support is checked with `find_spec` and a
    settings read: no imports (torch costs seconds) and no network.

    This one genuinely has a `not_configured` state, unlike the metadata
    providers: an environment with no embedding stack cannot compute vectors at
    all, and saying so is the whole point of the pill.
    """
    from alma.ai.import_state import module_available

    local_ready = all(module_available(name) for name in LOCAL_AI_MODULES)
    missing = [name for name in LOCAL_AI_MODULES if not module_available(name)]

    hosted_ready = False
    if not local_ready:
        try:
            from alma.ai.providers import OpenAIProvider

            hosted_ready = OpenAIProvider().is_available()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("OpenAI availability check failed: %s", exc)

    if local_ready:
        label = "SPECTER2"
    elif hosted_ready:
        label = "OpenAI embeddings"
    else:
        label = "Embeddings"

    return _record(
        key="ai",
        label=label,
        stake="Nothing gets a vector here, so Discovery and the maps stand still.",
        href=SETTINGS_AI,
        configured=local_ready or hosted_ready,
        unconfigured_detail=(
            "This environment cannot compute embeddings — missing "
            + ", ".join(missing)
            + ". Papers still get vectors from Semantic Scholar."
        ),
        running=_running(db, AI_OPERATION_PREFIXES),
        last=_last_conclusive_run(db, AI_OPERATION_PREFIXES),
    )


def assess_connections(db: sqlite3.Connection) -> list[dict[str, Any]]:
    """Report every external connection Home watches. Pure read.

    Returns one uniform record per connection in a stable order — capture
    channels, then metadata providers, then the embedding stack — so the rail
    never reorders itself between loads.
    """
    from alma.api.helpers import table_exists

    if not table_exists(db, "operation_status"):
        return []

    providers = [
        _record(
            key=dependency.key,
            label=dependency.label,
            stake=dependency.stake,
            href=dependency.href,
            # Both providers work keyless, so there is no unconfigured state.
            configured=True,
            unconfigured_detail="",
            running=_running(db, dependency.operation_prefixes),
            last=_last_conclusive_run(db, dependency.operation_prefixes),
        )
        for dependency in PROVIDERS
    ]
    return [*_capture_connections(db), *providers, _ai_connection(db)]
