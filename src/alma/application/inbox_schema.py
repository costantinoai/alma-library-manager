"""The channel-agnostic Inbox contract.

The Inbox is a **buffer layer**: papers you sent yourself from somewhere else,
already full corpus citizens, parked at ``status='inbox'`` until you triage
them. Slack is the first delivery channel, but nothing below knows that. Any
channel — email, a Telegram bot, a share-sheet endpoint, an RSS bridge — becomes
a valid source by honouring this schema. That is the whole point: the capture
pipeline is written ONCE.

Three pieces:

* :class:`InboundMessage` — what a channel hands in. Normalised, channel-neutral.
* :class:`CaptureResult` — what the pipeline hands back. The channel turns this
  into whatever acknowledgement it can express (a Slack reaction, an email
  reply, an HTTP status).
* :class:`InboundChannel` — the protocol a channel implements: fetch new
  messages since a cursor, then acknowledge each outcome.

The persisted `inbox_messages` table (migration 34) is keyed on
``(channel, external_id)``. That pair is the **idempotency key**, and it is why
delivery can be at-least-once — polling a channel twice, or crashing halfway
through a batch, must never capture the same paper twice.

**Papers do NOT live here.** A resolved capture is a normal `papers` row that
enrichment, search, dedup and the semantic map all see immediately; the
`inbox_messages` row only records that a message arrived and what became of it.
See `docs/concepts/inbox.md`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------

#: What happened to one inbound message.
#:
#: ``resolved``   — identified a paper; it is in the corpus and in the Inbox.
#: ``duplicate``  — identified a paper already in your Library; deliberately NOT
#:                  parked in the Inbox (D2: never demote a save, and the Inbox
#:                  stays clean — a paper you already own needs no triage).
#: ``unresolved`` — no usable identifier, or nothing upstream recognised it.
#:                  Persisted so it can be fixed by hand; never silently dropped.
#: ``error``      — the pipeline itself failed (network, upstream 5xx). Distinct
#:                  from ``unresolved`` because this one is worth RETRYING.
CaptureOutcome = Literal["resolved", "duplicate", "unresolved", "error"]

#: Every outcome value, for validators and CHECK-style assertions.
CAPTURE_OUTCOMES: tuple[CaptureOutcome, ...] = (
    "resolved",
    "duplicate",
    "unresolved",
    "error",
)

#: Outcomes worth attempting again on a later sweep. `unresolved` is NOT here:
#: a New York Times link will never resolve, and retrying it forever is noise.
RETRYABLE_OUTCOMES: frozenset[str] = frozenset({"error"})


@dataclass(frozen=True, slots=True)
class ExtractedIdentifiers:
    """Whatever the extractor could pull out of a message, before resolution.

    All fields optional: a message may carry a bare DOI, a publisher URL with no
    DOI at all, or nothing but prose. Resolution tries these in the order
    OpenAlex handles best (``openalex_id`` → ``doi`` → ``arxiv_id`` → ``url`` →
    ``title``), mirroring `openalex_manual._resolve_work_from_inputs`.
    """

    doi: str | None = None
    arxiv_id: str | None = None
    openalex_id: str | None = None
    url: str | None = None
    title: str | None = None

    def is_empty(self) -> bool:
        """True when nothing resolvable was found — the `unresolved` fast path."""
        return not any(
            (self.doi, self.arxiv_id, self.openalex_id, self.url, self.title)
        )

    def as_dict(self) -> dict[str, str]:
        """Non-empty fields only — what gets persisted as `extracted_json`."""
        return {
            key: value
            for key, value in (
                ("doi", self.doi),
                ("arxiv_id", self.arxiv_id),
                ("openalex_id", self.openalex_id),
                ("url", self.url),
                ("title", self.title),
            )
            if value
        }


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """One captured message, normalised away from its channel's own shape.

    A channel adapter's ONLY job is turning its native payload into this. A
    Slack message, an IMAP email and an HTTP share-sheet POST all reduce to the
    same five meaningful fields, so the capture pipeline never branches on
    origin.
    """

    #: Which adapter produced this — ``'slack'``, ``'email'``, … Persisted, and
    #: used as the paper's `added_from` provenance when the row is brand new.
    channel: str

    #: Channel-unique, STABLE id for this message. Slack: the message `ts`.
    #: Email: the RFC-5322 `Message-ID`. It must be stable across re-fetches —
    #: it is half the idempotency key, so a value that changes between polls
    #: (a row number, an index) would re-capture the same paper forever.
    external_id: str

    #: When the CHANNEL says it arrived, ISO-8601. Not when we polled — ordering
    #: and the poll cursor both key on this, so a fabricated timestamp would
    #: silently reorder or skip messages (lessons.md: never fabricate timestamps).
    received_at: str

    #: The raw message body, verbatim. Kept for the extractor, for hand-fixing
    #: an unresolved capture, and as the audit trail of what you actually sent.
    text: str = ""

    #: Links the CHANNEL already knows about. Slack sends these pre-parsed, and a
    #: channel's own parse beats a regex over display text every time (Slack
    #: wraps links as `<url|label>`). Merged with regex hits, never replaced by
    #: them.
    urls: tuple[str, ...] = ()

    #: Channel-specific extras — Slack `channel_id`/`thread_ts`, email
    #: `subject`/`from`. Opaque to the pipeline, persisted as `metadata_json`
    #: so an adapter can find its way back to the original message to reply.
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Fail loudly at the boundary. A message missing either half of the
        # idempotency key cannot be deduplicated, and silently accepting it
        # would re-capture the same paper on every poll.
        if not str(self.channel or "").strip():
            raise ValueError("InboundMessage.channel is required")
        if not str(self.external_id or "").strip():
            raise ValueError(
                f"InboundMessage.external_id is required (channel={self.channel!r}) "
                "— it is the dedupe key for at-least-once delivery"
            )
        if not str(self.received_at or "").strip():
            raise ValueError(
                f"InboundMessage.received_at is required (channel={self.channel!r}) "
                "— the poll cursor keys on it"
            )


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """What the pipeline made of one :class:`InboundMessage`."""

    outcome: CaptureOutcome

    #: The corpus paper, when one was identified. None for unresolved/error.
    paper_id: str | None = None

    #: Display title of the resolved paper — so a channel can acknowledge with
    #: something human ("Saved: Attention Is All You Need") instead of an id.
    title: str | None = None

    #: What the extractor found. Persisted even on failure: it is the difference
    #: between "no DOI in the message" and "DOI found but upstream didn't know it".
    extracted: ExtractedIdentifiers = field(default_factory=ExtractedIdentifiers)

    #: Why it failed, for `unresolved` / `error`. Shown to the user, so write it
    #: for a human.
    error: str | None = None

    @property
    def is_retryable(self) -> bool:
        return self.outcome in RETRYABLE_OUTCOMES

    @property
    def captured(self) -> bool:
        """Did this message put a paper in front of the user to triage?"""
        return self.outcome == "resolved"


@runtime_checkable
class InboundChannel(Protocol):
    """What a delivery channel must implement to feed the Inbox.

    Deliberately tiny. A channel does NOT resolve papers, touch the database, or
    know what the Inbox is — it fetches and it acknowledges. Everything between
    those two calls is `application.inbound_capture`.
    """

    #: Stable channel id, matching `InboundMessage.channel` and the `channel`
    #: column. Also the `added_from` provenance stamped on brand-new papers.
    name: str

    def is_configured(self) -> bool:
        """Is this channel set up enough to poll? (Credentials, target, …)

        A channel that returns False is skipped silently — AI-is-opt-in applies
        here too: an unconfigured channel is not an error, it is simply off.
        """
        ...

    def fetch(self, *, since_cursor: str | None) -> list[InboundMessage]:
        """New messages after ``since_cursor``, oldest first.

        ``since_cursor`` is the highest `external_id` this channel has already
        processed (from `inbox_messages`), or None on the first ever poll.
        Returning already-seen messages is SAFE — the pipeline deduplicates on
        `(channel, external_id)` — so a channel may over-fetch when its API
        can't express an exact cursor. Under-fetching loses captures; prefer
        overlap.
        """
        ...

    def acknowledge(self, message: InboundMessage, result: CaptureResult) -> None:
        """Tell the user, in the channel, what happened.

        The receipt is what makes capture trustworthy from a phone: you flick a
        link and see it land without opening the app. Best-effort — an
        acknowledgement failure must never fail the capture, because the paper
        is already saved and the message is already recorded.
        """
        ...
