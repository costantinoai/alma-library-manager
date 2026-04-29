"""Slack notification client using slack_sdk and Block Kit formatting.

This module is responsible for actually delivering messages to Slack.
It is separate from the plugin system (alma.plugins.slack) so that
the alert evaluation engine can use it directly without going through
the legacy plain-text formatting pipeline.

Configuration is resolved via:
1. Explicit constructor arguments (token, default_channel)
2. Environment variables: SLACK_TOKEN / SLACK_CHANNEL
3. Unified secret store (token) + settings.json (channel)
4. Plugin config: config/slack.json or config/slack.config (api_token, default_channel)
"""

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Maximum papers per Slack message (Block Kit has a 50-block limit)
_MAX_PAPERS_PER_MESSAGE = 15


class SlackResolveError(RuntimeError):
    """Raised when a Slack channel/user name cannot be resolved to an ID."""


class SlackNotifier:
    """Send rich Block Kit messages to Slack channels.

    Args:
        token: Slack Bot User OAuth Token.
        default_channel: Fallback channel name or ID when none is specified.

    If *token* is ``None`` the notifier operates in **dry-run mode**: all
    public methods log the intended action but never call the Slack API.
    This allows callers to construct a notifier unconditionally and degrade
    gracefully when Slack is not configured.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        default_channel: Optional[str] = None,
    ) -> None:
        self._token = token
        self._default_channel = default_channel
        self._client = None  # lazy-initialized WebClient
        # Cache: user-supplied name (channel name OR display name) → Slack channel ID.
        # Avoids re-listing on every send within a single process lifetime.
        self._resolved_channel_cache: Dict[str, str] = {}

        if self._token:
            logger.info(
                "SlackNotifier initialized (token=%s..., channel=%s)",
                self._token[:10],
                self._default_channel or "<not set>",
            )
        else:
            logger.warning(
                "SlackNotifier initialized without a token; "
                "messages will not be sent"
            )

    # ------------------------------------------------------------------
    # Lazy WebClient initialization
    # ------------------------------------------------------------------

    def _get_client(self):
        """Return a ``slack_sdk.WebClient``, creating it on first use.

        Raises:
            RuntimeError: If no token was provided.
        """
        if self._client is not None:
            return self._client

        if not self._token:
            raise RuntimeError(
                "Slack token is not configured. Set SLACK_TOKEN env var "
                "or configure slack_token in Settings."
            )

        from slack_sdk import WebClient

        self._client = WebClient(token=self._token)
        return self._client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def is_configured(self) -> bool:
        """Return ``True`` when a Slack token is available."""
        return bool(self._token)

    def resolve_channel(self, channel: Optional[str] = None) -> str:
        """Return the effective channel string, falling back to *default_channel*.

        This returns the user-supplied label (which may be a channel name like
        ``general``, a Slack ID like ``C0123…``, or a display name like
        ``Andrea Costantino``). Resolution to a concrete Slack ID happens at
        send time via :meth:`_resolve_target`.

        Raises:
            ValueError: If no channel can be determined.
        """
        ch = channel or self._default_channel
        if not ch:
            raise ValueError(
                "No Slack channel specified and no default_channel configured"
            )
        return ch

    # ------------------------------------------------------------------
    # Channel-name / display-name resolution
    # ------------------------------------------------------------------

    def _resolve_target(self, target: str) -> str:
        """Resolve a Slack channel name or user display name to a channel ID.

        Mirrors ``scholar-slack-bot``'s ``send_to_slack`` semantics: try the
        input as a channel name first; if that misses, try it as a user
        display name and open a DM. Already-formed Slack IDs (``C…`` /
        ``D…`` / ``G…``) and explicitly prefixed names (``#general``) pass
        straight through.

        Resolved IDs are cached on the notifier instance so a long-lived
        process doesn't re-list workspace members on every send.

        Raises:
            SlackResolveError: When the target cannot be resolved.
        """
        if not target:
            raise SlackResolveError("empty Slack target")

        # Cache hit — fastest path.
        if target in self._resolved_channel_cache:
            return self._resolved_channel_cache[target]

        # Already a Slack ID (channel C…, group G…, DM D…). Use directly.
        if len(target) >= 9 and target[0] in {"C", "D", "G"} and target[1:].isalnum():
            self._resolved_channel_cache[target] = target
            return target

        # Pre-prefixed channel names (#general): chat.postMessage accepts them
        # but `conversations_open` won't, so still pass-through.
        if target.startswith("#"):
            self._resolved_channel_cache[target] = target
            return target

        bare = target.lstrip("@").strip()
        if not bare:
            raise SlackResolveError(f"empty Slack target after strip: {target!r}")

        client = self._get_client()

        # Try as channel name first.
        try:
            resp = client.conversations_list(
                types="public_channel,private_channel",
                limit=1000,
            )
            for ch in resp.get("channels", []) or []:
                name = str(ch.get("name") or "").strip().lower()
                if name and name == bare.lower():
                    resolved = str(ch.get("id"))
                    self._resolved_channel_cache[target] = resolved
                    logger.info(
                        "Resolved %r via conversations.list -> %s", target, resolved
                    )
                    return resolved
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning(
                "conversations.list lookup for %r failed: %s", target, exc
            )

        # Fall back to user display name -> open DM.
        try:
            resp = client.users_list(limit=1000)
            for user in resp.get("members", []) or []:
                if user.get("deleted") or user.get("is_bot"):
                    continue
                profile = user.get("profile") or {}
                candidates = {
                    str(user.get("name") or "").strip(),
                    str(user.get("real_name") or "").strip(),
                    str(profile.get("display_name") or "").strip(),
                    str(profile.get("real_name") or "").strip(),
                }
                if any(c and c.lower() == bare.lower() for c in candidates):
                    user_id = str(user.get("id") or "")
                    if not user_id:
                        continue
                    dm = client.conversations_open(users=user_id)
                    if dm.get("ok"):
                        channel_obj = dm.get("channel") or {}
                        resolved = str(channel_obj.get("id") or "")
                        if resolved:
                            self._resolved_channel_cache[target] = resolved
                            logger.info(
                                "Resolved %r via users.list -> DM %s",
                                target,
                                resolved,
                            )
                            return resolved
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("users.list lookup for %r failed: %s", target, exc)

        raise SlackResolveError(f"channel_not_found: {target!r}")

    # ---- Paper alert -------------------------------------------------

    async def send_paper_alert(
        self,
        channel: Optional[str],
        papers: List[dict],
        alert_name: str,
    ) -> bool:
        """Format *papers* as a Block Kit message and post to *channel*.

        Args:
            channel: Target Slack channel (name or ID). Falls back to default.
            papers: List of publication dicts. Expected keys:
                title, authors, year, journal, url, citations.
            alert_name: Human-readable alert name shown in the header.

        Returns:
            ``True`` on success, ``False`` on failure.
        """
        if not self.is_configured:
            logger.warning("Slack not configured; skipping paper alert")
            return False

        if not papers:
            logger.info("No papers to send for alert '%s'", alert_name)
            return True

        target = self.resolve_channel(channel)

        # Chunk into runs of _MAX_PAPERS_PER_MESSAGE so a 16+ paper fire
        # produces multiple Slack messages instead of silently truncating.
        # All-or-nothing semantics: return True only when every chunk
        # delivers; the alert evaluator commits `alerted_publications`
        # only on True, so a partial failure leaves the un-acked papers
        # eligible for retry next fire.
        total = len(papers)
        chunk_size = _MAX_PAPERS_PER_MESSAGE
        chunks = [papers[i : i + chunk_size] for i in range(0, total, chunk_size)]
        for index, chunk in enumerate(chunks, start=1):
            start = (index - 1) * chunk_size + 1
            end = start + len(chunk) - 1
            header = (
                f"Alert: {alert_name} -- papers {start}-{end} of {total}"
                if len(chunks) > 1
                else f"Alert: {alert_name} -- {total} new paper(s) found"
            )
            blocks = self._build_paper_alert_blocks(chunk, header)
            fallback_text = header
            ok = await self._post_message(target, blocks, fallback_text)
            if not ok:
                logger.error(
                    "Slack chunk %d/%d failed for alert '%s'; aborting "
                    "remaining sends",
                    index,
                    len(chunks),
                    alert_name,
                )
                return False
        return True

    # ---- Recommendations ---------------------------------------------

    async def send_recommendation(
        self,
        channel: Optional[str],
        recommendations: List[dict],
    ) -> bool:
        """Send a recommendation digest to Slack.

        Args:
            channel: Target channel (name or ID). Falls back to default.
            recommendations: List of recommendation dicts. Expected keys:
                title, authors, score, source_type, url.

        Returns:
            ``True`` on success, ``False`` on failure.
        """
        if not self.is_configured:
            logger.warning("Slack not configured; skipping recommendations")
            return False

        if not recommendations:
            return True

        target = self.resolve_channel(channel)
        blocks = self._build_recommendation_blocks(recommendations)
        fallback_text = (
            f"New Recommendations -- {len(recommendations)} paper(s) you might like"
        )
        return await self._post_message(target, blocks, fallback_text)

    # ---- Test message -------------------------------------------------

    async def send_test_message(self, channel: Optional[str] = None) -> bool:
        """Send a brief test message to verify connectivity.

        Returns:
            ``True`` on success, ``False`` on failure.
        """
        if not self.is_configured:
            logger.warning("Slack not configured; cannot send test message")
            return False

        target = self.resolve_channel(channel)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "ALMa -- Connection Test",
                    "emoji": True,
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "If you can see this message, the ALMa Slack "
                        "integration is working correctly.\n\n"
                        f"*Timestamp:* {now}\n"
                        f"*Target:* {target}"
                    ),
                },
            },
        ]
        return await self._post_message(target, blocks, "ALMa connectivity test")

    # ------------------------------------------------------------------
    # Block Kit builders
    # ------------------------------------------------------------------

    def _build_paper_alert_blocks(
        self, papers: List[dict], header_text: str
    ) -> List[dict]:
        """Build Block Kit blocks for a paper alert message.

        ``header_text`` is the exact string rendered as the message header.
        Chunking semantics live in :meth:`send_paper_alert`; this builder
        renders whatever subset of papers it is handed, no slicing or
        overflow note.
        """
        blocks: List[dict] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header_text,
                    "emoji": True,
                },
            },
            {"type": "divider"},
        ]

        for paper in papers:
            blocks.append(self._format_paper_block(paper))
            blocks.append({"type": "divider"})

        # Footer with timestamp
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Sent by ALMa | {now}",
                    }
                ],
            }
        )

        return blocks

    def _format_paper_block(self, paper: dict) -> dict:
        """Create a Block Kit section for a single paper.

        Expected paper dict keys (all optional except title):
            title, authors, year, journal, url, citations, doi, abstract,
            publication_date.
        """
        title = paper.get("title") or "Untitled"
        url = paper.get("url") or paper.get("pub_url") or paper.get("doi") or ""
        if url and url.startswith("10."):
            # Bare DOI -> resolvable link
            url = f"https://doi.org/{url}"
        authors = paper.get("authors", "")
        year = paper.get("year") or ""
        # Prefer a YYYY-MM-DD when we have one; fall back to bare year.
        pub_date = str(paper.get("publication_date") or "").strip()
        journal = paper.get("journal") or paper.get("venue") or ""
        citations = paper.get("citations")
        if citations is None:
            citations = paper.get("cited_by_count")
        abstract = str(paper.get("abstract") or "").strip()
        # Provenance: which rule (monitor / keyword / etc.) triggered this
        # paper. Set by _evaluate_rule. Multiple sources are pre-joined
        # with ", " by _deduplicate_papers when the same paper matches
        # several rules in the same alert.
        alert_source = str(paper.get("alert_source") or "").strip()

        # Title as clickable link if URL is available
        if url:
            title_line = f"*<{url}|{title}>*"
        else:
            title_line = f"*{title}*"

        # Build detail lines
        lines: List[str] = [title_line]
        if authors:
            # Abbreviate long author lists, mirroring scholar-slack-bot's
            # "First, [+N], Last" shape.
            author_list = [a.strip() for a in str(authors).split(",") if a.strip()]
            if len(author_list) > 4:
                authors_text = (
                    f"{author_list[0]}, [+{len(author_list) - 2}], "
                    f"{author_list[-1]}"
                )
            else:
                authors_text = ", ".join(author_list)
            lines.append(f"Authors: {authors_text}")

        meta_parts: List[str] = []
        if pub_date:
            meta_parts.append(pub_date)
        elif year:
            meta_parts.append(str(year))
        if journal:
            meta_parts.append(str(journal))
        if citations is not None and str(citations).strip() not in ("", "None"):
            try:
                citation_count = int(citations)
                meta_parts.append(f"{citation_count} citation(s)")
            except (TypeError, ValueError):
                pass
        if meta_parts:
            lines.append(" | ".join(meta_parts))

        if alert_source:
            lines.append(f"_{alert_source}_")

        if abstract:
            # Slack section text caps at 3000 chars; keep abstracts tight so
            # 15 papers in one message stay well under the limit, and so
            # the inbox doesn't read like a full-text dump.
            snippet = abstract.replace("\n", " ").strip()
            if len(snippet) > 280:
                snippet = snippet[:280].rstrip() + "..."
            lines.append(snippet)

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(lines),
            },
        }

    def _build_recommendation_blocks(
        self, recommendations: List[dict]
    ) -> List[dict]:
        """Build Block Kit blocks for a recommendation digest."""
        count = len(recommendations)
        blocks: List[dict] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"New Recommendations -- {count} paper(s) you might like",
                    "emoji": True,
                },
            },
            {"type": "divider"},
        ]

        for rec in recommendations[:_MAX_PAPERS_PER_MESSAGE]:
            blocks.append(self._format_recommendation_block(rec))
            blocks.append({"type": "divider"})

        if count > _MAX_PAPERS_PER_MESSAGE:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"_...and {count - _MAX_PAPERS_PER_MESSAGE} "
                                f"more recommendation(s) not shown._"
                            ),
                        }
                    ],
                }
            )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Sent by ALMa | {now}",
                    }
                ],
            }
        )

        return blocks

    def _format_recommendation_block(self, rec: dict) -> dict:
        """Create a Block Kit section for one recommendation."""
        title = rec.get("recommended_title") or rec.get("title", "Untitled")
        url = (
            rec.get("recommended_url")
            or rec.get("url")
            or ""
        )
        authors = rec.get("recommended_authors") or rec.get("authors", "")
        score = rec.get("score")
        source_type = rec.get("source_type", "")

        if url:
            title_line = f"*<{url}|{title}>*"
        else:
            title_line = f"*{title}*"

        lines = [title_line]
        if authors:
            lines.append(f"Authors: {authors}")
        meta = []
        if score is not None:
            meta.append(f"Score: {score:.2f}")
        if source_type:
            meta.append(f"Source: {source_type}")
        if meta:
            lines.append(" | ".join(meta))

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(lines),
            },
        }

    # ------------------------------------------------------------------
    # Low-level send
    # ------------------------------------------------------------------

    async def _post_message(
        self,
        channel: str,
        blocks: List[dict],
        fallback_text: str,
    ) -> bool:
        """Post a Block Kit message to Slack.

        This method catches all Slack API errors and returns ``False``
        rather than propagating exceptions, so callers can handle
        failures through the return value.
        """
        try:
            target = self._resolve_target(channel)
        except SlackResolveError as exc:
            logger.error("Failed to resolve Slack target %r: %s", channel, exc)
            return False
        try:
            client = self._get_client()
            response = client.chat_postMessage(
                channel=target,
                blocks=blocks,
                text=fallback_text,
            )
            if response.get("ok"):
                logger.info(
                    "Slack message sent to %s (ts=%s)",
                    target,
                    response.get("ts"),
                )
                return True
            else:
                logger.error(
                    "Slack API returned ok=False: %s",
                    response.get("error", "unknown"),
                )
                return False
        except RuntimeError:
            # Token not configured
            raise
        except Exception as e:
            logger.error("Failed to send Slack message to %s: %s", target, e)
            return False


# ======================================================================
# Factory helper
# ======================================================================


def get_slack_notifier() -> SlackNotifier:
    """Create a :class:`SlackNotifier` from the current configuration.

    Resolution order for the token:
    1. ``SLACK_TOKEN`` environment variable
    2. Unified secret store
    3. Plugin config file (config/slack.json or config/slack.config ``api_token``)

    Resolution order for the default channel:
    1. ``SLACK_CHANNEL`` environment variable
    2. ``slack_channel`` key in settings.json
    3. Plugin config file ``default_channel``

    Returns:
        A configured :class:`SlackNotifier`. If no token is found the
        notifier is returned in dry-run mode (``is_configured == False``).
    """
    token = None
    channel = None
    try:
        from alma.config import get_slack_channel, get_slack_token
        token = get_slack_token()
        channel = get_slack_channel()
    except Exception:
        token = os.getenv("SLACK_TOKEN")
        channel = os.getenv("SLACK_CHANNEL")

    # Fall back to plugin config files
    if not token:
        try:
            from alma.plugins.config import load_plugin_config

            config = load_plugin_config("slack")
            if config:
                if not token:
                    token = config.get("api_token")
                if not channel:
                    channel = config.get("default_channel")
        except Exception:
            pass

    return SlackNotifier(token=token, default_channel=channel)
