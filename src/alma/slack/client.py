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
        """Return the effective channel, falling back to *default_channel*.

        Raises:
            ValueError: If no channel can be determined.
        """
        ch = channel or self._default_channel
        if not ch:
            raise ValueError(
                "No Slack channel specified and no default_channel configured"
            )
        return ch

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
        blocks = self._build_paper_alert_blocks(papers, alert_name)
        fallback_text = (
            f"Alert: {alert_name} -- {len(papers)} new paper(s) found"
        )
        return await self._post_message(target, blocks, fallback_text)

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
                    "text": "Scholar Slack Bot -- Connection Test",
                    "emoji": True,
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "If you can see this message, the Slack integration "
                        "is working correctly.\n\n"
                        f"*Timestamp:* {now}\n"
                        f"*Channel:* {target}"
                    ),
                },
            },
        ]
        return await self._post_message(
            target, blocks, "Scholar Slack Bot test message"
        )

    # ------------------------------------------------------------------
    # Block Kit builders
    # ------------------------------------------------------------------

    def _build_paper_alert_blocks(
        self, papers: List[dict], alert_name: str
    ) -> List[dict]:
        """Build Block Kit blocks for a paper alert message."""
        count = len(papers)
        blocks: List[dict] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Alert: {alert_name} -- {count} new paper(s) found",
                    "emoji": True,
                },
            },
            {"type": "divider"},
        ]

        for paper in papers[:_MAX_PAPERS_PER_MESSAGE]:
            blocks.append(self._format_paper_block(paper))
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
                                f"more paper(s) not shown._"
                            ),
                        }
                    ],
                }
            )

        # Footer with timestamp
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Sent by Scholar Slack Bot | {now}",
                    }
                ],
            }
        )

        return blocks

    def _format_paper_block(self, paper: dict) -> dict:
        """Create a Block Kit section for a single paper.

        Expected paper dict keys (all optional except title):
            title, authors, year, journal, url, citations, doi
        """
        title = paper.get("title", "Untitled")
        url = paper.get("url") or paper.get("pub_url") or ""
        authors = paper.get("authors", "")
        year = paper.get("year", "")
        journal = paper.get("journal", "")
        citations = paper.get("citations")

        # Title as clickable link if URL is available
        if url:
            title_line = f"*<{url}|{title}>*"
        else:
            title_line = f"*{title}*"

        # Build detail lines
        lines = [title_line]
        if authors:
            # Abbreviate long author lists
            author_list = [a.strip() for a in authors.split(",")]
            if len(author_list) > 4:
                authors_text = (
                    f"{author_list[0]}, ... (+{len(author_list) - 2}), "
                    f"{author_list[-1]}"
                )
            else:
                authors_text = authors
            lines.append(f"Authors: {authors_text}")

        meta_parts = []
        if year:
            meta_parts.append(str(year))
        if journal:
            meta_parts.append(journal)
        if citations is not None:
            meta_parts.append(f"{citations} citations")
        if meta_parts:
            lines.append(" | ".join(meta_parts))

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
                        "text": f"Sent by Scholar Slack Bot | {now}",
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
            client = self._get_client()
            response = client.chat_postMessage(
                channel=channel,
                blocks=blocks,
                text=fallback_text,
            )
            if response.get("ok"):
                logger.info(
                    "Slack message sent to %s (ts=%s)",
                    channel,
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
            logger.error("Failed to send Slack message to %s: %s", channel, e)
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
