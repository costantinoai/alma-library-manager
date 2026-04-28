"""Slack notification client for Scholar Slack Bot.

This module provides the SlackNotifier class for sending rich Block Kit
messages to Slack channels. It is used by the alert evaluation engine
and the settings test endpoint.
"""

from alma.slack.client import SlackNotifier

__all__ = ["SlackNotifier"]
