"""Alerts: rule-based digests of new papers, sent by Slack or email.

This file is the manifest and stays light. Everything heavy sits behind the lazy
callables below, because core reaches this module while it is still importing.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from alma.addons.alerts.schema import TABLES, has_user_data
from alma.core.addons import AddOn, JobSpec
from alma.core.job_policy import JobClass, JobPolicy, ResourceKind

_JOB_POLICIES = (
    JobPolicy(
        namespace="alerts",
        job_class=JobClass.NOTIFICATION,
        priority=40,
        resources=frozenset({ResourceKind.DB_WRITER, ResourceKind.NETWORK}),
        max_concurrency=1,
    ),
)


def _routers() -> Sequence[tuple[Any, str, Sequence[str]]]:
    from alma.addons.alerts.routes import router

    return ((router, "/api/v1/alerts", ("alerts",)),)


def _plugins() -> Sequence[Any]:
    from alma.addons.alerts.email_plugin import EMAIL_PLUGIN

    return (EMAIL_PLUGIN,)


def _evaluate_scheduled_alerts() -> None:
    from alma.addons.alerts.jobs import evaluate_scheduled_alerts

    evaluate_scheduled_alerts()


def _interval_minutes() -> int:
    from alma.addons.alerts.jobs import alert_check_interval_minutes

    return alert_check_interval_minutes()


def _home_counts(db: sqlite3.Connection, day_start: str) -> list[dict[str, Any]]:
    from alma.addons.alerts.surfaces import home_counts

    return home_counts(db, day_start)


def _home_pills(db: sqlite3.Connection, now: datetime) -> list[dict[str, Any]]:
    from alma.addons.alerts.surfaces import home_pill

    return home_pill(db, now)


def _health_checks(db: sqlite3.Connection) -> list[dict[str, Any]]:
    from alma.addons.alerts.surfaces import delivery_check

    return delivery_check(db)


def _nav_badge(db: sqlite3.Connection) -> int:
    from alma.addons.alerts.surfaces import nav_badge

    return nav_badge(db)


ADDON = AddOn(
    id="alerts",
    name="Alerts",
    description="Scheduled digests of new papers from your rules, sent by Slack or email.",
    docs_path="concepts/alerts/",
    has_user_data=has_user_data,
    routers=_routers,
    jobs=(
        JobSpec(
            job_id="evaluate_alerts",
            func=_evaluate_scheduled_alerts,
            name="Evaluate scheduled alerts",
            description="Checks all non-manual alerts on their interval",
            interval_minutes=_interval_minutes,
        ),
    ),
    job_policies=lambda: _JOB_POLICIES,
    tables=TABLES,
    paper_tables=(("alerted_publications", True), ("alert_history", False)),
    settings_defaults={
        "plugins.email.enabled": False,
        "smtp_host": None,
        "smtp_port": 587,
        "smtp_username": None,
        "smtp_from": None,
        "smtp_to": None,
        "smtp_use_tls": True,
    },
    secret_keys=("smtp.password",),
    plugins=_plugins,
    home_counts=_home_counts,
    home_pills=_home_pills,
    health_checks=_health_checks,
    nav_badge=_nav_badge,
    operation_prefixes=("alerts",),
)
