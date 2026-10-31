"""What Alerts reports to Home, Health and the sidebar."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from alma.addons.alerts import service
from alma.application.home_status import (
    SETTINGS_PLUGINS,
    ago,
    integration_direction,
    pill,
    provider_names,
    scalar,
)
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

#: How far back a failed run still reads as a current delivery problem.
_FAILURE_WINDOW_DAYS = 30


def home_counts(db: sqlite3.Connection, day_start: str) -> list[dict[str, Any]]:
    """Today's delivered digests, for Home's activity row."""
    return [
        {
            "label": "alerts delivered",
            "hint": "successful digests",
            "today": service.count_delivered_since(db, day_start),
            "href": "#/alerts?tab=history",
        }
    ]


def nav_badge(db: sqlite3.Connection) -> int:
    """Enabled digests, the count on the sidebar entry."""
    return int(db.execute("SELECT COUNT(*) FROM alerts WHERE enabled = 1").fetchone()[0])


def delivery_check(db: sqlite3.Connection) -> list[dict[str, Any]]:
    """The one Health check: can enabled digests be delivered, and are they?

    A digest with no working sender is the worse state, so it is reported alone.
    """
    enabled = db.execute("SELECT channels FROM alerts WHERE enabled = 1").fetchall()
    ready = set(service.configured_delivery_integrations())
    unavailable = sum(1 for row in enabled if not ready.intersection(json.loads(row["channels"]) or []))
    if unavailable:
        return [
            {
                "id": "alert_delivery_unavailable",
                "label": "Some enabled Alerts have no active delivery integration",
                "severity": "critical",
                "detail": (
                    f"{unavailable} enabled "
                    f"{'Alert has' if unavailable == 1 else 'Alerts have'} "
                    "no activated and configured sender."
                ),
                "href": SETTINGS_PLUGINS,
            }
        ]
    since = (utcnow() - timedelta(days=_FAILURE_WINDOW_DAYS)).isoformat()
    failed = int(
        db.execute(
            """
            SELECT COUNT(DISTINCT alert_id) FROM alert_history
            WHERE status = 'failed' AND sent_at >= ? AND COALESCE(alert_id, '') != ''
            """,
            (since,),
        ).fetchone()[0]
    )
    if failed:
        return [
            {
                "id": "failed_alert_delivery",
                "label": "Some alerts have recent delivery failures",
                "severity": "warning",
                "detail": f"{failed} alerts show recent failed runs and should be re-evaluated or repaired.",
                "href": "#/alerts?tab=history",
            }
        ]
    return []


def _latest_delivery(
    db: sqlite3.Connection,
    provider_ids: list[str],
) -> dict[str, Any] | None:
    """Latest sent/failed Alert history row for a currently ready provider."""
    try:
        columns = {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in db.execute("PRAGMA table_info(alert_history)").fetchall()
        }
        if "channel" in columns:
            placeholders = ", ".join("?" for _ in provider_ids)
            where = f"AND channel IN ({placeholders})"
            params: tuple[Any, ...] = tuple(provider_ids)
        else:
            where = ""
            params = ()
        error_select = "error_message" if "error_message" in columns else "NULL"
        row = db.execute(
            f"""
            SELECT status, sent_at, {error_select} AS error_message
            FROM alert_history
            WHERE status IN ('sent', 'failed') {where}
            ORDER BY sent_at DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None


def home_pill(db: sqlite3.Connection, now: datetime) -> list[dict[str, Any]]:
    """Alerts health, independent of its send integration."""
    return [_status(db, now=now)]


def _status(db: sqlite3.Connection, *, now: datetime) -> dict[str, Any]:
    configured, ready = integration_direction("send")
    if not configured:
        return pill(
            key="alerts",
            label="Alerts",
            state="off",
            metric="no provider",
            detail="No active send integration is configured.",
            tier="always",
            href=SETTINGS_PLUGINS,
        )
    if not ready:
        return pill(
            key="alerts",
            label="Alerts",
            state="failed",
            metric="provider not working",
            detail=(
                f"{provider_names(configured)} is configured but cannot send. "
                "Finish or test it in Settings → Plugins."
            ),
            tier="always",
            href=SETTINGS_PLUGINS,
        )

    latest = _latest_delivery(
        db,
        [manifest.id for manifest, _status in ready],
    )
    if latest and latest["status"] == "failed":
        return pill(
            key="alerts",
            label="Alerts",
            state="failed",
            metric="last delivery failed",
            detail=str(latest.get("error_message") or "").strip()
            or "The most recent Alert delivery failed.",
            tier="always",
            href="#/alerts?tab=history",
            checked_at=str(latest["sent_at"]),
        )

    rules = int(scalar(db, "SELECT COUNT(*) FROM alert_rules") or 0)
    checked_at = str(latest["sent_at"]) if latest else None
    detail = f"{provider_names(ready)} is configured and ready to send."
    if rules == 0:
        detail += " No Alert rules exist yet."
    return pill(
        key="alerts",
        label="Alerts",
        state="ok",
        metric=f"sent {ago(checked_at, now=now)}" if checked_at else "provider ready",
        detail=detail,
        tier="always",
        href="#/alerts",
        checked_at=checked_at,
    )
