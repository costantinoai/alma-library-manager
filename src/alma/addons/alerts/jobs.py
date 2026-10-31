"""The hourly sweep that evaluates due digests."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3

from alma.core.db_write import commit_with_retry
from alma.core.time import utcnow

logger = logging.getLogger(__name__)


def alert_check_interval_minutes() -> int:
    """Minutes between sweeps, from ``ALERT_CHECK_INTERVAL_HOURS`` (default 1)."""
    try:
        return max(1, int(os.getenv("ALERT_CHECK_INTERVAL_HOURS", "1"))) * 60
    except (ValueError, TypeError):
        return 60


def evaluate_scheduled_alerts() -> None:
    """Sweep all enabled, non-manual digests and evaluate due ones."""
    from alma.addons.alerts import service as alerts_app
    from alma.api.deps import open_db_connection
    from alma.api.scheduler import add_job_log, set_job_status

    job_id = "periodic_alert_evaluation"
    operation_key = "alerts.evaluate_scheduled"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        operation_key=operation_key,
        started_at=utcnow().isoformat(),
        message="Evaluating scheduled alerts",
    )
    logger.info("Starting scheduled alert evaluation sweep")
    try:
        conn = open_db_connection()

        try:
            alerts = conn.execute(
                "SELECT * FROM alerts WHERE enabled = 1 AND schedule != 'manual'"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            logger.warning("Cannot query alerts table (may not exist yet): %s", exc)
            conn.close()
            return

        now = utcnow()
        evaluated_count = 0
        sent_total = 0
        failed_total = 0

        for alert_row in alerts:
            alert = dict(alert_row)
            alert_id = alert["id"]
            schedule = str(alert.get("schedule") or "").strip().lower()
            schedule_config = {}
            try:
                raw_schedule = alert.get("schedule_config")
                if raw_schedule:
                    schedule_config = json.loads(raw_schedule)
                    if not isinstance(schedule_config, dict):
                        schedule_config = {}
            except Exception:
                schedule_config = {}
            last_eval_str = str(alert.get("last_evaluated_at") or "").strip() or None

            try:
                if not alerts_app.is_due(
                    schedule=schedule,
                    schedule_config=schedule_config,
                    last_evaluated_at=last_eval_str,
                    now=now,
                ):
                    continue

                logger.info("Alert %s (%s) is due -- evaluating", alert["name"], alert_id)
                result = asyncio.run(
                    alerts_app.evaluate_digest(conn, alert_id, trigger_source="scheduler")
                )
                # `open_db_connection` does NOT auto-commit (unlike the
                # request-scoped `get_db` dependency).  `evaluate_digest`
                # writes to `alerted_publications`, `alert_history`, and
                # `alerts.last_evaluated_at`; without an explicit commit
                # the connection close at the end of the sweep rolls them
                # back -- the Slack DM goes out (network side-effect) but
                # the dedup record is lost, so the same paper re-fires on
                # the next sweep.  Same class of fix as
                # `routes.py:evaluate_alert` in v0.10.0.
                # evaluate_digest's writes (dedup / history / last_evaluated_at)
                # all run AFTER its search + Slack send, so the lock is never
                # held across network; commit_with_retry flushes them with
                # transient-lock retry on this own (scheduler) connection.
                commit_with_retry(conn, label="evaluate_scheduled_alerts")
                if result is None:
                    continue
                sent_total += int(result.get("papers_sent") or 0)
                failed_total += int(result.get("papers_failed") or 0)
                evaluated_count += 1

            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.exception("Error evaluating alert %s", alert_id)

        # Retention: trim history older than the application-defined window
        # (kept well above the 90-day Insights trend horizon). Same own-
        # connection commit discipline as the evaluations above.
        pruned = 0
        try:
            pruned = alerts_app.prune_alert_history(conn)
            if pruned:
                commit_with_retry(conn, label="prune_alert_history")
                logger.info("Pruned %d alert_history rows past retention", pruned)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.exception("alert_history retention prune failed")

        conn.close()
        logger.info(
            "Alert evaluation sweep complete: %d/%d alerts evaluated",
            evaluated_count,
            len(alerts),
        )
        set_job_status(
            job_id,
            status="completed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=utcnow().isoformat(),
            message="Scheduled alert evaluation complete",
            result={
                "evaluated": evaluated_count,
                "total": len(alerts),
                "papers_sent": sent_total,
                "papers_failed": failed_total,
                "history_pruned": pruned,
            },
        )

    except Exception as exc:
        logger.exception("Fatal error in evaluate_scheduled_alerts")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=utcnow().isoformat(),
            message="Scheduled alert evaluation failed",
            error=f"{type(exc).__name__}: {exc}",
        )
        add_job_log(
            job_id, f"Fatal error: {type(exc).__name__}: {exc}", step="fatal", level="error"
        )
