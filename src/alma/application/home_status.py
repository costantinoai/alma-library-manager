"""Is ALMa working for you? One pill per subsystem, for Home's status line.

The line sits UNDER Home's figures because that is what it is for: a zero should
never be a mystery. "0 new Feed papers" next to *Feed · 96 monitors · checked 2d
ago* is a diagnosis; on its own it is a shrug. Every pill therefore names a
subsystem, says how recently it did its job, and links to where you fix it.

**Three tiers, so the line stays glanceable:**

* `always`  — the things that produce or deliver the numbers above. Inbox and
  Alerts appear as core capabilities; integration names belong in Settings,
  never in this rail.
* `problem` — the rest. Absent while healthy, loud when not. Suppliers and the
  map layout live here: a real OpenAlex outage shows up as Feed going stale
  anyway, so a green OpenAlex dot every day is noise.

**Derived from local state, never a live probe.** Home is a pure read on a hot
path. `operation_status` already records the outcome of every job that talked to
each provider; freshness is a MAX over timestamps; AI capability is a `find_spec`
away. The cost is that a pill claims "last time we used this, it worked" rather
than "it works right now" — stated in the payload as `checked_at` rather than
hidden, with live re-probing one click away in Settings.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

logger = logging.getLogger(__name__)

#: Job outcomes that PROVE something about a subsystem.
#:
#: `cancelled` is deliberately absent and is the whole reason this set exists:
#: a cancelled run says the user (or a shutdown) stopped the job, not that the
#: provider answered. Reading it as either success or failure would be a lie,
#: and cancellations are the single most common row in the ledger.
CONCLUSIVE_STATUSES = ("completed", "noop", "failed")

#: Modules the local SPECTER2 encoder needs. Checked with `find_spec`, never
#: imported — importing torch on a page load would cost seconds.
LOCAL_AI_MODULES = ("torch", "transformers", "adapters", "numpy")

#: A Feed that has not refreshed in this long is stale enough to explain a zero.
#: Two days rather than one: the nightly author refresh legitimately skips a day
#: when nothing upstream changed, and crying wolf on that would train the user to
#: ignore the pill.
FEED_STALE_AFTER = timedelta(days=2)
#: Lenses are refreshed deliberately, not nightly, so the bar is a week.
LENS_STALE_AFTER = timedelta(days=7)
#: Coverage below the readiness bar means Discovery is working with a partial
#: corpus. Mirrors `services.health`'s own 80% readiness gate.
EMBEDDING_READY_PCT = 80.0
#: Papers with a vector but no place on the map, as a share of the embedded set,
#: before the map is worth complaining about.
MAP_GAP_TOLERANCE_PCT = 5.0

PillState = Literal["ok", "warning", "failed", "running", "unknown", "off"]
PillTier = Literal["always", "problem"]

SETTINGS_PLUGINS = "#/settings?anchor=plugins"
SETTINGS_CONNECTIONS = "#/settings?anchor=connections"
SETTINGS_AI = "#/settings?anchor=ai-config"
SETTINGS_BACKGROUND = "#/settings?anchor=background-ops"

#: `state` → the severity the UI paints. `off` is a choice not yet made, never a
#: fault: it must not wear a warning colour or the user learns to ignore the row.
STATE_SEVERITY: dict[str, str] = {
    "ok": "ok",
    "warning": "warning",
    "failed": "critical",
    "running": "info",
    "unknown": "unknown",
    "off": "unknown",
}


def _pill(
    *,
    key: str,
    label: str,
    state: PillState,
    metric: str,
    detail: str,
    tier: PillTier,
    href: str,
    checked_at: str | None = None,
) -> dict[str, Any]:
    """One uniform pill record. `metric` is the glanceable half of the truth,
    `detail` the sentence behind it on hover."""
    return {
        "key": key,
        "label": label,
        "state": state,
        "severity": STATE_SEVERITY.get(state, "unknown"),
        "metric": metric,
        "detail": detail,
        "tier": tier,
        "href": href,
        "checked_at": checked_at,
    }


# ---------------------------------------------------------------------------
# Shared reads
# ---------------------------------------------------------------------------


def _parse(stamp: object) -> datetime | None:
    """Parse one of ALMa's stored timestamps as aware UTC, or None.

    Stored values are UTC-naive ISO text (the project's SQLite convention) but
    a few carry an offset; both are accepted rather than one being trusted.
    """
    text = str(stamp or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _age(stamp: object, *, now: datetime) -> timedelta | None:
    parsed = _parse(stamp)
    return None if parsed is None else now - parsed


def _ago(stamp: object, *, now: datetime) -> str:
    """ "2d ago" / "4m ago" / "never". Compact enough for a pill's metric."""
    age = _age(stamp, now=now)
    if age is None:
        return "never"
    seconds = max(0, int(age.total_seconds()))
    if seconds < 90:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def _last_conclusive_run(
    db: sqlite3.Connection, prefixes: tuple[str, ...]
) -> dict[str, Any] | None:
    """The most recent run of any of `prefixes` that reached a verdict.

    Prefixes are matched with `LIKE prefix || '%'` because ledger keys are
    parameterized (`papers.rehydrate_metadata:openalex:metadata:target:<id>`).
    Every listed prefix must be a job that actually TALKS to the subsystem — one
    that merely reads the DB afterwards would report a green light for a dead
    connection.
    """
    if not prefixes:
        return None
    like = " OR ".join("operation_key LIKE ? || '%'" for _ in prefixes)
    statuses = ",".join("?" for _ in CONCLUSIVE_STATUSES)
    row = db.execute(
        f"""
        SELECT operation_key, status, message, error,
               COALESCE(finished_at, updated_at) AS at
        FROM operation_status
        WHERE ({like}) AND status IN ({statuses})
        ORDER BY at DESC
        LIMIT 1
        """,
        (*prefixes, *CONCLUSIVE_STATUSES),
    ).fetchone()
    return dict(row) if row else None


def _scalar(db: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
    """One value, or None when the table is absent on a pre-migration DB."""
    try:
        row = db.execute(sql, params).fetchone()
    except sqlite3.OperationalError as exc:
        logger.debug("home_status read skipped (%s): %s", sql.split()[3:5], exc)
        return None
    if row is None:
        return None
    return row[0]


# ---------------------------------------------------------------------------
# always — the subsystems that produce Home's figures
# ---------------------------------------------------------------------------


def _automation(db: sqlite3.Connection) -> dict[str, Any]:
    """The master switch. When it is off, every other pill is frozen and every
    number on the page is a historical record rather than today's state."""
    from alma.api.scheduler import _scheduler, list_jobs

    try:
        from alma.api.routes.scheduler import _scheduler_enabled

        enabled = bool(_scheduler_enabled())
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Scheduler enablement unreadable: %s", exc)
        enabled = True

    running = bool(_scheduler is not None and getattr(_scheduler, "running", False))
    job_count = 0
    if running:
        try:
            job_count = len(list_jobs())
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Scheduler job list unreadable: %s", exc)

    if not enabled:
        return _pill(
            key="automation",
            label="Automation",
            state="off",
            metric="switched off",
            detail=(
                "Background jobs are disabled, so nothing refreshes on its own. "
                "Every figure above is whatever it was when you last ran things "
                "by hand."
            ),
            tier="always",
            href=SETTINGS_BACKGROUND,
        )
    if not running:
        return _pill(
            key="automation",
            label="Automation",
            state="failed",
            metric="not running",
            detail=(
                "The scheduler is enabled but not running, so no job will fire. "
                "Every figure above is frozen. Restart the backend."
            ),
            tier="always",
            href=SETTINGS_BACKGROUND,
        )
    return _pill(
        key="automation",
        label="Automation",
        state="ok",
        metric=f"{job_count} jobs",
        detail=f"The scheduler is running with {job_count} registered jobs.",
        tier="always",
        href=SETTINGS_BACKGROUND,
    )


def _feed(db: sqlite3.Connection, *, now: datetime) -> dict[str, Any]:
    """Monitors and when they last delivered — the pill that explains a zero."""
    from alma.application import feed as feed_app

    enabled = int(_scalar(db, "SELECT COUNT(*) FROM feed_monitors WHERE enabled = 1") or 0)
    broken = int(
        _scalar(
            db,
            "SELECT COUNT(*) FROM feed_monitors WHERE enabled = 1 "
            "AND COALESCE(last_status, '') = 'error'",
        )
        or 0
    )
    try:
        _, last_refresh = feed_app.latest_feed_fetch_window(db)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Feed refresh window unreadable: %s", exc)
        last_refresh = None

    if enabled == 0:
        return _pill(
            key="feed",
            label="Feed",
            state="off",
            metric="no monitors",
            detail=(
                "You are not monitoring any authors, journals or queries yet, so "
                "the Feed has nothing to collect."
            ),
            tier="always",
            href="#/settings?anchor=feed-monitors",
        )

    plural = "monitor" if enabled == 1 else "monitors"
    metric = f"{enabled} {plural} · {_ago(last_refresh, now=now)}"
    age = _age(last_refresh, now=now)

    if broken:
        return _pill(
            key="feed",
            label="Feed",
            state="failed",
            metric=f"{broken} of {enabled} failing",
            detail=(
                f"{broken} {'monitor' if broken == 1 else 'monitors'} last failed "
                "to fetch. Those sources are silently delivering nothing."
            ),
            tier="always",
            href="#/settings?anchor=feed-monitors",
            checked_at=last_refresh,
        )
    if age is None:
        return _pill(
            key="feed",
            label="Feed",
            state="unknown",
            metric=f"{enabled} {plural} · never checked",
            detail="These monitors have never been refreshed, so nothing has arrived yet.",
            tier="always",
            href="#/feed",
        )
    if age > FEED_STALE_AFTER:
        return _pill(
            key="feed",
            label="Feed",
            state="warning",
            metric=metric,
            detail=(
                f"Your {enabled} {plural} last delivered {_ago(last_refresh, now=now)}. "
                "That is why today's Feed figure is low — refresh it from the Feed page."
            ),
            tier="always",
            href="#/feed",
            checked_at=last_refresh,
        )
    return _pill(
        key="feed",
        label="Feed",
        state="ok",
        metric=metric,
        detail=f"All {enabled} {plural} are healthy and last delivered {_ago(last_refresh, now=now)}.",
        tier="always",
        href="#/feed",
        checked_at=last_refresh,
    )


def _discovery(db: sqlite3.Connection, *, now: datetime) -> dict[str, Any]:
    """Active lenses and when they last produced suggestions."""
    active = int(_scalar(db, "SELECT COUNT(*) FROM discovery_lenses WHERE is_active = 1") or 0)
    last = _scalar(db, "SELECT MAX(last_refreshed_at) FROM discovery_lenses WHERE is_active = 1")

    if active == 0:
        return _pill(
            key="discovery",
            label="Discovery",
            state="off",
            metric="no active lens",
            detail=(
                "No Discovery lens is active, so nothing is looking for papers "
                "beyond what you already monitor."
            ),
            tier="always",
            href="#/discovery",
        )

    plural = "lens" if active == 1 else "lenses"
    metric = f"{active} {plural} · {_ago(last, now=now)}"
    age = _age(last, now=now)
    if age is None or age > LENS_STALE_AFTER:
        return _pill(
            key="discovery",
            label="Discovery",
            state="warning",
            metric=metric,
            detail=(
                f"Your {plural} last refreshed {_ago(last, now=now)}. New suggestions "
                "only appear when a lens runs — refresh one from Discovery."
            ),
            tier="always",
            href="#/discovery",
            checked_at=last,
        )
    return _pill(
        key="discovery",
        label="Discovery",
        state="ok",
        metric=metric,
        detail=f"{active} active {plural}, last refreshed {_ago(last, now=now)}.",
        tier="always",
        href="#/discovery",
        checked_at=last,
    )


def _embeddings(db: sqlite3.Connection) -> dict[str, Any]:
    """One pill for the whole vector story: can this box compute, and how much of
    the corpus actually has a vector.

    Named for the stack that would really run — Discovery's similarity, the maps
    and every "papers like this" all read the same vectors, so partial coverage
    is a quality ceiling rather than a cosmetic gap. Capability is a `find_spec`
    check and coverage is two counts; neither imports torch or touches the
    network.
    """
    from alma.ai.import_state import module_available

    missing = [name for name in LOCAL_AI_MODULES if not module_available(name)]
    local_ready = not missing

    hosted_ready = False
    if not local_ready:
        try:
            from alma.ai.providers import OpenAIProvider

            hosted_ready = OpenAIProvider().is_available()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("OpenAI availability check failed: %s", exc)

    label = "SPECTER2" if local_ready else ("OpenAI embeddings" if hosted_ready else "Embeddings")

    papers = int(_scalar(db, "SELECT COUNT(*) FROM papers") or 0)
    embedded = int(_scalar(db, "SELECT COUNT(*) FROM publication_embeddings") or 0)
    coverage = round((embedded / papers) * 100, 1) if papers else 0.0

    if not local_ready and not hosted_ready:
        return _pill(
            key="embeddings",
            label=label,
            state="warning",
            metric=f"{coverage:g}% · cannot compute here",
            detail=(
                "This environment cannot compute embeddings — missing "
                + ", ".join(missing)
                + ". Papers still get vectors from Semantic Scholar, so coverage "
                "grows but never catches up."
            ),
            tier="always",
            href=SETTINGS_AI,
        )
    if coverage < EMBEDDING_READY_PCT:
        return _pill(
            key="embeddings",
            label=label,
            state="warning",
            metric=f"{coverage:g}% covered",
            detail=(
                f"Only {coverage:g}% of your {papers:,} papers have a vector "
                f"(readiness starts at {EMBEDDING_READY_PCT:g}%). Discovery "
                "similarity and the maps can only see the covered part."
            ),
            tier="always",
            href=SETTINGS_AI,
        )
    return _pill(
        key="embeddings",
        label=label,
        state="ok",
        metric=f"{coverage:g}% covered",
        detail=(f"{embedded:,} of {papers:,} papers have a vector, computed by {label}."),
        tier="always",
        href=SETTINGS_AI,
    )


# ---------------------------------------------------------------------------
# Core Inbox + Alerts capabilities, backed by integration providers
# ---------------------------------------------------------------------------


def _integration_direction(
    capability: str,
) -> tuple[list[tuple[Any, dict[str, Any]]], list[tuple[Any, dict[str, Any]]]]:
    """Return (configured, ready) active providers for one core direction."""
    try:
        from alma.plugins.registry import get_plugin_registry
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Integration registry unavailable: %s", exc)
        return [], []

    configured: list[tuple[Any, dict[str, Any]]] = []
    ready: list[tuple[Any, dict[str, Any]]] = []
    readiness_key = f"can_{capability}"
    for manifest in get_plugin_registry().all():
        if not manifest.can(capability) or not manifest.is_enabled():
            continue
        status = manifest.status() or {}
        if status.get("configured"):
            configured.append((manifest, status))
        if status.get(readiness_key):
            ready.append((manifest, status))
    return configured, ready


def _provider_names(providers: list[tuple[Any, dict[str, Any]]]) -> str:
    return ", ".join(manifest.display_name for manifest, _status in providers)


def _inbox_pill(db: sqlite3.Connection, *, now: datetime) -> dict[str, Any]:
    """Core Inbox health, independent of its receive integration."""
    configured, ready = _integration_direction("receive")
    if not configured:
        return _pill(
            key="inbox",
            label="Inbox",
            state="off",
            metric="no provider",
            detail="No active receive integration is configured.",
            tier="always",
            href=SETTINGS_PLUGINS,
        )
    if not ready:
        return _pill(
            key="inbox",
            label="Inbox",
            state="failed",
            metric="provider not working",
            detail=(
                f"{_provider_names(configured)} is configured but cannot receive. "
                "Finish or test it in Settings → Plugins."
            ),
            tier="always",
            href=SETTINGS_PLUGINS,
        )

    last = _last_conclusive_run(db, ("inbox.capture_sweep",))
    if last and str(last.get("status")) == "failed":
        return _pill(
            key="inbox",
            label="Inbox",
            state="failed",
            metric="last sweep failed",
            detail=str(last.get("error") or last.get("message") or "").strip()
            or "The last capture sweep failed, so papers you send are not arriving.",
            tier="always",
            href=SETTINGS_PLUGINS,
            checked_at=last.get("at"),
        )
    at = (last or {}).get("at")
    return _pill(
        key="inbox",
        label="Inbox",
        state="ok",
        metric=f"swept {_ago(at, now=now)}" if last else "provider ready",
        detail=(
            str((last or {}).get("message") or "").strip()
            or f"{_provider_names(ready)} is configured and ready to receive."
        ),
        tier="always",
        href=SETTINGS_PLUGINS,
        checked_at=at,
    )


def _latest_alert_delivery(
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


def _alerts_pill(db: sqlite3.Connection, *, now: datetime) -> dict[str, Any]:
    """Core Alerts health, independent of its send integration."""
    configured, ready = _integration_direction("send")
    if not configured:
        return _pill(
            key="alerts",
            label="Alerts",
            state="off",
            metric="no provider",
            detail="No active send integration is configured.",
            tier="always",
            href=SETTINGS_PLUGINS,
        )
    if not ready:
        return _pill(
            key="alerts",
            label="Alerts",
            state="failed",
            metric="provider not working",
            detail=(
                f"{_provider_names(configured)} is configured but cannot send. "
                "Finish or test it in Settings → Plugins."
            ),
            tier="always",
            href=SETTINGS_PLUGINS,
        )

    latest = _latest_alert_delivery(
        db,
        [manifest.id for manifest, _status in ready],
    )
    if latest and latest["status"] == "failed":
        return _pill(
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

    rules = int(_scalar(db, "SELECT COUNT(*) FROM alert_rules") or 0)
    checked_at = str(latest["sent_at"]) if latest else None
    detail = f"{_provider_names(ready)} is configured and ready to send."
    if rules == 0:
        detail += " No Alert rules exist yet."
    return _pill(
        key="alerts",
        label="Alerts",
        state="ok",
        metric=f"sent {_ago(checked_at, now=now)}" if checked_at else "provider ready",
        detail=detail,
        tier="always",
        href="#/alerts",
        checked_at=checked_at,
    )


# ---------------------------------------------------------------------------
# problem — absent while healthy
# ---------------------------------------------------------------------------

#: Suppliers, and the jobs that prove they answered. They sit in the
#: problem tier on purpose: a real outage manifests as Feed or Embeddings going
#: stale, which is already reported, so a green dot each day is pure noise.
SUPPLIERS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    (
        "openalex",
        "OpenAlex",
        "Feed monitors, author refreshes and paper metadata all stall.",
        (
            "papers.rehydrate_metadata:openalex",
            "authors.deep_refresh",
            "authors.rehydrate_metadata",
        ),
    ),
    (
        "semantic_scholar",
        "Semantic Scholar",
        "New papers stop getting vectors, so Discovery cannot see them.",
        ("ai.backfill_s2_vectors", "ai.title_resolution_sweep"),
    ),
)


def _supplier_pills(db: sqlite3.Connection) -> list[dict[str, Any]]:
    pills = []
    for key, label, stake, prefixes in SUPPLIERS:
        last = _last_conclusive_run(db, prefixes)
        if not last or str(last.get("status")) != "failed":
            continue
        pills.append(
            _pill(
                key=key,
                label=label,
                state="failed",
                metric="last call failed",
                detail=f"{stake} {str(last.get('error') or last.get('message') or '').strip()}",
                tier="problem",
                href=SETTINGS_CONNECTIONS,
                checked_at=last.get("at"),
            )
        )
    return pills


def _map_pill(db: sqlite3.Connection) -> dict[str, Any] | None:
    """Papers that have a vector but no place on the map.

    Keyed on the EMBEDDING set rather than `papers.updated_at`, per the semantic-
    map rule: hydration touches most rows weekly, so a paper-timestamp gauge
    would report the layout stale every week regardless of the truth.
    """
    embedded = int(_scalar(db, "SELECT COUNT(*) FROM publication_embeddings") or 0)
    if embedded == 0:
        return None
    placed = int(_scalar(db, "SELECT COUNT(DISTINCT paper_id) FROM publication_clusters") or 0)
    missing = max(0, embedded - placed)
    if missing == 0 or (missing / embedded) * 100 <= MAP_GAP_TOLERANCE_PCT:
        return None
    return _pill(
        key="maps",
        label="Maps",
        state="warning",
        metric=f"{missing:,} papers unplaced",
        detail=(
            f"{missing:,} papers have a vector but no position in the layout, so "
            "the map is missing them. Rebuild the layout from the Map page."
        ),
        tier="problem",
        href="#/map",
    )


# ---------------------------------------------------------------------------


def assess(db: sqlite3.Connection, *, now: datetime | None = None) -> list[dict[str, Any]]:
    """Every pill Home's status line should carry, in reading order. Pure read.

    Order is fixed so the line never reorders between loads: the master switch,
    then the subsystems that produce the figures, the two core communication
    capabilities, then whatever is broken.
    """
    from alma.api.helpers import table_exists

    if not table_exists(db, "operation_status"):
        return []

    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)

    pills: list[dict[str, Any]] = [
        _automation(db),
        _feed(db, now=moment),
        _discovery(db, now=moment),
        _embeddings(db),
        _inbox_pill(db, now=moment),
        _alerts_pill(db, now=moment),
        *_supplier_pills(db),
    ]
    map_pill = _map_pill(db)
    if map_pill is not None:
        pills.append(map_pill)
    return pills
