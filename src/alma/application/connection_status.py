"""Is this set, and does it work? One answer per dependency.

Settings, Home and onboarding all read this module, so a dependency cannot be
"Working" on one page and failing on another. It is the owner of the state
vocabulary below; a UI renders `state`, `severity` and `status_label`, it never
derives them.

| state              | meaning                                                  |
|--------------------|----------------------------------------------------------|
| `missing_required` | not set, and something does not work until it is         |
| `missing_optional` | not set, and that is fine; says what setting it adds     |
| `unverified`       | saved, never checked, or changed since the last check    |
| `working`          | a real check passed; `checked_at` says when              |
| `failing`          | a real check or job failed; `detail` is the real error   |
| `checking`         | a check is running now                                   |
| `cannot_check`     | the question could not be asked (network off, 429, DNS)  |
| `off`              | the plugin or feature is switched off; its values stay   |
| `last_used`        | cannot be probed, but it has delivered; says when        |
| `not_seen`         | cannot be probed, and it has not delivered yet           |

**Saved never means working.** Only a check that ran against the exact current
value earns `working`: each verdict carries a keyed stamp of what it tested, so
a value changed in the UI, in the environment or on disk drops the old verdict
by itself, without a write on the read path.

**A verdict needs an answer.** Network off, a timeout, a 429 or a 5xx say the
server did not answer the question, so they read `cannot_check`, never
`failing`. Only a refusal (401, `invalid_auth`, wrong password) is `failing`.

**What cannot be asked is never Working.** A browser extension or a Zotero import
runs at the other end, on no schedule of ours, so all ALMa knows is when it last
delivered. Those rows are `passive` and read `last_used` or `not_seen`.

**No live probe on a read.** `assess` is a pure local read, safe on Home's hot
path. Checks run through :func:`start_check`, on save or on an explicit press,
as Activity jobs, so the verdict lives in `operation_status`. `cancelled` is not
a verdict, because it says who stopped a job and nothing about the provider.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets as secrets_module
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import requests

from alma.config import (
    get_backend,
    get_contact_email,
    get_openalex_api_key,
    get_semantic_scholar_api_key,
    get_setting,
)
from alma.core.network_policy import NetworkAccessDisabledError, network_access_enabled
from alma.core.redaction import redact_sensitive_text
from alma.core.secrets import (
    SECRET_CONNECTION_STAMP_SALT,
    SECRET_OPENALEX_API_KEY,
    SECRET_SEMANTIC_SCHOLAR_API_KEY,
    get_secret,
    set_secret,
)
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

ConnectionState = Literal[
    "missing_required",
    "missing_optional",
    "unverified",
    "working",
    "failing",
    "checking",
    "cannot_check",
    "off",
    "last_used",
    "not_seen",
]

#: Job outcomes that PROVE something. `cancelled` is deliberately absent: it is
#: the most common row in the ledger and proves nothing about a provider.
CONCLUSIVE_STATUSES = ("completed", "noop", "failed")

#: A check that has been silent this long died with its process: read the last
#: verdict rather than spinning forever on an orphaned row.
CHECK_STALE_AFTER = timedelta(minutes=5)

#: What a dependency's job failed with when the network switch stopped it. That
#: is `cannot_check` territory, so it is not evidence against the provider.
_NETWORK_OFF_ERROR = "External network access is disabled"

STATE_SEVERITY: dict[str, str] = {
    "missing_required": "warning",
    "missing_optional": "unknown",
    "unverified": "info",
    "working": "ok",
    "failing": "critical",
    "checking": "info",
    "cannot_check": "unknown",
    "off": "unknown",
    "last_used": "unknown",
    "not_seen": "unknown",
}

STATE_LABEL: dict[str, str] = {
    "missing_required": "Needs setting",
    "missing_optional": "Not set",
    "unverified": "Set, not checked",
    "working": "Working",
    "failing": "Not working",
    "checking": "Checking",
    "cannot_check": "Can't check",
    "off": "Off",
    "last_used": "Last used",
    "not_seen": "Not seen yet",
}

#: Worst first, for a section header. A value nothing can check sits below
#: `working` (see `_rank`), or that section could never read as healthy.
_WORST_FIRST = (
    "failing",
    "missing_required",
    "checking",
    "cannot_check",
    "unverified",
    "working",
    "settled",
    "last_used",
    "missing_optional",
    "not_seen",
    "off",
)

#: Section headline per worst state; `{n}` is how many rows share that state.
_SECTION_LABEL: dict[str, str] = {
    "failing": "{n} not working",
    "missing_required": "{n} needs setting",
    "checking": "Checking",
    "cannot_check": "{n} can't be checked",
    "unverified": "{n} not checked",
    "working": "Working",
    "settled": "Set",
    "last_used": "In use",
    "missing_optional": "Nothing set",
    "not_seen": "Nothing set",
    "off": "Off",
}


@dataclass(frozen=True)
class FieldReading:
    """One input a person can fill in."""

    name: str
    label: str
    is_set: bool
    required: bool = False
    #: What filling it in adds. Shown on an empty optional field.
    adds: str = ""


@dataclass(frozen=True)
class Reading:
    """What is configured right now. Local and cheap: no socket, no job."""

    is_set: bool
    required: bool = False
    #: The values a verdict is about. May hold secrets; only ever keyed-hashed.
    stamp_parts: tuple[str, ...] = ()
    #: Switched off, so nothing else about it matters.
    off: bool = False
    off_detail: str = "Switched off. What you typed is kept."
    #: Certain from here, needing no check (a missing package, say).
    broken: str | None = None
    #: Sentence for the not-set states.
    missing_detail: str = ""
    #: Where the value comes from, when that is not ALMa's own store.
    source: Literal["saved", "environment"] | None = None
    #: Why no check exists. Set means the dependency stays `unverified`, honestly.
    no_check_reason: str | None = None
    #: Replaces "Set, not checked" for a value there is nothing to check about.
    set_label: str | None = None
    fields: tuple[FieldReading, ...] = ()
    #: Replaces "Saved, but never tested." where nothing was saved.
    never_checked_detail: str = "Saved, but never tested."
    #: Nothing can be asked: the row reads `last_used` or `not_seen`, never `working`.
    passive: bool = False
    #: When it last delivered, for a row the job ledger cannot answer for.
    last_used_at: object = None
    #: Passed to the check, for one that needs a stored id.
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Connection:
    """One thing the user sets, and how to tell whether it works."""

    id: str
    label: str
    #: The Settings section whose header summarises it.
    section: str
    read: Callable[[sqlite3.Connection], Reading]
    #: `{ok: True|False|None, message, error?, action?}`. Absent means no check.
    check: Callable[[Reading], dict[str, Any]] | None = None
    operation_key: str = ""
    #: Jobs that talk to this dependency. A `failed` one newer than the last
    #: check is evidence; a `completed` one is not, since most run without a key.
    job_prefixes: tuple[str, ...] = ()
    #: Jobs whose success DOES prove it, for a stack with no separate check.
    proving_job_prefixes: tuple[str, ...] = ()
    #: False for what never leaves this machine, so the network switch is moot.
    external: bool = True
    #: What in ALMa relies on it, in the user's words.
    used_by: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Ledger reads
# ---------------------------------------------------------------------------


def last_conclusive_run(
    db: sqlite3.Connection, prefixes: tuple[str, ...]
) -> dict[str, Any] | None:
    """The most recent run of any of `prefixes` that reached a verdict.

    Prefixes are matched with `LIKE prefix || '%'` because ledger keys are
    parameterized. Every listed prefix must be a job that actually TALKS to the
    subsystem; one that reads the DB afterwards would vouch for a dead provider.
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


def _last_check(db: sqlite3.Connection, operation_key: str) -> dict[str, Any] | None:
    """The newest finished check, with its result parsed."""
    row = db.execute(
        """
        SELECT status, error, result_json, COALESCE(finished_at, updated_at) AS at
        FROM operation_status
        WHERE operation_key = ? AND status IN ('completed', 'failed')
        ORDER BY at DESC
        LIMIT 1
        """,
        (operation_key,),
    ).fetchone()
    if row is None:
        return None
    try:
        result = json.loads(row["result_json"]) if row["result_json"] else {}
    except (TypeError, ValueError):
        result = {}
    return {"at": row["at"], "error": row["error"], "result": result}


def _parse(stamp: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(stamp or "").strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _check_running(db: sqlite3.Connection, operation_key: str, now: datetime) -> bool:
    row = db.execute(
        """
        SELECT updated_at FROM operation_status
        WHERE operation_key = ? AND status IN ('queued', 'scheduled', 'running')
        ORDER BY updated_at DESC LIMIT 1
        """,
        (operation_key,),
    ).fetchone()
    if row is None:
        return False
    at = _parse(row["updated_at"])
    return at is not None and now - at < CHECK_STALE_AFTER


def _newer(a: object, b: object) -> bool:
    """`a` is strictly later than `b`; a missing `b` loses."""
    parsed_a, parsed_b = _parse(a), _parse(b)
    if parsed_a is None:
        return False
    return parsed_b is None or parsed_a > parsed_b


# ---------------------------------------------------------------------------
# Config stamp
# ---------------------------------------------------------------------------


def config_stamp(parts: tuple[str, ...], *, create: bool = False) -> str | None:
    """A keyed digest of what a check tested, or None when no key exists yet.

    Keyed because the ledger is exported in JSON snapshots and may hold the
    digest of a low-entropy SMTP password: a plain hash could be brute-forced
    from a snapshot alone. The key sits in the secret store, out of the ledger.
    """
    salt = get_secret(SECRET_CONNECTION_STAMP_SALT)
    if salt is None:
        if not create:
            return None
        salt = secrets_module.token_hex(32)
        set_secret(SECRET_CONNECTION_STAMP_SALT, salt)
    digest = hashlib.blake2b(
        json.dumps(list(parts)).encode("utf-8"),
        key=bytes.fromhex(salt),
        digest_size=16,
    )
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# One verdict
# ---------------------------------------------------------------------------


def _record(
    connection: Connection,
    reading: Reading,
    state: ConnectionState,
    detail: str,
    *,
    checked_at: object = None,
    last_worked_at: object = None,
    last_used_at: object = None,
    action: str | None = None,
    results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    label = STATE_LABEL[state]
    if state == "unverified" and reading.set_label:
        label = reading.set_label
    return {
        "id": connection.id,
        "label": connection.label,
        "section": connection.section,
        "state": state,
        "severity": STATE_SEVERITY[state],
        "status_label": label,
        "detail": redact_sensitive_text(detail),
        "action": redact_sensitive_text(action) if action else None,
        "required": reading.required,
        "source": reading.source,
        # A check needs a value to test and a network to ask over.
        "checkable": (
            connection.check is not None
            and reading.is_set
            and not reading.off
            and network_access_enabled()
        ),
        "checked_at": checked_at,
        "last_worked_at": last_worked_at,
        "last_used_at": last_used_at,
        "used_by": list(connection.used_by),
        "results": results or [],
        "fields": [
            {
                "name": f.name,
                "label": f.label,
                "is_set": f.is_set,
                "required": f.required,
                "adds": f.adds,
            }
            for f in reading.fields
        ],
    }


def _job_failure(db: sqlite3.Connection, connection: Connection) -> dict[str, Any] | None:
    """The newest job verdict, when it is a failure that says something."""
    last = last_conclusive_run(db, connection.job_prefixes)
    if not last or str(last.get("status")) != "failed":
        return None
    text = str(last.get("error") or last.get("message") or "").strip()
    if text.startswith(_NETWORK_OFF_ERROR):
        return None
    return {"at": last.get("at"), "text": text or "The last job that used it failed."}


def assess_connection(
    db: sqlite3.Connection, connection: Connection, *, now: datetime | None = None
) -> dict[str, Any]:
    """One dependency's state, most decisive fact first. Pure local read."""
    moment = now or datetime.now(timezone.utc)
    reading = connection.read(db)

    used = last_conclusive_run(db, connection.job_prefixes) if connection.job_prefixes else None
    used_at = reading.last_used_at or (
        used.get("at") if used and str(used.get("status")) == "completed" else None
    )

    def out(state: ConnectionState, detail: str, **extra: Any) -> dict[str, Any]:
        return _record(connection, reading, state, detail, last_used_at=used_at, **extra)

    if reading.off:
        return out("off", reading.off_detail)
    if reading.passive:
        if reading.last_used_at:
            return out("last_used", str(reading.no_check_reason or ""))
        return out("not_seen", reading.missing_detail)
    if not reading.is_set and reading.required:
        return out("missing_required", reading.missing_detail)
    if connection.operation_key and _check_running(db, connection.operation_key, moment):
        return out("checking", "Checking now.")
    if reading.broken:
        return out("failing", reading.broken)

    network_on = network_access_enabled() or not connection.external
    last = _last_check(db, connection.operation_key) if connection.operation_key else None
    result = (last or {}).get("result") or {}
    current = config_stamp(reading.stamp_parts) if reading.stamp_parts else None
    tested_this_value = current is not None and result.get("config_stamp") == current
    verdict: bool | None = result.get("ok") if tested_this_value else None
    checked_at = last["at"] if tested_this_value else None
    worked_at = checked_at if verdict is True else None

    # A job that failed after the last check is newer evidence than the check.
    if network_on:
        failure = _job_failure(db, connection)
        if failure and _newer(failure["at"], (last or {}).get("at")):
            return out(
                "failing",
                failure["text"],
                checked_at=failure["at"],
                action="Check it again once the cause is fixed.",
            )

    if not reading.is_set:
        return out("missing_optional", reading.missing_detail)

    if connection.proving_job_prefixes:
        proof = last_conclusive_run(db, connection.proving_job_prefixes)
        if proof and str(proof.get("status")) == "completed":
            return out(
                "working",
                str(proof.get("message") or "Its last run finished.").strip(),
                checked_at=proof.get("at"),
                last_worked_at=proof.get("at"),
            )

    if not network_on:
        return out(
            "cannot_check",
            "External network access is off, so nothing can be asked right now.",
            checked_at=checked_at,
            last_worked_at=worked_at,
            action="Turn on External network access, then check again.",
        )

    if tested_this_value and "ok" in result:
        message = str(result.get("message") or "").strip()
        rows = result.get("results") if isinstance(result.get("results"), list) else None
        if verdict is True:
            return out("working", message, checked_at=checked_at, last_worked_at=worked_at, results=rows)
        if verdict is False:
            return out(
                "failing",
                message or str(result.get("error") or "The check failed."),
                checked_at=checked_at,
                action=result.get("action"),
                results=rows,
            )
        return out(
            "cannot_check",
            message or "The check did not get an answer.",
            checked_at=checked_at,
            action=result.get("action"),
            results=rows,
        )

    if reading.no_check_reason is not None:
        return out("unverified", reading.no_check_reason)
    if last is not None:
        return out("unverified", "Changed since the last check.")
    return out("unverified", reading.never_checked_detail)


# ---------------------------------------------------------------------------
# Core connections
# ---------------------------------------------------------------------------


def _secret_source(value: str | None, env_key: str, secret_key: str) -> Literal["saved", "environment"] | None:
    """Which store a resolved value came from. Environment wins in every resolver."""
    if not value:
        return None
    if (os.getenv(env_key) or "").strip() and get_secret(secret_key) != value:
        return "environment"
    return "saved"


def _read_openalex(_db: sqlite3.Connection) -> Reading:
    key = get_openalex_api_key()
    required = get_backend() == "openalex"
    return Reading(
        is_set=bool(key),
        required=required,
        stamp_parts=(key or "",),
        missing_detail=(
            "Without a key OpenAlex allows 100 credits a day, then Feed, author refreshes "
            "and paper metadata stop."
        ),
        source=_secret_source(key, "OPENALEX_API_KEY", SECRET_OPENALEX_API_KEY),
        fields=(
            FieldReading(
                "openalex_api_key",
                "API key",
                bool(key),
                required=required,
                adds="10,000 credits a day instead of 100",
            ),
        ),
    )


def _check_openalex(_reading: Reading) -> dict[str, Any]:
    from alma.openalex.http import get_client

    probe = get_client().probe_credentials()
    return {
        "ok": probe.get("valid"),
        "message": probe.get("detail"),
        "action": "Paste a new key from openalex.org." if probe.get("valid") is False else None,
    }


def _read_contact_email(_db: sqlite3.Connection) -> Reading:
    email = get_contact_email()
    env_set = any(
        (os.getenv(name) or "").strip()
        for name in ("ALMA_CONTACT_EMAIL", "CONTACT_EMAIL", "CROSSREF_MAILTO", "OPENALEX_EMAIL")
    )
    adds = "Unpaywall in PDF search, and a contact address for OpenAlex and Crossref"
    return Reading(
        is_set=bool(email),
        stamp_parts=(email or "",),
        missing_detail=f"Setting it adds {adds}.",
        source=("environment" if env_set else "saved") if email else None,
        no_check_reason="No service confirms an address, so this one cannot be checked.",
        fields=(FieldReading("openalex_email", "Email", bool(email), adds=adds),),
    )


def _read_semantic_scholar(_db: sqlite3.Connection) -> Reading:
    key = get_semantic_scholar_api_key()
    adds = "a rate of your own, instead of the shared pool that often answers 429"
    return Reading(
        is_set=bool(key),
        stamp_parts=(key or "",),
        missing_detail=f"Works without one. A key adds {adds}.",
        source=_secret_source(key, "SEMANTIC_SCHOLAR_API_KEY", SECRET_SEMANTIC_SCHOLAR_API_KEY),
        fields=(FieldReading("semantic_scholar_api_key", "API key", bool(key), adds=adds),),
    )


def _check_semantic_scholar(_reading: Reading) -> dict[str, Any]:
    """One `/paper/search?limit=1` with the current key, no retries."""
    from alma.core.http_sources import get_source_http_client

    try:
        resp = get_source_http_client("semantic_scholar").get(
            "/paper/search",
            params={"query": "machine learning", "limit": 1, "fields": "title"},
            headers={"Accept": "application/json"},
            timeout=8,
            max_retries=0,
        )
    except requests.RequestException as exc:
        return {"ok": None, "message": f"Could not reach Semantic Scholar ({exc.__class__.__name__})."}
    if resp.status_code == 200:
        return {"ok": True, "message": "Semantic Scholar accepted the key."}
    if resp.status_code in (401, 403):
        return {
            "ok": False,
            "message": f"Semantic Scholar refused the key (HTTP {resp.status_code}).",
            "action": "Paste a new key from semanticscholar.org.",
        }
    if resp.status_code == 429:
        return {
            "ok": None,
            "message": "Semantic Scholar is rate limiting, so it said nothing about the key.",
            "action": "Try again in a minute.",
        }
    return {"ok": None, "message": f"Unexpected Semantic Scholar response (HTTP {resp.status_code})."}


def _read_crossref(_db: sqlite3.Connection) -> Reading:
    return Reading(
        is_set=True,
        stamp_parts=("crossref",),
        set_label="Not checked",
        never_checked_detail="Needs no setup. ALMa has not asked it yet.",
    )


def _check_crossref(_reading: Reading) -> dict[str, Any]:
    """One `/works?rows=0`. Crossref takes no key, so this asks only if it answers."""
    from alma.core.http_sources import get_source_http_client

    try:
        resp = get_source_http_client("crossref").get(
            "/works", params={"rows": 0}, timeout=8, max_retries=0
        )
    except requests.RequestException as exc:
        return {"ok": None, "message": f"Could not reach Crossref ({exc.__class__.__name__})."}
    if resp.status_code == 200:
        return {"ok": True, "message": "Crossref answered."}
    return {"ok": None, "message": f"Crossref did not answer normally (HTTP {resp.status_code})."}


def _read_browser_connector(db: sqlite3.Connection) -> Reading:
    last = db.execute(
        "SELECT MAX(added_at) FROM papers WHERE added_from = 'browser_extension'"
    ).fetchone()[0]
    return Reading(
        is_set=bool(last),
        passive=True,
        last_used_at=last,
        missing_detail="No paper has arrived from the browser connector yet.",
        no_check_reason="ALMa cannot see whether it is installed, only when it last delivered a paper.",
    )


def _read_zotero(db: sqlite3.Connection) -> Reading:
    from alma.core.secrets import SECRET_ZOTERO_API_KEY

    last = db.execute(
        """
        SELECT MAX(COALESCE(finished_at, updated_at)) FROM operation_status
        WHERE operation_key LIKE 'imports.zotero:%' AND status = 'completed'
        """
    ).fetchone()[0]
    has_key = bool(get_secret(SECRET_ZOTERO_API_KEY))
    return Reading(
        is_set=has_key,
        passive=True,
        last_used_at=last,
        missing_detail=(
            "A key is saved. It is only tested when you import, in the Import dialog."
            if has_key
            else "Set up in the Import dialog, where it is tested when you import."
        ),
        no_check_reason="A check needs your library id, which is asked for in the Import dialog.",
    )


def _read_google_scholar(_db: sqlite3.Connection) -> Reading:
    in_use = (
        get_backend() == "scholar"
        or bool(get_setting("id_resolution_scholar_scrape_auto_enabled", False))
        or bool(get_setting("id_resolution_scholar_scrape_manual_enabled", False))
    )
    return Reading(
        is_set=in_use,
        off=not in_use,
        off_detail="Off. ALMa does not scrape Google Scholar.",
        no_check_reason="A check would itself scrape Google, so this one is not tested.",
    )


def _read_name(db: sqlite3.Connection) -> Reading:
    row = db.execute("SELECT value FROM discovery_settings WHERE key = 'user.name'").fetchone()
    name = str(row["value"] or "").strip() if row else ""
    adds = "the greeting on Home"
    return Reading(
        is_set=bool(name),
        stamp_parts=(name,),
        missing_detail=f"Setting it adds {adds}.",
        no_check_reason="Used only for the greeting on Home. There is nothing to check.",
        set_label="Set",
        fields=(FieldReading("name", "Your name", bool(name), adds=adds),),
    )


def _read_owner(db: sqlite3.Connection) -> Reading:
    row = db.execute(
        """
        SELECT a.openalex_id AS openalex_id
        FROM followed_authors fa LEFT JOIN authors a ON a.id = fa.author_id
        WHERE fa.is_owner = 1 LIMIT 1
        """
    ).fetchone()
    openalex_id = str(row["openalex_id"] or "").strip() if row else ""
    adds = "your own papers in Library, and your output kept apart from everyone else's"
    return Reading(
        is_set=bool(row),
        stamp_parts=(openalex_id,) if row else (),
        missing_detail=f"Claiming your profile adds {adds}.",
        no_check_reason=None if openalex_id else "This profile has no OpenAlex id to look up.",
        context={"openalex_id": openalex_id},
        fields=(FieldReading("owner", "Your author profile", bool(row), adds=adds),),
    )


def _check_owner(reading: Reading) -> dict[str, Any]:
    from alma.openalex.client import fetch_author_profile

    openalex_id = str(reading.context.get("openalex_id") or "")
    profile = fetch_author_profile(openalex_id) if openalex_id else None
    if profile:
        return {"ok": True, "message": f"OpenAlex has this profile: {profile.get('display_name')}."}
    return {"ok": None, "message": "OpenAlex did not return this profile, so it could not be confirmed."}


def _read_ai(db: sqlite3.Connection) -> Reading:
    from alma.ai.import_state import module_available
    from alma.ai.providers import configured_provider_name

    provider = configured_provider_name(db)
    is_set = provider not in ("", "none")
    missing = [m for m in ("torch", "transformers", "adapters", "numpy") if not module_available(m)]
    return Reading(
        is_set=is_set,
        stamp_parts=(provider,),
        broken=(
            f"This environment cannot compute embeddings: missing {', '.join(missing)}."
            if is_set and missing
            else None
        ),
        missing_detail=(
            "Downloaded vectors and lexical ranking still work. A provider adds vectors "
            "for papers Semantic Scholar does not cover."
        ),
        no_check_reason="Packages are present. It counts as working once a compute job has finished.",
        fields=(FieldReading("provider", "Embedding provider", is_set),),
    )


CORE_CONNECTIONS: tuple[Connection, ...] = (
    Connection(id="name", label="Your name", section="you", read=_read_name, external=False),
    Connection(
        id="owner",
        label="Your author profile",
        section="you",
        read=_read_owner,
        check=_check_owner,
        operation_key="integrations.owner.check",
    ),
    Connection(
        id="openalex",
        label="OpenAlex",
        section="connections",
        read=_read_openalex,
        check=_check_openalex,
        operation_key="integrations.openalex.check",
        used_by=("Feed", "Authors", "Discovery", "Paper metadata"),
        job_prefixes=(
            "papers.rehydrate_metadata:openalex",
            "authors.deep_refresh",
            "authors.rehydrate_metadata",
        ),
    ),
    Connection(
        id="contact_email",
        label="Contact email",
        section="connections",
        read=_read_contact_email,
        external=False,
    ),
    Connection(
        id="semantic_scholar",
        label="Semantic Scholar",
        section="connections",
        read=_read_semantic_scholar,
        check=_check_semantic_scholar,
        operation_key="integrations.semantic_scholar.check",
        job_prefixes=("ai.backfill_s2_vectors", "ai.title_resolution_sweep"),
        used_by=("Discovery", "Embeddings", "Identifier resolution"),
    ),
    Connection(
        id="crossref",
        label="Crossref",
        section="connections",
        read=_read_crossref,
        check=_check_crossref,
        operation_key="integrations.crossref.check",
        used_by=("Paper metadata", "Abstract recovery"),
    ),
    Connection(
        id="zotero",
        label="Zotero",
        section="connections",
        read=_read_zotero,
        used_by=("Library import",),
    ),
    Connection(
        id="browser_connector",
        label="Browser connector",
        section="connections",
        read=_read_browser_connector,
        external=False,
        used_by=("Library",),
    ),
    Connection(
        id="google_scholar",
        label="Google Scholar",
        section="connections",
        read=_read_google_scholar,
        external=False,
        used_by=("Identifier resolution",),
    ),
    Connection(
        id="ai",
        label="AI embeddings",
        section="intelligence",
        read=_read_ai,
        proving_job_prefixes=("ai.compute_embeddings",),
        job_prefixes=("ai.compute_embeddings",),
        external=False,
    ),
)


# ---------------------------------------------------------------------------
# Plugin connections: one per manifest, so a new plugin needs no entry here
# ---------------------------------------------------------------------------


def _plugin_fields(manifest: Any, config: dict[str, Any]) -> tuple[FieldReading, ...]:
    """Fields a person fills in, from the plugin's own schema. Switches and
    defaulted numbers are always "set", so they have no state to show."""
    fields = []
    properties = manifest.config_schema().get("properties", {})
    for name, schema in properties.items():
        if schema.get("type") != "string":
            continue
        fields.append(
            FieldReading(
                name=name,
                label=str(schema.get("title") or name),
                is_set=bool(str(config.get(name) or "").strip()),
                required=bool(schema.get("x-alma-required")),
                adds=str(schema.get("x-alma-adds") or ""),
            )
        )
    return tuple(fields)


def _plugin_reading(manifest: Any, db: sqlite3.Connection) -> Reading:
    """Read a plugin through its manifest: local by the `status_factory` contract."""
    status = manifest.status(db)
    config = manifest.config(db)
    fields = _plugin_fields(manifest, config)
    can = any(status.get(f"can_{verb}") for verb in ("send", "receive", "fetch"))
    empty = [f.label for f in fields if f.required and not f.is_set]
    is_set = bool(status.get("configured")) and (can or bool(status.get("blocked")))
    missing = (
        f"Needs {', '.join(empty)}."
        if empty
        else str(status.get("missing") or "Finish its setup below.")
    )
    secrets_in_use = manifest.secret_values() if manifest.secret_values else ()
    return Reading(
        is_set=is_set,
        required=True,
        stamp_parts=(json.dumps(config, sort_keys=True), *secrets_in_use),
        off=not manifest.is_enabled(),
        broken=status.get("blocked"),
        missing_detail=missing,
        source=status.get("source"),
        no_check_reason=(
            None
            if manifest.has_connection_test()
            else "It has no test. A failure shows on the paper you asked for."
        ),
        fields=fields,
    )


#: What in ALMa relies on a plugin's capability.
USED_BY_CAPABILITY = {"send": "Alerts", "receive": "Inbox", "pdf_source": "PDF search"}


def _plugin_connection(manifest: Any) -> Connection:
    def check(_reading: Reading) -> dict[str, Any]:
        return asyncio.run(manifest.test_connection())

    prefixes = ("inbox.capture_sweep",) if manifest.can("receive") else ()
    return Connection(
        id=manifest.id,
        label=manifest.display_name,
        section="plugins",
        read=lambda db: _plugin_reading(manifest, db),
        check=check if manifest.has_connection_test() else None,
        operation_key=f"integrations.{manifest.id}.test",
        job_prefixes=prefixes,
        used_by=tuple(USED_BY_CAPABILITY[c] for c in manifest.capabilities),
    )


def connections() -> tuple[Connection, ...]:
    """Every dependency, in reading order. Plugins come and go with add-ons."""
    from alma.plugins.registry import get_plugin_registry

    plugins = tuple(_plugin_connection(m) for m in get_plugin_registry().all())
    return (*CORE_CONNECTIONS, *plugins)


def get_connection(connection_id: str) -> Connection:
    for connection in connections():
        if connection.id == connection_id:
            return connection
    raise KeyError(connection_id)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def assess(db: sqlite3.Connection, *, now: datetime | None = None) -> list[dict[str, Any]]:
    """One record per dependency. Pure local read, safe on a hot path."""
    moment = now or datetime.now(timezone.utc)
    return [assess_connection(db, connection, now=moment) for connection in connections()]


def state_of(
    db: sqlite3.Connection, connection_id: str, *, now: datetime | None = None
) -> dict[str, Any] | None:
    """One dependency's record, for a surface that shows only that one."""
    try:
        connection = get_connection(connection_id)
    except KeyError:
        return None
    return assess_connection(db, connection, now=now)


def _rank(record: dict[str, Any]) -> str:
    """A state for the summary. A value no check exists for cannot pull its
    section down: it would read "1 not checked" for ever, with nothing to do."""
    if record["state"] == "unverified" and not record["checkable"]:
        return "settled"
    return record["state"]


def _section_label(worst: str, count: int) -> str:
    label = _SECTION_LABEL[worst].format(n=count)
    return label.replace("needs", "need") if count > 1 else label


def summarise(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Each section's worst state, for its header."""
    sections: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        sections.setdefault(record["section"], []).append(record)

    out: dict[str, dict[str, Any]] = {}
    for section, rows in sections.items():
        ranks = [_rank(row) for row in rows]
        worst = min(ranks, key=_WORST_FIRST.index)
        state = "unverified" if worst == "settled" else worst
        out[section] = {
            "state": state,
            "severity": STATE_SEVERITY[state],
            "status_label": _section_label(worst, ranks.count(worst)),
        }
    return out


# ---------------------------------------------------------------------------
# The one way to run a check
# ---------------------------------------------------------------------------


class CheckRefusedError(Exception):
    """This check cannot start. `status_code` says why, `str()` says what to do."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


def _clean(value: object) -> object:
    return redact_sensitive_text(value) if isinstance(value, str) else value


def _run_check(connection: Connection, reading: Reading, stamp: str | None) -> dict[str, Any]:
    """Ask, and report. A check that raises did not get an answer, so it is
    `ok: None`, never a verdict against a value nobody refused."""
    try:
        outcome = dict(connection.check(reading))  # type: ignore[misc]
    except NetworkAccessDisabledError as exc:
        outcome = {"ok": None, "message": str(exc)}
    except Exception as exc:  # noqa: BLE001 - an external boundary reports, never raises
        logger.warning("Check of %s did not finish: %s", connection.id, redact_sensitive_text(str(exc)))
        outcome = {"ok": None, "message": f"The check did not finish: {exc}"}
    outcome = {key: _clean(value) for key, value in outcome.items()}
    if not outcome.get("message"):
        outcome["message"] = {True: "Check passed.", False: "Check failed."}.get(
            outcome.get("ok"), "The check did not get an answer."
        )
    return {**outcome, "config_stamp": stamp}


def start_check(db: sqlite3.Connection, connection_id: str, *, actor: str = "api_user") -> dict[str, Any]:
    """Queue a check as an Activity job and return its envelope.

    What is tested is read here, once, so a value changed while the job waits
    cannot earn a verdict for itself. Raises :class:`CheckRefusedError`.
    """
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    try:
        connection = get_connection(connection_id)
    except KeyError:
        raise CheckRefusedError(404, f"Unknown connection '{connection_id}'") from None
    if connection.check is None:
        raise CheckRefusedError(400, f"{connection.label} has no check.")
    reading = connection.read(db)
    if reading.off:
        raise CheckRefusedError(409, f"{connection.label} is switched off.")
    if not reading.is_set:
        raise CheckRefusedError(400, f"{connection.label} is not set, so there is nothing to check.")
    if not network_access_enabled():
        raise CheckRefusedError(409, "External network access is off, so nothing can be checked.")

    operation_key = connection.operation_key
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            existing["job_id"],
            status=str(existing.get("status") or "running"),
            operation_key=operation_key,
            message=f"{connection.label} check already running",
            already_running=True,
        )

    stamp = config_stamp(reading.stamp_parts, create=True)
    job_id = f"connection_{connection.id}_check_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        actor=actor,
        started_at=utcnow().isoformat(),
        message=f"Checking {connection.label}",
    )
    schedule_immediate(job_id, _run_check, connection, reading, stamp)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"{connection.label} check queued",
    )
