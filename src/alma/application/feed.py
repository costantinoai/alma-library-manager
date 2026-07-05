"""Feed use-cases backed by v3 feed_items table."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from concurrent.futures import as_completed

from alma.core.concurrency import bounded_thread_pool
from datetime import datetime, timedelta
import uuid
from typing import Optional

from alma.application.feed_query_language import (
    FeedQuerySyntaxError,
    keyword_expression_matches,
    keyword_retrieval_query,
)
from . import feed_monitors as monitor_app
from . import library as library_app
from alma.core.db_retry import commit_with_retry
from alma.core.db_write import run_write_unit, write_section
from alma.core.scoring_math import clamp
from alma.core.settings_helpers import (
    setting_bool as _setting_bool,
    setting_float as _setting_float,
    setting_int as _setting_int,
)
from alma.core.http_sources import (
    openalex_usage_delta,
    openalex_usage_snapshot,
    source_diagnostics_scope,
)
from alma.core.paper_updates import fill_only_update_paper
from alma.core.components import (
    link_orphan_components,
    not_component_sql,
    resolve_component,
)
from alma.core.utils import (
    clean_display_text,
    normalize_doi,
    normalize_title_key,
    resolve_existing_paper_id,
)

logger = logging.getLogger(__name__)

_MONITOR_QUERY_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "this",
    "that",
    "using",
    "based",
    "study",
    "analysis",
    "approach",
    "approaches",
    "method",
    "methods",
}

VALID_FEED_ACTIONS = {
    "add",
    "like",
    "love",
    "dislike",
    "dismiss",
}
# Resting `feed_items.status` values. Note that `dismiss` is an *action* that
# settles to the `dismissed` status (hidden from the inbox forever); every
# other action settles to a status that matches its own name.
VALID_FEED_STATUSES = {"new", "dismissed", *(VALID_FEED_ACTIONS - {"dismiss"})}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _commit_if_pending(db: sqlite3.Connection) -> None:
    """Release the SQLite writer lock before a remote call or a new phase.

    Applies the rule from ``tasks/lessons.md`` ("Bulk background jobs must
    commit per unit of work"): any implicit transaction left open from a
    previous iteration's DML will hold the writer lock across the next
    network call or CPU-heavy phase and freeze concurrent page reads.

    Uses ``commit_with_retry`` rather than a raw commit so a transient
    cross-process lock at the flush boundary is retried, not dropped.
    """
    if db.in_transaction:
        commit_with_retry(db, label="feed flush")


def clear_feed_items_for_monitor(db: sqlite3.Connection, monitor_id: str) -> int:
    """Delete feed rows owned by one monitor definition."""
    if not _table_exists(db, "feed_items"):
        return 0
    cursor = db.execute(
        "DELETE FROM feed_items WHERE COALESCE(monitor_id, '') = ?",
        (str(monitor_id or "").strip(),),
    )
    return int(cursor.rowcount or 0)


def prune_feed_items_for_missing_monitors(db: sqlite3.Connection) -> int:
    """Remove feed rows whose monitor definition no longer exists."""
    if not _table_exists(db, "feed_items") or not _table_exists(db, "feed_monitors"):
        return 0
    cursor = db.execute(
        """
        DELETE FROM feed_items
        WHERE COALESCE(monitor_id, '') <> ''
          AND NOT EXISTS (
              SELECT 1
              FROM feed_monitors fm
              WHERE fm.id = feed_items.monitor_id
          )
        """
    )
    return int(cursor.rowcount or 0)




def _parse_json_dict(raw: object) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}
    return {}


def _normalize_publication_date(raw: object) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    cleaned = text.replace("/", "-")
    full_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", cleaned)
    if full_match:
        year, month, day = full_match.groups()
        return f"{year}-{month}-{day}"
    month_match = re.match(r"^(\d{4})-(\d{2})$", cleaned)
    if month_match:
        year, month = month_match.groups()
        return f"{year}-{month}-01"
    year_match = re.match(r"^(\d{4})$", cleaned)
    if year_match:
        year = year_match.group(1)
        return f"{year}-01-01"
    return None


def _candidate_publication_date(candidate: dict) -> str | None:
    """Return a real YYYY-MM-DD publication date, or None.

    We deliberately do NOT fall back to ``{year}-01-01`` when only a
    publication_year is known. Storing a fabricated Jan-1 date makes every
    downstream date filter (the Feed's chronological window, "recent" sort)
    silently wrong. Consumers should fall back to ``feed_items.fetched_at``
    or ``papers.fetched_at`` when ``publication_date`` is empty — that is
    the historical fetch log the system already maintains.
    """
    for key in ("publication_date", "published_date", "date"):
        normalized = _normalize_publication_date(candidate.get(key))
        if normalized:
            return normalized
    return None


def _candidate_year(candidate: dict) -> int | None:
    raw_year = candidate.get("year")
    try:
        if raw_year is not None and str(raw_year).strip():
            return int(raw_year)
    except (TypeError, ValueError):
        pass
    publication_date = _candidate_publication_date(candidate)
    if publication_date:
        try:
            return int(publication_date[:4])
        except (TypeError, ValueError):
            return None
    return None


def _query_terms(query: str) -> list[str]:
    terms = re.findall(r"[a-z0-9]+", str(query or "").lower())
    ordered: list[str] = []
    seen: set[str] = set()
    for term in terms:
        if len(term) < 3 or term in _MONITOR_QUERY_STOPWORDS or term in seen:
            continue
        seen.add(term)
        ordered.append(term)
    return ordered


def _text_tokens(value: object) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


def _normalized_text(value: object) -> str:
    return " ".join(_text_tokens(value))


def _term_matches_tokens(term: str, tokens: list[str]) -> bool:
    if not term or not tokens:
        return False
    return term in set(tokens)


def _terms_within_window(terms: list[str], tokens: list[str], *, window: int) -> bool:
    if not terms or not tokens:
        return False
    if len(terms) == 1:
        return _term_matches_tokens(terms[0], tokens)
    span = max(window, len(terms))
    for start in range(0, max(0, len(tokens) - span + 1)):
        segment = tokens[start:start + span]
        if all(term in segment for term in terms):
            return True
    return False


def _phrase_matches_tokens(terms: list[str], tokens: list[str]) -> bool:
    if not terms or not tokens:
        return False
    phrase_len = len(terms)
    for start in range(0, max(0, len(tokens) - phrase_len + 1)):
        if tokens[start:start + phrase_len] == terms:
            return True
    return False


def _candidate_topic_texts(candidate: dict) -> list[str]:
    texts: list[str] = []
    for topic in candidate.get("topics") or []:
        if isinstance(topic, dict):
            for key in ("term", "display_name", "field", "subfield", "domain"):
                value = str(topic.get(key) or "").strip()
                if value:
                    texts.append(value)
        else:
            value = str(topic or "").strip()
            if value:
                texts.append(value)
    for keyword in candidate.get("keywords") or []:
        if isinstance(keyword, dict):
            value = str(keyword.get("keyword") or keyword.get("display_name") or "").strip()
        else:
            value = str(keyword or "").strip()
        if value:
            texts.append(value)
    category = str(candidate.get("category") or "").strip()
    if category:
        texts.append(category)
    seen: set[str] = set()
    ordered: list[str] = []
    for value in texts:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def _monitor_match_details(query: str, candidate: dict) -> dict[str, float | int]:
    terms = _query_terms(query)
    if not terms:
        return {
            "score": 1.0,
            "coverage": 1.0,
            "title_matches": 0,
            "abstract_matches": 0,
            "topic_matches": 0,
            "journal_matches": 0,
            "phrase_hit": 0,
            "topic_phrase_hit": 0,
            "title_window_hit": 0,
            "abstract_window_hit": 0,
            "topic_window_hit": 0,
        }

    title_tokens = _text_tokens(candidate.get("title"))
    abstract_tokens = _text_tokens(candidate.get("abstract"))
    topic_texts = _candidate_topic_texts(candidate)
    topic_tokens = _text_tokens(" ".join(topic_texts))
    context_tokens = title_tokens + abstract_tokens + topic_tokens

    coverage = sum(1 for term in terms if _term_matches_tokens(term, context_tokens)) / len(terms)
    title_coverage = sum(1 for term in terms if _term_matches_tokens(term, title_tokens)) / len(terms)
    title_matches = sum(1 for term in terms if _term_matches_tokens(term, title_tokens))
    abstract_coverage = sum(1 for term in terms if _term_matches_tokens(term, abstract_tokens)) / len(terms)
    abstract_matches = sum(1 for term in terms if _term_matches_tokens(term, abstract_tokens))
    topic_coverage = sum(1 for term in terms if _term_matches_tokens(term, topic_tokens)) / len(terms)
    topic_matches = sum(1 for term in terms if _term_matches_tokens(term, topic_tokens))
    proximity_window = max(len(terms) + 2, 5)
    phrase_hit = int(
        _phrase_matches_tokens(terms, title_tokens)
        or _phrase_matches_tokens(terms, abstract_tokens)
    )
    topic_phrase_hit = int(
        any(_phrase_matches_tokens(terms, _text_tokens(text)) for text in topic_texts)
    )
    title_window_hit = int(_terms_within_window(terms, title_tokens, window=proximity_window))
    abstract_window_hit = int(_terms_within_window(terms, abstract_tokens, window=proximity_window))
    topic_window_hit = int(_terms_within_window(terms, topic_tokens, window=proximity_window))
    score = (
        (title_coverage * 0.38)
        + (abstract_coverage * 0.32)
        + (topic_coverage * 0.18)
        + (coverage * 0.12)
        + (0.12 if phrase_hit else 0.0)
        + (0.08 if topic_phrase_hit else 0.0)
    )
    return {
        "score": round(min(1.0, score), 4),
        "coverage": round(coverage, 4),
        "title_matches": title_matches,
        "abstract_matches": abstract_matches,
        "topic_matches": topic_matches,
        "journal_matches": 0,
        "phrase_hit": phrase_hit,
        "topic_phrase_hit": topic_phrase_hit,
        "title_window_hit": title_window_hit,
        "abstract_window_hit": abstract_window_hit,
        "topic_window_hit": topic_window_hit,
    }


def _monitor_match_score(query: str, candidate: dict) -> float:
    return float(_monitor_match_details(query, candidate)["score"])


def _monitor_has_explicit_support(
    monitor_type: str,
    query: str,
    candidate: dict,
    match_details: dict[str, float | int],
) -> bool:
    terms = _query_terms(query)
    if not terms:
        return True

    normalized_monitor_type = str(monitor_type or "").strip().lower()
    title_matches = int(match_details.get("title_matches") or 0)
    abstract_matches = int(match_details.get("abstract_matches") or 0)
    topic_matches = int(match_details.get("topic_matches") or 0)
    phrase_hit = bool(match_details.get("phrase_hit"))
    topic_phrase_hit = bool(match_details.get("topic_phrase_hit"))
    title_window_hit = bool(match_details.get("title_window_hit"))
    abstract_window_hit = bool(match_details.get("abstract_window_hit"))
    topic_window_hit = bool(match_details.get("topic_window_hit"))
    source_api = str(candidate.get("source_api") or "").strip().lower()
    if len(terms) == 1:
        return (title_matches + abstract_matches + topic_matches) >= 1
    if normalized_monitor_type == "topic":
        return phrase_hit or (source_api == "openalex" and topic_phrase_hit)
    if phrase_hit:
        return True
    if title_matches >= len(terms) and title_window_hit:
        return True
    if abstract_matches >= len(terms) and abstract_window_hit:
        return True
    if source_api == "openalex" and (topic_phrase_hit or (topic_matches >= len(terms) and topic_window_hit)):
        return True
    return False


def _monitor_search_limit(monitor: dict, base_limit: int) -> int:
    monitor_type = str(monitor.get("monitor_type") or "").strip().lower()
    if monitor_type == "topic":
        return min(50, max(base_limit, base_limit * 2))
    return base_limit


def _filter_monitor_candidates(
    *,
    monitor: dict,
    query: str,
    candidates: list[dict],
    from_year: int | None,
) -> tuple[list[dict], dict[str, int]]:
    monitor_type = str(monitor.get("monitor_type") or "").strip().lower()
    filtered: list[dict] = []
    rejected_recency = 0
    rejected_match = 0

    for candidate in candidates:
        next_candidate = dict(candidate)
        candidate_year = _candidate_year(next_candidate)
        if from_year is not None and candidate_year is not None and candidate_year < from_year:
            rejected_recency += 1
            continue

        if monitor_type == "query":
            title = str(next_candidate.get("title") or "").strip()
            abstract = str(next_candidate.get("abstract") or "").strip()
            matches = keyword_expression_matches(expression=query, title=title, abstract=abstract)
            next_candidate["monitor_match_score"] = 1.0 if matches else 0.0
            next_candidate["monitor_match_coverage"] = 1.0 if matches else 0.0
            next_candidate["monitor_title_matches"] = 0
            next_candidate["monitor_journal_matches"] = 0
            if not matches:
                rejected_match += 1
                continue
        elif monitor_type == "topic":
            match_details = _monitor_match_details(query, next_candidate)
            match_score = float(match_details["score"])
            next_candidate["monitor_match_score"] = match_score
            next_candidate["monitor_match_coverage"] = float(match_details["coverage"])
            next_candidate["monitor_title_matches"] = int(match_details["title_matches"])
            next_candidate["monitor_journal_matches"] = int(match_details["journal_matches"])
            has_explicit_support = _monitor_has_explicit_support(monitor_type, query, next_candidate, match_details)
            if match_score < 0.45 or not has_explicit_support:
                rejected_match += 1
                continue

        filtered.append(next_candidate)

    return filtered, {
        "raw_candidates": len(candidates),
        "accepted_candidates": len(filtered),
        "rejected_recency": rejected_recency,
        "rejected_match": rejected_match,
    }


def _resolve_feed_from_year(settings: dict[str, str]) -> int:
    from alma.config import get_fetch_year

    current_year = datetime.utcnow().year
    recency_years = _setting_int(settings, "monitor_defaults.recency_years", 2, 0, 10)
    recent_floor = current_year - recency_years
    global_fetch_year = get_fetch_year()
    if global_fetch_year is None:
        return recent_floor
    try:
        return max(recent_floor, int(global_fetch_year))
    except (TypeError, ValueError):
        return recent_floor


def _upsert_candidate_paper(
    db: sqlite3.Connection,
    candidate: dict,
    *,
    now: str,
    pending_hydration_ids: set[str] | None = None,
) -> str | None:
    # Clean the display fields at the boundary: figure / SI titles arrive
    # HTML-wrapped (`<p>…</p>`). The fill_only_update_paper path re-cleans
    # (idempotent), but the direct INSERT below relies on this.
    title = clean_display_text(str(candidate.get("title") or "").strip())
    if not title:
        return None

    openalex_id = str(candidate.get("openalex_id") or "").strip()
    canonical_doi = normalize_doi(str(candidate.get("canonical_doi") or "").strip())
    doi = normalize_doi(str(candidate.get("doi") or "").strip()) or canonical_doi or str(candidate.get("doi") or "").strip()
    if canonical_doi and not doi:
        doi = canonical_doi
    publication_date = _candidate_publication_date(candidate)
    try:
        year = int(candidate.get("year")) if candidate.get("year") is not None and str(candidate.get("year")).strip() else None
    except (TypeError, ValueError):
        year = None
    authors = clean_display_text(str(candidate.get("authors") or "").strip())
    journal = clean_display_text(str(candidate.get("journal") or candidate.get("published_journal") or "").strip())
    abstract = clean_display_text(str(candidate.get("abstract") or "").strip())
    url = str(candidate.get("url") or "").strip()
    try:
        cited_by_count = int(candidate.get("cited_by_count") or 0)
    except (TypeError, ValueError):
        cited_by_count = 0

    # OpenAlex `type`; falls back to a bare `type` key from non-author sources.
    work_type = str(candidate.get("work_type") or candidate.get("type") or "").strip()

    # Is this a paper, or a *part* of one (figure / SI / dataset / author
    # response)? Components are upserted into the corpus but the `_insert_feed_item`
    # gate keeps them out of the inbox; they surface inside the parent's popup.
    component_type, parent_paper_id = resolve_component(db, doi=doi, work_type=work_type)

    existing_paper_id = resolve_existing_paper_id(
        db,
        openalex_id=openalex_id,
        doi=doi,
        title=title,
        year=year,
    )

    if existing_paper_id:
        # `prefer_specific_date_fields` upgrades a stored YYYY-01-01
        # year-only fallback when the candidate carries a full date,
        # so older refreshes that only knew the year don't pin the
        # paper to Jan 1 forever (see `tasks/lessons.md` →
        # "Don't fabricate missing timestamps").
        fill_only_update_paper(
            db,
            str(existing_paper_id),
            fill_fields={
                "authors": authors,
                "journal": journal,
                "abstract": abstract,
                "url": url,
                "doi": doi,
                "openalex_id": openalex_id,
                "work_type": work_type,
            },
            # Component facts are structural and set-once (skipped when None,
            # so a normal paper is untouched and a re-classified parent link
            # isn't churned).
            fill_null_fields={
                "year": year,
                "component_type": component_type,
                "parent_paper_id": parent_paper_id,
            },
            max_int_fields={"cited_by_count": cited_by_count},
            prefer_specific_date_fields={"publication_date": publication_date or ""},
        )
        paper_id = str(existing_paper_id)
    else:
        paper_id = uuid.uuid4().hex
        db.execute(
            """INSERT OR IGNORE INTO papers
               (id, title, authors, year, journal, abstract, url, doi, publication_date,
                openalex_id, cited_by_count, work_type, component_type, parent_paper_id,
                status, added_from, added_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'tracked', 'feed', ?)""",
            (
                paper_id,
                title,
                authors,
                year,
                journal,
                abstract,
                url,
                doi,
                publication_date,
                openalex_id or None,
                cited_by_count,
                work_type or None,
                component_type,
                parent_paper_id,
                now,
            ),
        )

    # Orphan reconcile: if THIS paper is a parent (not itself a component) with a
    # DOI, adopt any suffix children (figures / SI) that were ingested before it
    # in this same refresh and are still unlinked.
    if component_type is None and doi:
        link_orphan_components(db, parent_paper_id=paper_id, parent_doi=doi)

    try:
        from alma.openalex.client import upsert_work_sidecars

        upsert_work_sidecars(
            db,
            paper_id,
            topics=candidate.get("topics") if isinstance(candidate.get("topics"), list) else None,
            institutions=candidate.get("institutions") if isinstance(candidate.get("institutions"), list) else None,
            authorships=candidate.get("authorships") if isinstance(candidate.get("authorships"), list) else None,
            referenced_works=candidate.get("referenced_works") if isinstance(candidate.get("referenced_works"), list) else None,
        )
    except Exception as exc:
        logger.debug("Feed candidate sidecar upsert failed for %s: %s", paper_id, exc)
    # Enqueue for cross-source metadata hydration so a Feed-discovered paper
    # missing an abstract enters the rehydration runner's candidate pool.
    # S-4: `auto_schedule=False` — writing the ledger row per candidate is
    # cheap, but auto-scheduling per candidate fires one schedule_with_envelope
    # + operation_status scan PER paper (N+1). The caller collects the new ids
    # and fires ONE schedule_pending_hydration_sweep after the monitor loops.
    try:
        from alma.services.corpus_rehydrate import enqueue_pending_hydration

        needs_hydration = enqueue_pending_hydration(db, paper_id, auto_schedule=False)
        if needs_hydration and pending_hydration_ids is not None:
            pending_hydration_ids.add(paper_id)
    except Exception as exc:
        logger.debug("Feed candidate hydration enqueue skipped for %s: %s", paper_id, exc)
    return paper_id


def _insert_feed_item(
    db: sqlite3.Connection,
    *,
    paper_id: str,
    author_id: str,
    fetched_at: str,
    monitor_id: str | None,
    monitor_type: str | None,
    monitor_label: str | None,
) -> bool:
    # Components (figures / SI / datasets / author responses) live in the
    # corpus but never enter the Feed inbox — they're shown inside their parent
    # paper's popup. ONE gate here keeps every monitor loop DRY and also
    # protects retroactively (a paper re-classified as a component by the
    # backfill stops producing new feed items).
    row = db.execute("SELECT component_type FROM papers WHERE id = ?", (paper_id,)).fetchone()
    if row is not None and str(row["component_type"] or "").strip():
        return False

    feed_id = uuid.uuid4().hex
    db.execute(
        """INSERT OR IGNORE INTO feed_items
           (id, paper_id, author_id, monitor_id, monitor_type, monitor_label, fetched_at, status, signal_value)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'new', 0)""",
        (feed_id, paper_id, author_id, monitor_id, monitor_type, monitor_label, fetched_at),
    )
    return db.execute("SELECT changes()").fetchone()[0] > 0


def latest_feed_fetch_window(db: sqlite3.Connection) -> tuple[str | None, str | None]:
    """Return the started/finished window for the latest completed Feed fetch."""
    try:
        row = db.execute(
            """
            SELECT started_at, finished_at
            FROM operation_status
            WHERE status = 'completed'
              AND (
                operation_key = 'feed.refresh_inbox'
                OR operation_key LIKE 'feed.monitor.refresh:%'
              )
              AND COALESCE(started_at, '') <> ''
              AND COALESCE(finished_at, '') <> ''
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError:
        return None, None
    if not row:
        return None, None
    return str(row["started_at"] or "").strip() or None, str(row["finished_at"] or "").strip() or None


def _is_new_since_latest_fetch(
    *,
    status: object,
    fetched_at: object,
    latest_fetch_window: tuple[str | None, str | None],
) -> bool:
    if str(status or "new").strip().lower() != "new":
        return False
    start, finish = latest_fetch_window
    if not start or not finish:
        return False
    fetched = str(fetched_at or "").strip()
    return bool(fetched and start <= fetched <= finish)


def count_new_feed_items_since_latest_fetch(db: sqlite3.Connection, *, since_days: int = 60) -> int:
    """Count distinct untriaged feed papers first surfaced by the latest fetch.

    A paper credited to multiple authors has multiple ``feed_items`` rows; the
    badge counts the *paper* not the row, and only when the paper's earliest
    surfacing falls inside the latest fetch window. Otherwise a paper from a
    prior fetch re-lights every time a different author monitor surfaces it.
    """
    start, finish = latest_feed_fetch_window(db)
    if not start or not finish:
        return 0
    cutoff = (datetime.utcnow() - timedelta(days=int(since_days))).isoformat()
    try:
        row = db.execute(
            """
            WITH per_paper AS (
                SELECT paper_id,
                       MIN(fetched_at) AS earliest,
                       MAX(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS has_new
                FROM feed_items
                GROUP BY paper_id
            )
            SELECT COUNT(*) AS c
            FROM per_paper pp
            LEFT JOIN papers p ON p.id = pp.paper_id
            WHERE pp.has_new = 1
              AND pp.earliest >= ?
              AND pp.earliest <= ?
              AND COALESCE(NULLIF(p.publication_date, ''), pp.earliest) >= ?
              AND """ + not_component_sql("p") + """
            """,
            (start, finish, cutoff),
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int((row["c"] if row else 0) or 0)


def list_feed_items(
    db: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    sort: str = "chronological",
    limit: int = 50,
    offset: int = 0,
    since_days: Optional[int] = None,
) -> tuple[list[dict], int]:
    """List feed inbox items with joined paper/author data.

    Args:
        sort: ``"chronological"`` (default) or ``"relevance"`` (by signal_value desc).
        since_days: If set, only include items whose effective publication or
            fetch timestamp is within the last ``since_days`` days. Keeps the
            chronological feed bounded and avoids scanning unbounded history.
    """
    # NOTE: pure read. Writes previously done here (sync_author_monitors /
    # prune_feed_items_for_missing_monitors) were the source of SQLite lock
    # contention during page loads; they now run only during refresh paths and
    # other mutation flows that already write.
    where = ["1=1"]
    params: list[object] = []
    # Dismissed items are hidden from the inbox forever — the user said "not
    # interested, get it out of my Feed". Only the durable negative signal in
    # the corpus survives. Excluded across every status filter (including
    # "all"), so a dismissed paper never reappears on refresh.
    where.append("COALESCE(fi.status, 'new') <> 'dismissed'")
    # Components (figures / SI / datasets / author responses) never appear in the
    # inbox — they're shown inside their parent paper's popup. Defense in depth
    # alongside the `_insert_feed_item` gate: this also hides any historical
    # component row the backfill re-classified, with no feed_items deletion.
    where.append(not_component_sql("p"))
    requested_status = str(status or "").strip().lower()
    latest_fetch_window = latest_feed_fetch_window(db)
    filter_to_new_papers = False
    if requested_status and requested_status != "all":
        if requested_status not in VALID_FEED_STATUSES:
            raise ValueError(f"Invalid feed status: {requested_status}")
        if requested_status == "new":
            # "new" is a paper-level concept (earliest row in the latest fetch
            # window AND at least one row still untriaged). We must NOT filter
            # rows by `fi.fetched_at IN window` here — that would hide the
            # pre-window rows the aggregator needs to detect papers the user
            # has already seen. Restrict to in-window paper_ids and let the
            # aggregator make the per-paper decision; then drop is_new=False
            # papers post-aggregation.
            if latest_fetch_window[0] and latest_fetch_window[1]:
                where.append(
                    "fi.paper_id IN (SELECT paper_id FROM feed_items "
                    "WHERE status = 'new' AND fetched_at >= ? AND fetched_at <= ?)"
                )
                params.extend([latest_fetch_window[0], latest_fetch_window[1]])
                filter_to_new_papers = True
            else:
                where.append("1=0")
        else:
            where.append("fi.status = ?")
            params.append(requested_status)

    if since_days is not None and since_days > 0:
        # Filter by paper publication date when known, else by the timestamp at
        # which we first saw the paper (feed_items.fetched_at). We deliberately
        # skip a year-only fallback — stamping papers with YYYY-01-01 corrupts
        # chronological filters for every paper without a full date.
        cutoff = (datetime.utcnow() - timedelta(days=int(since_days))).isoformat()
        where.append(
            """COALESCE(
                NULLIF(p.publication_date, ''),
                fi.fetched_at
            ) >= ?"""
        )
        params.append(cutoff)

    if sort == "relevance":
        order = "fi.signal_value DESC, fi.fetched_at DESC"
    else:
        order = """COALESCE(
            NULLIF(p.publication_date, ''),
            fi.fetched_at
        ) DESC, fi.fetched_at DESC"""

    query = f"""
        SELECT
            fi.id,
            fi.paper_id,
            fi.author_id,
            a.name AS author_name,
            fi.monitor_id,
            COALESCE(fm.monitor_type, fi.monitor_type) AS monitor_type,
            COALESCE(fm.label, fi.monitor_label) AS monitor_label,
            fi.fetched_at,
            fi.status,
            fi.signal_value,
            fi.score_breakdown,
            p.id AS p_id,
            p.title AS p_title,
            p.authors AS p_authors,
            p.year AS p_year,
            p.journal AS p_journal,
            p.abstract AS p_abstract,
            p.url AS p_url,
            p.doi AS p_doi,
            p.publication_date AS p_publication_date,
            p.status AS p_status,
            p.rating AS p_rating,
            p.notes AS p_notes,
            p.added_at AS p_added_at,
            p.added_from AS p_added_from,
            p.reading_status AS p_reading_status,
            p.openalex_id AS p_openalex_id,
            p.cited_by_count AS p_cited_by_count,
            p.tldr AS p_tldr,
            p.influential_citation_count AS p_influential_citation_count
        FROM feed_items fi
        LEFT JOIN papers p ON p.id = fi.paper_id
        LEFT JOIN authors a ON a.id = fi.author_id
        LEFT JOIN feed_monitors fm ON fm.id = fi.monitor_id
        WHERE {" AND ".join(where)}
        ORDER BY {order}
    """
    rows = db.execute(query, params).fetchall()
    aggregated = _aggregate_feed_rows(rows, latest_fetch_window=latest_fetch_window)
    if filter_to_new_papers:
        aggregated = [item for item in aggregated if item.get("is_new")]
    total = len(aggregated)
    return aggregated[offset:offset + limit], total


def get_feed_item(db: sqlite3.Connection, feed_item_id: str) -> Optional[dict]:
    """Get one feed item by ID with joined paper details."""
    items, _ = list_feed_items_for_ids(db, [feed_item_id])
    return items[0] if items else None


def list_feed_items_for_ids(db: sqlite3.Connection, feed_item_ids: list[str]) -> tuple[list[dict], int]:
    """Bulk read feed items by ID list."""
    if not feed_item_ids:
        return [], 0

    placeholders = ", ".join("?" for _ in feed_item_ids)
    rows = db.execute(
        f"""
        SELECT
            fi.id,
            fi.paper_id,
            fi.author_id,
            a.name AS author_name,
            fi.monitor_id,
            COALESCE(fm.monitor_type, fi.monitor_type) AS monitor_type,
            COALESCE(fm.label, fi.monitor_label) AS monitor_label,
            fi.fetched_at,
            fi.status,
            fi.signal_value,
            fi.score_breakdown,
            p.id AS p_id,
            p.title AS p_title,
            p.authors AS p_authors,
            p.year AS p_year,
            p.journal AS p_journal,
            p.abstract AS p_abstract,
            p.url AS p_url,
            p.doi AS p_doi,
            p.publication_date AS p_publication_date,
            p.status AS p_status,
            p.rating AS p_rating,
            p.notes AS p_notes,
            p.added_at AS p_added_at,
            p.added_from AS p_added_from,
            p.reading_status AS p_reading_status,
            p.openalex_id AS p_openalex_id,
            p.cited_by_count AS p_cited_by_count,
            p.tldr AS p_tldr,
            p.influential_citation_count AS p_influential_citation_count
        FROM feed_items fi
        LEFT JOIN papers p ON p.id = fi.paper_id
        LEFT JOIN authors a ON a.id = fi.author_id
        LEFT JOIN feed_monitors fm ON fm.id = fi.monitor_id
        WHERE fi.id IN ({placeholders})
        """,
        feed_item_ids,
    ).fetchall()
    latest_fetch_window = latest_feed_fetch_window(db)
    mapped = [_map_feed_row(r, latest_fetch_window=latest_fetch_window) for r in rows]
    return mapped, len(mapped)


def apply_feed_action(
    db: sqlite3.Connection,
    feed_item_id: str,
    action: str,
) -> Optional[dict]:
    """Apply a feed action and mutate both feed item and paper status."""
    if action not in VALID_FEED_ACTIONS:
        raise ValueError(f"Invalid feed action: {action}")

    row = db.execute(
        "SELECT id, paper_id, status FROM feed_items WHERE id = ?",
        (feed_item_id,),
    ).fetchone()
    if not row:
        return None

    paper_id = row["paper_id"]
    feed_item = get_feed_item(db, feed_item_id) or {}
    now = datetime.utcnow().isoformat()

    def _persist() -> None:
        # One atomic feed-action unit (writer gate + BEGIN IMMEDIATE + retry):
        # membership change → cross-surface reconcile → signal event → settle
        # the feed rows, all committed together. Replaces the former two-commit
        # path (record_feedback used to commit, then this function committed
        # again). `add_to_library` defers its enrichment scheduling past the
        # gate, and `record_feedback` (the shared engine) no longer commits.
        if action in {"add", "like", "love"}:
            target_rating = {"add": 3, "like": 4, "love": 5}[action]
            current_rating_row = db.execute(
                "SELECT rating FROM papers WHERE id = ?",
                (paper_id,),
            ).fetchone()
            current_rating = int((current_rating_row["rating"] if current_rating_row else 0) or 0)
            # Monotonic upgrade — never downgrade a paper already rated higher.
            next_rating = max(current_rating, target_rating)
            library_app.add_to_library(
                db,
                paper_id,
                rating=next_rating,
                added_from="feed",
            )
        elif action == "dislike":
            library_app.sink_disliked_paper(db, paper_id)
        elif action == "dismiss":
            # Dismiss is signal-only: no Library membership change and — unlike
            # `dislike` — no hard rating=1 stamp on the paper. The only corpus
            # effect is the small negative feedback event recorded below; the
            # paper is hidden from the inbox forever via the `dismissed` resting
            # status set at the end of this unit.
            pass

        library_app.sync_surface_resolution(
            db,
            paper_id,
            action=action,
            source_surface="feed",
        )

        # Record the signal through the shared engine. Best-effort: a failure
        # here must not lose the user's membership/triage action, so it is
        # logged and swallowed (same intent as before the migration).
        try:
            from alma.services.signal_lab import record_feedback

            score_breakdown = feed_item.get("score_breakdown") or {}
            if not isinstance(score_breakdown, dict):
                score_breakdown = {}
            record_feedback(
                db,
                event_type="feed_action",
                entity_type="publication",
                entity_id=paper_id,
                value={
                    "action": action,
                    "rating": {"add": 3, "like": 4, "love": 5, "dislike": 1}.get(action),
                    # `dismiss` is a small (-1) negative signal with no rating
                    # stamp — lighter than an explicit `dislike` (which also
                    # sets the star rating to 1).
                    "signal_value": {"add": 0, "like": 1, "love": 2, "dislike": -1, "dismiss": -1}.get(action, 0),
                },
                context={
                    "mode": "feed",
                    "surface": "feed",
                    "feed_item_id": feed_item_id,
                    "paper_id": paper_id,
                    "monitor_id": feed_item.get("monitor_id"),
                    "monitor_type": feed_item.get("monitor_type"),
                    "source_type": score_breakdown.get("source_type") or feed_item.get("monitor_type"),
                    "source_key": score_breakdown.get("source_key") or feed_item.get("monitor_label") or feed_item.get("author_id"),
                    "acted_at": now,
                },
            )
        except Exception as exc:
            logger.debug("Feed action feedback recording failed for %s: %s", feed_item_id, exc)

        # `dismiss` settles every feed row for this paper to the `dismissed`
        # resting status so `list_feed_items` drops it from the inbox for good;
        # every other action settles to a status matching its own name.
        resting_status = "dismissed" if action == "dismiss" else action
        db.execute(
            """
            UPDATE feed_items
            SET status = ?
            WHERE paper_id = ?
            """,
            (resting_status, paper_id),
        )

    run_write_unit(db, _persist, label="feed_action")

    return {
        "feed_item_id": feed_item_id,
        "paper_id": paper_id,
        "action": action,
    }


def undo_feed_dismiss(db: sqlite3.Connection, feed_item_id: str) -> Optional[dict]:
    """Reverse a Feed ``dismiss``: restore the paper to the inbox and drop the
    small negative signal the dismissal recorded.

    Mirrors the connector's ``undo_from_extension`` precedent — it deletes the
    feedback event the action wrote rather than surgically un-applying the
    derived ``preference_profiles`` delta (an aggregate that decays and is
    recomputed over time). Every dismissed feed row for the paper is restored
    to ``new`` so the card reappears in the chronological inbox.

    Returns ``None`` when the feed item no longer exists.
    """
    row = db.execute(
        "SELECT id, paper_id FROM feed_items WHERE id = ?",
        (feed_item_id,),
    ).fetchone()
    if not row:
        return None

    paper_id = row["paper_id"]

    def _persist() -> None:
        # One atomic unit: restore inbox visibility + drop the negative signal.
        # Restore visibility: every `dismissed` row for this paper goes back to
        # `new` (dismiss had overwritten all of them, so the prior per-row
        # status is unrecoverable — `new` is the honest untriaged default).
        db.execute(
            """
            UPDATE feed_items
            SET status = 'new'
            WHERE paper_id = ?
              AND COALESCE(status, 'new') = 'dismissed'
            """,
            (paper_id,),
        )

        # Drop the small negative signal — the most recent feed dismiss event
        # for this paper. The action lives in the event's `value` JSON.
        if _table_exists(db, "feedback_events"):
            db.execute(
                """
                DELETE FROM feedback_events
                WHERE id = (
                    SELECT id FROM feedback_events
                    WHERE entity_type = 'publication' AND entity_id = ?
                      AND event_type = 'feed_action'
                      AND value LIKE '%"action": "dismiss"%'
                    ORDER BY created_at DESC, rowid DESC
                    LIMIT 1
                )
                """,
                (paper_id,),
            )

    run_write_unit(db, _persist, label="feed_undo_dismiss")
    return {
        "feed_item_id": feed_item_id,
        "paper_id": paper_id,
        "action": "undo_dismiss",
    }


def score_feed_items(db: sqlite3.Connection, *, ctx=None) -> int:
    """Score unscored feed items using the full 10-signal scoring pipeline.

    Uses the same ``score_candidate()`` function that powers Discovery
    recommendations, so feed and discovery scores are directly comparable.

    When ``ctx`` is provided, emits periodic progress updates so the
    Activity row surfaces "Scoring N of M items" while the loop runs
    instead of appearing frozen on the "scoring" phase message.

    Returns the number of feed items scored.
    """
    import json as _json

    try:
        from alma.discovery.scoring import (
            compute_preference_profile,
            score_candidate,
            compute_centroid_from_ids,
            load_settings as load_scoring_settings,
        )
    except Exception:
        logger.debug("Could not import scoring module; skipping feed scoring")
        return 0

    # Release any caller-held writer txn before the scoring preamble
    # (settings load, preference profile build, centroid compute). These
    # are read-heavy but run before we take the writer lock ourselves —
    # holding a stale txn here would serialise them behind every other
    # writer.
    _commit_if_pending(db)
    settings = load_scoring_settings(db)

    # ── Gather positive / negative library papers ──
    try:
        pos_rows = db.execute(
            "SELECT id, title, authors, year, journal, abstract, doi, url, "
            "cited_by_count FROM papers WHERE status = 'library' AND rating >= 4"
        ).fetchall()
        neg_rows = db.execute(
            "SELECT id, title, authors, year, journal, abstract, doi, url, "
            "cited_by_count FROM papers WHERE status = 'library' AND rating <= 2 AND rating > 0"
        ).fetchall()
    except Exception:
        pos_rows, neg_rows = [], []

    positive_pubs = [dict(r) for r in pos_rows]
    negative_pubs = [dict(r) for r in neg_rows]

    if not positive_pubs:
        logger.debug("No highly-rated library papers; skipping feed scoring")
        return 0

    # ── Build preference profile (topics, author/journal affinity, feedback) ──
    preference_profile = compute_preference_profile(db, positive_pubs, negative_pubs, settings)

    # ── Embedding centroids ──
    pos_ids = [p["id"] for p in positive_pubs if p.get("id")]
    neg_ids = [p["id"] for p in negative_pubs if p.get("id")]
    try:
        positive_centroid = compute_centroid_from_ids(db, pos_ids)
    except Exception:
        positive_centroid = None
    try:
        negative_centroid = compute_centroid_from_ids(db, neg_ids) if neg_ids else None
    except Exception:
        negative_centroid = None

    # ── Texts for lexical fallback ──
    from alma.discovery import similarity as sim_module

    positive_texts = [sim_module.build_similarity_text(p, conn=db) for p in positive_pubs]
    positive_texts = [text for text in positive_texts if text] or None
    negative_texts = [sim_module.build_similarity_text(p, conn=db) for p in negative_pubs]
    negative_texts = [text for text in negative_texts if text] or None

    # ── Fetch unscored feed items ──
    try:
        feed_rows = db.execute(
            """SELECT fi.id AS feed_item_id, fi.author_id, fi.monitor_type, fi.monitor_id, fi.monitor_label,
                      p.id, p.title, p.authors, p.year, p.journal,
                      p.abstract, p.doi, p.url, p.cited_by_count
               FROM feed_items fi
               LEFT JOIN papers p ON p.id = fi.paper_id
               WHERE fi.signal_value = 0 AND fi.status = 'new'
                 AND p.title IS NOT NULL"""
        ).fetchall()
    except Exception:
        return 0

    if not feed_rows:
        return 0

    # ── Pre-load followed author IDs for source_relevance ──
    # Pure read of the followed set for source_relevance. Legacy-row
    # canonicalization is a mutation/maintenance concern, not a scoring-path
    # one (43.1) — never write here.
    followed_ids: set[str] = set()
    try:
        fa_rows = db.execute("SELECT author_id FROM followed_authors").fetchall()
        followed_ids = {r["author_id"] for r in fa_rows}
    except Exception:
        pass

    # ── Phase 1: score (CPU-bound, NO writes, gate NOT held) ──
    # score_candidate uses the centroids/texts built above; there is no network
    # or DB write per item, so the (potentially expensive) scoring of several
    # hundred items must NOT hold the writer lock. Collect (signal, breakdown,
    # feed_item_id) tuples, then persist them in Phase 2.
    total_rows = len(feed_rows)
    scored_updates: list[tuple[int, str, str]] = []
    for fr in feed_rows:
        try:
            candidate = dict(fr)
            # Set source_relevance based on followed-author status
            author_id = fr["author_id"] or ""
            monitor_type = str(fr["monitor_type"] or "").strip().lower()
            if monitor_type == "author":
                candidate["source_relevance"] = 0.8 if author_id in followed_ids else 0.5
                candidate["source_type"] = "followed_author"
                candidate["source_key"] = author_id
            elif monitor_type == "topic":
                candidate["source_relevance"] = 0.7
                candidate["source_type"] = "topic_monitor"
                candidate["source_key"] = str(fr["monitor_label"] or fr["monitor_id"] or author_id)
            elif monitor_type == "venue":
                candidate["source_relevance"] = 0.72
                candidate["source_type"] = "venue_monitor"
                candidate["source_key"] = str(fr["monitor_label"] or fr["monitor_id"] or author_id)
            elif monitor_type == "preprint":
                candidate["source_relevance"] = 0.69
                candidate["source_type"] = "preprint_monitor"
                candidate["source_key"] = str(fr["monitor_label"] or fr["monitor_id"] or author_id)
            elif monitor_type == "branch":
                candidate["source_relevance"] = 0.74
                candidate["source_type"] = "branch_monitor"
                candidate["source_key"] = str(fr["monitor_label"] or fr["monitor_id"] or author_id)
            else:
                candidate["source_relevance"] = 0.65
                candidate["source_type"] = "query_monitor"
                candidate["source_key"] = str(fr["monitor_label"] or fr["monitor_id"] or author_id)

            score, breakdown = score_candidate(
                candidate,
                preference_profile,
                positive_centroid,
                negative_centroid,
                positive_texts,
                negative_texts,
                conn=db,
                settings=settings,
            )

            signal_value = max(0, min(100, int(round(score))))
            breakdown_json = _json.dumps(breakdown, default=str)
            scored_updates.append((signal_value, breakdown_json, fr["feed_item_id"]))
        except Exception as exc:
            logger.debug("Failed to score feed item %s: %s", fr["feed_item_id"], exc)
            continue

    # ── Phase 2: persist in writer-gated chunks ──
    # Each chunk is one BEGIN IMMEDIATE + writer-gate window (write_section); a
    # bounded chunk keeps the lock window short and mid-run visibility for other
    # readers while cutting WAL fsyncs vs per-item commits. The scoring above is
    # already done, so the gate is held only for the short batch of UPDATEs.
    _SCORE_COMMIT_BATCH = 50
    scored = 0
    for start in range(0, len(scored_updates), _SCORE_COMMIT_BATCH):
        chunk = scored_updates[start:start + _SCORE_COMMIT_BATCH]
        with write_section(db, label="feed score batch"):
            for signal_value, breakdown_json, feed_item_id in chunk:
                db.execute(
                    "UPDATE feed_items SET signal_value = ?, score_breakdown = ? WHERE id = ?",
                    (signal_value, breakdown_json, feed_item_id),
                )
        scored += len(chunk)
        if ctx is not None:
            # Item counts live in the message text (not processed/total) so the
            # caller's "monitors complete" progress bar stays on one scale.
            ctx.log_step(
                "score_progress",
                f"Feed refresh: scored {scored}/{total_rows} new items",
                data={"scored": scored, "total": total_rows},
            )

    logger.info("Scored %d feed items via 10-signal pipeline", scored)
    return scored


def _monitor_search_plan(
    monitor: dict,
    *,
    base_settings: dict[str, str],
    search_temperature: float,
) -> tuple[str, str, dict[str, str], float]:
    monitor_type = str(monitor.get("monitor_type") or "").strip().lower()
    config = monitor.get("config") if isinstance(monitor.get("config"), dict) else {}
    monitor_query = monitor_app.get_monitor_query(monitor)
    search_query = monitor_query
    settings = dict(base_settings)
    temperature = search_temperature

    if monitor_type == "venue":
        settings["sources.arxiv.enabled"] = "false"
        settings["sources.biorxiv.enabled"] = "false"
        search_query = str((config or {}).get("query") or monitor.get("label") or monitor_query).strip()
    elif monitor_type == "query":
        search_query = keyword_retrieval_query(monitor_query)
        temperature = min(search_temperature, 0.12)
    elif monitor_type == "preprint":
        settings["sources.openalex.enabled"] = "false"
        settings["sources.crossref.enabled"] = "false"
        settings["sources.semantic_scholar.enabled"] = str((config or {}).get("semantic_scholar_enabled") or "false").lower()
        settings["sources.arxiv.enabled"] = "true"
        settings["sources.biorxiv.enabled"] = "true"
        temperature = max(search_temperature, 0.32)
    elif monitor_type == "branch":
        branch_query = str((config or {}).get("query") or monitor_query).strip()
        branch_label = str((config or {}).get("branch_label") or monitor.get("label") or "").strip()
        search_query = f"{branch_query} {branch_label}".strip()
        temperature = max(search_temperature, float((config or {}).get("temperature") or 0.34))

    return monitor_query, search_query, settings, temperature


def _read_monitor_refresh_settings(discovery_settings: dict):
    """Resolve the monitor-defaults config shared by the full-inbox and
    single-monitor feed refresh paths (was copy-pasted verbatim). Returns a
    6-tuple unpacked into the legacy local names."""
    author_per_refresh = _setting_int(discovery_settings, "monitor_defaults.author_per_refresh", 20, 1, 100)
    search_limit = _setting_int(discovery_settings, "monitor_defaults.search_limit", 15, 1, 50)
    search_temperature = _setting_float(discovery_settings, "monitor_defaults.search_temperature", 0.22, 0.0, 1.0)
    include_preprints = _setting_bool(discovery_settings, "monitor_defaults.include_preprints", True)
    semantic_scholar_bulk = _setting_bool(discovery_settings, "monitor_defaults.semantic_scholar_bulk", True)
    monitor_search_settings = dict(discovery_settings)
    if not include_preprints:
        monitor_search_settings["sources.arxiv.enabled"] = "false"
        monitor_search_settings["sources.biorxiv.enabled"] = "false"
    return (
        author_per_refresh,
        search_limit,
        search_temperature,
        include_preprints,
        semantic_scholar_bulk,
        monitor_search_settings,
    )


def _normalize_author_work_to_candidate(work: dict) -> dict:
    """Map a normalized OpenAlex work to a feed candidate dict (shared by the
    author-monitor loops in refresh_feed_inbox + refresh_feed_monitor)."""
    return {
        "title": work.get("title"),
        "authors": work.get("authors"),
        "authorships": work.get("authorships") or [],
        "year": work.get("year"),
        "publication_date": work.get("publication_date"),
        "journal": work.get("journal"),
        "abstract": work.get("abstract"),
        "url": work.get("pub_url"),
        "doi": work.get("doi"),
        "openalex_id": work.get("openalex_id"),
        "cited_by_count": work.get("num_citations", 0),
        # The OpenAlex `type` (article / preprint / dataset / peer-review /
        # other …). Carried forward so the component classifier can tell a
        # dataset / author-response apart from a paper at ingest time, before
        # any later hydration runs (see alma.core.components).
        "work_type": work.get("type"),
        "topics": work.get("topics") or [],
        "keywords": work.get("keywords") or [],
        "institutions": work.get("institutions") or [],
        "referenced_works": work.get("referenced_works"),
        "source_api": "openalex",
    }


def refresh_feed_inbox(db: sqlite3.Connection, *, ctx=None) -> dict:
    """Refresh the monitoring inbox from author and non-author monitors.

    Progress is surfaced to the Activity panel on two channels:

    * ``ctx.log_step`` appends one row to ``operation_logs`` per phase /
      per monitor, so the expanded Activity view can tail the job.
    * Every ``ctx.log_step`` call also pushes ``message`` + ``processed`` +
      ``total`` into ``operation_status``, so the collapsed row shows the
      current phase and "N of M monitors" instead of sitting on the
      initial queued message until the job finishes.

    The SQLite writer lock is released (``_commit_if_pending``) before
    each remote call and before each new phase so concurrent Library /
    Authors reads don't queue behind an implicit transaction.
    """
    from alma.application.discovery import read_settings as read_discovery_settings
    from alma.discovery import source_search
    from alma.openalex.client import _normalize_work, batch_fetch_recent_works_for_authors
    from alma.openalex.http import get_client as get_openalex_client

    monitors_total = 0
    monitor_idx = 0

    def _log(
        step: str,
        message: str,
        *,
        data: dict | None = None,
        processed: int | None = None,
        total: int | None = None,
    ) -> None:
        if ctx is None:
            return
        # Default to the running monitor counter so every log entry
        # advances the progress bar on the Activity row.
        effective_processed = processed if processed is not None else monitor_idx
        effective_total = total if total is not None else monitors_total
        ctx.log_step(
            step,
            message,
            data=data,
            processed=effective_processed,
            total=effective_total,
        )

    monitor_app.sync_author_monitors(db)
    prune_feed_items_for_missing_monitors(db)
    commit_with_retry(db, label="feed refresh setup")
    monitors = [monitor for monitor in monitor_app.list_feed_monitors(db) if monitor.get("enabled", True)]
    monitors_total = len(monitors)
    if not monitors:
        _log("no_monitors", "Feed refresh: no active monitors configured")
        return {
            "authors": 0,
            "non_author_monitors": 0,
            "monitors_total": 0,
            "monitors_ready": 0,
            "monitors_degraded": 0,
            "papers_found": 0,
            "items_created": 0,
            "scored": 0,
            "monitor_diagnostics": [],
            "source_diagnostics": {"openalex": {}, "http": {}},
        }

    author_monitors = [m for m in monitors if m.get("monitor_type") == "author"]
    non_author_monitors = [m for m in monitors if m.get("monitor_type") != "author"]
    ready_monitors = [m for m in monitors if m.get("health") == "ready"]
    degraded_monitors = [m for m in monitors if m.get("health") != "ready"]
    discovery_settings = read_discovery_settings(db)
    (
        author_per_refresh,
        search_limit,
        search_temperature,
        include_preprints,
        semantic_scholar_bulk,
        monitor_search_settings,
    ) = _read_monitor_refresh_settings(discovery_settings)
    from_year = _resolve_feed_from_year(discovery_settings)
    now = datetime.utcnow().isoformat()

    _log(
        "query_monitors",
        f"Feed refresh: {len(monitors)} monitors loaded ({len(author_monitors)} author, {len(non_author_monitors)} non-author)",
        data={
            "monitors_total": len(monitors),
            "authors": len(author_monitors),
            "non_author_monitors": len(non_author_monitors),
        },
        processed=0,
        total=monitors_total,
    )

    monitor_diagnostics: list[dict] = []
    papers_found = 0
    items_created = 0
    usage_before = openalex_usage_snapshot()
    # S-4: collect papers needing hydration across BOTH monitor phases and fire
    # ONE background sweep after the loops, instead of auto-scheduling a job per
    # candidate (N+1 schedule_with_envelope + operation_status scans).
    pending_hydration_ids: set[str] = set()

    # Operation-scoped OpenAlex cache: duplicate work/author fetches within
    # this one sweep (incl. negative 404s) are served from the in-run cache
    # instead of re-hitting the network. Read-path only — the interleaved DB
    # writes below are unaffected. The cache lives on the client instance, so
    # the Phase A pool threads share it too.
    with get_openalex_client().operation_cache("feed_refresh"), source_diagnostics_scope() as source_diag:
        author_ready = [m for m in author_monitors if m.get("health") == "ready" and m.get("openalex_id")]
        author_by_openalex = {
            str(m.get("openalex_id") or "").strip(): m for m in author_ready
        }
        if author_by_openalex:
            openalex_ids = list(author_by_openalex.keys())
            preview = ", ".join(
                str(author_by_openalex[oa_id].get("label") or oa_id)
                for oa_id in openalex_ids[:5]
            )
            if len(openalex_ids) > 5:
                preview += f" (+{len(openalex_ids) - 5} more)"
            _log(
                "fetch_author_works",
                f"Feed refresh: fetching recent works for {len(openalex_ids)} author monitors ({preview})",
                data={"authors": len(openalex_ids)},
            )
            # Release any implicit writer lock before the OpenAlex batch
            # call — otherwise Library / Authors reads block for the full
            # network round-trip.
            _commit_if_pending(db)
            try:
                author_works = batch_fetch_recent_works_for_authors(
                    openalex_ids,
                    from_year=from_year,
                    per_author_limit=author_per_refresh,
                )
            except Exception as exc:
                logger.error("Feed author batch fetch failed: %s", exc)
                author_works = {}
                for monitor in author_ready:
                    error_text = str(exc)
                    diag = {
                        "monitor_id": monitor["id"],
                        "monitor_type": "author",
                        "label": monitor["label"],
                        "status": "failed",
                        "reason": error_text,
                        "papers_found": 0,
                        "items_created": 0,
                    }
                    monitor_diagnostics.append(diag)
                    monitor_app.update_feed_monitor_result(
                        db,
                        str(monitor["id"]),
                        status="failed",
                        result=diag,
                        error=error_text,
                    )
                commit_with_retry(db, label="feed refresh")
                _log("author_fetch_error", f"Feed refresh: author monitor batch fetch failed: {exc}")
            else:
                for oa_author_id, monitor in author_by_openalex.items():
                    works = author_works.get(oa_author_id) or []
                    author_items = 0
                    found = 0
                    for raw_work in works:
                        try:
                            work = _normalize_work(raw_work)
                        except Exception:
                            continue
                        found += 1
                        papers_found += 1
                        candidate = _normalize_author_work_to_candidate(work)
                        paper_id = _upsert_candidate_paper(
                            db, candidate, now=now, pending_hydration_ids=pending_hydration_ids
                        )
                        if not paper_id:
                            continue
                        if _insert_feed_item(
                            db,
                            paper_id=paper_id,
                            author_id=str(monitor.get("author_id") or monitor.get("id") or oa_author_id),
                            fetched_at=now,
                            monitor_id=str(monitor.get("id") or ""),
                            monitor_type="author",
                            monitor_label=str(monitor.get("label") or ""),
                        ):
                            items_created += 1
                            author_items += 1
                        # S-8: per-candidate commit dropped — the per-author
                        # commit below (after update_feed_monitor_result) flushes
                        # this author's writes in one fsync instead of one/paper.
                    status_value = "completed" if author_items > 0 or found > 0 else "noop"
                    diag = {
                        "monitor_id": monitor["id"],
                        "monitor_type": "author",
                        "label": monitor["label"],
                        "status": status_value,
                        "reason": None,
                        "papers_found": found,
                        "items_created": author_items,
                        "openalex_id": oa_author_id,
                    }
                    monitor_diagnostics.append(diag)
                    monitor_app.update_feed_monitor_result(
                        db,
                        str(monitor["id"]),
                        status=status_value,
                        result=diag,
                        error=None,
                    )
                    commit_with_retry(db, label="feed refresh")
                    monitor_idx += 1
                    _log(
                        "author_monitor_done",
                        f"Feed refresh: {monitor['label']} produced {author_items} new items",
                        data=diag,
                    )

        for monitor in author_monitors:
            if monitor.get("health") == "ready":
                continue
            diag = {
                "monitor_id": monitor["id"],
                "monitor_type": "author",
                "label": monitor["label"],
                "status": "degraded",
                "reason": monitor.get("health_reason"),
                "papers_found": 0,
                "items_created": 0,
            }
            monitor_diagnostics.append(diag)
            monitor_app.update_feed_monitor_result(
                db,
                str(monitor["id"]),
                status="failed",
                result=diag,
                error=str(monitor.get("health_reason") or "author_monitor_degraded"),
            )
            commit_with_retry(db, label="feed refresh")
            monitor_idx += 1

        # End of author phase — release the writer lock before the
        # non-author monitor loop starts doing remote searches.
        _commit_if_pending(db)

        # S-3: split the non-author monitors into a PARALLEL network phase
        # (Phase A, db-free) + a SEQUENTIAL db-write phase (Phase B), instead of
        # a strictly serial loop where each monitor blocked the next on its full
        # ~8s cross-source search (N monitors ≈ N×8s wall-clock). Phase A
        # overlaps the FAST sources across monitors (OpenAlex 100 req/s,
        # Crossref); S2 (1 rps) and arXiv (1 req/3s) stay globally serialized by
        # their per-source SourceHttpClient gates, so concurrency never exceeds
        # any documented rate limit — the win is wall-clock, not call volume.
        # Phase B keeps every write on the single SQLite writer, original order.
        _FEED_MONITOR_CONCURRENCY = 3

        def _plan_and_search(monitor: dict) -> dict:
            """DB-FREE worker: resolve the search plan, run the remote
            multi-source search, filter locally. Runs on a pool thread, so it
            must touch neither ``db`` nor shared mutable state. Returns a result
            dict (or ``{'error_reason': ...}``) drained sequentially below."""
            try:
                monitor_query, search_query, search_settings, monitor_temperature = _monitor_search_plan(
                    monitor,
                    base_settings=monitor_search_settings,
                    search_temperature=search_temperature,
                )
            except FeedQuerySyntaxError as exc:
                return {"error_reason": str(exc)}
            if not monitor_query or not search_query:
                return {"error_reason": "missing_query"}
            try:
                search_limit_for_monitor = _monitor_search_limit(monitor, search_limit)
                candidates = source_search.search_across_sources(
                    search_query,
                    limit=search_limit_for_monitor,
                    from_year=from_year,
                    settings=search_settings,
                    mode="core",
                    temperature=monitor_temperature,
                    semantic_scholar_mode="bulk" if semantic_scholar_bulk else "interactive",
                )
            except Exception as exc:
                logger.debug("Non-author monitor refresh failed for %s: %s", monitor["label"], exc)
                return {"error_reason": str(exc)}
            candidates, filter_stats = _filter_monitor_candidates(
                monitor=monitor,
                query=monitor_query,
                candidates=candidates,
                from_year=from_year,
            )
            return {
                "monitor_query": monitor_query,
                "search_query": search_query,
                "search_limit": search_limit_for_monitor,
                "candidates": candidates,
                "filter_stats": filter_stats,
            }

        # Phase A — concurrent network search. NB: the per-source HTTP
        # diagnostics (source_diag) are collected via a context that does not
        # propagate to pool threads, so http_source_diagnostics may under-count
        # parallel searches — an observability-only trade-off, not correctness.
        plan_by_monitor: dict[str, dict] = {}
        if non_author_monitors:
            with bounded_thread_pool(
                max(1, min(_FEED_MONITOR_CONCURRENCY, len(non_author_monitors))),
                thread_name_prefix="feed-monitor",
            ) as monitor_pool:
                future_map = {
                    monitor_pool.submit(_plan_and_search, monitor): str(monitor.get("id") or "")
                    for monitor in non_author_monitors
                }
                for future in as_completed(future_map):
                    mid = future_map[future]
                    try:
                        plan_by_monitor[mid] = future.result()
                    except Exception as exc:  # defensive; _plan_and_search already guards
                        plan_by_monitor[mid] = {"error_reason": str(exc)}

        # Phase B — sequential DB writes, original monitor order preserved so
        # Activity progress + monitor_idx stay deterministic.
        for monitor in non_author_monitors:
            plan = plan_by_monitor.get(str(monitor.get("id") or "")) or {"error_reason": "no_result"}
            error_reason = plan.get("error_reason")
            if error_reason:
                diag = {
                    "monitor_id": monitor["id"],
                    "monitor_type": monitor["monitor_type"],
                    "label": monitor["label"],
                    "status": "failed",
                    "reason": error_reason,
                    "papers_found": 0,
                    "items_created": 0,
                }
                monitor_diagnostics.append(diag)
                monitor_app.update_feed_monitor_result(
                    db,
                    str(monitor["id"]),
                    status="failed",
                    result=diag,
                    error=error_reason,
                )
                commit_with_retry(db, label="feed refresh")
                monitor_idx += 1
                continue

            monitor_query = plan["monitor_query"]
            search_query = plan["search_query"]
            search_limit_for_monitor = plan["search_limit"]
            candidates = plan["candidates"]
            filter_stats = plan["filter_stats"]
            _log(
                "refresh_monitor",
                f"Feed refresh: searched {monitor['monitor_type']} monitor '{monitor['label']}'",
                data={
                    "monitor_id": monitor["id"],
                    "monitor_type": monitor["monitor_type"],
                    "query": monitor_query,
                    "search_query": search_query,
                },
            )
            found = len(candidates)
            papers_found += found
            monitor_items = 0
            source_counts: dict[str, int] = {}
            for candidate in candidates:
                source_name = str(candidate.get("source_api") or "").strip()
                if source_name:
                    source_counts[source_name] = int(source_counts.get(source_name) or 0) + 1
                paper_id = _upsert_candidate_paper(
                    db, candidate, now=now, pending_hydration_ids=pending_hydration_ids
                )
                if not paper_id:
                    continue
                if _insert_feed_item(
                    db,
                    paper_id=paper_id,
                    author_id=str(monitor.get("id") or ""),
                    fetched_at=now,
                    monitor_id=str(monitor.get("id") or ""),
                    monitor_type=str(monitor.get("monitor_type") or ""),
                    monitor_label=str(monitor.get("label") or ""),
                ):
                    items_created += 1
                    monitor_items += 1
                # S-8: per-candidate commit dropped — the per-monitor commit
                # below (after update_feed_monitor_result) flushes this
                # monitor's writes in one fsync instead of one per paper.

            status_value = "completed" if monitor_items > 0 or found > 0 else "noop"
            diag = {
                "monitor_id": monitor["id"],
                "monitor_type": monitor["monitor_type"],
                "label": monitor["label"],
                "status": status_value,
                "reason": None,
                "papers_found": found,
                "items_created": monitor_items,
                "query": monitor_query,
                "search_query": search_query,
                "search_limit": search_limit_for_monitor,
                "source_counts": source_counts,
                **filter_stats,
            }
            monitor_diagnostics.append(diag)
            monitor_app.update_feed_monitor_result(
                db,
                str(monitor["id"]),
                status=status_value,
                result=diag,
                error=None,
            )
            commit_with_retry(db, label="feed refresh")
            monitor_idx += 1
            _log(
                "monitor_done",
                f"Feed refresh: monitor '{monitor['label']}' produced {monitor_items} new items",
                data=diag,
            )

        http_source_diagnostics = source_diag.summary()

    # End of monitor-fetch phase. Release the writer lock before
    # kicking off the scoring pass — score_feed_items runs a tight
    # per-item UPDATE loop and any leftover implicit txn here would
    # compound the lock pressure during scoring.
    _commit_if_pending(db)

    # S-4: one bounded, target-scoped hydration sweep for everything this
    # refresh discovered, instead of one auto-scheduled job per candidate.
    # Idempotent against an already-running sweep; no-op when nothing is new.
    if pending_hydration_ids:
        try:
            from alma.services.corpus_rehydrate import schedule_pending_hydration_sweep

            schedule_pending_hydration_sweep(
                reason="feed_refresh",
                target_paper_ids=list(pending_hydration_ids),
            )
        except Exception as exc:
            logger.debug("Feed refresh hydration sweep skipped: %s", exc)

    _log(
        "scoring",
        f"Feed refresh: scoring {items_created} new feed items",
        data={"items_created": items_created, "papers_found": papers_found},
        processed=monitors_total,
        total=monitors_total,
    )

    scored = 0
    try:
        scored = score_feed_items(db, ctx=ctx)
    except Exception as exc:
        logger.debug("Feed scoring after refresh failed: %s", exc)

    usage_after = openalex_usage_snapshot()
    openalex_diag = openalex_usage_delta(usage_before, usage_after)
    summary = {
        "authors": len(author_monitors),
        "non_author_monitors": len(non_author_monitors),
        "monitors_total": len(monitors),
        "monitors_ready": len(ready_monitors),
        "monitors_degraded": len(degraded_monitors),
        "from_year": from_year,
        "papers_found": papers_found,
        "items_created": items_created,
        "scored": scored,
        "monitor_diagnostics": monitor_diagnostics[:100],
        "source_diagnostics": {
            "openalex": openalex_diag,
            "http": http_source_diagnostics,
        },
    }
    logger.info(
        "Feed refresh: monitors=%d papers=%d items=%d scored=%d",
        len(monitors),
        papers_found,
        items_created,
        scored,
    )
    return summary


def refresh_feed_monitor(
    db: sqlite3.Connection,
    monitor_id: str,
    *,
    ctx=None,
) -> dict | None:
    """Refresh one feed monitor and return a monitor-scoped summary."""
    from alma.application.discovery import read_settings as read_discovery_settings
    from alma.discovery import source_search
    from alma.openalex.client import _normalize_work, batch_fetch_recent_works_for_authors

    def _log(step: str, message: str, **kwargs) -> None:
        if ctx is not None:
            ctx.log_step(step, message, **kwargs)

    # S-11: a single non-author monitor (query/venue/preprint/branch) already
    # exists in feed_monitors without the author-mirror sync, and the corpus-wide
    # orphan prune is pure waste for it. Only sync+prune when the target is an
    # author monitor (the sync materializes it) or isn't found yet (maybe a
    # freshly-followed author not yet mirrored).
    monitors = monitor_app.list_feed_monitors(db)
    monitor = next((item for item in monitors if str(item.get("id") or "") == str(monitor_id or "")), None)
    if monitor is None or str(monitor.get("monitor_type") or "") == "author":
        monitor_app.sync_author_monitors(db)
        prune_feed_items_for_missing_monitors(db)
        monitors = monitor_app.list_feed_monitors(db)
        monitor = next((item for item in monitors if str(item.get("id") or "") == str(monitor_id or "")), None)
    if monitor is None:
        return None

    # S-4: collect hydration targets for one post-refresh sweep (see refresh_feed_inbox).
    pending_hydration_ids: set[str] = set()
    discovery_settings = read_discovery_settings(db)
    (
        author_per_refresh,
        search_limit,
        search_temperature,
        include_preprints,
        semantic_scholar_bulk,
        monitor_search_settings,
    ) = _read_monitor_refresh_settings(discovery_settings)

    if not monitor.get("enabled", True):
        diag = {
            "monitor_id": monitor["id"],
            "monitor_type": monitor["monitor_type"],
            "label": monitor["label"],
            "status": "disabled",
            "reason": "monitor_disabled",
            "papers_found": 0,
            "items_created": 0,
        }
        monitor_app.update_feed_monitor_result(
            db,
            str(monitor["id"]),
            status="failed",
            result=diag,
            error="monitor_disabled",
        )
        commit_with_retry(db, label="feed refresh monitor")
        return diag

    from_year = _resolve_feed_from_year(discovery_settings)
    now = datetime.utcnow().isoformat()
    usage_before = openalex_usage_snapshot()
    with source_diagnostics_scope() as source_diag:
        if monitor.get("monitor_type") == "author":
            if monitor.get("health") != "ready" or not monitor.get("openalex_id"):
                diag = {
                    "monitor_id": monitor["id"],
                    "monitor_type": "author",
                    "label": monitor["label"],
                    "status": "degraded",
                    "reason": monitor.get("health_reason"),
                    "papers_found": 0,
                    "items_created": 0,
                }
                monitor_app.update_feed_monitor_result(
                    db,
                    str(monitor["id"]),
                    status="failed",
                    result=diag,
                    error=str(monitor.get("health_reason") or "author_monitor_degraded"),
                )
                commit_with_retry(db, label="feed refresh")
                return diag

            openalex_id = str(monitor.get("openalex_id") or "").strip()
            _log("refresh_author_monitor", f"Refreshing author monitor '{monitor['label']}'", data={"monitor_id": monitor["id"]})
            works = batch_fetch_recent_works_for_authors(
                [openalex_id],
                from_year=from_year,
                per_author_limit=author_per_refresh,
            ).get(openalex_id) or []
            found = 0
            items_created = 0
            for raw_work in works:
                try:
                    work = _normalize_work(raw_work)
                except Exception:
                    continue
                found += 1
                candidate = _normalize_author_work_to_candidate(work)
                paper_id = _upsert_candidate_paper(
                    db, candidate, now=now, pending_hydration_ids=pending_hydration_ids
                )
                if not paper_id:
                    continue
                if _insert_feed_item(
                    db,
                    paper_id=paper_id,
                    author_id=str(monitor.get("author_id") or monitor.get("id") or openalex_id),
                    fetched_at=now,
                    monitor_id=str(monitor.get("id") or ""),
                    monitor_type="author",
                    monitor_label=str(monitor.get("label") or ""),
                ):
                    items_created += 1
                # S-8: commit moved to the single per-monitor commit below.

            diag = {
                "monitor_id": monitor["id"],
                "monitor_type": "author",
                "label": monitor["label"],
                "status": "completed" if items_created > 0 or found > 0 else "noop",
                "reason": None,
                "papers_found": found,
                "items_created": items_created,
                "openalex_id": openalex_id,
            }
        else:
            try:
                monitor_query, search_query, search_settings, monitor_temperature = _monitor_search_plan(
                    monitor,
                    base_settings=monitor_search_settings,
                    search_temperature=search_temperature,
                )
            except FeedQuerySyntaxError as exc:
                error_text = str(exc)
                diag = {
                    "monitor_id": monitor["id"],
                    "monitor_type": monitor["monitor_type"],
                    "label": monitor["label"],
                    "status": "failed",
                    "reason": error_text,
                    "papers_found": 0,
                    "items_created": 0,
                }
                monitor_app.update_feed_monitor_result(
                    db,
                    str(monitor["id"]),
                    status="failed",
                    result=diag,
                    error=error_text,
                )
                commit_with_retry(db, label="feed refresh")
                return diag

            if not monitor_query or not search_query:
                diag = {
                    "monitor_id": monitor["id"],
                    "monitor_type": monitor["monitor_type"],
                    "label": monitor["label"],
                    "status": "failed",
                    "reason": "missing_query",
                    "papers_found": 0,
                    "items_created": 0,
                }
                monitor_app.update_feed_monitor_result(
                    db,
                    str(monitor["id"]),
                    status="failed",
                    result=diag,
                    error="missing_query",
                )
                commit_with_retry(db, label="feed refresh")
                return diag

            _log(
                "refresh_monitor",
                f"Refreshing {monitor['monitor_type']} monitor '{monitor['label']}'",
                data={"monitor_id": monitor["id"], "query": monitor_query, "search_query": search_query},
            )
            search_limit_for_monitor = _monitor_search_limit(monitor, search_limit)
            candidates = source_search.search_across_sources(
                search_query,
                limit=search_limit_for_monitor,
                from_year=from_year,
                settings=search_settings,
                mode="core",
                temperature=monitor_temperature,
                semantic_scholar_mode="bulk" if semantic_scholar_bulk else "interactive",
            )
            candidates, filter_stats = _filter_monitor_candidates(
                monitor=monitor,
                query=monitor_query,
                candidates=candidates,
                from_year=from_year,
            )
            found = len(candidates)
            items_created = 0
            source_counts: dict[str, int] = {}
            for candidate in candidates:
                source_name = str(candidate.get("source_api") or "").strip()
                if source_name:
                    source_counts[source_name] = int(source_counts.get(source_name) or 0) + 1
                paper_id = _upsert_candidate_paper(
                    db, candidate, now=now, pending_hydration_ids=pending_hydration_ids
                )
                if not paper_id:
                    continue
                if _insert_feed_item(
                    db,
                    paper_id=paper_id,
                    author_id=str(monitor.get("id") or ""),
                    fetched_at=now,
                    monitor_id=str(monitor.get("id") or ""),
                    monitor_type=str(monitor.get("monitor_type") or ""),
                    monitor_label=str(monitor.get("label") or ""),
                ):
                    items_created += 1
                # S-8: commit moved to the single per-monitor commit below.

            diag = {
                "monitor_id": monitor["id"],
                "monitor_type": monitor["monitor_type"],
                "label": monitor["label"],
                "status": "completed" if items_created > 0 or found > 0 else "noop",
                "reason": None,
                "papers_found": found,
                "items_created": items_created,
                "query": monitor_query,
                "search_query": search_query,
                "search_limit": search_limit_for_monitor,
                "source_counts": source_counts,
                **filter_stats,
            }

        monitor_app.update_feed_monitor_result(
            db,
            str(monitor["id"]),
            status=str(diag.get("status") or "completed"),
            result=diag,
            error=None if not diag.get("reason") else str(diag.get("reason")),
        )
        commit_with_retry(db, label="feed refresh monitor")
        http_source_diagnostics = source_diag.summary()

    # S-4: one bounded hydration sweep for this monitor's new papers.
    if pending_hydration_ids:
        try:
            from alma.services.corpus_rehydrate import schedule_pending_hydration_sweep

            schedule_pending_hydration_sweep(
                reason="feed_refresh",
                target_paper_ids=list(pending_hydration_ids),
            )
        except Exception as exc:
            logger.debug("Feed monitor refresh hydration sweep skipped: %s", exc)

    try:
        scored = score_feed_items(db)
    except Exception:
        scored = 0
    openalex_diag = openalex_usage_delta(usage_before, openalex_usage_snapshot())
    diag["scored"] = int(scored or 0)
    diag["from_year"] = from_year
    diag["source_diagnostics"] = {"openalex": openalex_diag, "http": http_source_diagnostics}
    return diag


def _map_feed_row(
    row: sqlite3.Row,
    *,
    latest_fetch_window: tuple[str | None, str | None],
) -> dict:
    monitor_type = str(row["monitor_type"] or "").strip().lower() or None
    author_id = str(row["author_id"] or "").strip()
    author_name = str(row["author_name"] or "").strip() or None
    monitor_id = str(row["monitor_id"] or "").strip() or None
    monitor_label = str(row["monitor_label"] or "").strip() or None
    is_author_monitor = monitor_type == "author"
    matched_author_ids = [author_id] if is_author_monitor and author_id else []
    matched_authors = [author_name] if is_author_monitor and author_name else []
    matched_monitors = (
        [
            {
                "monitor_id": monitor_id,
                "monitor_type": monitor_type,
                "monitor_label": monitor_label,
            }
        ]
        if monitor_type and not is_author_monitor
        else []
    )
    return {
        "id": row["id"],
        "paper_id": row["paper_id"],
        "author_id": author_id,
        "author_name": author_name,
        "matched_author_ids": matched_author_ids,
        "matched_authors": matched_authors,
        "matched_monitors": matched_monitors,
        "monitor_id": monitor_id,
        "monitor_type": monitor_type,
        "monitor_label": monitor_label,
        "fetched_at": row["fetched_at"],
        "status": row["status"],
        "signal_value": int(row["signal_value"] or 0),
        "is_new": _is_new_since_latest_fetch(
            status=row["status"],
            fetched_at=row["fetched_at"],
            latest_fetch_window=latest_fetch_window,
        ),
        "score_breakdown": _parse_json_dict(row["score_breakdown"]),
        "paper": {
            "id": row["p_id"],
            "title": row["p_title"] or "",
            "authors": row["p_authors"],
            "year": row["p_year"],
            "journal": row["p_journal"],
            "abstract": row["p_abstract"],
            "url": row["p_url"],
            "doi": row["p_doi"],
            "publication_date": row["p_publication_date"],
            "status": row["p_status"] or "tracked",
            "rating": int(row["p_rating"] or 0),
            "notes": row["p_notes"],
            "added_at": row["p_added_at"],
            "added_from": row["p_added_from"],
            "reading_status": row["p_reading_status"],
            "openalex_id": row["p_openalex_id"],
            "cited_by_count": int(row["p_cited_by_count"] or 0),
            # U-5: surface the S2 TLDR + influential-citation count the feed card
            # already renders (FeedPage passes paper.tldr / .influential_citation_count
            # to PaperCard). The query previously omitted them, so they were always
            # null/0 even when the paper row had them.
            "tldr": row["p_tldr"],
            "influential_citation_count": int(row["p_influential_citation_count"] or 0),
        }
        if row["p_id"]
        else None,
    }


def _aggregate_feed_rows(
    rows: list[sqlite3.Row],
    *,
    latest_fetch_window: tuple[str | None, str | None],
) -> list[dict]:
    """Collapse duplicate papers into one inbox card while preserving author provenance.

    `is_new` is recomputed against the paper's earliest `fetched_at` across all
    rows, not the most recent — otherwise a paper credited to multiple authors
    re-lights as new every time a different author monitor surfaces it in the
    latest fetch window.
    """
    aggregated: dict[str, dict] = {}
    ordered_ids: list[str] = []
    earliest_fetched: dict[str, str] = {}
    has_new_status: dict[str, bool] = {}

    for row in rows:
        mapped = _map_feed_row(row, latest_fetch_window=latest_fetch_window)
        group_key = str(mapped.get("paper_id") or mapped.get("id") or "").strip()
        if not group_key:
            continue
        row_fetched = str(row["fetched_at"] or "").strip()
        prev_fetched = earliest_fetched.get(group_key)
        if row_fetched and (not prev_fetched or row_fetched < prev_fetched):
            earliest_fetched[group_key] = row_fetched
        if str(row["status"] or "").strip().lower() == "new":
            has_new_status[group_key] = True
        existing = aggregated.get(group_key)
        if existing is None:
            aggregated[group_key] = mapped
            ordered_ids.append(group_key)
            continue

        _absorb_feed_card(existing, mapped)

    start, finish = latest_fetch_window
    for group_key, item in aggregated.items():
        earliest = earliest_fetched.get(group_key, "")
        item["is_new"] = bool(
            start
            and finish
            and earliest
            and start <= earliest <= finish
            and has_new_status.get(group_key, False)
        )

    # Second pass — logical-duplicate safety net. Pass 1 collapsed feed_items
    # that share a paper_id; this folds cards that are the SAME logical paper
    # under two DIFFERENT paper_ids (the v0.19.0 duplicate-identity regression:
    # two canonical rows, one normalized title+year), so the inbox never shows
    # the same title twice even before the background collapse merges the rows.
    return _consolidate_logical_duplicates(
        [aggregated[group_key] for group_key in ordered_ids]
    )


def _absorb_feed_card(existing: dict, other: dict) -> None:
    """Merge ``other``'s author / monitor provenance + signal into ``existing``.

    Shared by both aggregation passes: pass 1 (same paper_id, different author
    monitors) and pass 2 (same logical paper, different paper_id). Author /
    monitor lists dedupe; ``signal_value`` and ``is_new`` take the stronger."""
    for matched_author_id in other.get("matched_author_ids") or []:
        author_id = str(matched_author_id or "").strip()
        if author_id and author_id not in existing["matched_author_ids"]:
            existing["matched_author_ids"].append(author_id)
    for author_name in other.get("matched_authors") or []:
        normalized = str(author_name or "").strip()
        if normalized and normalized not in existing["matched_authors"]:
            existing["matched_authors"].append(normalized)
    for monitor in other.get("matched_monitors") or []:
        monitor_id = str((monitor or {}).get("monitor_id") or "").strip()
        monitor_type = str((monitor or {}).get("monitor_type") or "").strip().lower()
        monitor_label = str((monitor or {}).get("monitor_label") or "").strip()
        duplicate = any(
            str((item or {}).get("monitor_id") or "").strip() == monitor_id
            and str((item or {}).get("monitor_type") or "").strip().lower() == monitor_type
            and str((item or {}).get("monitor_label") or "").strip() == monitor_label
            for item in existing["matched_monitors"]
        )
        if not duplicate and (monitor_id or monitor_label or monitor_type):
            existing["matched_monitors"].append(
                {
                    "monitor_id": monitor_id or None,
                    "monitor_type": monitor_type or None,
                    "monitor_label": monitor_label or None,
                }
            )
    if not existing.get("author_name") and other.get("author_name"):
        existing["author_name"] = other["author_name"]
    if other.get("monitor_type") == "author" and other.get("author_id"):
        existing["author_id"] = other["author_id"]
    existing["signal_value"] = max(
        int(existing.get("signal_value") or 0), int(other.get("signal_value") or 0)
    )
    existing["is_new"] = bool(existing.get("is_new")) or bool(other.get("is_new"))


def _consolidate_logical_duplicates(cards: list[dict]) -> list[dict]:
    """Fold cards that are the SAME logical paper (year + normalized title) into
    one, preserving order and provenance. Strong-identifier conflicts (different
    non-empty DOIs / openalex_ids — provably distinct works) are NEVER folded."""
    from alma.core.utils import logical_dup_signature, strong_identifiers_conflict

    survivors: list[dict] = []
    by_sig: dict[str, dict] = {}
    for card in cards:
        paper = card.get("paper") or {}
        sig = logical_dup_signature(paper.get("title"), paper.get("year"))
        keeper = by_sig.get(sig) if sig is not None else None
        if keeper is not None and not strong_identifiers_conflict(
            incoming_doi=paper.get("doi"),
            incoming_openalex_id=paper.get("openalex_id"),
            candidate_doi=(keeper.get("paper") or {}).get("doi"),
            candidate_openalex_id=(keeper.get("paper") or {}).get("openalex_id"),
        ):
            _absorb_feed_card(keeper, card)
            continue
        if sig is not None:
            by_sig[sig] = card
        survivors.append(card)
    return survivors
