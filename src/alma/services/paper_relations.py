"""Ask the registries which published work a preprint became.

A title match cannot follow a preprint whose title changed in review, but
bioRxiv and Crossref say so outright. A link to a DOI outside the corpus waits in
`paper_relations` until that paper arrives. Each preprint is asked once a quarter:
the answer changes rarely and the calls are not free.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from typing import Any

from alma.core.db_write import write_section
from alma.core.paper_groups import is_preprint_row, record_paper_relations
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.utils import canonical_lookup_doi, normalize_doi
from alma.discovery import biorxiv, crossref
from alma.services.corpus_rehydrate import _write_ledger

logger = logging.getLogger(__name__)

SOURCE = "same_work_relations"
RECHECK_AFTER = timedelta(days=90)
_BIORXIV_PREFIXES = ("10.1101/", "10.64898/")


def _pending_rows(conn: sqlite3.Connection, limit: int | None) -> list[sqlite3.Row]:
    rows = conn.execute(
        f"""
        SELECT p.id, p.doi, p.journal, p.work_type, p.preprint_source, p.component_type
        FROM papers p
        LEFT JOIN paper_enrichment_status ledger
          ON ledger.paper_id = p.id AND ledger.source = '{SOURCE}'
        WHERE {standalone_paper_sql('p')}
          AND COALESCE(TRIM(p.doi), '') <> ''
          AND (ledger.next_retry_at IS NULL OR datetime(ledger.next_retry_at) <= datetime('now'))
        ORDER BY (p.status = 'library') DESC, p.id
        """
    ).fetchall()
    pending = [row for row in rows if is_preprint_row(row)]
    return pending if limit is None else pending[:limit]


def count_pending_relation_lookups(conn: sqlite3.Connection) -> int:
    return len(_pending_rows(conn, None))


def fetch_same_work_links(doi: str) -> list[tuple[str, str]]:
    """Registered same-work DOIs of one preprint. Network, so no transaction open."""
    links: list[tuple[str, str]] = []
    if str(doi).lower().startswith(_BIORXIV_PREFIXES):
        for server in ("biorxiv", "medrxiv"):
            published = canonical_lookup_doi(
                biorxiv._lookup_published_record(doi, server=server).get("published_doi")
            )
            if published:
                links.append(("is-preprint-of", published))
                break
    item = crossref.fetch_work_message(normalize_doi(doi) or doi)
    if item:
        links.extend(crossref.same_work_dois(item.get("relation")))
    return links


def run_relation_sweep(conn: sqlite3.Connection, *, limit: int) -> dict[str, Any]:
    """Ask for up to ``limit`` preprints, one short write after each answer."""
    summary: dict[str, Any] = {"asked": 0, "links": 0, "errors": 0}
    for row in _pending_rows(conn, limit):
        try:
            links = fetch_same_work_links(str(row["doi"]))
        except Exception as exc:
            logger.warning("Relation lookup failed for %s: %s", row["doi"], exc)
            summary["errors"] += 1
            continue
        with write_section(conn, label="paper relations"):
            summary["links"] += record_paper_relations(conn, str(row["id"]), links, source="registry")
            _write_ledger(
                conn,
                paper_id=str(row["id"]),
                source=SOURCE,
                lookup_key=f"doi:{str(row['doi']).lower()}",
                status="enriched" if links else "unchanged",
                reason="linked" if links else "no_link",
                fields_filled=[],
                fields_key="relations",
                retry_after=RECHECK_AFTER,
            )
        summary["asked"] += 1
    summary["finished_at"] = utcnow().isoformat()
    summary["message"] = (
        f"Asked about {summary['asked']} preprints, {summary['links']} new links, "
        f"{summary['errors']} failed"
    )
    return summary
