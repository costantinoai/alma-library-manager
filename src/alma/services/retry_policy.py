"""Shared enrichment retry and freshness horizons.

Status writers across paper, author, title-resolution, and duplicate-scan
pipelines must use the same clocks so Health can explain one policy truth.
"""

from datetime import timedelta

TRANSIENT_RETRY_AFTER = timedelta(hours=6)
UNCHANGED_RECHECK_AFTER = timedelta(days=30)
WORKS_RECHECK_AFTER = timedelta(days=14)
