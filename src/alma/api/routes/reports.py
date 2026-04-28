"""Reports endpoints — structured research intelligence reports."""

import logging
import sqlite3
from fastapi import APIRouter, Depends, HTTPException, Query, status

from alma.api.deps import get_db, get_current_user
from alma.api.helpers import raise_internal

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["reports"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


@router.get(
    "/weekly-brief",
    summary="Weekly research brief",
    description="New papers, trending topics, active authors over the past week.",
)
def get_weekly_brief(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.application.reports import weekly_research_brief
        return weekly_research_brief(db)
    except Exception as e:
        raise_internal("Failed to generate weekly brief", e)


@router.get(
    "/collection-intelligence",
    summary="Collection intelligence report",
    description="Per-collection analysis: growth, citations, topic diversity.",
)
def get_collection_intelligence(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.application.reports import collection_intelligence
        return collection_intelligence(db)
    except Exception as e:
        raise_internal("Failed to generate collection intelligence", e)


@router.get(
    "/topic-drift",
    summary="Topic drift report",
    description="Track how research topics shift across time windows.",
)
def get_topic_drift(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.application.reports import topic_drift
        return topic_drift(db)
    except Exception as e:
        raise_internal("Failed to generate topic drift report", e)


@router.get(
    "/signal-impact",
    summary="Signal impact report",
    description="Which scoring signals correlate with liked vs dismissed recommendations.",
)
def get_signal_impact(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.application.reports import signal_impact
        return signal_impact(db)
    except Exception as e:
        raise_internal("Failed to generate signal impact report", e)
