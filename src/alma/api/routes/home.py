"""Home API — a pure-read daily research brief."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from alma.api.deps import get_current_user, get_db
from alma.api.helpers import raise_internal
from alma.application import home as home_app

router = APIRouter(
    tags=["home"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


@router.get(
    "/brief",
    summary="Home daily research brief",
    description=(
        "Pure-read activity, carryover, source-balanced highlights, reading "
        "continuity, and user-fixable blockers for the browser's local day."
    ),
)
def get_home_brief(
    timezone_name: str = Query(
        "UTC",
        alias="timezone",
        description="Browser IANA timezone, for example Europe/Brussels.",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        return home_app.build_daily_brief(
            db,
            timezone_name=timezone_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise_internal("Failed to build the Home daily brief", exc)
