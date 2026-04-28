"""Alerts API endpoints: rules, delivery configs, evaluation, and history."""

import asyncio
import logging
import sqlite3
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from alma.api.deps import get_current_user, get_db
from alma.api.models import (
    AlertAutomationTemplate,
    AlertRuleCreate,
    AlertRuleResponse,
    AlertRuleAssignment,
    AlertHistoryResponse,
    AlertCreate,
    AlertUpdate,
    AlertResponse,
    AlertEvaluationResult,
)
from alma.api.helpers import raise_internal
from alma.application import alerts as alerts_app
from alma.core.operations import OperationOutcome, OperationRunner

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


# ===================================================================
# Alert Rules (existing CRUD - preserved)
# ===================================================================

@router.get(
    "/rules",
    response_model=List[AlertRuleResponse],
    summary="List alert rules",
)
def list_rules(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return all alert rules."""
    try:
        rows = alerts_app.list_rules(db)
        return [AlertRuleResponse(**r) for r in rows]
    except Exception as e:
        raise_internal("Failed to list alert rules", e)


@router.post(
    "/rules",
    response_model=AlertRuleResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an alert rule",
)
def create_rule(
    req: AlertRuleCreate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Create a new alert rule."""
    try:
        runner = OperationRunner(db)

        def _handler(_ctx):
            created = alerts_app.create_rule(
                db,
                name=req.name,
                rule_type=req.rule_type,
                rule_config=req.rule_config,
                channels=req.channels,
                enabled=req.enabled,
            )
            return OperationOutcome(
                status="completed",
                message=f"Created alert rule '{req.name}'",
                result={"rule_id": created["id"]},
            )

        op = runner.run(
            operation_key=f"alerts.rule.create:{req.rule_type}",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
        created = alerts_app.get_rule(db, str((op.get("result") or {}).get("rule_id") or ""))
        if created is None:
            raise HTTPException(status_code=500, detail="Rule created but not found")
        return AlertRuleResponse(**created)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise_internal("Failed to create alert rule", e)


@router.put(
    "/rules/{rule_id}",
    response_model=AlertRuleResponse,
    summary="Update an alert rule",
)
def update_rule(
    rule_id: str,
    req: AlertRuleCreate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Update an existing alert rule."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        updated = alerts_app.update_rule(
            db,
            rule_id,
            name=req.name,
            rule_type=req.rule_type,
            rule_config=req.rule_config,
            channels=req.channels,
            enabled=req.enabled,
        )
        if updated is None:
            return OperationOutcome(status="noop", message="Rule not found", result={"rule_id": rule_id})
        return OperationOutcome(status="completed", message="Rule updated", result={"rule_id": rule_id})

    try:
        op = runner.run(
            operation_key=f"alerts.rule.update:{rule_id}",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Alert rule not found")
    updated = alerts_app.get_rule(db, rule_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    try:
        return AlertRuleResponse(**updated)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.delete(
    "/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an alert rule",
)
def delete_rule(
    rule_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete an alert rule."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        deleted = alerts_app.delete_rule(db, rule_id)
        if not deleted:
            return OperationOutcome(status="noop", message="Rule not found", result={"rule_id": rule_id})
        return OperationOutcome(status="completed", message="Rule deleted", result={"rule_id": rule_id})

    op = runner.run(
        operation_key=f"alerts.rule.delete:{rule_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Alert rule not found")


@router.post(
    "/rules/{rule_id}/toggle",
    response_model=AlertRuleResponse,
    summary="Toggle alert rule enabled/disabled",
)
def toggle_rule(
    rule_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Toggle the enabled state of an alert rule."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        updated = alerts_app.toggle_rule(db, rule_id)
        if updated is None:
            return OperationOutcome(status="noop", message="Rule not found", result={"rule_id": rule_id})
        return OperationOutcome(
            status="completed",
            message="Rule toggled",
            result={"rule_id": rule_id, "enabled": bool(updated["enabled"])},
        )

    op = runner.run(
        operation_key=f"alerts.rule.toggle:{rule_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Alert rule not found")
    updated = alerts_app.get_rule(db, rule_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return AlertRuleResponse(**updated)


# ===================================================================
# Alert History (enhanced) - MUST come before /{alert_id} routes
# ===================================================================

@router.get(
    "/history",
    response_model=List[AlertHistoryResponse],
    summary="List alert history",
)
def list_history(
    rule_id: Optional[str] = Query(None, description="Filter by rule ID"),
    alert_id: Optional[str] = Query(None, description="Filter by alert ID"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return alert history entries with optional rule_id/alert_id filter and pagination."""
    try:
        rows = alerts_app.list_history(
            db,
            rule_id=rule_id,
            alert_id=alert_id,
            limit=limit,
            offset=offset,
        )
        return [AlertHistoryResponse(**r) for r in rows]
    except Exception as e:
        raise_internal("Failed to list alert history", e)


# ===================================================================
# Test Fire (preserved) - MUST come before /{alert_id} routes
# ===================================================================

@router.post(
    "/test/{rule_id}",
    summary="Test-fire an alert rule",
)
def test_fire_rule(
    rule_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Test-fire an alert rule: check matching publications without actually sending.

    Returns a list of matching publication titles based on the rule configuration.
    """
    result = alerts_app.test_fire_rule(db, rule_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return result


# ===================================================================
# Alerts (Delivery Configs) - NEW
# ===================================================================


@router.get(
    "/templates",
    response_model=List[AlertAutomationTemplate],
    summary="List suggested alert automations",
)
def list_alert_templates(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return one-click alert suggestions derived from monitors, branches, and workflow state."""
    try:
        rows = alerts_app.list_alert_templates(db)
        return [AlertAutomationTemplate(**row) for row in rows]
    except Exception as e:
        raise_internal("Failed to list alert templates", e)


def _build_alert_response(alert_dict: dict, db: sqlite3.Connection) -> AlertResponse:
    """Build an AlertResponse including assigned rules for the given alert row."""
    return AlertResponse(**alerts_app.build_alert_response(db, alert_dict))


@router.get(
    "/",
    response_model=List[AlertResponse],
    summary="List all alerts",
)
def list_alerts(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return all alerts (delivery configs) with their assigned rules."""
    rows = alerts_app.list_alerts(db)
    return [AlertResponse(**r) for r in rows]


@router.post(
    "/",
    response_model=AlertResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an alert",
)
def create_alert(
    req: AlertCreate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Create a new alert (delivery config)."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        created = alerts_app.create_alert(
            db,
            name=req.name,
            channels=req.channels,
            schedule=req.schedule,
            schedule_config=req.schedule_config,
            format_value=req.format,
            enabled=req.enabled,
            rule_ids=req.rule_ids or [],
        )
        return OperationOutcome(
            status="completed",
            message=f"Created alert '{req.name}'",
            result={"alert_id": created["id"]},
        )

    op = runner.run(
        operation_key=f"alerts.create:{req.schedule}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    created = alerts_app.get_alert(db, str((op.get("result") or {}).get("alert_id") or ""))
    if created is None:
        raise HTTPException(status_code=500, detail="Alert created but not found")
    return AlertResponse(**created)


@router.get(
    "/{alert_id}",
    response_model=AlertResponse,
    summary="Get a single alert",
)
def get_alert(
    alert_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Return a single alert by ID with its assigned rules."""
    row = alerts_app.get_alert(db, alert_id)
    if not row:
        raise HTTPException(status_code=404, detail="Alert not found")
    return AlertResponse(**row)


@router.put(
    "/{alert_id}",
    response_model=AlertResponse,
    summary="Update an alert",
)
def update_alert(
    alert_id: str,
    req: AlertUpdate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Update an existing alert's fields (partial update)."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        updated = alerts_app.update_alert(
            db,
            alert_id,
            name=req.name,
            channels=req.channels,
            schedule=req.schedule,
            schedule_config=req.schedule_config,
            format_value=req.format,
            enabled=req.enabled,
        )
        if updated is None:
            return OperationOutcome(status="noop", message="Alert not found", result={"alert_id": alert_id})
        return OperationOutcome(status="completed", message="Alert updated", result={"alert_id": alert_id})

    op = runner.run(
        operation_key=f"alerts.update:{alert_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Alert not found")
    updated = alerts_app.get_alert(db, alert_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return AlertResponse(**updated)


@router.delete(
    "/{alert_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an alert",
)
def delete_alert(
    alert_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete an alert and its rule assignments."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        deleted = alerts_app.delete_alert(db, alert_id)
        if not deleted:
            return OperationOutcome(status="noop", message="Alert not found", result={"alert_id": alert_id})
        return OperationOutcome(status="completed", message="Alert deleted", result={"alert_id": alert_id})

    op = runner.run(
        operation_key=f"alerts.delete:{alert_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Alert not found")


# ===================================================================
# Rule Assignments
# ===================================================================

@router.post(
    "/{alert_id}/rules",
    summary="Assign rules to an alert",
)
def assign_rules(
    alert_id: str,
    req: AlertRuleAssignment,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Assign one or more rules to an alert."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        result = alerts_app.assign_rules(db, alert_id, req.rule_ids)
        if result is None:
            return OperationOutcome(status="noop", message="Alert not found", result={"alert_id": alert_id})
        return OperationOutcome(
            status="completed",
            message=f"Assigned {len(result['assigned_rule_ids'])} rules",
            result=result,
        )

    op = runner.run(
        operation_key=f"alerts.assign_rules:{alert_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Alert not found")
    return (op.get("result") or {"alert_id": alert_id, "assigned_rule_ids": []})


@router.delete(
    "/{alert_id}/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Unassign a rule from an alert",
)
def unassign_rule(
    alert_id: str,
    rule_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Remove a rule assignment from an alert."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        deleted = alerts_app.unassign_rule(db, alert_id, rule_id)
        if not deleted:
            return OperationOutcome(status="noop", message="Assignment not found", result={"alert_id": alert_id, "rule_id": rule_id})
        return OperationOutcome(
            status="completed",
            message="Rule unassigned from alert",
            result={"alert_id": alert_id, "rule_id": rule_id},
        )

    op = runner.run(
        operation_key=f"alerts.unassign_rule:{alert_id}:{rule_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Assignment not found")


# ===================================================================
# Alert Evaluation Engine
# ===================================================================


@router.post(
    "/{alert_id}/evaluate",
    response_model=AlertEvaluationResult,
    summary="Evaluate and send alert",
)
async def evaluate_alert(
    alert_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Run all assigned rules, merge papers, filter already-alerted, send via channels.

    Stays ``async def`` because ``alerts_app.evaluate_digest`` is a coroutine
    (it awaits the Slack / channel delivery HTTP round-trips). The sync
    ``runner.run`` call afterwards persists operation lifecycle to SQLite and
    is dispatched through ``asyncio.to_thread`` so the commit does not block
    the event loop while another request is mid-flight.
    """
    runner = OperationRunner(db)

    try:
        evaluated = await alerts_app.evaluate_digest(db, alert_id, trigger_source="user")
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to evaluate alert", exc)
    if evaluated is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    result = evaluated

    def _handler(_ctx):
        return OperationOutcome(
            status="completed",
            message=f"Evaluated alert {alert_id}",
            result=result,
        )

    await asyncio.to_thread(
        runner.run,
        operation_key=f"alerts.evaluate:{alert_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    return AlertEvaluationResult(**result)


@router.post(
    "/{alert_id}/dry-run",
    response_model=AlertEvaluationResult,
    summary="Dry-run alert evaluation",
)
def dry_run_alert(
    alert_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Same as evaluate but don't send and don't record. Returns matching papers."""
    result = alerts_app.dry_run_digest(db, alert_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    runner = OperationRunner(db)

    def _handler(_ctx):
        return OperationOutcome(
            status="completed",
            message=f"Dry-run evaluated alert {alert_id}",
            result=result,
        )

    runner.run(
        operation_key=f"alerts.dry_run:{alert_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    return AlertEvaluationResult(**result)
