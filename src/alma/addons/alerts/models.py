"""Alerts request and response models (digest-based)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AlertRuleCreate(BaseModel):
    """Request model for creating an alert rule."""

    name: str
    rule_type: str = Field(
        ...,
        description=(
            "Rule type: author, collection, keyword, topic, similarity, "
            "discovery_lens, feed_monitor, branch, library_workflow"
        ),
    )
    rule_config: dict
    channels: list[str]
    enabled: bool = True


class AlertRuleResponse(BaseModel):
    """Response model for an alert rule."""

    id: str
    name: str
    rule_type: str
    rule_config: dict
    channels: list[str]
    enabled: bool
    created_at: str


class AlertCreate(BaseModel):
    """Request model for creating an alert (digest)."""

    name: str
    channels: list[str]
    schedule: str = "manual"
    schedule_config: dict | None = None
    format: str = "grouped"
    enabled: bool = True
    rule_ids: list[str] | None = None


class AlertUpdate(BaseModel):
    """Request model for updating an alert."""

    name: str | None = None
    channels: list[str] | None = None
    schedule: str | None = None
    schedule_config: dict | None = None
    format: str | None = None
    enabled: bool | None = None


class AlertResponse(BaseModel):
    """Response model for an alert (digest)."""

    id: str
    name: str
    channels: list[str]
    schedule: str
    schedule_config: dict | None = None
    format: str
    enabled: bool
    created_at: str
    last_evaluated_at: str | None = None
    # Worst status among the most recent evaluation's history rows
    # (failed > skipped > sent > empty), so a failure is never hidden.
    last_outcome: str | None = None
    # Next slot the hourly sweep can fire this digest; None for manual or
    # disabled digests.
    next_due_at: str | None = None
    rules: list[AlertRuleResponse] | None = None


class AlertRuleAssignment(BaseModel):
    """Request model for assigning rules to an alert."""

    rule_ids: list[str]


class AlertHistoryResponse(BaseModel):
    """Response model for an alert history entry."""

    id: str
    rule_id: str | None = None
    alert_id: str | None = None
    channel: str
    paper_id: str | None = None
    publications: list[str] | None = None
    publication_count: int = 0
    sent_at: str
    status: str
    message_preview: str | None = None
    error_message: str | None = None


class AlertEvaluationResult(BaseModel):
    """Response model for alert evaluation."""

    alert_id: str
    alert_name: str
    digest_id: str | None = None
    digest_name: str | None = None
    matched_rules: int | None = None
    papers_found: int
    papers_new: int
    papers_sent: int
    papers_failed: int | None = None
    channels: list[str]
    channel_results: dict | None = None
    trigger_source: str | None = None
    dry_run: bool
    papers: list[dict] | None = None


class AlertAutomationTemplateRule(BaseModel):
    """One rule payload proposed for an automation template."""

    name: str
    rule_type: str
    rule_config: dict
    channels: list[str] = Field(default_factory=list)
    enabled: bool = True


class AlertAutomationTemplateDigest(BaseModel):
    """One digest payload proposed for an automation template."""

    name: str
    channels: list[str] = Field(default_factory=list)
    schedule: str = "manual"
    schedule_config: dict | None = None
    format: str = "text"
    enabled: bool = True


class AlertAutomationTemplate(BaseModel):
    """Suggested alert automation derived from current product state."""

    key: str
    category: str
    title: str
    description: str
    rationale: str | None = None
    metrics: dict = Field(default_factory=dict)
    rule: AlertAutomationTemplateRule
    alert: AlertAutomationTemplateDigest


class AlertTemplateApplyResponse(BaseModel):
    """Result of materializing a suggested automation (rule + digest)."""

    template_key: str
    template_title: str
    rule: AlertRuleResponse
    alert: AlertResponse
