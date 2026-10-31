import { api, isJobEnvelope, waitForJob, type JobEnvelope } from '@/api/client'

export interface AlertRule {
  id: string
  name: string
  rule_type: 'author' | 'collection' | 'keyword' | 'topic' | 'similarity' | 'discovery_lens' | 'feed_monitor' | 'branch' | 'library_workflow'
  rule_config: Record<string, unknown>
  channels: string[]
  enabled: boolean
  created_at: string
}

export interface Alert {
  id: string
  name: string
  channels: string[]
  schedule: 'manual' | 'daily' | 'weekly' | string
  schedule_config?: Record<string, unknown>
  format: string
  enabled: boolean
  created_at: string
  last_evaluated_at?: string
  /** Worst status of the most recent evaluation batch (failed > skipped > sent > empty). */
  last_outcome?: string | null
  /** Next slot the hourly sweep can fire this digest; null for manual/disabled. */
  next_due_at?: string | null
  rules?: AlertRule[]
}

export interface AlertEvaluationResult {
  alert_id: string
  alert_name: string
  digest_id?: string
  digest_name?: string
  matched_rules?: number
  papers_found: number
  papers_new: number
  papers_sent: number
  papers_failed?: number
  channels: string[]
  channel_results?: Record<
    string,
    { status: string; error?: string | null; papers_new?: number; papers_sent?: number }
  >
  trigger_source?: string
  dry_run: boolean
  papers?: Record<string, unknown>[]
}

/** Dry-run result of one rule (no digest, no delivery, no dedup). */
export interface TestFireResult {
  rule_id: string
  rule_type: AlertRule['rule_type']
  matches_found: number
  matches: string[]
  note: string
}

export interface AlertHistory {
  id: string
  rule_id?: string
  alert_id?: string
  channel: string
  paper_id?: string
  publications?: string[]
  publication_count?: number
  sent_at: string
  status: 'sent' | 'failed' | 'pending' | 'empty' | 'skipped' | string
  message_preview?: string
  error_message?: string
}

export interface AlertAutomationTemplate {
  key: string
  category: 'author' | 'collection' | 'feed_monitor' | 'branch' | 'library_workflow' | string
  title: string
  description: string
  rationale?: string | null
  metrics: Record<string, number | string | boolean | null>
  rule: {
    name: string
    rule_type: AlertRule['rule_type']
    rule_config: Record<string, unknown>
    channels: string[]
    enabled: boolean
  }
  alert: {
    name: string
    channels: string[]
    schedule: string
    schedule_config?: Record<string, unknown>
    format: string
    enabled: boolean
  }
}

/**
 * Evaluate (and send) an alert digest. Returns the AlertEvaluationResult
 * after polling the Activity envelope to completion.
 *
 * The endpoint now runs the full evaluation -- including the Slack send --
 * on the scheduler thread pool, so the user can also watch progress in the
 * Activity tab via `operation_key="alerts.evaluate:<id>"`.
 */
export async function evaluateAlert(alertId: string): Promise<AlertEvaluationResult> {
  const resp = await api.post<JobEnvelope | AlertEvaluationResult>(
    `/alerts/${encodeURIComponent(alertId)}/evaluate`,
  )
  if (isJobEnvelope(resp)) {
    // The dedupe envelope keeps the job's real status ("running"/"queued")
    // and flags the duplicate via `already_running` — status itself is never
    // the string 'already_running'.
    if (resp.already_running === true) {
      // Surface an interpretable error for callers; matches what the user
      // sees if they double-click "Evaluate".
      throw new Error('Alert is already being evaluated; check Activity for progress.')
    }
    return waitForJob<AlertEvaluationResult>(resp.job_id, { timeoutMs: 120_000 })
  }
  // Backwards-compat path (shouldn't trigger after Phase D).
  return resp as AlertEvaluationResult
}

export function getAlertTemplates(): Promise<AlertAutomationTemplate[]> {
  return api.get<AlertAutomationTemplate[]>('/alerts/templates')
}

export interface AlertTemplateApplyResult {
  template_key: string
  template_title: string
  rule: AlertRule
  alert: Alert
}

/**
 * Materialize one suggested automation. The backend recomputes the template
 * from its key and creates the rule + digest in a single transaction, so a
 * mid-flight failure can't leave an orphan rule behind.
 */
export function applyAlertTemplate(templateKey: string): Promise<AlertTemplateApplyResult> {
  return api.post<AlertTemplateApplyResult>(
    `/alerts/templates/${encodeURIComponent(templateKey)}/apply`,
  )
}

/** Dry-run one rule: returns matching titles, sends nothing, records nothing. */
export function testFireRule(ruleId: string): Promise<TestFireResult> {
  return api.post<TestFireResult>(`/alerts/test/${encodeURIComponent(ruleId)}`)
}
