import { useState, useEffect, useRef, useMemo } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronUp,
  ChevronDown,
  ChevronRight,
  Activity,
  ScrollText,
  Loader2,
  CheckCircle,
  XCircle,
  Clock,
  UserRound,
  X,
  Square,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Progress } from '@/components/ui/progress'
import { ScrollArea } from '@/components/ui/scroll-area'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { SubPanel } from '@/components/ui/sub-panel'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { invalidateQueries } from '@/lib/queryHelpers'
import { parseAlmaTimestamp } from '@/lib/utils'
import { cn } from '@/lib/utils'
import { api } from '@/api/client'

// ── Types ──

interface LogEntry {
  timestamp: string
  level: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'CRITICAL'
  logger: string
  message: string
}

interface JobStatus {
  job_id: string
  status: string // 'running' | 'completed' | 'failed'
  operation_key?: string
  trigger_source?: string
  started_at?: string
  finished_at?: string
  message?: string
  current_author?: string
  processed?: number
  total?: number
  error?: string
  result?: Record<string, unknown>
  parent_job_id?: string
  stage?: string
  stage_label?: string
  stage_index?: number
  stage_total?: number
}

interface JobLogEntry {
  timestamp: string
  job_id: string
  level: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'CRITICAL'
  step?: string
  message: string
  data?: Record<string, unknown>
}

// ── Sub-components ──

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case 'running':
      return <Loader2 className="h-4 w-4 shrink-0 animate-spin text-slate-500" />
    case 'completed':
      return <CheckCircle className="h-4 w-4 shrink-0 text-green-500" />
    case 'failed':
      return <XCircle className="h-4 w-4 shrink-0 text-red-500" />
    case 'cancelling':
      return <Loader2 className="h-4 w-4 shrink-0 animate-spin text-amber-500" />
    case 'cancelled':
      return <XCircle className="h-4 w-4 shrink-0 text-amber-500" />
    default:
      return <Clock className="h-4 w-4 shrink-0 text-slate-400" />
  }
}

function ProgressBar({ processed, total }: { processed: number; total: number }) {
  const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0
  return (
    <div className="flex items-center gap-2">
      <Progress value={pct} className="h-1.5 flex-1 bg-slate-100 [&>div]:bg-slate-600" />
      <span className="text-xs tabular-nums text-slate-500">
        {processed}/{total}
      </span>
    </div>
  )
}

function formatTime(dateStr: string): string {
  try {
    const d = parseAlmaTimestamp(dateStr)
    return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return dateStr
  }
}

function formatTimestamp(dateStr: string): string {
  try {
    const d = parseAlmaTimestamp(dateStr)
    return d.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    })
  } catch {
    return dateStr
  }
}

function isActiveOperation(status: string): boolean {
  return status === 'running' || status === 'queued' || status === 'scheduled' || status === 'cancelling'
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled'])

function isTerminalStatus(status: string): boolean {
  return TERMINAL_STATUSES.has(status)
}

function formatDuration(started?: string | null, finished?: string | null): string | null {
  if (!started || !finished) return null
  const startMs = parseAlmaTimestamp(started).getTime()
  const endMs = parseAlmaTimestamp(finished).getTime()
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return null
  const ms = endMs - startMs
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  const totalSec = Math.round(ms / 1000)
  const m = Math.floor(totalSec / 60)
  const s = totalSec % 60
  return s === 0 ? `${m}m` : `${m}m ${s}s`
}

function statusAccent(status: string): string {
  switch (status) {
    case 'running':
      return 'border-l-slate-500'
    case 'queued':
    case 'scheduled':
      return 'border-l-slate-300'
    case 'cancelling':
      return 'border-l-amber-400'
    case 'failed':
      return 'border-l-rose-400'
    default:
      return 'border-l-transparent'
  }
}

function statusTone(status: string): StatusBadgeTone {
  switch (status) {
    case 'completed':
      return 'positive'
    case 'failed':
      return 'negative'
    case 'cancelled':
    case 'cancelling':
      return 'warning'
    case 'running':
      return 'accent'
    case 'queued':
    case 'scheduled':
      return 'info'
    default:
      return 'neutral'
  }
}

const LEVEL_ORDER: Record<string, number> = {
  DEBUG: 0,
  INFO: 1,
  WARNING: 2,
  ERROR: 3,
  CRITICAL: 4,
}

function levelTone(level: string): StatusBadgeTone {
  if (level === 'ERROR' || level === 'CRITICAL') return 'negative'
  if (level === 'WARNING') return 'warning'
  if (level === 'DEBUG') return 'neutral'
  return 'info'
}

function summarizeResult(
  result: Record<string, unknown> | undefined,
): Array<{ key: string; value: string }> | null {
  if (!result) return null
  const chips: Array<{ key: string; value: string }> = []
  for (const [key, value] of Object.entries(result)) {
    if (typeof value === 'number') {
      chips.push({
        key,
        value: Number.isInteger(value) ? String(value) : value.toFixed(2),
      })
    } else if (typeof value === 'boolean') {
      chips.push({ key, value: String(value) })
    } else if (typeof value === 'string' && value.length > 0 && value.length < 40) {
      chips.push({ key, value })
    }
    if (chips.length >= 8) break
  }
  return chips.length > 0 ? chips : null
}

function opSortKey(op: JobStatus): number {
  const updated = op.finished_at || op.started_at
  return updated ? parseAlmaTimestamp(updated).getTime() : 0
}

function OperationsView({
  ops,
  selectedJobId,
  onSelect,
  onDismiss,
  onCancel,
}: {
  ops: JobStatus[]
  selectedJobId: string | null
  onSelect: (jobId: string) => void
  onDismiss: (jobId: string) => void
  onCancel: (jobId: string) => void
}) {
  const [expandedParents, setExpandedParents] = useState<Record<string, boolean>>({})

  const byId = useMemo(() => {
    const map = new Map<string, JobStatus>()
    for (const op of ops) map.set(op.job_id, op)
    return map
  }, [ops])

  const parents = useMemo(() => {
    const out: JobStatus[] = []
    for (const op of ops) {
      if (op.parent_job_id && byId.has(op.parent_job_id)) continue
      out.push(op)
    }
    out.sort((a, b) => {
      const aActive = isActiveOperation(a.status)
      const bActive = isActiveOperation(b.status)
      if (aActive && !bActive) return -1
      if (!aActive && bActive) return 1
      return opSortKey(b) - opSortKey(a)
    })
    return out.slice(0, 100)
  }, [ops, byId])

  const childrenByParent = useMemo(() => {
    const grouped = new Map<string, JobStatus[]>()
    for (const op of ops) {
      const parentId = op.parent_job_id
      if (!parentId || !byId.has(parentId)) continue
      const arr = grouped.get(parentId) ?? []
      arr.push(op)
      grouped.set(parentId, arr)
    }
    for (const [, arr] of grouped.entries()) {
      arr.sort((a, b) => {
        const ai = a.stage_index ?? Number.MAX_SAFE_INTEGER
        const bi = b.stage_index ?? Number.MAX_SAFE_INTEGER
        if (ai !== bi) return ai - bi
        const aActive = isActiveOperation(a.status)
        const bActive = isActiveOperation(b.status)
        if (aActive && !bActive) return -1
        if (!aActive && bActive) return 1
        return opSortKey(b) - opSortKey(a)
      })
    }
    return grouped
  }, [ops, byId])

  useEffect(() => {
    const next: Record<string, boolean> = { ...expandedParents }
    let changed = false
    for (const parent of parents) {
      const children = childrenByParent.get(parent.job_id) ?? []
      if (children.length === 0) continue
      const hasActiveChild = children.some((c) => isActiveOperation(c.status))
      if (hasActiveChild && next[parent.job_id] !== true) {
        next[parent.job_id] = true
        changed = true
      }
    }
    if (changed) {
      setExpandedParents(next)
    }
  }, [parents, childrenByParent, expandedParents])

  const renderRow = (op: JobStatus, opts?: { isChild?: boolean; childCount?: number; parentId?: string }) => {
    const isChild = opts?.isChild === true
    const childCount = opts?.childCount ?? 0
    const isExpandableParent = !isChild && childCount > 0
    const isExpanded = expandedParents[op.job_id] !== false
    const displayMessage = op.stage_label || op.message || op.job_id
    const terminal = isTerminalStatus(op.status)
    const active = isActiveOperation(op.status)
    const duration = terminal ? formatDuration(op.started_at, op.finished_at) : null

    return (
      <button
        key={op.job_id}
        type="button"
        onClick={() => onSelect(op.job_id)}
        className={cn(
          'flex w-full items-start gap-3 border-l-2 px-4 py-3 text-left transition-colors hover:bg-slate-50',
          statusAccent(op.status),
          isChild && 'bg-slate-50/60 pl-8',
          selectedJobId === op.job_id && 'bg-slate-100',
        )}
      >
        {isExpandableParent ? (
          <button
            type="button"
            className="mt-0.5 rounded p-0.5 text-slate-400 hover:bg-slate-200 hover:text-slate-700"
            onClick={(e) => {
              e.stopPropagation()
              setExpandedParents((prev) => ({
                ...prev,
                [op.job_id]: prev[op.job_id] === false,
              }))
            }}
            title={isExpanded ? 'Collapse subtasks' : 'Expand subtasks'}
          >
            {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
          </button>
        ) : (
          <span className={cn('mt-1 w-4 shrink-0', isChild && 'w-2')} />
        )}
        <StatusIcon status={op.status} />
        <div className="min-w-0 flex-1 space-y-1">
          {/* Primary line: title + action buttons */}
          <div className="flex items-start justify-between gap-2">
            <span
              className={cn(
                'min-w-0 flex-1 truncate text-sm leading-tight',
                op.status === 'failed'
                  ? 'font-semibold text-rose-700'
                  : terminal
                    ? 'font-medium text-slate-500'
                    : 'font-semibold text-slate-800',
                isChild && 'text-xs',
              )}
              title={displayMessage}
            >
              {displayMessage}
            </span>
            <div className="flex shrink-0 items-center gap-1">
              {!terminal && (
                <button
                  type="button"
                  className="rounded p-0.5 text-slate-400 hover:bg-amber-100 hover:text-amber-700"
                  title="Cancel operation"
                  onClick={(e) => {
                    e.stopPropagation()
                    onCancel(op.job_id)
                  }}
                >
                  <Square className="h-3.5 w-3.5" />
                </button>
              )}
              <button
                type="button"
                className="rounded p-0.5 text-slate-400 hover:bg-slate-200 hover:text-slate-700"
                title="Dismiss operation"
                onClick={(e) => {
                  e.stopPropagation()
                  onDismiss(op.job_id)
                }}
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>

          {/* Context line: current author */}
          {op.current_author && (
            <p className="flex items-center gap-1.5 text-xs text-slate-600">
              <UserRound className="h-3 w-3 shrink-0 text-slate-400" />
              <span className="truncate font-medium">{op.current_author}</span>
            </p>
          )}

          {/* Metadata chips \u2014 canonical StatusBadge */}
          <div className="flex flex-wrap items-center gap-1.5">
            {op.stage_index != null && op.stage_total != null && (
              <StatusBadge tone="neutral" size="sm" className="uppercase tracking-wide">
                Phase {op.stage_index}/{op.stage_total}
              </StatusBadge>
            )}
            {childCount > 0 && !isChild && (
              <StatusBadge tone="neutral" size="sm" className="uppercase tracking-wide">
                {childCount} subtasks
              </StatusBadge>
            )}
            {op.trigger_source && (
              <StatusBadge tone="neutral" size="sm" className="uppercase tracking-wide">
                {op.trigger_source}
              </StatusBadge>
            )}
            <span className="font-mono text-[10px] text-slate-400">
              {op.job_id.length > 12 ? `${op.job_id.slice(0, 12)}...` : op.job_id}
            </span>
          </div>

          {/* Error \u2014 elevated card for failed jobs, inline text otherwise */}
          {op.error && (
            <p
              className={cn(
                'text-xs text-rose-700',
                op.status === 'failed' && 'rounded-md border border-rose-200 bg-rose-50/70 px-2 py-1',
              )}
            >
              {op.error}
            </p>
          )}

          {/* Progress bar: active jobs only */}
          {active && op.processed != null && op.total != null && op.total > 0 && (
            <div className="pt-0.5">
              <ProgressBar processed={op.processed} total={op.total} />
            </div>
          )}

          {/* Terminal summary: counts \u00b7 duration \u00b7 finished timestamp */}
          {terminal && (op.processed != null || op.started_at || duration) && (
            <p className="flex flex-wrap items-center gap-x-2 text-[11px] tabular-nums text-slate-500">
              {op.processed != null && op.total != null && op.total > 0 && (
                <span className="text-slate-600">
                  {op.processed}/{op.total}
                </span>
              )}
              {duration && <span>in {duration}</span>}
              {op.finished_at && (
                <span className="text-slate-400">\u00b7 Finished {formatTimestamp(op.finished_at)}</span>
              )}
            </p>
          )}

          {/* Active/unknown: just started timestamp */}
          {!terminal && op.started_at && (
            <p className="text-[11px] text-slate-400">Started {formatTimestamp(op.started_at)}</p>
          )}
        </div>
      </button>
    )
  }

  if (parents.length === 0) {
    return (
      <div className="flex items-center justify-center py-12 text-sm text-slate-400">
        No recent operations
      </div>
    )
  }

  return (
    <div className="divide-y divide-slate-100">
      {parents.map((parent) => {
        const children = childrenByParent.get(parent.job_id) ?? []
        const isExpanded = expandedParents[parent.job_id] !== false
        return (
          <div key={parent.job_id}>
            {renderRow(parent, { childCount: children.length })}
            {children.length > 0 && isExpanded && (
              <div className="border-t border-slate-100">
                {children.map((child) => renderRow(child, { isChild: true, parentId: parent.job_id }))}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null
}

function formatMetric(value: unknown): string {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(3).replace(/\.?0+$/, '')
  }
  return String(value ?? '')
}

function renderMetricChips(record: Record<string, unknown> | null) {
  if (!record) return null
  const entries = Object.entries(record)
  if (entries.length === 0) return null
  return (
    <div className="mt-2 flex flex-wrap gap-1.5">
      {entries.map(([key, value]) => (
        <span key={key} className="rounded-sm bg-white px-2 py-1 text-[10px] text-slate-600 ring-1 ring-slate-200">
          {key.replace(/_/g, ' ')}: {formatMetric(value)}
        </span>
      ))}
    </div>
  )
}

function renderDiscoveryLogData(step: string | undefined, data: Record<string, unknown> | undefined) {
  if (!step || !data) return null

  // The Activity panel intentionally drops the warm cream/parchment palette
  // in favor of a cool slate/white surface — it's a developer/utility view,
  // not a reading view, and the visual separation reinforces that distinction.
  // SubPanel `tone="cool"` carries the inset depth without the warmth.

  if (step === 'retrieval_channels') {
    return (
      <SubPanel tone="cool" padded={false} className="mt-2 space-y-2 p-2">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Channels</p>
          {renderMetricChips(asRecord(data.channels))}
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">External Lanes</p>
          {renderMetricChips(asRecord(data.external_lanes))}
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Graph Cache</p>
          {renderMetricChips(asRecord(data.graph_cache))}
        </div>
      </SubPanel>
    )
  }

  if (step === 'retrieval_detail') {
    const graphFallback = asRecord(data.graph_fallback)
    const laneRuns = Array.isArray(data.external_lane_runs) ? data.external_lane_runs.slice(0, 8) : []
    return (
      <SubPanel tone="cool" padded={false} className="mt-2 space-y-2 p-2">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Graph Fallback</p>
          {renderMetricChips(graphFallback)}
        </div>
        {laneRuns.length > 0 && (
          <div className="space-y-1">
            <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">External Lane Runs</p>
            {laneRuns.map((lane, idx) => {
              const record = asRecord(lane)
              if (!record) return null
              return (
                <div key={`${record.query ?? 'lane'}-${idx}`} className="rounded-sm border border-slate-200 bg-white px-2 py-1">
                  <div className="flex flex-wrap items-center gap-1.5 text-[10px] text-slate-700">
                    <span className="font-medium">{String(record.lane_type ?? 'lane')}</span>
                    {'query' in record && <span>{String(record.query)}</span>}
                    {'branch_label' in record && <span className="text-slate-500">({String(record.branch_label)})</span>}
                    {'result_count' in record && <span className="rounded-sm bg-slate-100 px-1.5 py-0.5">{String(record.result_count)} hits</span>}
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </SubPanel>
    )
  }

  if (step === 'scoring_inputs') {
    return (
      <SubPanel tone="cool" padded={false} className="mt-2 p-2">
        {renderMetricChips({
          positive_docs: data.positive_texts,
          negative_docs: data.negative_texts,
          candidate_texts: data.candidate_texts,
          candidate_embeddings: data.candidate_embeddings_ready,
          positive_examples: data.positive_examples_ready,
          negative_examples: data.negative_examples_ready,
          centroid_ms: data.centroid_prep_ms,
          lexical_profile_ms: data.lexical_profile_ms,
          candidate_text_ms: data.candidate_text_ms,
          candidate_embedding_batch_ms: data.candidate_embedding_batch_ms,
        })}
      </SubPanel>
    )
  }

  if (step === 'scoring_profile') {
    return (
      <SubPanel tone="cool" padded={false} className="mt-2 space-y-2 p-2">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Score Range</p>
          {renderMetricChips(asRecord(data.score_range))}
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Similarity</p>
          {renderMetricChips(asRecord(data.raw_similarity))}
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Modes</p>
          {renderMetricChips({
            ...((asRecord(data.text_similarity_modes) ?? {})),
            ...Object.fromEntries(
              Object.entries(asRecord(data.topic_match_modes) ?? {}).map(([key, value]) => [`topic_${key}`, value]),
            ),
          })}
        </div>
      </SubPanel>
    )
  }

  if (step === 'scoring_result') {
    const topCandidates = Array.isArray(data.top_candidates) ? data.top_candidates.slice(0, 5) : []
    if (topCandidates.length === 0) return null
    return (
      <SubPanel tone="cool" padded={false} className="mt-2 space-y-1 p-2">
        <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Top Ranked Candidates</p>
        {topCandidates.map((item, idx) => {
          const record = asRecord(item)
          if (!record) return null
          return (
            <div key={`${record.title ?? 'candidate'}-${idx}`} className="rounded-sm border border-slate-200 bg-white px-2 py-1">
              <div className="text-[11px] font-medium text-slate-700">{String(record.title ?? '')}</div>
              <div className="mt-0.5 flex flex-wrap gap-1.5 text-[10px] text-slate-500">
                {'score' in record && <span>score {String(record.score)}</span>}
                {'source_type' in record && <span>{String(record.source_type)}</span>}
                {'branch_label' in record && Boolean(record.branch_label) && <span>{String(record.branch_label)}</span>}
              </div>
            </div>
          )
        })}
      </SubPanel>
    )
  }

  if (step === 'done') {
    return (
      <SubPanel tone="cool" padded={false} className="mt-2 space-y-2 p-2">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Channels</p>
          {renderMetricChips(asRecord(data.channels))}
        </div>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Timings</p>
          {renderMetricChips(asRecord(data.timings_ms))}
        </div>
      </SubPanel>
    )
  }

  return null
}

function LogsView({ logs }: { logs: LogEntry[] }) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const prevCountRef = useRef(logs.length)

  // Auto-scroll to bottom when new entries arrive
  useEffect(() => {
    if (logs.length > prevCountRef.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
    prevCountRef.current = logs.length
  }, [logs.length])

  if (logs.length === 0) {
    return (
      <div className="flex items-center justify-center py-12 text-sm text-slate-400">
        No log entries
      </div>
    )
  }

  return (
    <ScrollArea ref={scrollRef} className="h-full">
      <div className="space-y-0">
        {logs.map((entry, i) => (
          <div
            key={`${entry.timestamp}-${i}`}
            className="flex items-start gap-2 px-4 py-1 font-mono text-xs hover:bg-slate-50"
          >
            <span className="shrink-0 tabular-nums text-slate-400">
              {formatTime(entry.timestamp)}
            </span>
            <StatusBadge
              tone={levelTone(entry.level)}
              size="sm"
              className="shrink-0 uppercase tracking-wide"
            >
              {entry.level}
            </StatusBadge>
            <span className="min-w-0 break-all text-slate-700">{entry.message}</span>
          </div>
        ))}
      </div>
    </ScrollArea>
  )
}

function OperationDetailView({
  job,
  logs,
}: {
  job: JobStatus | null
  logs: JobLogEntry[]
}) {
  const [levelFilter, setLevelFilter] = useState<string>('ALL')
  const scrollRef = useRef<HTMLDivElement>(null)
  const prevCountRef = useRef(logs.length)
  const jobActive = job ? isActiveOperation(job.status) : false

  const counts = useMemo(() => {
    const c = { DEBUG: 0, INFO: 0, WARNING: 0, ERROR: 0, CRITICAL: 0 }
    for (const log of logs) {
      if (log.level in c) c[log.level as keyof typeof c]++
    }
    return c
  }, [logs])

  const filteredLogs = useMemo(() => {
    if (levelFilter === 'ALL') return logs
    const min = LEVEL_ORDER[levelFilter] ?? 0
    return logs.filter((log) => (LEVEL_ORDER[log.level] ?? 0) >= min)
  }, [logs, levelFilter])

  useEffect(() => {
    if (!jobActive) {
      prevCountRef.current = logs.length
      return
    }
    const el = scrollRef.current
    if (!el) return
    if (logs.length > prevCountRef.current) {
      // Only auto-scroll if user is already near the bottom (within 40px)
      const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40
      if (nearBottom) {
        el.scrollTop = el.scrollHeight
      }
    }
    prevCountRef.current = logs.length
  }, [logs.length, jobActive])

  if (!job) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2 px-4 py-12 text-center">
        <Activity className="h-10 w-10 text-slate-300" strokeWidth={1.5} />
        <p className="text-sm font-medium text-slate-600">No operation selected</p>
        <p className="max-w-xs text-xs text-slate-400">
          Pick a row from the list to inspect its status, result, and step-by-step logs.
        </p>
      </div>
    )
  }

  const terminal = isTerminalStatus(job.status)
  const active = isActiveOperation(job.status)
  const duration = formatDuration(job.started_at, job.finished_at)
  const title = job.stage_label || job.message || job.job_id
  const resultSummary = summarizeResult(job.result)
  const resultKeys = job.result ? Object.keys(job.result) : []
  const hasResult = resultKeys.length > 0
  const errorsPlus = counts.ERROR + counts.CRITICAL

  return (
    <div className="flex h-full flex-col">
      {/* ── HEADER ── */}
      <div
        className={cn(
          'space-y-2 border-b border-slate-200 px-4 py-3',
          job.status === 'failed' && 'bg-rose-50/30',
        )}
      >
        <div className="flex items-start gap-2">
          <StatusIcon status={job.status} />
          <div className="min-w-0 flex-1">
            <h3
              className={cn(
                'text-sm font-semibold leading-tight',
                job.status === 'failed' ? 'text-rose-700' : 'text-slate-800',
              )}
              title={title}
            >
              {title}
            </h3>
            <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
              <StatusBadge tone={statusTone(job.status)} size="sm" className="capitalize">
                {job.status}
              </StatusBadge>
              {job.operation_key && (
                <StatusBadge
                  tone="neutral"
                  size="sm"
                  className="font-mono normal-case tracking-normal"
                >
                  {job.operation_key}
                </StatusBadge>
              )}
              {job.trigger_source && (
                <StatusBadge tone="neutral" size="sm" className="uppercase tracking-wide">
                  {job.trigger_source}
                </StatusBadge>
              )}
              {job.stage_index != null && job.stage_total != null && (
                <StatusBadge tone="info" size="sm" className="uppercase tracking-wide">
                  Phase {job.stage_index}/{job.stage_total}
                </StatusBadge>
              )}
            </div>
          </div>
        </div>

        {active && job.processed != null && job.total != null && job.total > 0 && (
          <ProgressBar processed={job.processed} total={job.total} />
        )}

        {(terminal || (!active && job.started_at)) && (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px] tabular-nums text-slate-500">
            {terminal && job.processed != null && job.total != null && job.total > 0 && (
              <span className="font-medium text-slate-700">
                {job.processed}/{job.total}
              </span>
            )}
            {duration && <span>took {duration}</span>}
            {job.started_at && <span>started {formatTimestamp(job.started_at)}</span>}
            {job.finished_at && <span>finished {formatTimestamp(job.finished_at)}</span>}
          </div>
        )}

        {job.error && (
          <p className="rounded-md border border-rose-200 bg-rose-50/70 px-2 py-1 text-xs text-rose-700">
            {job.error}
          </p>
        )}

        <p className="font-mono text-[10px] text-slate-400">{job.job_id}</p>
      </div>

      {/* ── RESULT ── */}
      {hasResult && (
        <div className="border-b border-slate-100 px-4 py-3">
          <div className="flex items-center justify-between gap-2">
            <EyebrowLabel tone="muted">Result</EyebrowLabel>
            <span className="text-[10px] text-slate-400">
              {resultKeys.length} field{resultKeys.length === 1 ? '' : 's'}
            </span>
          </div>
          {resultSummary && resultSummary.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5">
              {resultSummary.map(({ key, value }) => (
                <StatusBadge
                  key={key}
                  tone="neutral"
                  size="sm"
                  className="normal-case tracking-normal"
                >
                  <span className="text-slate-500">{key.replace(/_/g, ' ')}</span>
                  <span className="ml-1 font-mono text-slate-700">{value}</span>
                </StatusBadge>
              ))}
            </div>
          )}
          <details className="group mt-2">
            <summary className="flex cursor-pointer select-none items-center gap-1 text-[10px] font-medium uppercase tracking-wide text-slate-500 hover:text-slate-700">
              <ChevronRight className="h-3 w-3 transition-transform group-open:rotate-90" />
              Raw JSON
            </summary>
            <pre className="mt-2 max-h-96 overflow-auto rounded-sm border border-slate-200 bg-slate-50 p-3 text-[11px] leading-relaxed text-slate-700 shadow-paper-inset-cool">
              {JSON.stringify(job.result, null, 2)}
            </pre>
          </details>
        </div>
      )}

      {/* ── LOG STREAM TOOLBAR ── */}
      <div className="sticky top-0 z-10 flex flex-wrap items-center gap-2 border-b border-slate-200 bg-white/95 px-4 py-2 backdrop-blur">
        <EyebrowLabel tone="muted">Log stream</EyebrowLabel>
        <span className="text-[11px] tabular-nums text-slate-400">
          {filteredLogs.length}/{logs.length}
        </span>
        {errorsPlus > 0 && (
          <StatusBadge tone="negative" size="sm">
            {errorsPlus} error{errorsPlus === 1 ? '' : 's'}
          </StatusBadge>
        )}
        {counts.WARNING > 0 && (
          <StatusBadge tone="warning" size="sm">
            {counts.WARNING} warning{counts.WARNING === 1 ? '' : 's'}
          </StatusBadge>
        )}
        <Select value={levelFilter} onValueChange={setLevelFilter}>
          <SelectTrigger className="ml-auto h-7 w-32 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="ALL">All levels</SelectItem>
            <SelectItem value="INFO">Info +</SelectItem>
            <SelectItem value="WARNING">Warnings +</SelectItem>
            <SelectItem value="ERROR">Errors only</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* ── LOG ROWS ── */}
      <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto">
        {filteredLogs.length === 0 ? (
          <div className="px-4 py-6 text-xs text-slate-400">
            {logs.length === 0
              ? active
                ? 'Waiting for the first log entry…'
                : 'No detailed logs for this operation.'
              : 'No logs match the current filter.'}
          </div>
        ) : (
          filteredLogs.map((entry, idx) => {
            const isError = entry.level === 'ERROR' || entry.level === 'CRITICAL'
            const isWarning = entry.level === 'WARNING'
            return (
              <div
                key={`${entry.timestamp}-${idx}`}
                className={cn(
                  'border-b border-l-2 border-slate-100 px-4 py-2 text-xs',
                  isError && 'border-l-rose-400 bg-rose-50/30',
                  isWarning && 'border-l-amber-400 bg-amber-50/30',
                  !isError && !isWarning && 'border-l-transparent',
                )}
              >
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-mono tabular-nums text-slate-400">
                    {formatTime(entry.timestamp)}
                  </span>
                  <StatusBadge
                    tone={levelTone(entry.level)}
                    size="sm"
                    className="uppercase tracking-wide"
                  >
                    {entry.level}
                  </StatusBadge>
                  {entry.step && (
                    <StatusBadge
                      tone="neutral"
                      size="sm"
                      className="font-mono normal-case tracking-normal"
                    >
                      {entry.step}
                    </StatusBadge>
                  )}
                </div>
                <p className="mt-1 whitespace-pre-wrap break-words text-slate-700">
                  {entry.message}
                </p>
                {entry.data && Object.keys(entry.data).length > 0 && (
                  <>
                    {renderDiscoveryLogData(entry.step, entry.data)}
                    <details className="mt-2">
                      <summary className="cursor-pointer select-none text-[10px] font-medium uppercase tracking-wide text-slate-500 hover:text-slate-700">
                        Raw data
                      </summary>
                      <pre className="mt-2 max-h-48 overflow-auto rounded-sm border border-slate-200 bg-slate-50 p-2 font-mono text-[10px] leading-4 text-slate-600 shadow-paper-inset-cool">
                        {JSON.stringify(entry.data, null, 2)}
                      </pre>
                    </details>
                  </>
                )}
              </div>
            )
          })
        )}
      </div>
    </div>
  )
}

function QueryErrorView({ title, message }: { title: string; message: string }) {
  return (
    <div className="m-3 rounded border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
      <p className="font-medium">{title}</p>
      <p className="mt-1 break-all">{message}</p>
    </div>
  )
}

// ── Main component ──

export function ActivityPanel() {
  const [isOpen, setIsOpen] = useState(false)
  const [activeTab, setActiveTab] = useState<'operations' | 'logs'>('operations')
  const [logLevel, setLogLevel] = useState<string>('ALL')
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)
  const [panelHeight, setPanelHeight] = useState(360)
  const resizeStateRef = useRef<{ startY: number; startHeight: number } | null>(null)
  const queryClient = useQueryClient()

  const handleResizeStart = (e: React.MouseEvent) => {
    e.preventDefault()
    resizeStateRef.current = { startY: e.clientY, startHeight: panelHeight }
  }

  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      const state = resizeStateRef.current
      if (!state) return
      const delta = state.startY - e.clientY
      const maxH = Math.max(260, Math.floor(window.innerHeight * 0.85))
      const next = Math.max(220, Math.min(maxH, state.startHeight + delta))
      setPanelHeight(next)
    }
    const onUp = () => {
      resizeStateRef.current = null
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
  }, [])

  // Poll operations every 3s when open, 15s when closed
  const opsQuery = useQuery({
    queryKey: ['activity-operations'],
    queryFn: () => api.get<JobStatus[]>('/activity'),
    refetchInterval: isOpen ? 3000 : 15000,
  })

  // Poll logs every 5s only when the logs tab is active and panel is open
  const logsQuery = useQuery({
    queryKey: ['activity-logs', logLevel],
    queryFn: () =>
      api.get<LogEntry[]>(
        `/logs?limit=100${logLevel !== 'ALL' ? `&level=${logLevel}` : ''}`,
      ),
    refetchInterval: isOpen && activeTab === 'logs' ? 5000 : false,
    enabled: isOpen && activeTab === 'logs',
  })

  const operations = useMemo(() => opsQuery.data ?? [], [opsQuery.data])
  const topLevelOperations = useMemo(
    () => operations.filter((op) => !op.parent_job_id),
    [operations],
  )
  useEffect(() => {
    if (operations.length === 0) {
      setSelectedJobId(null)
      return
    }
    if (selectedJobId && operations.some((op) => op.job_id === selectedJobId)) {
      return
    }
    const seed = topLevelOperations.length > 0 ? topLevelOperations : operations
    const sorted = [...seed].sort((a, b) => {
      const aActive = isActiveOperation(a.status)
      const bActive = isActiveOperation(b.status)
      if (aActive && !bActive) return -1
      if (!aActive && bActive) return 1
      return opSortKey(b) - opSortKey(a)
    })
    setSelectedJobId(sorted[0]?.job_id ?? null)
  }, [operations, topLevelOperations, selectedJobId])

  const selectedOperation = selectedJobId
    ? operations.find((op) => op.job_id === selectedJobId) ?? null
    : null
  const selectedOpIsActive = selectedOperation
    ? isActiveOperation(selectedOperation.status)
    : false

  const selectedOpLogsQuery = useQuery({
    queryKey: ['activity-operation-logs', selectedJobId],
    queryFn: () => api.get<JobLogEntry[]>(`/activity/${selectedJobId}/logs?limit=200`),
    enabled: isOpen && activeTab === 'operations' && !!selectedJobId,
    refetchInterval:
      isOpen && activeTab === 'operations' && !!selectedJobId && selectedOpIsActive
        ? 3000
        : false,
  })

  const activeOps = topLevelOperations.filter((op) => isActiveOperation(op.status))
  const dismissMutation = useMutation({
    mutationFn: (jobId: string) => api.delete<{ success: boolean; job_id: string }>(`/activity/${encodeURIComponent(jobId)}`),
    onSuccess: (_data, jobId) => {
      if (selectedJobId === jobId) {
        setSelectedJobId(null)
      }
      void invalidateQueries(queryClient, ['activity-operations'], ['activity-operation-logs', jobId])
    },
  })
  const cancelMutation = useMutation({
    mutationFn: (jobId: string) =>
      api.post<{
        success: boolean
        job_id: string
        status: string
        cancel_requested: boolean
        message: string
      }>(`/activity/${encodeURIComponent(jobId)}/cancel`),
    onSuccess: (_data, jobId) => {
      void invalidateQueries(queryClient, ['activity-operations'], ['activity-operation-logs', jobId])
    },
  })
  const selectedOpLogs = (selectedOpLogsQuery.data ?? []).slice(-100)
  const logs = (logsQuery.data ?? []).slice(-100)
  const opsError = opsQuery.error instanceof Error ? opsQuery.error.message : null
  const selectedOpError =
    selectedOpLogsQuery.error instanceof Error ? selectedOpLogsQuery.error.message : null
  const logsError = logsQuery.error instanceof Error ? logsQuery.error.message : null

  return (
    <>
      {/* Mobile backdrop — closes the panel when tapped, blocks underlying content */}
      {isOpen && (
        <div
          onClick={() => setIsOpen(false)}
          className="fixed inset-0 z-30 bg-black/40 lg:hidden"
          aria-hidden
        />
      )}

      <div className="fixed bottom-0 left-0 right-0 z-40 lg:left-[260px]">
        {/* Toggle bar -- always visible.
            Activity intentionally lives in a cool slate/white palette,
            visually detaching it from the warm cream/parchment reading
            surfaces. The dev/utility tone reads as a separate "console"
            inside the same shell. */}
        <button
          onClick={() => setIsOpen(!isOpen)}
          className="flex w-full items-center justify-between border-t border-slate-200 bg-white px-4 py-2 shadow-sm hover:bg-slate-50"
        >
          <div className="flex items-center gap-2">
            <Activity className="h-4 w-4 text-slate-500" />
            <span className="text-sm font-medium text-slate-700">Activity</span>
            {activeOps.length > 0 && (
              <Badge className="bg-slate-700 text-white text-xs">
                {activeOps.length} running
              </Badge>
            )}
          </div>
          {isOpen ? (
            <ChevronDown className="h-4 w-4 text-slate-400" />
          ) : (
            <ChevronUp className="h-4 w-4 text-slate-400" />
          )}
        </button>

        {/* Expanded panel.
            On mobile we cap at 75vh so the bottom bar + drawer never clip the page's
            top area; on desktop we respect the user-resizable panelHeight. */}
        {isOpen && (
          <div
            className="flex max-h-[75vh] flex-col border-t border-slate-200 bg-white shadow-2xl lg:max-h-none"
            style={{ height: `${panelHeight}px` }}
          >
          {/* Resize handle */}
          <div
            onMouseDown={handleResizeStart}
            className="h-2 cursor-ns-resize border-b border-slate-200 bg-slate-50 hover:bg-slate-100"
            title="Drag to resize activity panel"
          />

          {/* Tab bar — shadcn Tabs primitive with a custom underline look so
              it matches the rest of the panel (horizontal filters on the
              same row, not the default pill-in-grey container). */}
          <Tabs
            value={activeTab}
            onValueChange={(v) => setActiveTab(v as 'operations' | 'logs')}
          >
            <div className="flex items-center gap-2 border-b border-slate-200 px-2">
              <TabsList className="h-auto rounded-none border-0 bg-transparent p-0">
                <TabsTrigger
                  value="operations"
                  className="gap-1.5 rounded-none border-b-2 border-transparent px-3 py-2 text-xs font-medium text-slate-500 shadow-none data-[state=active]:border-slate-700 data-[state=active]:bg-transparent data-[state=active]:text-slate-800 data-[state=active]:shadow-none"
                >
                  <Activity className="h-3.5 w-3.5" />
                  Operations
                  {activeOps.length > 0 && (
                    <Badge className="ml-1 bg-slate-700 text-white text-[10px] px-1.5 py-0">
                      {activeOps.length}
                    </Badge>
                  )}
                </TabsTrigger>
                <TabsTrigger
                  value="logs"
                  className="gap-1.5 rounded-none border-b-2 border-transparent px-3 py-2 text-xs font-medium text-slate-500 shadow-none data-[state=active]:border-slate-700 data-[state=active]:bg-transparent data-[state=active]:text-slate-800 data-[state=active]:shadow-none"
                >
                  <ScrollText className="h-3.5 w-3.5" />
                  Logs
                </TabsTrigger>
              </TabsList>

              {/* Log level filter -- only when logs tab is active */}
              {activeTab === 'logs' && (
                <Select value={logLevel} onValueChange={setLogLevel}>
                  <SelectTrigger className="ml-auto mr-2 h-8 w-36 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ALL">All Levels</SelectItem>
                    <SelectItem value="ERROR">Errors</SelectItem>
                    <SelectItem value="WARNING">Warnings</SelectItem>
                    <SelectItem value="INFO">Info</SelectItem>
                    <SelectItem value="DEBUG">Debug</SelectItem>
                  </SelectContent>
                </Select>
              )}
            </div>
          </Tabs>

          {/* Content area — ScrollArea wraps both split panes so long
              operation / log lists get a styled scrollbar and never clip
              the surrounding drawer chrome. */}
          <div className="min-h-0 flex-1 overflow-hidden">
            {activeTab === 'operations' ? (
              opsError ? (
                <QueryErrorView title="Failed to load operations" message={opsError} />
              ) : (
                <div className="grid h-full min-h-0 grid-cols-1 md:grid-cols-2">
                  <ScrollArea className="h-full border-r border-slate-100">
                    <OperationsView
                      ops={operations}
                      selectedJobId={selectedJobId}
                      onSelect={setSelectedJobId}
                      onCancel={(jobId) => cancelMutation.mutate(jobId)}
                      onDismiss={(jobId) => dismissMutation.mutate(jobId)}
                    />
                  </ScrollArea>
                  <ScrollArea className="h-full">
                    {selectedOpError ? (
                      <QueryErrorView title="Failed to load operation logs" message={selectedOpError} />
                    ) : (
                      <OperationDetailView job={selectedOperation} logs={selectedOpLogs} />
                    )}
                  </ScrollArea>
                </div>
              )
            ) : (
              logsError ? (
                <QueryErrorView title="Failed to load logs" message={logsError} />
              ) : (
                <LogsView logs={logs} />
              )
            )}
          </div>
        </div>
      )}
      </div>
    </>
  )
}
