import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { MaintenanceOperation } from '@/api/client'
import { RepairCard } from './RepairCard'
import { isOpAttention } from './healthFormat'

const operation: MaintenanceOperation = {
  key: 'repair', label: 'Repair metadata', description: 'Fill missing metadata',
  cost: 'cheap', sources: [], local_compute: false, destructive: false,
  stage: 'metadata', order: 1, unit: 'papers', target_kind: 'paper',
  supports_targets: false, prerequisites: [], dependencies: [], blocked_by: [],
  unlocks: [], optional: false, manual_gate: false, readiness: 'healthy',
  recommended: false, repairs: [], operation_key: 'repair', candidates_pending: 0,
  eta: null, quota: null, params_spec: { dry_run: { default: true } },
  request_batch_size: null, request_batch_default: null, request_batch_max: null,
  request_batch_unit: null, auto_enabled: false, default_auto_enabled: false,
  paused_by_user_until: null, auto_daily_cap: 10, max_auto_daily_cap: 100,
  manual_limit: 10, default_manual_limit: 10, max_manual_limit: 100,
  last_run: null, last_success_at: null,
}

const assessmentError = {
  task_key: 'repair', cause: 'OperationalError',
  message: 'Pending work could not be measured.',
  recovery: 'Re-assess Health after the database becomes available.',
}

describe('unknown maintenance assessments', () => {
  it('disables empty item repairs while explaining that no work is eligible', () => {
    const client = new QueryClient()
    render(
      <QueryClientProvider client={client}>
        <RepairCard op={operation} dims={[]} onRun={vi.fn()} onConfig={vi.fn()}
          onResume={vi.fn()} onOpenDim={vi.fn()} running={false} />
      </QueryClientProvider>,
    )
    expect(screen.getByText(/No eligible work/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Run now' })).toBeDisabled()
  })
  it('keeps unknown counts visible for attention while measured zero is all clear', () => {
    expect(isOpAttention(operation, [])).toBe(false)
    expect(isOpAttention({ ...operation, candidates_pending: null }, [])).toBe(true)
    expect(isOpAttention({ ...operation, blocked_by: [{ key: 'upstream', label: 'Upstream', pending: null, required: true }] }, [])).toBe(true)
  })

  it('shows unknown and recovery instead of zero and disables both run paths', () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(
      <QueryClientProvider client={client}>
        <RepairCard
          op={{ ...operation, candidates_pending: null, readiness: 'assessment_failed', assessment_error: assessmentError }}
          dims={[]} onRun={vi.fn()} onConfig={vi.fn()} onResume={vi.fn()}
          onOpenDim={vi.fn()} running={false}
        />
      </QueryClientProvider>,
    )
    expect(screen.getByText('Unknown')).toBeInTheDocument()
    expect(screen.getByRole('alert')).toHaveTextContent(assessmentError.message)
    expect(screen.getByRole('alert')).toHaveTextContent(assessmentError.cause)
    expect(screen.getByRole('alert')).toHaveTextContent(assessmentError.recovery)
    expect(screen.getByRole('button', { name: 'Run now' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Preview' })).toBeDisabled()
  })

  it('prevents running when a required prerequisite cannot be measured', () => {
    const client = new QueryClient()
    render(
      <QueryClientProvider client={client}>
        <RepairCard
          op={{ ...operation, candidates_pending: 5, readiness: 'blocked', blocked_by: [{ key: 'upstream', label: 'Upstream repair', pending: null, required: true, assessment_error: assessmentError }] }}
          dims={[]} onRun={vi.fn()} onConfig={vi.fn()} onResume={vi.fn()}
          onOpenDim={vi.fn()} running={false}
        />
      </QueryClientProvider>,
    )
    expect(screen.getByText(/Upstream repair \(unknown\)/)).toHaveTextContent(assessmentError.recovery)
    expect(screen.getByRole('button', { name: 'Run now' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Preview' })).toBeDisabled()
  })
})
