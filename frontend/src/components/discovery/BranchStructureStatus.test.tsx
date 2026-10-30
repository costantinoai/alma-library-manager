import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { LensBranchItem, LensBranchPreview } from '@/api/client'
import { BranchStructureStatus } from './BranchStructureStatus'

const refreshLens = vi.fn()

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  refreshLens: (...args: unknown[]) => refreshLens(...args),
}))

const branches = [{ id: 'new', label: 'Vision / cortex' }] as LensBranchItem[]

function preview(overrides: Partial<NonNullable<LensBranchPreview['structure']>> = {}, retired: LensBranchPreview['retired_branches'] = []) {
  return {
    lens_id: 'lens-1',
    context_type: 'library_global',
    seed_count: 548,
    temperature: 0.28,
    generated_at: '',
    branches,
    retired_branches: retired,
    structure: {
      revision: 1,
      built_at: '2026-10-30T10:00:00',
      trigger: 'initial',
      seed_count_at_build: 547,
      rebuild_reason: null,
      lineage: null,
      joined: 1,
      left: 0,
      misfit: 0,
      turnover: 0.002,
      misfit_share: 0,
      rebuild_recommended: false,
      ...overrides,
    },
  } as LensBranchPreview
}

function show(data: LensBranchPreview) {
  const client = new QueryClient()
  render(
    <QueryClientProvider client={client}>
      <BranchStructureStatus lensId="lens-1" preview={data} branches={branches} />
    </QueryClientProvider>,
  )
}

describe('BranchStructureStatus', () => {
  beforeEach(() => refreshLens.mockReset().mockResolvedValue({ status: 'queued', job_id: 'j' }))

  it('says what moved and rebuilds only after confirmation', async () => {
    show(preview())
    expect(screen.getByText(/1 joined, 0 left since/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Rebuild branches' }))
    expect(refreshLens).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Rebuild' }))
    await waitFor(() => expect(refreshLens).toHaveBeenCalledWith('lens-1', 50, { rebuildBranches: true }))
  })

  it('recommends a rebuild only when the numbers do', () => {
    show(preview({ rebuild_recommended: true }))
    expect(screen.getByText(/a rebuild would fit these papers better/)).toBeInTheDocument()
  })

  it('tells the truth about a branch that ended and the control it held', () => {
    show(
      preview({}, [
        { id: 'old', label: 'Syntax / grammar', reason: 'merged', successor_ids: ['new'], retired_at: '', control_state: 'pinned' },
        { id: 'gone', label: 'Audio', reason: 'emptied', successor_ids: [], retired_at: '', control_state: null },
      ]),
    )
    expect(screen.getByText('Syntax / grammar merged in a rebuild into Vision / cortex; it was pinned')).toBeInTheDocument()
    expect(screen.getByText('Audio lost its last paper')).toBeInTheDocument()
  })

  it('marks a changed Cluster detail as a preview of a rebuild', () => {
    show(
      preview({
        rebuild_reason: 'resolution',
        lineage: { continued: ['a'], retired: [{ id: 'b', reason: 'split', successor_ids: [] }], controls_carried: {}, controls_dropped: [] },
      }),
    )
    expect(screen.getByText(/1 branches continue, 1 end/)).toBeInTheDocument()
  })
})
