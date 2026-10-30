import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import type { BranchCoverage } from '@/api/client'

import { branchReach } from '@/lib/branchReach'

import { BranchCoverageRows } from './BranchCoverageRows'

const row = (fields: Partial<BranchCoverage>): BranchCoverage => ({
  branch_id: 'b',
  label: 'expertise / chess',
  state: 'active',
  seed_count: 3,
  retrieved: 0,
  attributed: 0,
  eligible: 0,
  cards: 0,
  floor_cards: 0,
  reason: 'no_retrieval_hit',
  ...fields,
})

describe('BranchCoverageRows', () => {
  it('says why a branch has no card', () => {
    render(<BranchCoverageRows coverage={[row({})]} />)

    expect(
      screen.getByText('expertise / chess: no search found anything for it'),
    ).toBeInTheDocument()
  })

  it('stays silent for a muted branch and a branch with cards', () => {
    const { container } = render(
      <BranchCoverageRows
        coverage={[
          row({ state: 'muted', reason: 'muted' }),
          row({ branch_id: 'c', cards: 2, reason: null }),
        ]}
      />,
    )

    expect(container).toBeEmptyDOMElement()
  })

  it('counts reach without muted branches', () => {
    expect(
      branchReach([
        row({ branch_id: 'a', cards: 2, reason: null }),
        row({ branch_id: 'b' }),
        row({ branch_id: 'c', state: 'muted', reason: 'muted' }),
      ]),
    ).toEqual({ reached: 1, total: 2 })
  })
})
