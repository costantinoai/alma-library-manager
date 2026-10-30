import type { BranchCoverage } from '@/api/client'

/** Branches that can reach the deck and how many did. Muted ones were asked not to. */
export function branchReach(coverage: BranchCoverage[]): { reached: number; total: number } {
  const open = coverage.filter((branch) => branch.state !== 'muted')
  return { reached: open.filter((branch) => branch.cards > 0).length, total: open.length }
}
