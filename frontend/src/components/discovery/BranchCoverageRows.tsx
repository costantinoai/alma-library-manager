import type { BranchCoverage } from '@/api/client'
import { StatusRow } from '@/components/shared/StatusRow'

/** The stage that lost a branch, in the words of `branch_coverage().reason`. */
const REASON_LABEL: Record<string, string> = {
  no_retrieval_hit: 'no search found anything for it',
  only_shared_hits: 'everything it found is shared with another branch',
  all_ineligible: 'everything it found is in your Library or was dismissed',
  no_slot: 'no place left in this deck',
}

/** One line per branch that has no card in the current deck, saying why. */
export function BranchCoverageRows({ coverage }: { coverage: BranchCoverage[] }) {
  const empty = coverage.filter((branch) => branch.reason && branch.reason !== 'muted')
  if (empty.length === 0) return null
  return (
    <div className="space-y-1.5">
      {empty.map((branch) => (
        <StatusRow
          key={branch.branch_id}
          severity={branch.state === 'probing' ? 'info' : 'warning'}
          label={`${branch.label ?? 'Unnamed branch'}: ${REASON_LABEL[branch.reason ?? ''] ?? branch.reason}`}
          title={`${branch.retrieved} retrieved, ${branch.attributed} attributed, ${branch.eligible} eligible`}
        />
      ))}
    </div>
  )
}
