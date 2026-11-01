import { useMutation } from '@tanstack/react-query'
import { GitBranch, Loader2 } from 'lucide-react'
import { useState } from 'react'

import {
  getApiErrorMessage,
  refreshLens,
  type LensBranchItem,
  type LensBranchPreview,
  type RetiredBranch,
} from '@/api/client'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { StatusRow } from '@/components/shared/StatusRow'
import { useToast } from '@/hooks/useToast'
import { describeJobLaunch } from '@/lib/activity'
import { formatPaperDate } from '@/lib/format'

const REBUILD_REFRESH_LIMIT = 50

const RETIRED_REASON: Record<string, string> = {
  emptied: 'lost its last paper',
  split: 'split in a rebuild',
  merged: 'merged in a rebuild',
  replaced: 'replaced in a rebuild',
}

function retiredLine(item: RetiredBranch, branches: LensBranchItem[]): string {
  const successors = item.successor_ids
    .map((id) => branches.find((branch) => branch.id === id)?.label)
    .filter(Boolean)
  const into = successors.length > 0 ? ` into ${successors.join(', ')}` : ''
  const held = item.control_state ? `; it was ${item.control_state}` : ''
  return `${item.label || item.id} ${RETIRED_REASON[item.reason] ?? item.reason}${into}${held}`
}

function structureLine(preview: LensBranchPreview): string {
  const structure = preview.structure
  if (!structure?.built_at) return 'Built by the first refresh of this lens.'
  const built = `Built ${formatPaperDate(structure.built_at)}`
  const moved = `${structure.joined} joined, ${structure.left} left since`
  return `${built}; ${moved}`
}

/** What the stored branch structure is, what ended, and the one way to rebuild it. */
export function BranchStructureStatus({
  lensId,
  preview,
  branches,
}: {
  lensId: string
  preview: LensBranchPreview | null | undefined
  branches: LensBranchItem[]
}) {
  const { toast } = useToast()
  const [confirming, setConfirming] = useState(false)
  const rebuild = useMutation({
    mutationFn: () => refreshLens(lensId, REBUILD_REFRESH_LIMIT, { rebuildBranches: true }),
    onSuccess: (envelope) => toast(describeJobLaunch(envelope, 'Branch rebuild')),
    onError: (error) =>
      toast({ title: 'Branch rebuild failed', description: getApiErrorMessage(error) }),
  })
  if (!preview?.structure) return null
  const { structure } = preview
  const pending = structure.rebuild_reason === 'resolution'
  const lineage = structure.lineage

  return (
    <div className="space-y-2">
      <StatusRow
        severity={structure.rebuild_recommended ? 'warning' : 'ok'}
        label={
          structure.rebuild_recommended
            ? `${structureLine(preview)}; a rebuild would fit these papers better`
            : structureLine(preview)
        }
        metric={
          <Button
            type="button"
            size="sm"
            variant="outline"
            className="shrink-0"
            onClick={() => setConfirming(true)}
            disabled={rebuild.isPending}
          >
            {rebuild.isPending ? (
              <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
            ) : (
              <GitBranch className="mr-1 h-3.5 w-3.5" />
            )}
            Rebuild branches
          </Button>
        }
      />
      {pending && lineage ? (
        <StatusRow
          severity="info"
          label={`Rebuild preview at this Cluster detail: ${lineage.continued.length} branches continue, ${lineage.retired.length} end. Apply Controls and the next refresh does it.`}
        />
      ) : null}
      {preview.retired_branches.map((item) => (
        <StatusRow key={item.id} severity="info" label={retiredLine(item, branches)} />
      ))}
      <AlertDialog open={confirming} onOpenChange={setConfirming}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Rebuild branches for this lens?</AlertDialogTitle>
            <AlertDialogDescription>
              Clustering runs again on every paper, and most branches come back different. A
              branch that keeps its papers keeps its history. Any other branch starts with no
              outcomes. A pin, boost or mute follows the majority of its papers, and the rest are
              listed here afterwards. This also refreshes the lens.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="default" onClick={() => rebuild.mutate()}>Rebuild</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
