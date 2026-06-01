import { useQuery } from '@tanstack/react-query'
import { Compass, Volume2 } from 'lucide-react'
import { BranchExplorerPanel } from '@/components/discovery/BranchExplorerPanel'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { SubPanel } from '@/components/ui/sub-panel'
import { listLenses } from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

export function StepBranches({ state, next, back }: StepContext) {
  const { data: lenses } = useQuery({ queryKey: ['lenses'], queryFn: listLenses, staleTime: 10_000 })
  const lens = (lenses ?? []).find((l) => l.id === state.lensId) ?? (lenses ?? [])[0] ?? null

  return (
    <StepShell
      eyebrow="How discovery thinks"
      title="Your lens grows branches."
      lead="A lens isn't one flat search — ALMa carves your library into branches, each a cluster of related work. Here's what they look like and how they steer Discovery."
      footer={<StepNav onBack={back} onContinue={next} continueLabel="Continue" />}
    >
      <div className="space-y-5">
        <ConceptCallout
          eyebrow="What is a branch?"
          summary="The lens is the trunk; branches are clusters of your papers. Each branch steers what Discovery looks for."
          defaultOpen
        >
          <p>
            The lens is the <span className="font-medium text-alma-800">trunk</span>. From it, ALMa grows{' '}
            <span className="font-medium text-alma-800">branches</span> — clusters it finds by grouping your
            library's papers by similarity (their embeddings, or their distinctive terms when embeddings
            aren't ready). A focused library grows one or two branches; a wide-ranging one grows several.
          </p>
          <p>
            Each branch pulls in two directions:{' '}
            <span className="inline-flex items-center gap-1 font-medium text-alma-folio">
              <Volume2 className="h-3.5 w-3.5" /> core pull
            </span>{' '}
            stays on the topics every paper in the cluster shares, while{' '}
            <span className="inline-flex items-center gap-1 font-medium text-gold-600">
              <Compass className="h-3.5 w-3.5" /> explore push
            </span>{' '}
            reaches toward neighbouring topics for fresh angles.
          </p>
          <p>
            Branches tune themselves from your reactions: ones you save from get{' '}
            <span className="font-medium text-alma-800">pushed</span> (more of Discovery's effort), ones you
            dismiss get <span className="font-medium text-alma-800">pruned</span> back — and a branch that
            keeps missing is eventually muted. The defaults are usually right; you can always pin, boost, or
            mute a branch from the Branch Studio in Discovery, or in Settings.
          </p>
        </ConceptCallout>

        {lens ? (
          <BranchExplorerPanel lens={lens} />
        ) : (
          <SubPanel className="text-sm text-slate-600">
            No lens yet — go back a step to create your library lens, and your branches will appear here.
          </SubPanel>
        )}
      </div>
    </StepShell>
  )
}
