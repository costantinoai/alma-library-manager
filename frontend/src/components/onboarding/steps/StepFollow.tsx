import { useQuery } from '@tanstack/react-query'
import { SuggestedAuthorsRail } from '@/components/authors/SuggestedAuthorsRail'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { listFollowedAuthors } from '@/api/client'
import { StepShell, StepNav, GoalMeter } from '../StepShell'
import type { StepContext } from '../types'

const TARGET = 5

export function StepFollow({ state, next, back }: StepContext) {
  // The rail invalidates ['library-followed-authors'] on every follow, so this
  // count updates live as the user follows. The owner is excluded from the goal.
  const { data: followed } = useQuery({
    queryKey: ['library-followed-authors'],
    queryFn: listFollowedAuthors,
    staleTime: 10_000,
  })
  const nonOwner = (followed ?? []).filter((f) => f.author_id !== state.owner?.author_id)
  const count = nonOwner.length

  return (
    <StepShell
      eyebrow="Build your circle"
      title="Follow a few authors."
      lead="These suggestions come from your own work and the people around it. Following an author tells ALMa to track them — their new papers land in your Feed, and we pull in their back catalogue to learn from."
      footer={
        <StepNav
          onBack={back}
          onSkip={count >= TARGET ? undefined : next}
          skipLabel="Skip for now"
          onContinue={next}
          continueLabel="Continue"
          hint={count >= TARGET ? undefined : `Try to follow at least ${TARGET} — it gives the suggestion engine room to work.`}
        />
      }
    >
      <div className="space-y-4">
        <div className="flex items-center justify-between gap-3">
          <ConceptCallout
            className="flex-1"
            eyebrow="What does following do?"
            summary="Followed authors are monitored — new work appears in your Feed, and their papers feed your suggestions."
          >
            <p>
              When you follow someone, ALMa creates a monitor for them and fetches their publication
              history in the background (a "deep refresh"). That history becomes signal: it sharpens who
              else to suggest and what to surface in Discovery. You can unfollow anyone later from the
              Authors page.
            </p>
          </ConceptCallout>
        </div>

        <div className="flex justify-end">
          <GoalMeter done={count} target={TARGET} noun="followed" />
        </div>

        {/* 3-up inside the narrow onboarding modal (the page default escalates
            to 5-up on xl, which squishes cards here). */}
        <SuggestedAuthorsRail
          gridClassName="grid gap-3 sm:grid-cols-2 lg:grid-cols-3"
          collapsedCount={6}
        />
      </div>
    </StepShell>
  )
}
