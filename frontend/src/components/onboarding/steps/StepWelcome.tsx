import { BookMarked, Compass, Users } from 'lucide-react'
import { RevealItem } from '@/components/ui/reveal'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

const POINTS = [
  { icon: Users, text: 'Put your own work and the authors you follow at the centre.' },
  { icon: BookMarked, text: 'Curate a starting library and tune what we watch for you.' },
  { icon: Compass, text: 'Open up Discovery — papers beyond what you already track.' },
]

export function StepWelcome({ next }: StepContext) {
  return (
    <StepShell
      eyebrow="Welcome"
      title="Let's set up your library."
      lead="ALMa is a quiet research companion — it watches the literature so you don't have to. The next few minutes shape who and what it pays attention to. Take your time; you can change any of it later."
      footer={<StepNav onContinue={next} continueLabel="Let's begin" />}
    >
      <ul className="space-y-3">
        {POINTS.map((p, i) => (
          <RevealItem key={p.text} index={i}>
            <li className="flex items-start gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-3">
              <span className="mt-0.5 grid h-8 w-8 shrink-0 place-items-center rounded-sm bg-alma-folio-soft text-alma-folio">
                <p.icon className="h-4 w-4" aria-hidden />
              </span>
              <span className="text-sm leading-relaxed text-slate-600">{p.text}</span>
            </li>
          </RevealItem>
        ))}
      </ul>
    </StepShell>
  )
}
