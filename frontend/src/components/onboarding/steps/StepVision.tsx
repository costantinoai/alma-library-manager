import { HardDrive, Lock, Mail } from 'lucide-react'
import { RevealItem } from '@/components/ui/reveal'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

/**
 * Vision + privacy. ALMa is free and self-hosted: nothing leaves the machine
 * except the contact email (if set) that goes to OpenAlex / Semantic Scholar
 * with the lookups used to fetch public paper metadata. No other data is sent
 * anywhere — everything else happens locally.
 */
const POINTS = [
  {
    icon: HardDrive,
    title: 'Free and self-hosted',
    body: 'ALMa runs entirely on your own machine. Your library, your notes, your reading — all of it lives in a local database you own. There is no account and no cloud.',
  },
  {
    icon: Lock,
    title: 'Your data stays put',
    body: 'No usage tracking, no analytics, no syncing to anyone. What you save and how you react never leaves this computer.',
  },
  {
    icon: Mail,
    title: 'The one thing that goes out',
    body: 'To fetch public paper and author metadata, ALMa queries OpenAlex and Semantic Scholar. Those requests carry your contact email (if you add one on the next screen) — that is the polite way to use those open APIs and gets you faster, more reliable access. Nothing else is shared.',
  },
]

export function StepVision({ next, back }: StepContext) {
  return (
    <StepShell
      eyebrow="What ALMa is"
      title="Yours, and only yours."
      lead="A personal suggestion engine for academic literature — built to be private by default."
      footer={<StepNav onBack={back} onContinue={next} continueLabel="Got it" />}
    >
      <ul className="space-y-3">
        {POINTS.map((p, i) => (
          <RevealItem key={p.title} index={i}>
            <li className="flex items-start gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-3">
              <span className="mt-0.5 grid h-8 w-8 shrink-0 place-items-center rounded-sm bg-alma-folio-soft text-alma-folio">
                <p.icon className="h-4 w-4" aria-hidden />
              </span>
              <div className="min-w-0">
                <p className="text-sm font-semibold text-alma-800">{p.title}</p>
                <p className="mt-0.5 text-sm leading-relaxed text-slate-600">{p.body}</p>
              </div>
            </li>
          </RevealItem>
        ))}
      </ul>
    </StepShell>
  )
}
