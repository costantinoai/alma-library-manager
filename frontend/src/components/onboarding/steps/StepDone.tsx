import { useQuery } from '@tanstack/react-query'
import { Activity, BarChart3, BookMarked, Compass, HeartPulse, Settings as SettingsIcon, Users } from 'lucide-react'
import { MetricTile } from '@/components/shared'
import { RevealItem } from '@/components/ui/reveal'
import { BrandRule } from '@/components/ui/brand-rule'
import { getOnboardingStatus } from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

export function StepDone({ state, back, finish, finishing }: StepContext) {
  const { data } = useQuery({
    queryKey: ['onboarding-status'],
    queryFn: getOnboardingStatus,
    staleTime: 5_000,
  })

  const libraryCount = data?.library_count ?? 0
  const followedCount = data?.followed_count ?? 0
  const firstName = state.name ? state.name.split(' ')[0] : null

  return (
    <StepShell
      eyebrow="All set"
      title={firstName ? `You're ready, ${firstName}.` : "You're ready."}
      lead="ALMa now has a sense of who you are and what you care about. It keeps working in the background — the more you save, like, and dismiss, the sharper it gets."
      footer={
        <StepNav
          onBack={back}
          primary={undefined}
          onContinue={finish}
          continueLabel="Take me in"
          continueLoading={finishing}
        />
      }
    >
      <div className="space-y-6">
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <MetricTile label="In your library" value={libraryCount} tone="accent" align="center" icon={BookMarked} iconColor="var(--color-alma-folio)" />
          <MetricTile label="Authors followed" value={followedCount} tone="info" align="center" icon={Users} iconColor="var(--color-info-500)" />
          <MetricTile label="Keyword monitors" value={state.keywords.length} tone="neutral" align="center" icon={Activity} />
          <MetricTile label="Lenses" value={state.lensId ? 1 : 0} tone="neutral" align="center" icon={Compass} />
        </div>

        <BrandRule center="diamond" tone="gold" />

        <div className="space-y-3">
          <p className="text-sm font-medium text-alma-800">A few places worth knowing about:</p>
          <ul className="space-y-3 text-sm leading-relaxed text-slate-600">
            <RevealItem index={0}>
              <li className="flex items-start gap-3">
                <HeartPulse className="mt-0.5 h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
                <span>
                  <span className="font-medium text-alma-800">Health</span> — ALMa's data needs occasional
                  upkeep. Look in now and then to keep your corpus tidy and complete.
                </span>
              </li>
            </RevealItem>
            <RevealItem index={1}>
              <li className="flex items-start gap-3">
                <BarChart3 className="mt-0.5 h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
                <span>
                  <span className="font-medium text-alma-800">Insights</span> — maps, clusters and trends
                  across everything you've collected. Have a wander.
                </span>
              </li>
            </RevealItem>
            <RevealItem index={2}>
              <li className="flex items-start gap-3">
                <SettingsIcon className="mt-0.5 h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
                <span>
                  <span className="font-medium text-alma-800">Settings</span> — keys, monitors, notifications,
                  and a button to run this welcome again whenever you like.
                </span>
              </li>
            </RevealItem>
          </ul>
        </div>
      </div>
    </StepShell>
  )
}
