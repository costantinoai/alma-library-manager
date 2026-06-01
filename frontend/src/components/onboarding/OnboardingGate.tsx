import { AnimatePresence } from 'framer-motion'
import { useQuery } from '@tanstack/react-query'
import { getBootstrap } from '@/api/client'
import { OnboardingFlow } from './OnboardingFlow'

/**
 * OnboardingGate — shows the first-run flow whenever onboarding hasn't been
 * completed. Reuses the shared ['bootstrap'] query (no extra fetch); the flow's
 * `finish` flips `onboarding.completed` and invalidates the query so the gate
 * animates out. Mounted once in App, above the app shell.
 */
export function OnboardingGate() {
  const { data } = useQuery({
    queryKey: ['bootstrap'],
    queryFn: getBootstrap,
    staleTime: 60_000,
  })

  const show = data?.onboarding?.completed === false

  return (
    <AnimatePresence>{show ? <OnboardingFlow key="onboarding" /> : null}</AnimatePresence>
  )
}
