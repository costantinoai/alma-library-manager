import { AnimatePresence } from 'framer-motion'
import { OnboardingFlow } from './OnboardingFlow'
import { useWalkthroughs, WELCOME_WALKTHROUGH } from './useWalkthrough'

/**
 * OnboardingGate — shows the first-run flow only when the server says the
 * `welcome` walkthrough is pending. Nothing renders while that request loads or
 * after it fails. Closing the flow updates the shared query, so the gate animates
 * out. Mounted once in App, above the app shell.
 */
export function OnboardingGate() {
  const { data } = useWalkthroughs()
  const show = data?.[WELCOME_WALKTHROUGH]?.pending === true

  return (
    <AnimatePresence>{show ? <OnboardingFlow key="onboarding" /> : null}</AnimatePresence>
  )
}
