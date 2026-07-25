import { StatusBadge } from '@/components/ui/status-badge'
import { SIGNAL_KINDS, type SignalKind } from './signalKinds'

export type { SignalKind } from './signalKinds'

export interface SignalChipProps {
  kind: SignalKind
  children: React.ReactNode
  /** Overrides the registry hint. Use for evidence strings that carry real
   * figures ("Shares 12 references with …"). */
  title?: string
  size?: 'sm' | 'default' | 'lg'
  /** Drop the glyph when a dense chip row already has a category icon. */
  hideIcon?: boolean
  className?: string
}

/** Render one fact through the shared signal-kind registry. */
export function SignalChip({
  kind,
  children,
  title,
  size = 'sm',
  hideIcon = false,
  className,
}: SignalChipProps) {
  const spec = SIGNAL_KINDS[kind] ?? SIGNAL_KINDS.meta
  return (
    <StatusBadge
      tone={spec.tone}
      size={size}
      icon={hideIcon ? undefined : spec.icon}
      title={title ?? spec.hint}
      className={className}
    >
      {children}
    </StatusBadge>
  )
}
