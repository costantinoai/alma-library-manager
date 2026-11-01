import { useState } from 'react'

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { StatusBadge } from '@/components/ui/status-badge'
import { SIGNAL_KINDS, type SignalKind } from './signalKinds'

export type { SignalKind } from './signalKinds'

export interface SignalChipProps {
  kind: SignalKind
  children: React.ReactNode
  /** Overrides the registry hint. Use for evidence strings that carry real
   * figures ("Shares 12 references with …"). */
  title?: string
  /** The chip's full meaning, for a chip whose label is cut short. Opens on
   * hover with a mouse and on tap on a phone, where a `title` never shows. */
  explain?: React.ReactNode
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
  explain,
  size = 'sm',
  hideIcon = false,
  className,
}: SignalChipProps) {
  const [open, setOpen] = useState(false)
  const spec = SIGNAL_KINDS[kind] ?? SIGNAL_KINDS.meta
  const chip = (
    <StatusBadge
      tone={spec.tone}
      size={size}
      icon={hideIcon ? undefined : spec.icon}
      title={explain ? undefined : (title ?? spec.hint)}
      className={className}
    >
      {children}
    </StatusBadge>
  )
  if (!explain) return chip

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="inline-flex max-w-full cursor-help rounded-full focus-ring"
          // Chips sit inside clickable cards: a tap explains, it never opens the card.
          onClick={(event) => event.stopPropagation()}
          onPointerEnter={(event) => { if (event.pointerType === 'mouse') setOpen(true) }}
          onPointerLeave={(event) => { if (event.pointerType === 'mouse') setOpen(false) }}
        >
          {chip}
        </button>
      </PopoverTrigger>
      <PopoverContent
        side="top"
        align="start"
        className="w-64 p-2.5 text-xs leading-snug text-slate-600"
        onOpenAutoFocus={(event) => event.preventDefault()}
        onClick={(event) => event.stopPropagation()}
      >
        {explain}
      </PopoverContent>
    </Popover>
  )
}
