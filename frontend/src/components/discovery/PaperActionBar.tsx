import { BookOpenCheck, BookPlus, BookmarkCheck, Heart, Plus, ThumbsDown, ThumbsUp, X } from 'lucide-react'
import type { ComponentType, ReactNode } from 'react'

import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { ACTION_QUEUE_CLASSES } from '@/lib/palette'

type Tone = 'neutral' | 'queue' | 'add' | 'like' | 'love' | 'dismiss' | 'dislike'

export type PaperReaction = 'like' | 'love' | 'dislike' | null

interface PaperActionBarProps {
  onDismiss?: () => void
  onQueue?: () => void
  onAdd?: () => void
  onLike?: () => void
  onLove?: () => void
  onDislike?: () => void
  /** Per-aspect toggle-off. When supplied, re-clicking an already-applied
   *  action undoes only that button's effect: Save → 'membership', Queue →
   *  'reading', the active reaction → 'rating'. Each removes the interaction
   *  AND the matching signal. */
  onUndo?: (aspect: 'membership' | 'rating' | 'reading') => void
  disabled?: boolean
  compact?: boolean
  dismissLabel?: string
  dismissTitle?: string
  dislikeLabel?: string
  dislikeTitle?: string
  /** Current reaction on the paper. like/love/dislike are mutually exclusive. */
  reaction?: PaperReaction
  /** Whether the paper is already saved to Library. Toggles Save → Saved. */
  isSaved?: boolean
  /** When saved, clicking Save removes the paper from Library (a true toggle).
   *  Only surfaces whose Save handler actually removes (e.g. Feed) set this;
   *  it switches the saved-state title from "Already saved" to "Remove from
   *  library" so the affordance stays truthful elsewhere. */
  savedClickRemoves?: boolean
  /** Whether the paper is already on the reading list. Toggles Queue → Queued. */
  isQueued?: boolean
  /** Explicit label-visibility override. When unset, compact hides labels. */
  showLabels?: boolean
}

/**
 * Per-tone resting and active styling, post-rebrand softening.
 *
 * Each reaction keeps a distinct hue (semantic at-a-glance), but the
 * active state is now a *soft tinted chip* rather than a saturated
 * solid fill — colored bg at the 50/100 step, darker text + icon at
 * the 700/800 step, with a 200-step border. Reads as a stamped /
 * highlighted index card, not a punch-in-the-eye SaaS button. Fits
 * the v2 paper-warm story where nothing on the page should compete
 * with content for attention.
 */
const toneClasses: Record<Tone, { icon: string; hover: string; active: string }> = {
  neutral: {
    icon: 'text-slate-500',
    hover: 'hover:bg-surface-2 hover:text-alma-900',
    active: 'border-[var(--color-border)] bg-surface-2 text-slate-800',
  },
  // Queue — violet (centralized in the palette, 44.5). Reading list is
  // pre-commit limbo: neither a library save nor a negative signal, so its
  // identity color sits outside the semantic state tokens.
  queue: ACTION_QUEUE_CLASSES,
  // Save — amber. Warm counterpoint to alma teal; reads as "bookmarked".
  add: {
    icon: 'text-warning-600',
    hover: 'hover:bg-warning-50 hover:text-warning-700',
    active: 'border-warning-100 bg-warning-50 text-warning-700',
  },
  like: {
    icon: 'text-success-600',
    hover: 'hover:bg-success-50 hover:text-success-700',
    active: 'border-success-100 bg-success-50 text-success-700',
  },
  love: {
    icon: 'text-critical-500',
    hover: 'hover:bg-critical-50 hover:text-critical-700',
    active: 'border-critical-100 bg-critical-50 text-critical-700',
  },
  // Dismiss / Remove — rose. Destructive intent but the active chip
  // stays soft to match the rest of the bar.
  dismiss: {
    icon: 'text-slate-500',
    hover: 'hover:bg-critical-50 hover:text-critical-700',
    active: 'border-critical-100 bg-critical-50 text-critical-700',
  },
  // Dislike — blue. Distinct from emerald like so the two thumbs
  // read clearly at a glance.
  dislike: {
    icon: 'text-info-600',
    hover: 'hover:bg-info-50 hover:text-info-700',
    active: 'border-info-100 bg-info-50 text-info-700',
  },
}

interface ActionButtonProps {
  icon: ComponentType<{ className?: string }>
  label: ReactNode
  tone: Tone
  compact: boolean
  disabled: boolean
  showLabel: boolean
  title: string
  onClick: () => void
  iconFilled?: boolean
  active?: boolean
}

function ActionButton({
  icon: Icon,
  label,
  tone,
  compact,
  disabled,
  showLabel,
  title,
  onClick,
  iconFilled = false,
  active = false,
}: ActionButtonProps) {
  const { icon: iconColor, hover, active: activeClass } = toneClasses[tone]
  return (
    <Button
      type="button"
      variant="ghost"
      onClick={onClick}
      disabled={disabled}
      title={title}
      aria-label={title}
      aria-pressed={active}
      className={cn(
        // Route through the Button primitive but keep the per-tone soft-chip
        // language: a modest 6px corner (bookish/index-card per the v2 brand)
        // and a soft hairline border throughout. The variant + tone classes
        // below override the ghost defaults so the toggle states stay intact.
        'gap-1.5 whitespace-nowrap rounded-md border font-medium',
        'focus-visible:ring-offset-1',
        'disabled:opacity-40',
        compact ? 'h-7 px-2.5 text-[11px]' : 'h-8 px-3 text-xs',
        active
          ? activeClass
          : cn('border-[var(--color-border)] bg-surface-1 text-alma-900', hover),
      )}
    >
      <Icon
        className={cn(
          'shrink-0',
          compact ? 'h-3.5 w-3.5' : 'h-4 w-4',
          // Active state keeps the colored icon (matches text), not white.
          // Soft tinted chip means we never need to invert the foreground.
          active ? 'text-current' : iconColor,
          iconFilled && 'fill-current',
        )}
      />
      {showLabel && <span className="leading-none">{label}</span>}
    </Button>
  )
}

export function PaperActionBar({
  onDismiss,
  onQueue,
  onAdd,
  onLike,
  onLove,
  onDislike,
  onUndo,
  disabled = false,
  compact = false,
  dismissLabel = 'Skip',
  dismissTitle = 'Dismiss — hide from discovery',
  dislikeLabel = 'Dislike',
  dislikeTitle = 'Negative signal — keeps the paper visible',
  reaction = null,
  isSaved = false,
  savedClickRemoves = false,
  isQueued = false,
  showLabels,
}: PaperActionBarProps) {
  const showLabel = showLabels ?? !compact
  const hasRemove = !!onDismiss
  const hasReactions = !!(onQueue || onAdd || onDislike || onLike || onLove)
  // Re-clicking an applied action toggles off only that button's effect via
  // `onUndo(aspect)` when a surface supplies it; otherwise it re-fires the
  // original handler.
  const click =
    (active: boolean, aspect: 'membership' | 'rating' | 'reading', handler?: () => void) =>
    () =>
      active && onUndo ? onUndo(aspect) : handler?.()
  const canUndo = !!onUndo

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {onDismiss && (
        <ActionButton
          icon={X}
          label={dismissLabel}
          tone="dismiss"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title={dismissTitle}
          onClick={onDismiss}
        />
      )}

      {hasRemove && hasReactions && <div className="mx-0.5 h-4 w-px bg-slate-200" aria-hidden />}

      {onQueue && (
        <ActionButton
          icon={isQueued ? BookOpenCheck : BookPlus}
          label={isQueued ? 'Queued' : 'Queue'}
          tone="queue"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title={isQueued ? 'Remove from reading list' : 'Add to reading list — decide later'}
          onClick={click(isQueued, 'reading', onQueue)}
          active={isQueued}
        />
      )}

      {onAdd && (
        <ActionButton
          icon={isSaved ? BookmarkCheck : Plus}
          label={isSaved ? 'Saved' : 'Save'}
          tone="add"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title={isSaved ? (savedClickRemoves || canUndo ? 'Remove from library' : 'Already saved to library') : 'Save to library'}
          onClick={click(isSaved, 'membership', onAdd)}
          iconFilled={isSaved}
          active={isSaved}
        />
      )}

      {onDislike && (
        <ActionButton
          icon={ThumbsDown}
          label={dislikeLabel}
          tone="dislike"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title={dislikeTitle}
          onClick={click(reaction === 'dislike', 'rating', onDislike)}
          active={reaction === 'dislike'}
        />
      )}

      {onLike && (
        <ActionButton
          icon={ThumbsUp}
          label="Like"
          tone="like"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title="Like — save to library with a positive signal"
          onClick={click(reaction === 'like', 'rating', onLike)}
          active={reaction === 'like'}
        />
      )}

      {onLove && (
        <ActionButton
          icon={Heart}
          label="Love"
          tone="love"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title="Love — save to library with a strong positive signal"
          onClick={click(reaction === 'love', 'rating', onLove)}
          // Heart fills only when actively loved; empty outline at rest.
          active={reaction === 'love'}
        />
      )}
    </div>
  )
}
