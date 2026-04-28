import { BookOpenCheck, BookPlus, BookmarkCheck, Heart, Plus, ThumbsDown, ThumbsUp, X } from 'lucide-react'
import type { ComponentType, ReactNode } from 'react'

import { cn } from '@/lib/utils'

type Tone = 'neutral' | 'queue' | 'add' | 'like' | 'love' | 'dismiss' | 'dislike'

export type PaperReaction = 'like' | 'love' | 'dislike' | null

interface PaperActionBarProps {
  onDismiss?: () => void
  onQueue?: () => void
  onAdd?: () => void
  onLike?: () => void
  onLove?: () => void
  onDislike?: () => void
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
    hover: 'hover:bg-parchment-100 hover:text-alma-900',
    active: 'border-[var(--color-border)] bg-parchment-100 text-slate-800',
  },
  // Queue — violet. Reading list is pre-commit limbo: neither a
  // library save nor a negative signal. Violet keeps it visually
  // separate from amber Save and emerald Like.
  queue: {
    icon: 'text-violet-600',
    hover: 'hover:bg-violet-50 hover:text-violet-800',
    active: 'border-violet-200 bg-violet-50 text-violet-800',
  },
  // Save — amber. Warm counterpoint to alma teal; reads as "bookmarked".
  add: {
    icon: 'text-amber-600',
    hover: 'hover:bg-amber-50 hover:text-amber-800',
    active: 'border-amber-200 bg-amber-50 text-amber-800',
  },
  like: {
    icon: 'text-emerald-600',
    hover: 'hover:bg-emerald-50 hover:text-emerald-800',
    active: 'border-emerald-200 bg-emerald-50 text-emerald-800',
  },
  love: {
    icon: 'text-rose-500',
    hover: 'hover:bg-rose-50 hover:text-rose-700',
    active: 'border-rose-200 bg-rose-50 text-rose-700',
  },
  // Dismiss / Remove — rose. Destructive intent but the active chip
  // stays soft to match the rest of the bar.
  dismiss: {
    icon: 'text-slate-500',
    hover: 'hover:bg-rose-50 hover:text-rose-700',
    active: 'border-rose-200 bg-rose-50 text-rose-700',
  },
  // Dislike — blue. Distinct from emerald like so the two thumbs
  // read clearly at a glance.
  dislike: {
    icon: 'text-blue-600',
    hover: 'hover:bg-blue-50 hover:text-blue-800',
    active: 'border-blue-200 bg-blue-50 text-blue-800',
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
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={title}
      aria-label={title}
      aria-pressed={active}
      className={cn(
        // Drop the rounded-full pill — modest 6px corner reads bookish/
        // index-card per the v2 brand language. Soft hairline border
        // throughout.
        'inline-flex items-center gap-1.5 whitespace-nowrap rounded-md border font-medium transition-colors duration-150',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-1',
        'disabled:pointer-events-none disabled:opacity-40',
        compact ? 'h-7 px-2.5 text-[11px]' : 'h-8 px-3 text-xs',
        active
          ? activeClass
          : cn('border-[var(--color-border)] bg-alma-chrome text-alma-900', hover),
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
    </button>
  )
}

export function PaperActionBar({
  onDismiss,
  onQueue,
  onAdd,
  onLike,
  onLove,
  onDislike,
  disabled = false,
  compact = false,
  dismissLabel = 'Skip',
  dismissTitle = 'Dismiss — hide from discovery',
  dislikeLabel = 'Dislike',
  dislikeTitle = 'Negative signal — keeps the paper visible',
  reaction = null,
  isSaved = false,
  isQueued = false,
  showLabels,
}: PaperActionBarProps) {
  const showLabel = showLabels ?? !compact
  const hasRemove = !!onDismiss
  const hasReactions = !!(onQueue || onAdd || onDislike || onLike || onLove)

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
          onClick={onQueue}
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
          title={isSaved ? 'Already saved to library' : 'Save to library'}
          onClick={onAdd}
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
          onClick={onDislike}
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
          onClick={onLike}
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
          onClick={onLove}
          // Heart fills only when actively loved; empty outline at rest.
          active={reaction === 'love'}
        />
      )}
    </div>
  )
}
