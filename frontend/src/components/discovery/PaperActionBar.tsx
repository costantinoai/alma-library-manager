import { BookOpenCheck, BookPlus, BookmarkCheck, FolderPlus, Heart, Plus, ThumbsDown, ThumbsUp, X } from 'lucide-react'
import type { ComponentType, ReactNode } from 'react'

import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

type Tone = 'neutral' | 'queue' | 'add' | 'collection' | 'like' | 'love' | 'dismiss' | 'dislike'

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
  /** Label shown when `isSaved` (default "Saved"). A collection lens sets
   *  "In library" for a paper that is in the Library but not in this collection. */
  savedLabel?: string
  /** When saved, make the Save button a passive indicator (no remove-on-click).
   *  Used on Discovery collection-lens cards: an already-in-Library paper shows a
   *  checked "In library" state, but the destructive remove lives in the Library,
   *  not the discovery feed. */
  savedReadOnly?: boolean
  /** Distinct "Add to collection" action (folio-accent, folder icon), separate
   *  from Save. A collection lens uses it to file the paper into the linked
   *  collection — including papers already in the Library from another collection. */
  onAddToCollection?: () => void
  addToCollectionLabel?: string
  addToCollectionTitle?: string
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
  collectionAction?: ReactNode
}

/**
 * Per-tone icon / hover / active styling.
 *
 * COLOUR IS VALENCE, ICON IS ACTION — the same two-channel rule the chips
 * follow (CLAUDE.md → "Chips & pills"). Four groups, not six identity hues:
 *
 *   critical  arguing AGAINST  — Skip, Dislike
 *   neutral   no valence       — Queue (deferred; neither a save nor a signal)
 *   accent    commit to library— Save, Add to collection
 *   success   YOUR positive feedback — Like, Love
 *
 * The previous map spent a distinct hue on every button, which left colour
 * unable to answer the only question that matters at triage speed. Worse, it
 * pointed the wrong way: Save was amber (warning = "proceed with care"), Love
 * was critical red (= destructive), and Dislike was info blue (= neutral
 * reference). Two of the three positive actions read as caution or danger.
 *
 * Within a group the ICON disambiguates (✕ vs 👎, ＋ vs 📁), and Love outranks
 * Like by filling its heart. Active states use the shared chip wash
 * (`hue-700 @ 10%` + `hue-800` text), so an applied action has exactly the
 * weight of the pills above it rather than the heavier retired `-50/-100`
 * tint pair.
 */
const toneClasses: Record<Tone, { icon: string; hover: string; active: string }> = {
  neutral: {
    icon: 'text-slate-500',
    hover: 'hover:bg-control-quiet-hover hover:text-alma-900',
    active: 'border-transparent bg-control-track text-alma-900',
  },
  // Queue — deferred. Reading list is pre-commit limbo: neither a library
  // save nor a signal, so it carries no valence colour at all.
  queue: {
    icon: 'text-slate-500',
    hover: 'hover:bg-control-quiet-hover hover:text-alma-900',
    active: 'border-transparent bg-control-track text-alma-900',
  },
  // Save — the primary affirmative: this paper joins the library. Accent
  // (folio) is the app's single interactive identity.
  add: {
    icon: 'text-alma-folio',
    hover: 'hover:bg-accent-soft hover:text-alma-folio',
    active: 'border-transparent bg-accent-soft text-alma-folio',
  },
  // Add to collection — the same commit family; the folder icon separates it.
  collection: {
    icon: 'text-alma-folio',
    hover: 'hover:bg-accent-soft hover:text-alma-folio',
    active: 'border-transparent bg-accent-soft text-alma-folio',
  },
  like: {
    icon: 'text-success-600',
    hover: 'hover:bg-success-700/10 hover:text-success-800',
    active: 'border-transparent bg-success-700/10 text-success-800',
  },
  // Love — same valence as Like, one step stronger. The filled heart carries
  // the difference; a second hue would have to lie about the direction.
  love: {
    icon: 'text-success-600',
    hover: 'hover:bg-success-700/10 hover:text-success-800',
    active: 'border-transparent bg-success-700/15 text-success-800',
  },
  // Dismiss / Skip — negative.
  dismiss: {
    icon: 'text-slate-500',
    hover: 'hover:bg-critical-700/10 hover:text-critical-700',
    active: 'border-transparent bg-critical-700/10 text-critical-700',
  },
  // Dislike — negative, same as Skip. The thumb icon says which one.
  dislike: {
    icon: 'text-slate-500',
    hover: 'hover:bg-critical-700/10 hover:text-critical-700',
    active: 'border-transparent bg-critical-700/10 text-critical-700',
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
        // Route through the Button primitive, keeping the per-tone chip
        // language. Shape matches every other button in the app: `rounded-sm`
        // letterpress corner, control hairline, ink well at rest.
        'gap-1.5 whitespace-nowrap rounded-sm border font-medium',
        'focus-visible:ring-offset-1',
        'disabled:opacity-40',
        compact ? 'h-7 px-2.5 text-[11px]' : 'h-8 px-3 text-xs',
        active
          ? activeClass
          : cn('border-control-edge bg-control-well text-alma-900', hover),
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
  savedLabel,
  savedReadOnly = false,
  onAddToCollection,
  addToCollectionLabel = 'Add to collection',
  addToCollectionTitle = 'Add to this collection',
  reaction = null,
  isSaved = false,
  savedClickRemoves = false,
  isQueued = false,
  showLabels,
  collectionAction,
}: PaperActionBarProps) {
  const showLabel = showLabels ?? !compact
  const hasRemove = !!onDismiss
  const hasReactions = !!(onQueue || onAdd || onAddToCollection || collectionAction || onDislike || onLike || onLove)
  // Re-clicking an applied action toggles off only that button's effect via
  // `onUndo(aspect)` when a surface supplies it; otherwise it re-fires the
  // original handler.
  const click =
    (active: boolean, aspect: 'membership' | 'rating' | 'reading', handler?: () => void) =>
    () =>
      active && onUndo ? onUndo(aspect) : handler?.()
  const canUndo = !!onUndo

  return (
    <div className="flex flex-wrap items-center gap-1.5" data-testid="paper-actions">
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

      {hasRemove && hasReactions && <div className="mx-0.5 h-4 w-px bg-control-edge" aria-hidden />}

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
          label={isSaved ? (savedLabel ?? 'Saved') : 'Save'}
          tone="add"
          compact={compact}
          // Read-only saved state: a passive "in library" indicator, not a
          // click target (the destructive remove lives in the Library).
          disabled={disabled || (isSaved && savedReadOnly)}
          showLabel={showLabel}
          title={
            isSaved
              ? (savedReadOnly
                  ? 'In your library'
                  : (savedClickRemoves || canUndo ? 'Remove from library' : 'Already saved to library'))
              : 'Save to library'
          }
          onClick={click(isSaved, 'membership', onAdd)}
          iconFilled={isSaved}
          active={isSaved}
        />
      )}

      {onAddToCollection && (
        <ActionButton
          icon={FolderPlus}
          label={addToCollectionLabel}
          tone="collection"
          compact={compact}
          disabled={disabled}
          showLabel={showLabel}
          title={addToCollectionTitle}
          onClick={onAddToCollection}
        />
      )}

      {collectionAction}

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
