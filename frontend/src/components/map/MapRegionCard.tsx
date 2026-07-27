/**
 * MapRegionCard — the ONE on-plate shell every map host drops after a lasso.
 *
 * Why it exists (user report 2026-07-26: "the selection tool does not seem to
 * work on the map"): Discovery pinned its region card INSIDE the plate, so it
 * appeared the instant the drag was released. The Map page wired the lasso
 * correctly but rendered its result as a Card BELOW a 560px plate — off-screen
 * at the moment of the gesture, which reads as nothing having happened. The
 * Authors map passed no lasso props at all.
 *
 * A selection is a transient, spatial act: its feedback belongs where the
 * gesture ended, not below the fold. This component owns that shell — position,
 * chrome, eyebrow, dismiss, and the pending/insufficient states — so no host can
 * drift from the others again. Hosts supply only the region's own meaning and
 * their own actions.
 */
import { Loader2, X } from 'lucide-react'

export interface MapRegionCardProps {
  /** Eyebrow label — the host's word for a region ("Direction", "Area"). */
  kind: string
  /** Eyebrow icon, in the host's vocabulary. */
  icon: React.ReactNode
  /** How many nodes the lasso caught — shown while characterising. */
  count: number
  /** Characterisation still in flight. */
  pending?: boolean
  /**
   * Region too small to say anything honest about. A map must not invent a
   * description for four dots.
   */
  insufficient?: boolean
  insufficientMessage?: string
  onClose: () => void
  /** Region meaning + host actions. Rendered only once ready. */
  children?: React.ReactNode
  /** Shared follow-up action, available even for a sparse selection. */
  actions?: React.ReactNode
}

export function MapRegionCard({
  kind,
  icon,
  count,
  pending = false,
  insufficient = false,
  insufficientMessage = 'Too few here to characterize — select a larger patch (5+).',
  onClose,
  children,
  actions,
}: MapRegionCardProps) {
  return (
    <div className="absolute right-3 top-3 z-20 w-72 rounded-sm border border-[var(--color-border)] bg-surface-2 p-3 shadow-paper-lg">
      <div className="mb-2 flex items-start justify-between gap-2">
        <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
          {icon}
          {kind}
        </div>
        <button
          type="button"
          onClick={onClose}
          className="rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
          aria-label="Cancel selection"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      {pending ? (
        <div className="flex items-center gap-2 py-3 text-xs text-slate-500">
          <Loader2 className="h-4 w-4 animate-spin text-alma-folio" />
          Characterizing {count}…
        </div>
      ) : insufficient ? (
        <p className="py-2 text-xs text-slate-500">{insufficientMessage}</p>
      ) : (
        children
      )}
      {actions && (
        <div className="mt-3 border-t border-[var(--color-border)] pt-2">
          {actions}
        </div>
      )}
    </div>
  )
}
