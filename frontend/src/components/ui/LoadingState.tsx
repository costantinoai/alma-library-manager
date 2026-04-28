import { DotLoader } from '@/components/ui/dot-loader'

interface LoadingStateProps {
  message?: string
}

/**
 * Full-block loading state — page-level "we're fetching this surface"
 * placeholder. Centers itself vertically and horizontally in a tall
 * block so the loader sits in the middle of the page rather than
 * pinned to the top-left, and uses the brand serif for the caption so
 * the moment feels editorial rather than utilitarian.
 *
 * Layout: min-h tall enough to vertically center on the typical page
 * region (≈60% viewport height), full width so flex centering works
 * inside any parent column. Three gold dots pulse above a Merriweather
 * caption; on `prefers-reduced-motion` the dots dim instead of pulsing.
 *
 * Inline button spinners stay on `Loader2` (consistency with shadcn);
 * this primitive is reserved for stand-alone loading moments where
 * giving the page a beat before content lands feels right.
 */
export function LoadingState({ message = 'Loading…' }: LoadingStateProps) {
  return (
    <div
      role="status"
      aria-live="polite"
      aria-label={message}
      className="flex min-h-[60vh] w-full flex-col items-center justify-center gap-5 px-6 text-center"
    >
      <DotLoader size="lg" className="!py-0" aria-hidden />
      <p className="font-brand text-sm font-medium tracking-wide text-alma-700">
        {message}
      </p>
    </div>
  )
}
