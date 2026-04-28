import { useRef, useState } from 'react'
import { Star, X } from 'lucide-react'
import { cn } from '@/lib/utils'

interface StarRatingProps {
  value: number
  onChange: (rating: number) => void
  size?: 'sm' | 'md'
}

export function StarRating({ value, onChange, size = 'sm' }: StarRatingProps) {
  const [hovered, setHovered] = useState(0)
  const [focusedStar, setFocusedStar] = useState(0)
  const groupRef = useRef<HTMLDivElement>(null)

  const iconSize = size === 'sm' ? 'h-4 w-4' : 'h-5 w-5'

  function handleKeyDown(e: React.KeyboardEvent): void {
    const current = focusedStar || value || 1

    if (e.key === 'ArrowRight' || e.key === 'ArrowUp') {
      e.preventDefault()
      const next = Math.min(current + 1, 5)
      setFocusedStar(next)
      onChange(next)
    } else if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') {
      e.preventDefault()
      const next = Math.max(current - 1, 1)
      setFocusedStar(next)
      onChange(next)
    }
  }

  return (
    <div
      ref={groupRef}
      className="inline-flex items-center gap-0.5"
      role="radiogroup"
      aria-label={`Rating: ${value} out of 5 stars`}
      tabIndex={0}
      onKeyDown={handleKeyDown}
      onFocus={() => {
        if (focusedStar === 0) setFocusedStar(value || 1)
      }}
      onBlur={() => setFocusedStar(0)}
    >
      {[1, 2, 3, 4, 5].map((i) => {
        const active = hovered > 0 ? i <= hovered : i <= value
        return (
          <button
            key={i}
            type="button"
            role="radio"
            aria-checked={i === value}
            aria-label={`${i} ${i === 1 ? 'star' : 'stars'}`}
            tabIndex={-1}
            onClick={() => onChange(i)}
            onMouseEnter={() => setHovered(i)}
            onMouseLeave={() => setHovered(0)}
            className="cursor-pointer rounded-sm p-0.5 transition-colors hover:bg-amber-50"
          >
            <Star
              className={cn(
                iconSize,
                active
                  ? 'fill-amber-400 text-amber-400'
                  : 'text-slate-300',
              )}
            />
          </button>
        )
      })}
      {value > 0 && (
        <button
          type="button"
          onClick={() => onChange(0)}
          className="ml-1 cursor-pointer rounded-sm p-0.5 text-slate-400 transition-colors hover:bg-red-50 hover:text-red-500"
          aria-label="Clear rating"
          tabIndex={-1}
        >
          <X className={iconSize} />
        </button>
      )}
    </div>
  )
}
