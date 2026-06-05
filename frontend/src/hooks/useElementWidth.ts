import { useEffect, useRef, useState, type RefObject } from 'react'

/**
 * Observe an element's content width via ResizeObserver.
 *
 * Returns `[ref, width]` — attach `ref` to the element to measure; `width`
 * is `null` until the first observation lands (ResizeObserver fires once
 * immediately on observe, so that's within a frame of mount).
 *
 * Use this when layout must key off the CONTAINER, not the viewport —
 * Tailwind breakpoints can't see a fixed-width modal or a sidebar-squeezed
 * panel, a ResizeObserver can. First consumer: the Suggested Authors rail,
 * which derives its column count from the measured width so cards never
 * compress below their minimum size.
 */
export function useElementWidth<T extends HTMLElement>(): [RefObject<T | null>, number | null] {
  const ref = useRef<T | null>(null)
  const [width, setWidth] = useState<number | null>(null)

  useEffect(() => {
    const el = ref.current
    if (!el) return
    const observer = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width
      if (typeof w === 'number') setWidth(w)
    })
    observer.observe(el)
    return () => observer.disconnect()
  }, [])

  return [ref, width]
}
