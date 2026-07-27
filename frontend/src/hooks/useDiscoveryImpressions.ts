import { useCallback, useEffect, useRef } from 'react'

import {
  recordDiscoveryImpressions,
  type DiscoveryImpression,
} from '@/api/client'

const VISIBILITY_THRESHOLD = 0.5
const VISIBILITY_DWELL_MS = 500
const BATCH_DELAY_MS = 250

type ImpressionSurface = DiscoveryImpression['surface']

interface TargetMetadata {
  recommendation_id: string
  position: number
  surface: ImpressionSurface
  sort_mode: DiscoveryImpression['sort_mode']
}

/** Record first real viewport visibility; no render/view inference. */
export function useDiscoveryImpressions(
  surface: ImpressionSurface,
  sortMode: DiscoveryImpression['sort_mode'],
) {
  const observerRef = useRef<IntersectionObserver | null>(null)
  const metadataRef = useRef(new WeakMap<Element, TargetMetadata>())
  const elementsByIdRef = useRef(new Map<string, Element>())
  const dwellTimersRef = useRef(new Map<Element, number>())
  const observedIdsRef = useRef(new Set<string>())
  const pendingRef = useRef(new Map<string, DiscoveryImpression>())
  const flushTimerRef = useRef<number | null>(null)
  const flushRef = useRef<() => void>(() => undefined)
  const activeRef = useRef(true)

  const flush = useCallback(() => {
    if (flushTimerRef.current != null) {
      window.clearTimeout(flushTimerRef.current)
      flushTimerRef.current = null
    }
    const items = Array.from(pendingRef.current.values())
    if (items.length === 0) return
    pendingRef.current.clear()
    void recordDiscoveryImpressions(items).catch(() => {
      items.forEach((item) => pendingRef.current.set(item.recommendation_id, item))
      if (activeRef.current && flushTimerRef.current == null) {
        flushTimerRef.current = window.setTimeout(
          () => flushRef.current(),
          1_000,
        )
      }
    })
  }, [])
  flushRef.current = flush

  const enqueue = useCallback((item: DiscoveryImpression) => {
    if (observedIdsRef.current.has(item.recommendation_id)) return
    observedIdsRef.current.add(item.recommendation_id)
    pendingRef.current.set(item.recommendation_id, item)
    if (flushTimerRef.current == null) {
      flushTimerRef.current = window.setTimeout(flush, BATCH_DELAY_MS)
    }
  }, [flush])

  useEffect(() => {
    activeRef.current = true
    const dwellTimers = dwellTimersRef.current
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          const metadata = metadataRef.current.get(entry.target)
          if (!metadata) return
          if (entry.isIntersecting && entry.intersectionRatio >= VISIBILITY_THRESHOLD) {
            if (dwellTimers.has(entry.target)) return
            const timer = window.setTimeout(() => {
              dwellTimers.delete(entry.target)
              enqueue(metadataRef.current.get(entry.target) ?? metadata)
              observer.unobserve(entry.target)
            }, VISIBILITY_DWELL_MS)
            dwellTimers.set(entry.target, timer)
          } else {
            const timer = dwellTimers.get(entry.target)
            if (timer != null) window.clearTimeout(timer)
            dwellTimers.delete(entry.target)
          }
        })
      },
      { threshold: VISIBILITY_THRESHOLD },
    )
    observerRef.current = observer
    elementsByIdRef.current.forEach((element) => observer.observe(element))
    return () => {
      activeRef.current = false
      observer.disconnect()
      observerRef.current = null
      dwellTimers.forEach((timer) => window.clearTimeout(timer))
      dwellTimers.clear()
      flush()
    }
  }, [enqueue, flush, surface])

  return useCallback(
    (element: Element | null, recommendationId: string, position: number) => {
      const previous = elementsByIdRef.current.get(recommendationId)
      if (previous && previous !== element) {
        observerRef.current?.unobserve(previous)
        const timer = dwellTimersRef.current.get(previous)
        if (timer != null) window.clearTimeout(timer)
        dwellTimersRef.current.delete(previous)
        metadataRef.current.delete(previous)
        elementsByIdRef.current.delete(recommendationId)
      }
      if (!element || observedIdsRef.current.has(recommendationId)) return
      elementsByIdRef.current.set(recommendationId, element)
      metadataRef.current.set(element, {
        recommendation_id: recommendationId,
        position,
        surface,
        sort_mode: sortMode,
      })
      observerRef.current?.observe(element)
    },
    [sortMode, surface],
  )
}
