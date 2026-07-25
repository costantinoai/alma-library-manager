/**
 * Session-scoped map preferences.
 *
 * Map pages are intentionally unmounted on navigation so we never retain
 * several large canvases and spatial indices. Their small, serialisable view
 * state lives here instead: returning to a map restores the user's camera and
 * controls without turning page components into global singletons.
 */
import {
  useEffect,
  useState,
  type Dispatch,
  type SetStateAction,
} from 'react'

const MAP_SESSION_PREFIX = 'alma.map.v1'

export const PAPER_MAP_DEFAULTS = {
  scope: 'corpus' as const,
  resolution: 1.5,
  sizeScale: 1,
  dotOpacity: 1,
  wordScale: 1,
  wordCount: 3,
  blend: { sem: 1, coauth: 0, refs: 0, cocite: 0 },
}

export const AUTHOR_MAP_DEFAULTS = {
  scope: 'library' as const,
  resolution: 1,
  sizeScale: 1,
  dotOpacity: 1,
  wordScale: 1,
  wordCount: 3,
}

function storageKey(mapKey: string, field: string): string {
  return `${MAP_SESSION_PREFIX}.${encodeURIComponent(mapKey)}.${encodeURIComponent(field)}`
}

export function readMapSessionValue<T>(
  mapKey: string,
  field: string,
  fallback: T,
): T {
  if (typeof window === 'undefined') return fallback
  try {
    const raw = window.sessionStorage.getItem(storageKey(mapKey, field))
    return raw == null ? fallback : (JSON.parse(raw) as T)
  } catch {
    return fallback
  }
}

function writeMapSessionValue<T>(mapKey: string, field: string, value: T): void {
  if (typeof window === 'undefined') return
  try {
    window.sessionStorage.setItem(storageKey(mapKey, field), JSON.stringify(value))
  } catch {
    // Private mode, a full quota, or disabled storage must not break the map.
    // React state remains the source of truth for the current mount.
  }
}

/**
 * `useState`, backed by sessionStorage. Writes may be delayed for high-rate
 * state such as the map camera; ordinary controls persist immediately.
 */
export function useMapSessionState<T>(
  mapKey: string,
  field: string,
  fallback: T,
  options?: { writeDelayMs?: number; persist?: boolean },
): [T, Dispatch<SetStateAction<T>>] {
  const persist = options?.persist ?? true
  const [value, setValue] = useState<T>(() =>
    persist ? readMapSessionValue(mapKey, field, fallback) : fallback,
  )
  const writeDelayMs = options?.writeDelayMs ?? 0

  useEffect(() => {
    if (!persist) return
    if (writeDelayMs <= 0) {
      writeMapSessionValue(mapKey, field, value)
      return
    }
    const timer = window.setTimeout(
      () => writeMapSessionValue(mapKey, field, value),
      writeDelayMs,
    )
    return () => window.clearTimeout(timer)
  }, [field, mapKey, persist, value, writeDelayMs])

  return [value, setValue]
}

/** Set-valued companion that serialises as a compact JSON array. */
export function useMapSessionSet<T extends string | number>(
  mapKey: string,
  field: string,
): [Set<T>, Dispatch<SetStateAction<Set<T>>>] {
  const [values, setValues] = useMapSessionState<T[]>(mapKey, field, [])
  const [set, setSet] = useState<Set<T>>(() => new Set(values))

  useEffect(() => {
    setValues([...set])
  }, [set, setValues])

  return [set, setSet]
}
