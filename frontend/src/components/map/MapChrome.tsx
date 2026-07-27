/**
 * MapChrome — the ONE toolbar / legend / toggle vocabulary for every map
 * host (task 50-K). Discovery frontier, the Map page, and the Authors
 * network all mount THESE shells, so a user landing on any map recognises
 * every control from the last map they used:
 *
 *   - `MapToolbar`   — the bar ABOVE the plate (never buttons floating on it)
 *   - `MapToggle`    — the one pill-toggle idiom (accent = on)
 *   - `MapLegend`    — the section BELOW the plate
 *   - `ClusterLegendChips` — cluster chips as VIEW-ONLY dim toggles
 *     (dimmed = 15% opacity, never hidden, never a discovery signal)
 */
import { useEffect, useState } from 'react'
import { Loader2, Settings2 } from 'lucide-react'

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Slider } from '@/components/ui/slider'
import { cn } from '@/lib/utils'

export function MapToolbar({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-[var(--color-border)] bg-surface-2 px-3 py-2">
      {children}
    </div>
  )
}

/** Honest transient transport state: a refresh keeps the current plate,
 * while a build means the server is computing a durable layout artifact. */
export function MapDataStatus({
  phase,
}: {
  phase: 'idle' | 'refreshing' | 'building'
}) {
  if (phase === 'idle') return null
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-sm border border-control-edge bg-control-quiet px-2 py-1 text-[11px] text-slate-500"
      role="status"
      aria-live="polite"
    >
      <Loader2 className="h-3 w-3 animate-spin text-alma-folio" />
      {phase === 'building' ? 'Building layout' : 'Refreshing data'}
    </span>
  )
}

export function MapToggle({
  active,
  onClick,
  title,
  children,
}: {
  active?: boolean
  onClick: () => void
  title?: string
  children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      title={title}
      className={cn(
        'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
        active
          ? 'border-accent-edge bg-accent-soft text-alma-folio'
          : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
      )}
    >
      {children}
    </button>
  )
}

/** The 47-H style either/or switch (one grouping at a time, etc.). */
export function MapModeSwitch<T extends string>({
  value,
  options,
  onChange,
}: {
  value: T
  options: Array<{ value: T; label: string; title?: string }>
  onChange: (value: T) => void
}) {
  return (
    <div className="inline-flex overflow-hidden rounded-sm border border-[var(--color-border)]">
      {options.map((opt) => (
        <button
          key={opt.value}
          type="button"
          onClick={() => onChange(opt.value)}
          title={opt.title}
          className={cn(
            'px-2 py-1 text-xs font-medium transition-colors',
            value === opt.value
              ? 'bg-accent-soft text-alma-folio'
              : 'bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}

export function MapLegend({ children }: { children: React.ReactNode }) {
  return (
    <div className="border-t border-[var(--color-border)] bg-surface-2 px-3 py-2.5 text-xs">
      {children}
    </div>
  )
}

export interface ClusterChipEntry {
  id: number
  label: string
  count: number
  color: string
}

/** Cluster chips as view-only dim toggles — identical on every host. */
export function ClusterLegendChips({
  clusters,
  dimmed,
  onToggle,
  limit = 8,
}: {
  clusters: ClusterChipEntry[]
  dimmed: ReadonlySet<number>
  onToggle: (id: number) => void
  limit?: number
}) {
  if (clusters.length === 0) return null
  return (
    <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
      {clusters.slice(0, limit).map((c) => {
        const isDim = dimmed.has(c.id)
        return (
          <button
            key={c.id}
            type="button"
            onClick={() => onToggle(c.id)}
            aria-pressed={!isDim}
            className={cn(
              'inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-control-edge bg-control-quiet px-1.5 py-0.5 text-slate-600 transition-opacity hover:bg-control-quiet-hover',
              isDim && 'opacity-40',
            )}
            title={isDim ? `Show "${c.label}" (${c.count})` : `Dim "${c.label}" (${c.count})`}
          >
            <span className="inline-block h-2 w-2 shrink-0 rounded-full" style={{ background: c.color }} />
            <span className={cn('truncate', isDim && 'line-through')}>{c.label}</span>
            <span className="shrink-0 text-slate-400">· {c.count}</span>
          </button>
        )
      })}
    </div>
  )
}


/**
 * SliderRow — the one labelled-slider idiom inside the tuning popover.
 * Local state while dragging; the host's knob commits on release.
 */
export function SliderRow({
  label,
  value,
  min,
  max,
  step,
  format,
  onCommit,
}: {
  label: string
  value: number
  min: number
  max: number
  step: number
  format: (v: number) => string
  onCommit: (v: number) => void
}) {
  const [local, setLocal] = useState(value)
  useEffect(() => setLocal(value), [value])

  return (
    <div>
      <div className="mb-1 flex items-center justify-between">
        <span className="text-slate-600">{label}</span>
        <span className="tabular-nums text-slate-400">{format(local)}</span>
      </div>
      <Slider
        value={[local]}
        min={min}
        max={max}
        step={step}
        onValueChange={([v]) => setLocal(v)}
        onValueCommit={([v]) => onCommit(v)}
      />
    </div>
  )
}

/**
 * MapTuningPopover — the ONE "Advanced" affordance every map host mounts
 * (user call 2026-07-25: the tuning knobs belong to every map, not just the
 * Map page). The shell is shared; hosts compose SliderRow /
 * MapDisplayTuningRows + their own extras (layout blend, rebuild buttons)
 * as children, so the same knob reads identically on every plate.
 */
export function MapTuningPopover({
  children,
  title = 'Fine tuning — terrain opacity, dot size, dot opacity, word size, words per cluster',
}: {
  children: React.ReactNode
  title?: string
}) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="inline-flex items-center gap-1.5 rounded-sm border border-control-edge bg-control-well px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-control-quiet"
          title={title}
        >
          <Settings2 className="h-3.5 w-3.5" />
          Advanced
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 space-y-4 text-xs">
        {children}
      </PopoverContent>
    </Popover>
  )
}

/** Display knobs every map shares: terrain/dot opacity, dot/word size, words.
 * One component so ranges and labels cannot drift per host. */
export function MapDisplayTuningRows({
  sizeScale,
  onSizeScale,
  dotOpacity,
  onDotOpacity,
  terrainOpacity,
  onTerrainOpacity,
  wordScale,
  onWordScale,
  wordCount,
  onWordCount,
}: {
  sizeScale: number
  onSizeScale: (v: number) => void
  dotOpacity: number
  onDotOpacity: (v: number) => void
  terrainOpacity: number
  onTerrainOpacity: (v: number) => void
  wordScale: number
  onWordScale: (v: number) => void
  wordCount: number
  onWordCount: (v: number) => void
}) {
  return (
    <>
      <SliderRow
        label="Dot size"
        value={sizeScale}
        min={0.6}
        max={2}
        step={0.1}
        format={(v) => `${v.toFixed(1)}×`}
        onCommit={onSizeScale}
      />
      <SliderRow
        label="Dot opacity"
        value={dotOpacity}
        min={0.2}
        max={1}
        step={0.05}
        format={(v) => `${Math.round(v * 100)}%`}
        onCommit={onDotOpacity}
      />
      <SliderRow
        label="Terrain opacity"
        value={terrainOpacity}
        min={0.1}
        max={1}
        step={0.05}
        format={(v) => `${Math.round(v * 100)}%`}
        onCommit={onTerrainOpacity}
      />
      <SliderRow
        label="Word size"
        value={wordScale}
        min={0.6}
        max={2}
        step={0.1}
        format={(v) => `${v.toFixed(1)}×`}
        onCommit={onWordScale}
      />
      <SliderRow
        label="Words per cluster"
        value={wordCount}
        min={1}
        max={3}
        step={1}
        format={(v) => String(v)}
        onCommit={onWordCount}
      />
    </>
  )
}

/**
 * ColourBarLegend — every colour ramp announces its scale: the gradient the
 * dots actually use, its min / (optional centre) / max, and the mean of the
 * plotted values. Divergent ramps pass their true centre (0 valence,
 * 3★ rating) so the midpoint label is a MEANING, not a coincidence.
 */
export function ColourBarLegend({
  gradient,
  min,
  mid,
  max,
  mean,
}: {
  gradient: string
  min: string
  mid?: string
  max: string
  mean?: string
}) {
  return (
    <span className="inline-flex items-center gap-2 text-[11px] text-slate-500">
      <span className="inline-flex flex-col">
        <span className="h-2 w-28 rounded-full" style={{ background: gradient }} />
        <span className="mt-0.5 flex justify-between tabular-nums text-slate-400">
          <span>{min}</span>
          {mid != null && <span>{mid}</span>}
          <span>{max}</span>
        </span>
      </span>
      {mean != null && <span className="tabular-nums">mean {mean}</span>}
    </span>
  )
}
