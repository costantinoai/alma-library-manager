import { Search, RotateCw, Tag, SlidersHorizontal, Sparkles, Wand2, Cloud } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import type { LabelMode, ColorBy, SizeBy } from './GraphPanel'
import type { GraphPhysicsConfig } from './ForceGraph'

interface ClusterSummary {
  id: number
  label: string
  topic_text?: string
  size: number
  avg_citations?: number
}

interface GraphControlsProps {
  searchQuery: string
  onSearchChange: (value: string) => void
  onRebuild: () => void
  isRebuilding?: boolean
  onExtraAction?: () => void
  extraActionLabel?: string
  isExtraActionPending?: boolean
  showLabelsToggle?: boolean
  showLabels?: boolean
  onToggleLabels?: () => void
  method?: string
  note?: string
  clusters?: ClusterSummary[]
  selectedClusterId?: number | null
  onClusterSelect?: (clusterId: number | null) => void
  isPaperMap?: boolean
  labelMode?: LabelMode
  onLabelModeChange?: (mode: LabelMode) => void
  colorBy?: ColorBy
  onColorByChange?: (mode: ColorBy) => void
  sizeBy?: SizeBy
  onSizeByChange?: (mode: SizeBy) => void
  showEdges?: boolean
  onShowEdgesChange?: (show: boolean) => void
  showTopics?: boolean
  onShowTopicsChange?: (show: boolean) => void
  showWordCloud?: boolean
  onShowWordCloudChange?: (show: boolean) => void
  includeCorpus?: boolean
  onIncludeCorpusChange?: (include: boolean) => void
  physics?: GraphPhysicsConfig
  onPhysicsChange?: (patch: Partial<GraphPhysicsConfig>) => void
  onResetPhysics?: () => void
  onRefreshLabels?: () => void
  isRefreshingLabels?: boolean
}

function SelectControl<T extends string>({
  label,
  value,
  onChange,
  options,
}: {
  label: string
  value: T
  onChange: (v: T) => void
  options: { value: T; label: string }[]
}) {
  return (
    <label className="flex items-center gap-1.5 text-xs text-slate-600">
      <span className="font-medium">{label}:</span>
      <Select value={String(value)} onValueChange={(v) => onChange(v as T)}>
        <SelectTrigger className="h-7 w-auto text-xs">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((option) => (
            <SelectItem key={String(option.value)} value={String(option.value)}>{option.label}</SelectItem>
          ))}
        </SelectContent>
      </Select>
    </label>
  )
}

function RangeControl({
  label,
  value,
  min,
  max,
  step,
  onChange,
}: {
  label: string
  value: number
  min: number
  max: number
  step: number
  onChange: (value: number) => void
}) {
  return (
    <label className="space-y-1 text-xs text-slate-600">
      <div className="flex items-center justify-between gap-2">
        <span className="font-medium">{label}</span>
        <span className="rounded bg-parchment-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-700">{value}</span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-alma-600"
      />
    </label>
  )
}

export function GraphControls({
  searchQuery,
  onSearchChange,
  onRebuild,
  isRebuilding,
  onExtraAction,
  extraActionLabel,
  isExtraActionPending,
  showLabelsToggle = true,
  showLabels,
  onToggleLabels,
  method,
  note,
  clusters,
  selectedClusterId,
  onClusterSelect,
  isPaperMap,
  labelMode = 'cluster',
  onLabelModeChange,
  colorBy = 'cluster',
  onColorByChange,
  sizeBy = 'citations',
  onSizeByChange,
  showEdges = true,
  onShowEdgesChange,
  showTopics = false,
  onShowTopicsChange,
  showWordCloud = false,
  onShowWordCloudChange,
  includeCorpus = false,
  onIncludeCorpusChange,
  physics,
  onPhysicsChange,
  onResetPhysics,
  onRefreshLabels,
  isRefreshingLabels,
}: GraphControlsProps) {
  const methodLabel =
    method === 'embeddings'
      ? 'AI Embeddings'
      : method === 'topic_cooccurrence'
        ? 'Topic Co-occurrence'
        : method === 'topics+keywords+metadata'
          ? 'Topics + Keywords + Metadata'
          : method === 'topic_similarity+shared_papers'
            ? 'Topic Similarity + Shared Papers'
            : method

  return (
    <div className="space-y-3">
      <div className="grid gap-3 lg:grid-cols-[1.5fr_1fr]">
        <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome/90 p-3 shadow-sm">
          <div className="flex flex-wrap items-center gap-2">
            <div className="relative min-w-[220px] flex-1">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
              <Input
                placeholder="Search nodes..."
                value={searchQuery}
                onChange={(e) => onSearchChange(e.target.value)}
                className="pl-9"
              />
            </div>

            {showLabelsToggle && onToggleLabels && (
              <Button
                variant={showLabels ? 'default' : 'outline'}
                size="sm"
                onClick={onToggleLabels}
              >
                <Tag className="mr-1 h-4 w-4" />
                Labels
              </Button>
            )}

            <Button
              variant="outline"
              size="sm"
              onClick={onRebuild}
              disabled={isRebuilding}
            >
              <RotateCw className={`mr-1 h-4 w-4 ${isRebuilding ? 'animate-spin' : ''}`} />
              Rebuild
            </Button>

            {onRefreshLabels && (
              <Button
                variant="outline"
                size="sm"
                onClick={onRefreshLabels}
                disabled={isRefreshingLabels}
                title="Recompute every cluster's TF-IDF top-terms label from its member titles + abstracts"
              >
                <Wand2 className={`mr-1 h-4 w-4 ${isRefreshingLabels ? 'animate-spin' : ''}`} />
                Relabel clusters
              </Button>
            )}

            {onExtraAction && extraActionLabel && (
              <Button
                variant="outline"
                size="sm"
                onClick={onExtraAction}
                disabled={isExtraActionPending}
              >
                <RotateCw className={`mr-1 h-4 w-4 ${isExtraActionPending ? 'animate-spin' : ''}`} />
                {extraActionLabel}
              </Button>
            )}
          </div>

          {isPaperMap && (
            <div className="mt-3 flex flex-wrap items-center gap-3">
              {onLabelModeChange && (
                <SelectControl
                  label="Labels"
                  value={labelMode}
                  onChange={onLabelModeChange}
                  options={[
                    { value: 'cluster', label: 'Cluster' },
                    { value: 'topic', label: 'Topics' },
                  ]}
                />
              )}
              {onColorByChange && (
                <SelectControl
                  label="Color"
                  value={colorBy}
                  onChange={onColorByChange}
                  options={[
                    { value: 'cluster', label: 'Cluster' },
                    { value: 'year', label: 'Year' },
                    { value: 'rating', label: 'Rating' },
                    { value: 'citations', label: 'Citations' },
                  ]}
                />
              )}
              {onSizeByChange && (
                <SelectControl
                  label="Size"
                  value={sizeBy}
                  onChange={onSizeByChange}
                  options={[
                    { value: 'citations', label: 'Citations' },
                    { value: 'rating', label: 'Rating' },
                    { value: 'uniform', label: 'Uniform' },
                  ]}
                />
              )}
              {onShowEdgesChange && (
                <label className="flex items-center gap-1.5 text-xs text-slate-600">
                  <Checkbox
                    checked={showEdges}
                    onCheckedChange={(checked) => onShowEdgesChange(checked === true)}
                  />
                  <span className="font-medium">Edges</span>
                </label>
              )}
              {onShowTopicsChange && (
                <label className="flex items-center gap-1.5 text-xs text-slate-600">
                  <Checkbox
                    checked={showTopics}
                    onCheckedChange={(checked) => onShowTopicsChange(checked === true)}
                  />
                  <span className="font-medium">Topic overlays</span>
                </label>
              )}
              {onShowWordCloudChange && (
                <label
                  className="flex items-center gap-1.5 text-xs text-slate-600"
                  title="Render a per-cluster word cloud above each centroid from member paper titles + abstracts"
                >
                  <Checkbox
                    checked={showWordCloud}
                    onCheckedChange={(checked) => onShowWordCloudChange(checked === true)}
                  />
                  <Cloud className="h-3 w-3 text-slate-500" />
                  <span className="font-medium">Word cloud</span>
                </label>
              )}
              {onIncludeCorpusChange && (
                <label className="flex items-center gap-1.5 text-xs text-slate-600" title="Include every stored paper (not just your Library) in the layout">
                  <Checkbox
                    checked={includeCorpus}
                    onCheckedChange={(checked) => onIncludeCorpusChange(checked === true)}
                  />
                  <span className="font-medium">Include full corpus</span>
                </label>
              )}
            </div>
          )}
          {!isPaperMap && onIncludeCorpusChange && (
            <div className="mt-3 flex flex-wrap items-center gap-3">
              <label className="flex items-center gap-1.5 text-xs text-slate-600" title="Include every stored paper (not just your Library) when computing author stats">
                <Checkbox
                  checked={includeCorpus}
                  onCheckedChange={(checked) => onIncludeCorpusChange(checked === true)}
                />
                <span className="font-medium">Include full corpus</span>
              </label>
            </div>
          )}

          {(method || note) && (
            <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-slate-500">
              {method && <span>Method: {methodLabel}</span>}
              {note && <Badge variant="outline" className="text-xs">{note}</Badge>}
            </div>
          )}
        </div>

        {physics && onPhysicsChange && (
          <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50/90 p-3 shadow-sm">
            <div className="mb-2 flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <SlidersHorizontal className="h-4 w-4 text-slate-500" />
                <div>
                  <p className="text-sm font-semibold text-alma-800">Physics</p>
                  <p className="text-xs text-slate-500">Tune pull, spacing, and motion.</p>
                </div>
              </div>
              {onResetPhysics && (
                <Button variant="ghost" size="sm" onClick={onResetPhysics}>
                  Reset
                </Button>
              )}
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <RangeControl label="Repulsion" value={physics.repulsion} min={-200} max={0} step={5} onChange={(value) => onPhysicsChange({ repulsion: value })} />
              <RangeControl label="Link Distance" value={physics.linkDistance} min={20} max={220} step={5} onChange={(value) => onPhysicsChange({ linkDistance: value })} />
              <RangeControl label="Link Attraction" value={physics.linkStrength} min={0} max={2} step={0.05} onChange={(value) => onPhysicsChange({ linkStrength: value })} />
              <RangeControl label="Velocity Decay" value={physics.velocityDecay} min={0.05} max={0.9} step={0.01} onChange={(value) => onPhysicsChange({ velocityDecay: value })} />
              <RangeControl label="Cooldown" value={physics.cooldownTicks} min={20} max={300} step={10} onChange={(value) => onPhysicsChange({ cooldownTicks: value })} />
              <RangeControl label="Node Scale" value={physics.nodeScale} min={0.6} max={2.5} step={0.05} onChange={(value) => onPhysicsChange({ nodeScale: value })} />
              <RangeControl label="Base Size" value={physics.baseSize} min={2} max={20} step={0.5} onChange={(value) => onPhysicsChange({ baseSize: value })} />
            </div>
          </div>
        )}
      </div>

      {clusters && clusters.length > 0 && (
        <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome/90 p-3 shadow-sm">
          <div className="mb-2 flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-slate-500" />
            <div>
              <p className="text-sm font-semibold text-alma-800">Cluster Studio</p>
              <p className="text-xs text-slate-500">Select a cluster to focus the map and inspect its papers.</p>
            </div>
          </div>
          {/* Cluster chips ride on `ToggleGroup type="single"` so exactly one
              cluster is selected at a time, with keyboard navigation + ARIA
              semantics inherited from Radix. The pill variant on Toggle
              shares its selected palette with `StatusBadge tone="accent"` —
              do not override `data-[state=on]` here. */}
          <ToggleGroup
            type="single"
            value={selectedClusterId != null ? String(selectedClusterId) : ''}
            onValueChange={(value) => {
              if (!onClusterSelect) return
              onClusterSelect(value === '' ? null : Number(value))
            }}
            className="flex flex-wrap justify-start gap-1.5"
          >
            {clusters.slice(0, 18).map((cluster) => {
              const badgeLabel = showTopics && cluster.topic_text ? cluster.topic_text : cluster.label
              return (
                <ToggleGroupItem
                  key={cluster.id}
                  value={String(cluster.id)}
                  variant="pill"
                  size="chip"
                  aria-label={`Focus cluster ${badgeLabel}`}
                >
                  <span>{badgeLabel}</span>
                  <span className="text-slate-500 data-[state=on]:text-alma-700/80 tabular-nums">({cluster.size})</span>
                </ToggleGroupItem>
              )
            })}
          </ToggleGroup>
        </div>
      )}
    </div>
  )
}
