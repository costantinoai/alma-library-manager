import { Search, RotateCw, Tag, SlidersHorizontal, Sparkles, Wand2, Cloud } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import type { LabelMode, ColorBy, SizeBy, AuthorColorBy, AuthorSizeBy } from './GraphPanel'
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
  showWordCloud?: boolean
  onShowWordCloudChange?: (show: boolean) => void
  showClusterLabels?: boolean
  onShowClusterLabelsChange?: (show: boolean) => void
  wordCloudDensity?: number
  onWordCloudDensityChange?: (value: number) => void
  wordCloudSize?: number
  onWordCloudSizeChange?: (value: number) => void
  includeCorpus?: boolean
  onIncludeCorpusChange?: (include: boolean) => void
  clusterResolution?: number
  onClusterResolutionChange?: (value: number) => void
  authorColorBy?: AuthorColorBy
  onAuthorColorByChange?: (mode: AuthorColorBy) => void
  authorSizeBy?: AuthorSizeBy
  onAuthorSizeByChange?: (mode: AuthorSizeBy) => void
  // PROTOTYPE (task 19): fused-layout weights — INDEPENDENT 0..1 per source.
  layoutSemanticWeight?: number
  onLayoutSemanticWeightChange?: (value: number) => void
  layoutCoauthWeight?: number
  onLayoutCoauthWeightChange?: (value: number) => void
  layoutBibWeight?: number
  onLayoutBibWeightChange?: (value: number) => void
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
  hint,
}: {
  label: string
  value: number
  min: number
  max: number
  step: number
  onChange: (value: number) => void
  hint?: string
}) {
  return (
    <label className="space-y-1 text-xs text-slate-600" title={hint}>
      <div className="flex items-center justify-between gap-2">
        <span className="font-medium">{label}</span>
        <span className="rounded bg-surface-2 px-1.5 py-0.5 font-mono text-[10px] text-slate-700">{value}</span>
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
  showWordCloud = false,
  onShowWordCloudChange,
  showClusterLabels = false,
  onShowClusterLabelsChange,
  wordCloudDensity = 1,
  onWordCloudDensityChange,
  wordCloudSize = 1,
  onWordCloudSizeChange,
  includeCorpus = false,
  onIncludeCorpusChange,
  clusterResolution = 1.0,
  onClusterResolutionChange,
  authorColorBy = 'cluster',
  onAuthorColorByChange,
  authorSizeBy = 'publications',
  onAuthorSizeByChange,
  layoutSemanticWeight = 1,
  onLayoutSemanticWeightChange,
  layoutCoauthWeight = 0,
  onLayoutCoauthWeightChange,
  layoutBibWeight = 0,
  onLayoutBibWeightChange,
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
        <div className="rounded-sm border border-[var(--color-border)] bg-surface-1/90 p-3 shadow-sm">
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
                <label
                  className="flex items-center gap-1.5 text-xs text-slate-600"
                  title="Draw the typed relationship edges (semantic / shared-references / shared-authors). Use the layer chips above the map to filter which kinds show."
                >
                  <Checkbox
                    checked={showEdges}
                    onCheckedChange={(checked) => onShowEdgesChange(checked === true)}
                  />
                  <span className="font-medium">Edges</span>
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
              {onClusterResolutionChange && (
                <label
                  className="flex items-center gap-2 text-xs text-slate-600"
                  title="Cluster detail. Higher splits the map into more, finer clusters; lower merges into fewer, broader ones. Non-default values re-cluster live (uncached)."
                >
                  <span className="font-medium whitespace-nowrap">Cluster detail</span>
                  <input
                    type="range"
                    min={0.5}
                    max={3}
                    step={0.25}
                    value={clusterResolution}
                    onChange={(e) => onClusterResolutionChange(Number(e.target.value))}
                    className="w-28 accent-alma-600"
                  />
                  <span className="rounded bg-surface-2 px-1.5 py-0.5 font-mono text-[10px] text-slate-700 tabular-nums">
                    {clusterResolution.toFixed(2)}×
                  </span>
                </label>
              )}
            </div>
          )}

          {/* PROTOTYPE (task 19): fused multi-view layout. Library scope only
              (dense O(N²)) — both the paper map AND the author network. */}
          {!includeCorpus && onLayoutSemanticWeightChange && onLayoutCoauthWeightChange && onLayoutBibWeightChange && (
            <div className="mt-3 rounded-sm border border-dashed border-edge-2 bg-surface-2 p-3">
              <div className="mb-1 flex items-center gap-2">
                <span className="text-xs font-semibold text-alma-800">Layout basis</span>
                <Badge variant="outline" className="text-[10px]">beta</Badge>
              </div>
              <p className="mb-2 text-[11px] text-slate-500">
                Blend what drives the <em>positions</em>. Each weight is independent (they
                don&apos;t need to add up) — all three are used together. The default
                (semantic 1, others 0) is the trustworthy similarity map; raise a weight to
                also pull co-authoring or reference-sharing nodes together. Clusters stay
                semantic.
              </p>
              <div className="flex flex-wrap items-center gap-4">
                {(
                  [
                    ['Semantic', layoutSemanticWeight, onLayoutSemanticWeightChange] as const,
                    ['Co-authorship', layoutCoauthWeight, onLayoutCoauthWeightChange] as const,
                    ['Bib. coupling', layoutBibWeight, onLayoutBibWeightChange] as const,
                  ]
                ).map(([label, value, onChange]) => (
                  <label key={label} className="flex items-center gap-2 text-xs text-slate-600">
                    <span className="font-medium whitespace-nowrap">{label}</span>
                    <input
                      type="range"
                      min={0}
                      max={1}
                      step={0.25}
                      value={value}
                      onChange={(e) => onChange(Number(e.target.value))}
                      className="w-24 accent-alma-600"
                    />
                    <span className="font-mono text-[10px] tabular-nums text-slate-500">
                      {value.toFixed(2)}
                    </span>
                  </label>
                ))}
                {(layoutCoauthWeight > 0 || layoutBibWeight > 0 || layoutSemanticWeight !== 1) && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      onLayoutSemanticWeightChange(1)
                      onLayoutCoauthWeightChange(0)
                      onLayoutBibWeightChange(0)
                    }}
                  >
                    Reset to semantic
                  </Button>
                )}
              </div>
            </div>
          )}

          {!isPaperMap && (
            <div className="mt-3 flex flex-wrap items-center gap-3">
              {/* Author-appropriate encodings (parity with the paper map, but over
                  author attributes — productivity / h-index / citations). */}
              {onAuthorColorByChange && (
                <SelectControl
                  label="Color"
                  value={authorColorBy}
                  onChange={onAuthorColorByChange}
                  options={[
                    { value: 'cluster', label: 'Cluster' },
                    { value: 'citations', label: 'Citations' },
                    { value: 'h_index', label: 'h-index' },
                    { value: 'publications', label: 'Publications' },
                  ]}
                />
              )}
              {onAuthorSizeByChange && (
                <SelectControl
                  label="Size"
                  value={authorSizeBy}
                  onChange={onAuthorSizeByChange}
                  options={[
                    { value: 'publications', label: 'Publications' },
                    { value: 'citations', label: 'Citations' },
                    { value: 'h_index', label: 'h-index' },
                    { value: 'uniform', label: 'Uniform' },
                  ]}
                />
              )}
              {onShowEdgesChange && (
                <label
                  className="flex items-center gap-1.5 text-xs text-slate-600"
                  title="Draw the typed relationship edges (semantic / shared-references / co-authorship). Use the layer chips above the map to filter which kinds show."
                >
                  <Checkbox
                    checked={showEdges}
                    onCheckedChange={(checked) => onShowEdgesChange(checked === true)}
                  />
                  <span className="font-medium">Edges</span>
                </label>
              )}
              {onShowWordCloudChange && (
                <label
                  className="flex items-center gap-1.5 text-xs text-slate-600"
                  title="Render a per-cluster word cloud above each centroid from the cluster's authors' topic terms"
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
                <label className="flex items-center gap-1.5 text-xs text-slate-600" title="Include every stored paper (not just your Library) when computing author stats + the neighbourhood">
                  <Checkbox
                    checked={includeCorpus}
                    onCheckedChange={(checked) => onIncludeCorpusChange(checked === true)}
                  />
                  <span className="font-medium">Include full corpus</span>
                </label>
              )}
              {onClusterResolutionChange && (
                <label
                  className="flex items-center gap-2 text-xs text-slate-600"
                  title="Cluster detail. Higher splits the network into more, finer author communities; lower merges into fewer, broader ones. Non-default values re-cluster live (uncached)."
                >
                  <span className="font-medium whitespace-nowrap">Cluster detail</span>
                  <input
                    type="range"
                    min={0.5}
                    max={3}
                    step={0.25}
                    value={clusterResolution}
                    onChange={(e) => onClusterResolutionChange(Number(e.target.value))}
                    className="w-28 accent-alma-600"
                  />
                  <span className="rounded bg-surface-2 px-1.5 py-0.5 font-mono text-[10px] text-slate-700 tabular-nums">
                    {clusterResolution.toFixed(2)}×
                  </span>
                </label>
              )}
            </div>
          )}

          {/* Shared overlay controls (both views): cluster-name labels + word-cloud
              density/size — only meaningful when the word cloud is on. */}
          <div className="mt-3 flex flex-wrap items-center gap-3">
            {onShowClusterLabelsChange && (
              <label
                className="flex items-center gap-1.5 text-xs text-slate-600"
                title="Show each cluster's name at its centre."
              >
                <Checkbox
                  checked={showClusterLabels}
                  onCheckedChange={(checked) => onShowClusterLabelsChange(checked === true)}
                />
                <span className="font-medium">Cluster labels</span>
              </label>
            )}
            {showWordCloud && onWordCloudDensityChange && (
              <label
                className="flex items-center gap-2 text-xs text-slate-600"
                title="How many word-cloud terms to show (also grows as you zoom in)."
              >
                <span className="font-medium whitespace-nowrap">Words</span>
                <input
                  type="range"
                  min={0.3}
                  max={2.5}
                  step={0.1}
                  value={wordCloudDensity}
                  onChange={(e) => onWordCloudDensityChange(Number(e.target.value))}
                  className="w-24 accent-alma-600"
                />
              </label>
            )}
            {showWordCloud && onWordCloudSizeChange && (
              <label
                className="flex items-center gap-2 text-xs text-slate-600"
                title="Word-cloud text size."
              >
                <span className="font-medium whitespace-nowrap">Word size</span>
                <input
                  type="range"
                  min={0.5}
                  max={2.5}
                  step={0.1}
                  value={wordCloudSize}
                  onChange={(e) => onWordCloudSizeChange(Number(e.target.value))}
                  className="w-24 accent-alma-600"
                />
              </label>
            )}
          </div>

          {(method || note) && (
            <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-slate-500">
              {method && <span>Method: {methodLabel}</span>}
              {note && <Badge variant="outline" className="text-xs">{note}</Badge>}
            </div>
          )}
        </div>

        {physics && onPhysicsChange && (
          <div className="rounded-sm border border-[var(--color-border)] bg-surface-2/90 p-3 shadow-sm">
            <div className="mb-2 flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <SlidersHorizontal className="h-4 w-4 text-slate-500" />
                <div>
                  <p className="text-sm font-semibold text-alma-800">Physics</p>
                  <p className="text-xs text-slate-500">Spacing + motion (small graphs) and node size.</p>
                </div>
              </div>
              {onResetPhysics && (
                <Button variant="ghost" size="sm" onClick={onResetPhysics}>
                  Reset
                </Button>
              )}
            </div>
            {/* Spacing/motion sliders relax the layout on SMALL graphs (the library)
                — large graphs stay pinned to the embedding layout for performance.
                Edge layers never feed these forces, so the geometry stays stable
                when you toggle edges. */}
            <div className="grid gap-3 sm:grid-cols-2">
              <RangeControl label="Repulsion" value={physics.repulsion} min={-200} max={0} step={5} onChange={(value) => onPhysicsChange({ repulsion: value })} hint="How strongly nodes push each other apart — more negative spreads the map out. (Small graphs only.)" />
              <RangeControl label="Link Distance" value={physics.linkDistance} min={20} max={220} step={5} onChange={(value) => onPhysicsChange({ linkDistance: value })} hint="Resting length of an edge — higher pushes connected nodes farther apart. (Small graphs only.)" />
              <RangeControl label="Link Attraction" value={physics.linkStrength} min={0} max={2} step={0.05} onChange={(value) => onPhysicsChange({ linkStrength: value })} hint="How hard edges pull their endpoints together — higher = tighter clusters. (Small graphs only.)" />
              <RangeControl label="Velocity Decay" value={physics.velocityDecay} min={0.05} max={0.9} step={0.01} onChange={(value) => onPhysicsChange({ velocityDecay: value })} hint="Friction — how quickly motion settles. Higher stops the layout sooner." />
              <RangeControl label="Cooldown" value={physics.cooldownTicks} min={20} max={300} step={10} onChange={(value) => onPhysicsChange({ cooldownTicks: value })} hint="How many simulation steps run before the layout freezes." />
              <RangeControl label="Node Scale" value={physics.nodeScale} min={0.6} max={2.5} step={0.05} onChange={(value) => onPhysicsChange({ nodeScale: value })} hint="Multiplier on every node's size — purely visual." />
              <RangeControl label="Base Size" value={physics.baseSize} min={2} max={20} step={0.5} onChange={(value) => onPhysicsChange({ baseSize: value })} hint="The base radius all node sizes scale from — purely visual." />
            </div>
          </div>
        )}
      </div>

      {clusters && clusters.length > 0 && (
        <div className="rounded-sm border border-[var(--color-border)] bg-surface-1/90 p-3 shadow-sm">
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
              const badgeLabel = cluster.label
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
