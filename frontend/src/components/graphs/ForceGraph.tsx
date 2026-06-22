import { useRef, useCallback, useState, useEffect, useMemo } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import type { GraphData, GraphNode } from '@/api/client'

export interface GraphPhysicsConfig {
  repulsion: number
  linkDistance: number
  linkStrength: number
  velocityDecay: number
  cooldownTicks: number
  nodeScale: number
  baseSize: number
}

interface ClusterSummary {
  id: number
  label: string
  topic_text?: string
  size: number
  word_cloud?: Array<{ term: string; weight: number }>
}

interface RenderedNode extends Record<string, unknown> {
  id: string
  name: string
  x: number
  y: number
  vx?: number
  vy?: number
  // Pinned position (d3-force keeps a node fixed when fx/fy are set). Used for
  // the static-layout fast path on large graphs (I-10).
  fx?: number
  fy?: number
  _initX: number
  _initY: number
  color: string
  size: number
  cluster_id?: number
  node_type: string
  metadata: Record<string, unknown>
  _highlighted: boolean
}

// Above this node count we DON'T run the force simulation — the backend already
// ships a UMAP layout, so we pin every node to those coordinates and render
// statically (I-10). Running d3-force charge+link over thousands of nodes for
// 100 cooldown ticks (each re-rendering every node) was the corpus-graph lag.
const LARGE_GRAPH_THRESHOLD = 1200

interface RenderedLink extends Record<string, unknown> {
  source: string | RenderedNode
  target: string | RenderedNode
  value: number
  edge_type: string
}

// Per-layer edge colours (Phase 3 / I-11). Canvas hex strings, not Tailwind
// classes — the semantic identity is folio-blue (the primary neighbourhood
// signal), with distinct hues for the corroborating layers. Exported so the
// GraphPanel legend/filter chips stay in sync (one source of truth).
export const LAYER_COLORS: Record<string, string> = {
  semantic: 'rgba(59,130,246,0.45)', // folio blue — the headline neighbourhood
  bibliographic_coupling: 'rgba(139,92,246,0.38)', // violet — shared literature
  co_authorship: 'rgba(16,185,129,0.38)', // emerald — shared authors
  topic: 'rgba(245,158,11,0.35)', // amber — topic overlay
}
const LAYER_FALLBACK_COLOR = 'rgba(203,213,225,0.30)'

// Human labels for the typed edge layers, shown in the filter chips + legend.
export const LAYER_LABELS: Record<string, string> = {
  semantic: 'Semantic (nearest work)',
  bibliographic_coupling: 'Shared references',
  co_authorship: 'Shared authors',
  topic: 'Topic',
}

interface ForceGraphProps {
  data: GraphData
  width?: number
  height?: number
  onNodeClick?: (node: GraphNode) => void
  showLabels?: boolean
  highlightSearch?: string
  selectedNodeId?: string | null
  selectedClusterId?: number | null
  showClusterLabels?: boolean
  showWordCloud?: boolean
  clusters?: ClusterSummary[]
  physics?: GraphPhysicsConfig
  /** Edge layers to render (Phase 3 / I-11). undefined ⇒ show every layer. */
  visibleLayers?: string[]
}

export function ForceGraph({
  data,
  width,
  height = 500,
  onNodeClick,
  showLabels = false,
  highlightSearch = '',
  selectedNodeId = null,
  selectedClusterId = null,
  showClusterLabels = false,
  showWordCloud = false,
  clusters = [],
  physics,
  visibleLayers,
}: ForceGraphProps) {
  const fgRef = useRef<InstanceType<typeof ForceGraph2D>>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dimensions, setDimensions] = useState({ width: width || 800, height })

  useEffect(() => {
    if (!width && containerRef.current) {
      const ro = new ResizeObserver((entries) => {
        for (const entry of entries) {
          const nextWidth = Math.floor(entry.contentRect.width)
          if (nextWidth > 0) {
            setDimensions((prev) => ({ ...prev, width: nextWidth }))
          }
        }
      })
      ro.observe(containerRef.current)
      return () => ro.disconnect()
    }
  }, [width])

  useEffect(() => {
    if (width && width > 0) {
      setDimensions({ width, height })
    } else {
      setDimensions((prev) => ({ ...prev, height }))
    }
  }, [width, height])

  const clusterLookup = useMemo(() => {
    return new Map<number, ClusterSummary>(clusters.map((cluster) => [cluster.id, cluster]))
  }, [clusters])

  const graphData = useMemo(() => {
    const w = Math.max(1, dimensions.width)
    const h = Math.max(1, dimensions.height)
    const seenNodeIds = new Set<string>()
    const nodes = data.nodes.flatMap((node) => {
      if (!node.id || seenNodeIds.has(node.id)) {
        return []
      }
      seenNodeIds.add(node.id)
      const rawX = Number.isFinite(node.x) ? node.x : 0.5
      const rawY = Number.isFinite(node.y) ? node.y : 0.5
      const rawSize = Number.isFinite(node.size) && node.size > 0 ? node.size : 1
      const initX = (rawX - 0.5) * w
      const initY = (rawY - 0.5) * h
      return [
        {
          id: node.id,
          name: node.name,
          x: initX,
          y: initY,
          _initX: initX,
          _initY: initY,
          color: node.color || '#3B82F6',
          size: rawSize,
          cluster_id: node.cluster_id,
          node_type: node.node_type || 'paper',
          metadata: node.metadata,
          _highlighted: highlightSearch ? node.name.toLowerCase().includes(highlightSearch.toLowerCase()) : false,
        } satisfies RenderedNode,
      ]
    })
    const layerFilter = visibleLayers ? new Set(visibleLayers) : null
    const links = data.edges.flatMap((edge) => {
      const source = String(edge.source || '')
      const target = String(edge.target || '')
      if (!source || !target || source === target || !seenNodeIds.has(source) || !seenNodeIds.has(target)) {
        return []
      }
      const edgeType = String(edge.edge_type || 'semantic')
      // Layer filter (I-11): hide edges whose layer is toggled off.
      if (layerFilter && !layerFilter.has(edgeType)) {
        return []
      }
      return [
        {
          source,
          target,
          value: Number.isFinite(edge.weight) ? edge.weight : 1,
          edge_type: edgeType,
        } satisfies RenderedLink,
      ]
    })
    // Static-layout fast path (I-10): on large graphs, pin every node to its
    // backend UMAP coordinate so d3-force has nothing to solve — the canvas
    // only repaints on pan/zoom instead of on every simulation tick.
    if (nodes.length > LARGE_GRAPH_THRESHOLD) {
      for (const node of nodes) {
        ;(node as RenderedNode).fx = node._initX
        ;(node as RenderedNode).fy = node._initY
      }
    }
    return { nodes, links }
  }, [data, dimensions.width, dimensions.height, highlightSearch, visibleLayers])

  const isLargeGraph = graphData.nodes.length > LARGE_GRAPH_THRESHOLD

  useEffect(() => {
    const graph = fgRef.current
    if (!graph || !physics) {
      return
    }
    // Large graphs render statically from the pinned UMAP layout — no force
    // tuning, no reheat (the simulation is what made the corpus graph lag).
    if (isLargeGraph) {
      return
    }
    // Disable the built-in center force so precomputed cluster coordinates
    // aren't collapsed into a circular blob at the origin. Cast through
    // the setter form: react-force-graph's `d3Force` is overloaded
    // (getter / setter) but TS narrows to the getter when the second
    // arg is `null`.
    ;(graph.d3Force as (name: string, force: unknown) => unknown)('center', null)
    const charge = graph.d3Force('charge') as { strength?: (value: number) => unknown } | undefined
    charge?.strength?.(physics.repulsion)
    const linkForce = graph.d3Force('link') as {
      distance?: (value: number) => unknown
      strength?: (value: number) => unknown
    } | undefined
    linkForce?.distance?.(physics.linkDistance)
    linkForce?.strength?.(physics.linkStrength)
    // Snap each node back to its precomputed starting position and clear
    // inherited velocities so the new force params drive a fresh layout,
    // otherwise the already-settled nodes barely move and only the auto-fit
    // zoom appears to respond.
    for (const node of graphData.nodes) {
      node.x = node._initX
      node.y = node._initY
      // `vx` / `vy` are added by d3-force at simulation runtime; they
      // aren't on our static node shape, hence the inline cast.
      ;(node as { vx?: number; vy?: number }).vx = 0
      ;(node as { vx?: number; vy?: number }).vy = 0
    }
    graph.d3ReheatSimulation()
  }, [physics, graphData, isLargeGraph])

  const handleNodeClick = useCallback((node: Record<string, unknown>) => {
    if (onNodeClick) {
      onNodeClick(node as unknown as GraphNode)
    }
  }, [onNodeClick])

  const nodeCanvasObject = useCallback((node: Record<string, unknown>, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const label = String(node.name || '')
    const nodeId = String(node.id || '')
    const nodeClusterId = typeof node.cluster_id === 'number' ? node.cluster_id : null
    const isSelectedNode = !!selectedNodeId && nodeId === selectedNodeId
    const isSelectedCluster = selectedClusterId !== null && nodeClusterId === selectedClusterId
    const dimmed = selectedClusterId !== null && !isSelectedCluster
    const highlighted = Boolean(node._highlighted)
    const fontSize = Math.max(10 / globalScale, 1)
    const size = (((node.size as number) || 1) * (physics?.nodeScale || 1)) * (physics?.baseSize ?? 6)
    const nodeX = Number(node.x || 0)
    const nodeY = Number(node.y || 0)
    const nodeType = String(node.node_type || 'paper')

    ctx.save()
    // Slightly translucent fills so overlapping dots read as density and edges
    // stay visible underneath; selection/dim states keep their own alpha.
    ctx.globalAlpha = dimmed ? 0.15 : 0.8

    if (isSelectedCluster) {
      ctx.beginPath()
      ctx.arc(nodeX, nodeY, size + 5, 0, 2 * Math.PI)
      ctx.fillStyle = 'rgba(59, 130, 246, 0.10)'
      ctx.fill()
    }

    ctx.beginPath()
    if (nodeType === 'topic') {
      ctx.moveTo(nodeX, nodeY - size)
      ctx.lineTo(nodeX + size, nodeY)
      ctx.lineTo(nodeX, nodeY + size)
      ctx.lineTo(nodeX - size, nodeY)
      ctx.closePath()
    } else {
      ctx.arc(nodeX, nodeY, size, 0, 2 * Math.PI)
    }

    if (highlighted) {
      ctx.fillStyle = '#FBBF24'
      ctx.strokeStyle = '#F59E0B'
      ctx.lineWidth = 2
      ctx.stroke()
    } else {
      ctx.fillStyle = String(node.color || '#3B82F6')
    }
    ctx.fill()

    if (isSelectedNode) {
      ctx.beginPath()
      ctx.arc(nodeX, nodeY, size + 3, 0, 2 * Math.PI)
      ctx.strokeStyle = '#0F172A'
      ctx.lineWidth = Math.max(1.5, 3 / globalScale)
      ctx.stroke()
    }

    if (showLabels || highlighted || isSelectedNode || globalScale > 2) {
      ctx.font = `${fontSize}px sans-serif`
      ctx.textAlign = 'center'
      ctx.textBaseline = 'top'
      ctx.fillStyle = dimmed ? 'rgba(30, 41, 59, 0.45)' : '#1E293B'
      const truncLabel = label.length > 36 ? `${label.slice(0, 33)}...` : label
      ctx.fillText(truncLabel, nodeX, nodeY + size + 2)
    }

    ctx.restore()
  }, [physics?.nodeScale, physics?.baseSize, selectedClusterId, selectedNodeId, showLabels])

  // Hit area for hover/click. Without this, react-force-graph derives the
  // clickable region from a default node size, so only the centre of a
  // custom-drawn dot was reactive. We paint the SAME shape + radius the node is
  // drawn with, so the whole dot (whatever its size) is clickable/hoverable.
  const nodePointerAreaPaint = useCallback(
    (node: Record<string, unknown>, color: string, ctx: CanvasRenderingContext2D) => {
      const size = (((node.size as number) || 1) * (physics?.nodeScale || 1)) * (physics?.baseSize ?? 6)
      const nodeX = Number(node.x || 0)
      const nodeY = Number(node.y || 0)
      ctx.fillStyle = color
      ctx.beginPath()
      if (String(node.node_type || 'paper') === 'topic') {
        ctx.moveTo(nodeX, nodeY - size)
        ctx.lineTo(nodeX + size, nodeY)
        ctx.lineTo(nodeX, nodeY + size)
        ctx.lineTo(nodeX - size, nodeY)
        ctx.closePath()
      } else {
        ctx.arc(nodeX, nodeY, size, 0, 2 * Math.PI)
      }
      ctx.fill()
    },
    [physics?.nodeScale, physics?.baseSize],
  )

  const renderClusterWordClouds = useCallback(
    (ctx: CanvasRenderingContext2D, globalScale: number) => {
      if (!showWordCloud || graphData.nodes.length === 0) {
        return
      }
      const centroids = new Map<number, { x: number; y: number; radius: number; count: number }>()
      for (const node of graphData.nodes) {
        if (typeof node.cluster_id !== 'number') continue
        const entry = centroids.get(node.cluster_id) || { x: 0, y: 0, radius: 0, count: 0 }
        entry.x += Number(node.x || 0)
        entry.y += Number(node.y || 0)
        entry.radius = Math.max(entry.radius, Number(node.size || 1))
        entry.count += 1
        centroids.set(node.cluster_id, entry)
      }
      for (const [clusterId, stat] of centroids.entries()) {
        const cluster = clusterLookup.get(clusterId)
        const terms = cluster?.word_cloud || []
        if (!terms.length || stat.count === 0) continue
        const isSelected = selectedClusterId !== null && clusterId === selectedClusterId
        const dimmed = selectedClusterId !== null && !isSelected
        if (dimmed) continue
        const cx = stat.x / stat.count
        const cy = stat.y / stat.count
        const maxWeight = terms[0]?.weight || 1
        // Radial ring sized from the cluster span so terms hover around it.
        const approxRadius = Math.max(40, Math.min(180, 20 + stat.count * 1.6))
        const baseFont = Math.max(isSelected ? 11 : 9, isSelected ? 14 : 11) / globalScale
        ctx.save()
        ctx.globalAlpha = isSelected ? 0.9 : 0.65
        ctx.textAlign = 'center'
        ctx.textBaseline = 'middle'
        const slice = (2 * Math.PI) / terms.length
        terms.slice(0, 10).forEach((entry, idx) => {
          const scale = 0.5 + 0.8 * (entry.weight / maxWeight)
          const fontSize = baseFont * scale * (isSelected ? 1.25 : 1)
          const angle = idx * slice + clusterId * 0.37
          const radius = approxRadius * (0.75 + 0.25 * scale) / globalScale
          const tx = cx + radius * Math.cos(angle)
          const ty = cy + radius * Math.sin(angle)
          ctx.font = `600 ${fontSize}px ui-sans-serif, system-ui, sans-serif`
          const paddingX = 3 / globalScale
          const paddingY = 2 / globalScale
          const width = ctx.measureText(entry.term).width
          ctx.fillStyle = 'rgba(255,255,255,0.88)'
          ctx.fillRect(tx - width / 2 - paddingX, ty - fontSize / 2 - paddingY, width + paddingX * 2, fontSize + paddingY * 2)
          ctx.fillStyle = isSelected ? '#0F172A' : '#334155'
          ctx.fillText(entry.term, tx, ty)
        })
        ctx.restore()
      }
    },
    [clusterLookup, graphData.nodes, selectedClusterId, showWordCloud],
  )

  const renderClusterLabels = useCallback((ctx: CanvasRenderingContext2D, globalScale: number) => {
    if (!showClusterLabels || graphData.nodes.length === 0) {
      return
    }

    const grouped = new Map<number, { x: number; y: number; count: number }>()
    for (const node of graphData.nodes) {
      if (typeof node.cluster_id !== 'number') {
        continue
      }
      const current = grouped.get(node.cluster_id) || { x: 0, y: 0, count: 0 }
      current.x += Number(node.x || 0)
      current.y += Number(node.y || 0)
      current.count += 1
      grouped.set(node.cluster_id, current)
    }

    for (const [clusterId, stat] of grouped.entries()) {
      const cluster = clusterLookup.get(clusterId)
      const label = String(cluster?.topic_text || cluster?.label || `Cluster ${clusterId + 1}`)
      const x = stat.x / Math.max(stat.count, 1)
      const y = stat.y / Math.max(stat.count, 1)
      const isSelected = selectedClusterId !== null && clusterId === selectedClusterId
      const dimmed = selectedClusterId !== null && !isSelected
      const fontSize = Math.max((isSelected ? 18 : 14) / globalScale, 7)
      const paddingX = 8 / globalScale
      const paddingY = 4 / globalScale

      ctx.save()
      ctx.globalAlpha = dimmed ? 0.22 : isSelected ? 0.98 : 0.78
      ctx.font = `600 ${fontSize}px ui-serif, Georgia, serif`
      ctx.textAlign = 'center'
      ctx.textBaseline = 'middle'
      const textWidth = ctx.measureText(label).width
      ctx.fillStyle = 'rgba(255,255,255,0.92)'
      ctx.fillRect(
        x - textWidth / 2 - paddingX,
        y - fontSize / 2 - paddingY,
        textWidth + paddingX * 2,
        fontSize + paddingY * 2,
      )
      ctx.fillStyle = isSelected ? '#0F172A' : '#1E293B'
      ctx.fillText(label, x, y)
      ctx.restore()
    }
  }, [clusterLookup, graphData.nodes, selectedClusterId, showClusterLabels])

  const linkColor = useCallback((link: Record<string, unknown>) => {
    if (selectedClusterId === null) {
      // Default: colour by edge LAYER (I-11) so the typed neighbourhood reads
      // at a glance — semantic vs shared-refs vs shared-authors.
      return LAYER_COLORS[String(link.edge_type || 'semantic')] ?? LAYER_FALLBACK_COLOR
    }
    // Cluster focus mode: highlight in-cluster edges, mute the rest.
    const sourceCluster = typeof (link.source as RenderedNode)?.cluster_id === 'number'
      ? (link.source as RenderedNode).cluster_id
      : null
    const targetCluster = typeof (link.target as RenderedNode)?.cluster_id === 'number'
      ? (link.target as RenderedNode).cluster_id
      : null
    return sourceCluster === selectedClusterId && targetCluster === selectedClusterId
      ? '#3B82F6'
      : 'rgba(203,213,225,0.18)'
  }, [selectedClusterId])

  const linkWidth = useCallback((link: Record<string, unknown>) => {
    const base = Math.max(0.5, (Number(link.value || 1) || 1) * 0.5)
    if (selectedClusterId === null) {
      return base
    }
    const sourceCluster = typeof (link.source as RenderedNode)?.cluster_id === 'number'
      ? (link.source as RenderedNode).cluster_id
      : null
    const targetCluster = typeof (link.target as RenderedNode)?.cluster_id === 'number'
      ? (link.target as RenderedNode).cluster_id
      : null
    return sourceCluster === selectedClusterId && targetCluster === selectedClusterId ? base + 0.8 : 0.25
  }, [selectedClusterId])

  const linkOpacity = useCallback((link: Record<string, unknown>) => {
    if (selectedClusterId === null) {
      return 0.3
    }
    const sourceCluster = typeof (link.source as RenderedNode)?.cluster_id === 'number'
      ? (link.source as RenderedNode).cluster_id
      : null
    const targetCluster = typeof (link.target as RenderedNode)?.cluster_id === 'number'
      ? (link.target as RenderedNode).cluster_id
      : null
    return sourceCluster === selectedClusterId && targetCluster === selectedClusterId ? 0.55 : 0.08
  }, [selectedClusterId])

  // Rich hover tooltip (react-force-graph renders the returned HTML in its
  // default tooltip box). Surfaces the cluster label + the stats behind the
  // node — what an author/paper IS — instead of just its name, so hovering
  // explains the geometry. No raw colors: the library box provides the chrome;
  // we only structure + de-emphasize secondary lines with opacity.
  const nodeLabel = useCallback((node: Record<string, unknown>) => {
    const meta = (node.metadata ?? {}) as Record<string, unknown>
    const esc = (v: unknown) =>
      String(v ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    const rows: string[] = []
    // Cluster line — honest about uncertainty (I-6): outliers say so, and a
    // clustered node shows HDBSCAN's membership confidence when we have it.
    if (meta.is_outlier) {
      rows.push(`<div style="opacity:.85">Unclustered — too sparse to assign confidently</div>`)
    } else if (meta.cluster_label) {
      const conf = meta.cluster_confidence
      const confTxt =
        typeof conf === 'number' ? ` · ${Math.round(conf * 100)}% confidence` : ''
      rows.push(`<div style="opacity:.85">Cluster: ${esc(meta.cluster_label)}${confTxt}</div>`)
    }
    const isAuthor = node.node_type === 'author' || meta.pub_count != null
    if (isAuthor) {
      if (meta.affiliation) rows.push(`<div style="opacity:.8">${esc(meta.affiliation)}</div>`)
      const stats: string[] = []
      if (meta.pub_count != null) stats.push(`${esc(meta.pub_count)} papers`)
      if (meta.citation_count != null) stats.push(`${esc(meta.citation_count)} cites`)
      if (meta.h_index != null) stats.push(`h-index ${esc(meta.h_index)}`)
      if (stats.length) rows.push(`<div style="opacity:.7">${stats.join(' · ')}</div>`)
      if (meta.top_topic) rows.push(`<div style="opacity:.6;font-style:italic">${esc(meta.top_topic)}</div>`)
    } else {
      const stats: string[] = []
      if (meta.year != null) stats.push(esc(meta.year))
      const cites = meta.citations ?? meta.cited_by_count
      if (cites != null) stats.push(`${esc(cites)} cites`)
      if (stats.length) rows.push(`<div style="opacity:.7">${stats.join(' · ')}</div>`)
    }
    return `<div style="max-width:280px;line-height:1.35"><div style="font-weight:600;margin-bottom:2px">${esc(node.name)}</div>${rows.join('')}</div>`
  }, [])

  return (
    <div ref={containerRef} className="w-full" style={{ height }}>
      <ForceGraph2D
        ref={fgRef}
        graphData={graphData}
        width={dimensions.width}
        height={height}
        nodeCanvasObject={nodeCanvasObject}
        nodePointerAreaPaint={nodePointerAreaPaint}
        nodeLabel={nodeLabel}
        onNodeClick={handleNodeClick}
        onRenderFramePost={(ctx: unknown, globalScale: number) => {
          const c = ctx as CanvasRenderingContext2D
          renderClusterWordClouds(c, globalScale)
          renderClusterLabels(c, globalScale)
        }}
        linkColor={linkColor}
        linkWidth={linkWidth}
        linkOpacity={linkOpacity}
        d3VelocityDecay={physics?.velocityDecay ?? 0.4}
        // Static UMAP layout on large graphs (I-10): 0 ticks → no simulation,
        // the canvas only repaints on interaction. Small graphs keep the
        // interactive physics.
        cooldownTicks={isLargeGraph ? 0 : (physics?.cooldownTicks || 100)}
        warmupTicks={0}
        enableNodeDrag={!isLargeGraph}
        enableZoomInteraction={true}
        enablePanInteraction={true}
      />
    </div>
  )
}
