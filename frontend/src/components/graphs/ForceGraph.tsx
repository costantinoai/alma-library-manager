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
  _initX: number
  _initY: number
  color: string
  size: number
  cluster_id?: number
  node_type: string
  metadata: Record<string, unknown>
  _highlighted: boolean
}

interface RenderedLink extends Record<string, unknown> {
  source: string | RenderedNode
  target: string | RenderedNode
  value: number
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
    const links = data.edges.flatMap((edge) => {
      const source = String(edge.source || '')
      const target = String(edge.target || '')
      if (!source || !target || source === target || !seenNodeIds.has(source) || !seenNodeIds.has(target)) {
        return []
      }
      return [
        {
          source,
          target,
          value: Number.isFinite(edge.weight) ? edge.weight : 1,
        } satisfies RenderedLink,
      ]
    })
    return { nodes, links }
  }, [data, dimensions.width, dimensions.height, highlightSearch])

  useEffect(() => {
    const graph = fgRef.current
    if (!graph || !physics) {
      return
    }
    // Disable the built-in center force so precomputed cluster coordinates
    // aren't collapsed into a circular blob at the origin.
    graph.d3Force('center', null)
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
      node.vx = 0
      node.vy = 0
    }
    graph.d3ReheatSimulation()
  }, [physics, graphData])

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
    ctx.globalAlpha = dimmed ? 0.18 : 1

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
      return '#CBD5E1'
    }
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

  return (
    <div ref={containerRef} className="w-full" style={{ height }}>
      <ForceGraph2D
        ref={fgRef}
        graphData={graphData}
        width={dimensions.width}
        height={height}
        nodeCanvasObject={nodeCanvasObject}
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
        cooldownTicks={physics?.cooldownTicks || 100}
        enableZoomInteraction={true}
        enablePanInteraction={true}
      />
    </div>
  )
}
