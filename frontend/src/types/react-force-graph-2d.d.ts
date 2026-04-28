declare module 'react-force-graph-2d' {
  import { Component } from 'react'
  type GraphNode = Record<string, unknown>
  type GraphLink = Record<string, unknown>
  interface ForceLike {
    strength?: (value?: number) => ForceLike
    distance?: (value?: number) => ForceLike
  }
  interface ForceGraph2DProps {
    graphData: { nodes: GraphNode[]; links: GraphLink[] }
    width?: number
    height?: number
    nodeCanvasObject?: (node: GraphNode, ctx: CanvasRenderingContext2D, globalScale: number) => void
    onNodeClick?: (node: GraphNode) => void
    linkColor?: (link: GraphLink) => string
    linkWidth?: (link: GraphLink) => number
    linkOpacity?: number | ((link: GraphLink) => number)
    cooldownTicks?: number
    enableZoomInteraction?: boolean
    enablePanInteraction?: boolean
    [key: string]: unknown
  }
  export default class ForceGraph2D extends Component<ForceGraph2DProps> {
    d3Force(name: string): ForceLike | undefined
    d3VelocityDecay(value?: number): number
    d3ReheatSimulation(): void
  }
}
