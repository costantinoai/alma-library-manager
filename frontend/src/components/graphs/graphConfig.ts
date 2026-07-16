export interface GraphPhysicsConfig {
  repulsion: number
  linkDistance: number
  linkStrength: number
  velocityDecay: number
  cooldownTicks: number
  nodeScale: number
  baseSize: number
}

// Large graphs use the backend UMAP layout instead of an expensive force pass.
export const LARGE_GRAPH_THRESHOLD = 1200
export const LARGE_GRAPH_EDGE_THRESHOLD = 1500

// Canvas colours shared by the renderer and its filter legend.
export const LAYER_COLORS: Record<string, string> = {
  semantic: 'rgba(59,130,246,0.45)',
  bibliographic_coupling: 'rgba(139,92,246,0.38)',
  co_authorship: 'rgba(16,185,129,0.38)',
  topic: 'rgba(245,158,11,0.35)',
}

export const LAYER_FALLBACK_COLOR = 'rgba(203,213,225,0.30)'

export const LAYER_LABELS: Record<string, string> = {
  semantic: 'Semantic (nearest work)',
  bibliographic_coupling: 'Shared references',
  co_authorship: 'Shared authors',
  topic: 'Topic',
}
