export type GraphNode = {
  id: number
  title: string
  shortTitle: string
  authors: string
  year: number
  citations: number
  primaryTopic: string
  clusterId?: number
  clusterName?: string
  clusterDescription?: string
  searchText: string
  // force-graph injects these at runtime
  x?: number
  y?: number
}

export type GraphLink = {
  source: number | GraphNode
  target: number | GraphNode
  relationship_type: 'CITES' | 'SIMILAR' | 'SUPPORT' | 'CONTRADICT' | 'NEUTRAL'
  strength: number
  reason?: string
}
