export type GraphNode = {
  id: number
  title: string
  shortTitle: string
  authors: string
  year: number
  citations: number
  primaryTopic: string
  searchText: string
  // force-graph injects these at runtime
  x?: number
  y?: number
}

export type GraphLink = {
  source: number
  target: number
  relationship_type: 'CITES' | 'SIMILAR'
  strength: number
}
