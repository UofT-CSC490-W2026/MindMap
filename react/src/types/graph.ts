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
  arxiv_id?: string | null
  cluster_id?: number | null
  cluster_name?: string | null
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

// API response shapes from backend contracts
export type GraphMeta = {
  total_nodes: number
  total_links: number
  query?: string | null
}

export type GraphResponse = {
  graph_id: string
  query?: string | null
  nodes: GraphNode[]
  links: GraphLink[]
  meta: GraphMeta
}

export type GraphExpandResponse = {
  graph_id: string
  paper_id: string
  new_nodes: GraphNode[]
  new_links: GraphLink[]
}
