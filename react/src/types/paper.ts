export type Paper = {
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
}

export type Relationship = {
  source_paper_id: number
  target_paper_id: number
  relationship_type: 'CITES' | 'SIMILAR' | 'SUPPORT' | 'CONTRADICT' | 'NEUTRAL'
  strength: number
  reason?: string
  created_at?: string
}

export type Cluster = {
  cluster_id: number
  cluster_label: string
  cluster_name: string
  cluster_description: string
}
