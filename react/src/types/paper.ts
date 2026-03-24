export type Paper = {
  id: number
  title: string
  shortTitle: string
  authors: string
  year: number
  citations: number
  primaryTopic: string
}

export type Relationship = {
  source_paper_id: number
  target_paper_id: number
  relationship_type: 'CITES' | 'SIMILAR'
  strength: number
  created_at?: string
}
