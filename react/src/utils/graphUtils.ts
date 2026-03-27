import type { GraphNode, GraphLink } from '../types/graph'
import type { Paper, Relationship } from '../types/paper'

export function buildGraph(
  papers: Paper[],
  relationships: Relationship[],
): { nodes: GraphNode[]; links: GraphLink[] } {
  const nodes: GraphNode[] = papers.map((p) => ({
    id: p.id,
    title: p.title,
    shortTitle: p.shortTitle,
    authors: p.authors,
    year: p.year,
    citations: p.citations,
    primaryTopic: p.primaryTopic,
    clusterId: p.clusterId,
    clusterName: p.clusterName,
    clusterDescription: p.clusterDescription,
    searchText: `${p.title} ${p.shortTitle} ${p.authors} ${p.year} ${p.primaryTopic}`,
  }))

  const links: GraphLink[] = relationships.map((r) => ({
    source: r.source_paper_id,
    target: r.target_paper_id,
    relationship_type: r.relationship_type,
    strength: r.strength,
    reason: r.reason,
  }))

  return { nodes, links }
}

// Distinct colors for up to 10 clusters
export const CLUSTER_COLORS = [
  '#e57373', '#64b5f6', '#81c784', '#ffd54f',
  '#ba68c8', '#4db6ac', '#ff8a65', '#90a4ae',
  '#f06292', '#a5d6a7',
]

export function clusterColor(clusterId: number | undefined): string {
  if (clusterId == null) return '#112240'
  return CLUSTER_COLORS[clusterId % CLUSTER_COLORS.length]
}
