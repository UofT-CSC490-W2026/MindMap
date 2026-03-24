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
    searchText: `${p.title} ${p.shortTitle} ${p.authors} ${p.year} ${p.primaryTopic}`,
  }))

  const links: GraphLink[] = relationships.map((r) => ({
    source: r.source_paper_id,
    target: r.target_paper_id,
    relationship_type: r.relationship_type,
    strength: r.strength,
  }))

  return { nodes, links }
}
