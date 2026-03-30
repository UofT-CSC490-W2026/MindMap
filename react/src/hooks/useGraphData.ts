import { useState, useCallback } from 'react'
import { getPapers, getRelationships, queryGraph } from '../services/graphService'
import { buildGraph } from '../utils/graphUtils'
import type { GraphNode, GraphLink, GraphResponse } from '../types/graph'

type GraphData = {
  nodes: GraphNode[]
  links: GraphLink[]
}

export function useGraphData() {
  const [data, setData] = useState<GraphData>({ nodes: [], links: [] })
  const [loading, setLoading] = useState(false)

  // Load graph from papers + relationships (used by reload)
  const loadInitial = useCallback(async () => {
    setLoading(true)
    try {
      const [papers, relationships] = await Promise.all([getPapers(), getRelationships()])
      const graph = buildGraph(papers, relationships)
      setData({ nodes: graph.nodes, links: graph.links })
    } catch (err) {
      console.error('Failed to load graph data:', err)
      setData({ nodes: [], links: [] })
    } finally {
      setLoading(false)
    }
  }, [])

  // Search builds a graph from a query via the query endpoint
  const search = useCallback(async (query: string) => {
    setLoading(true)
    try {
      const response: GraphResponse = await queryGraph(query)
      setData({
        nodes: response.nodes.map((n: any) => ({
          ...n,
          id: Number(n.id),
          shortTitle: n.label ?? n.title?.slice(0, 40) ?? '',
          searchText: `${n.title} ${n.authors}`.toLowerCase(),
          primaryTopic: n.cluster_name ?? '',
          clusterId: n.cluster_id,
          clusterName: n.cluster_name,
        })),
        links: response.links.map((l: any) => ({
          source: Number(l.source),
          target: Number(l.target),
          relationship_type: l.relationship_type ?? l.kind ?? 'SIMILAR',
          strength: l.strength ?? 0.5,
          reason: l.reason,
        })),
      })
    } catch (err) {
      console.error('Failed to search graph:', err)
      setData({ nodes: [], links: [] })
    } finally {
      setLoading(false)
    }
  }, [])

  const reload = useCallback(async () => {
    await loadInitial()
  }, [loadInitial])

  return { data, loading, search, reload }
}
