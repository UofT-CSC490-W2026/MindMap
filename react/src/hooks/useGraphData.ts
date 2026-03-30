import { useState, useCallback, useRef } from 'react'
import { queryGraph } from '../services/graphService'
import type { GraphNode, GraphLink, GraphResponse } from '../types/graph'

type GraphData = {
  nodes: GraphNode[]
  links: GraphLink[]
}

export function useGraphData() {
  const [data, setData] = useState<GraphData>({ nodes: [], links: [] })
  const [loading, setLoading] = useState(false)
  const lastQueryRef = useRef<string | null>(null)

  const applyGraphResponse = useCallback((response: GraphResponse) => {
    setData({
      nodes: response.nodes.map((n: any) => ({
        ...n,
        id: Number(n.id),
        shortTitle: n.label ?? n.title?.slice(0, 40) ?? '',
        searchText: `${n.title} ${n.authors}`.toLowerCase(),
        primaryTopic: n.cluster_name ?? '',
        clusterId: n.cluster_id,
        clusterName: n.cluster_name,
        clusterDescription: n.cluster_description,
      })),
      links: response.links.map((l: any) => ({
        source: Number(l.source),
        target: Number(l.target),
        relationship_type: l.relationship_type ?? l.kind ?? 'SIMILAR',
        strength: l.strength ?? 0.5,
        reason: l.reason,
      })),
    })
  }, [])

  // Search builds a graph from a query via the query endpoint
  const search = useCallback(async (query: string) => {
    setLoading(true)
    try {
      const response: GraphResponse = await queryGraph(query)
      lastQueryRef.current = query
      applyGraphResponse(response)
    } catch (err) {
      console.error('Failed to search graph:', err)
      setData({ nodes: [], links: [] })
    } finally {
      setLoading(false)
    }
  }, [applyGraphResponse])

  // Reload re-runs the last query so the graph reflects newly ingested papers
  const reload = useCallback(async () => {
    const query = lastQueryRef.current
    if (!query) return
    setLoading(true)
    try {
      const response: GraphResponse = await queryGraph(query)
      applyGraphResponse(response)
    } catch (err) {
      console.error('Failed to reload graph:', err)
    } finally {
      setLoading(false)
    }
  }, [applyGraphResponse])

  return { data, loading, search, reload }
}
