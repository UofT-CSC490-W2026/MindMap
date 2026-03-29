import { useState, useCallback } from 'react'
import { queryGraph } from '../services/graphService'
import type { GraphNode, GraphLink, GraphResponse } from '../types/graph'

type GraphData = {
  nodes: GraphNode[]
  links: GraphLink[]
  graphId: string | null
  query: string | null
}

export function useGraphData() {
  const [data, setData] = useState<GraphData>({ nodes: [], links: [], graphId: null, query: null })
  const [loading, setLoading] = useState(false)

  const search = useCallback(async (query: string) => {
    setLoading(true)
    try {
      const response: GraphResponse = await queryGraph(query)
      setData({
        nodes: response.nodes.map((n: any) => ({
          ...n,
          shortTitle: n.label ?? n.title?.slice(0, 40) ?? '',
          searchText: `${n.title} ${n.authors}`.toLowerCase(),
          primaryTopic: n.cluster_name ?? '',
          clusterId: n.cluster_id,
          clusterName: n.cluster_name,
        })),
        links: response.links,
        graphId: response.graph_id,
        query: response.query ?? query,
      })
    } catch (err) {
      console.error('Failed to load graph data:', err)
      setData({ nodes: [], links: [], graphId: null, query })
    } finally {
      setLoading(false)
    }
  }, [])

  const reload = useCallback(async () => {
    if (!data.query) return
    await search(data.query)
  }, [data.query, search])

  return { data, loading, search, reload }
}
