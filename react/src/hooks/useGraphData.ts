import { useState, useEffect, useCallback } from 'react'
import { getPapers, getRelationships } from '../services/graphService'
import { buildGraph } from '../utils/graphUtils'
import type { GraphNode, GraphLink } from '../types/graph'

type GraphData = {
  nodes: GraphNode[]
  links: GraphLink[]
}

export function useGraphData() {
  const [data, setData] = useState<GraphData>({ nodes: [], links: [] })
  const [loading, setLoading] = useState(true)

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const [papers, relationships] = await Promise.all([
        getPapers(),
        getRelationships(),
      ])
      const graph = buildGraph(papers, relationships)
      setData(graph)
    } catch (err) {
      console.error('Failed to load graph data:', err)
      setData({ nodes: [], links: [] })
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    reload()
  }, [reload])

  return { data, loading, reload }
}
