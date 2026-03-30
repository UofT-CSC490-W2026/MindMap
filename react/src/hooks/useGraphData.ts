import { useState, useCallback, useEffect } from 'react'
import { getPapers, getRelationships } from '../services/graphService'
import { buildGraph } from '../utils/graphUtils'
import type { GraphNode, GraphLink } from '../types/graph'

type GraphData = {
  nodes: GraphNode[]
  links: GraphLink[]
}

async function loadData() {
  const [papers, relationships] = await Promise.all([getPapers(), getRelationships()])
  return buildGraph(papers, relationships)
}

export function useGraphData() {
  const [data, setData] = useState<GraphData>({ nodes: [], links: [] })
  const [loading, setLoading] = useState(true)

  const fetch = useCallback(async () => {
    setLoading(true)
    try {
      const graph = await loadData()
      setData({ nodes: graph.nodes, links: graph.links })
    } catch (err) {
      console.error('Failed to load graph data:', err)
      setData({ nodes: [], links: [] })
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void fetch()
  }, [fetch])

  const reload = useCallback(async () => {
    await fetch()
  }, [fetch])

  return { data, loading, reload }
}
