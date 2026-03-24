import { useState, useEffect } from 'react'
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

  useEffect(() => {
    async function load() {
      const [papers, relationships] = await Promise.all([
        getPapers(),
        getRelationships(),
      ])
      const graph = buildGraph(papers, relationships)
      setData(graph)
      setLoading(false)
    }
    load()
  }, [])

  return { data, loading }
}
