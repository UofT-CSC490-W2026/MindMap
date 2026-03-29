import { apiPost } from './apiClient'
import type { GraphResponse, GraphExpandResponse } from '../types/graph'

export async function queryGraph(query: string): Promise<GraphResponse> {
  return apiPost<GraphResponse>('/graphs/query', { query })
}

export async function expandGraph(graphId: string, paperId: string): Promise<GraphExpandResponse> {
  return apiPost<GraphExpandResponse>('/graphs/expand', { graph_id: graphId, paper_id: paperId })
}

export async function rebuildClusters(nClusters = 5): Promise<{ status: string; result?: unknown }> {
  return apiPost('/graphs/clusters/rebuild', { n_clusters: nClusters })
}
