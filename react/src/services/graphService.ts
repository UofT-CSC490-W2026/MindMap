import { apiFetch, apiPost } from './apiClient'
import type { GraphResponse, GraphExpandResponse } from '../types/graph'

export type Paper = {
  id: number
  title: string
  [key: string]: unknown
}

export type Relationship = {
  source_paper_id: number
  target_paper_id: number
  relationship_type: string
  strength: number
}

export async function getPapers(): Promise<Paper[]> {
  const res = await fetch(`${_baseUrl()}/papers`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`GET /papers failed: ${res.status} ${text}`)
  }
  return res.json()
}

export async function getRelationships(): Promise<Relationship[]> {
  const res = await fetch(`${_baseUrl()}/relationships`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`GET /relationships failed: ${res.status} ${text}`)
  }
  return res.json()
}

export async function rebuildClusters(nClusters = 5): Promise<{ status: string; result?: unknown }> {
  const url = `${_baseUrl()}/clusters/rebuild?n_clusters=${nClusters}`
  const res = await fetch(url, { method: 'POST' })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`POST /clusters/rebuild failed: ${res.status} ${text}`)
  }
  return res.json()
}

function _baseUrl(): string {
  const url = import.meta.env.VITE_API_URL ?? ''
  return url.replace(/\/$/, '')
}

export async function queryGraph(query: string): Promise<GraphResponse> {
  return apiPost<GraphResponse>('/graphs/query', { query })
}

export async function expandGraph(graphId: string, paperId: string): Promise<GraphExpandResponse> {
  return apiPost<GraphExpandResponse>('/graphs/expand', { graph_id: graphId, paper_id: paperId })
}
