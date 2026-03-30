import type { Paper, Relationship } from '../types/paper'

const USE_MOCK = false
const API_BASE = import.meta.env.VITE_API_URL ?? ''

export async function getPapers(): Promise<Paper[]> {
  if (USE_MOCK) {
    const data = await import('../data/mockPapers.json')
    return data.default as Paper[]
  }
  const res = await fetch(`${API_BASE}/papers`)
  if (!res.ok) throw new Error(`GET /papers failed: ${res.status} ${await res.text()}`)
  return res.json()
}

export async function getRelationships(): Promise<Relationship[]> {
  if (USE_MOCK) {
    const data = await import('../data/mockRelationships.json')
    return data.default as Relationship[]
  }
  const res = await fetch(`${API_BASE}/relationships`)
  if (!res.ok) throw new Error(`GET /relationships failed: ${res.status} ${await res.text()}`)
  return res.json()
}

export async function rebuildClusters(nClusters = 5): Promise<{
  status: string
  database?: string
  result?: unknown
}> {
  const res = await fetch(`${API_BASE}/clusters/rebuild?n_clusters=${encodeURIComponent(String(nClusters))}`, {
    method: 'POST',
  })
  if (!res.ok) throw new Error(`POST /clusters/rebuild failed: ${res.status} ${await res.text()}`)
  return res.json()
}
