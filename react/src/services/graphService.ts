import type { Paper, Relationship } from '../types/paper'

const USE_MOCK = true

export async function getPapers(): Promise<Paper[]> {
  if (USE_MOCK) {
    const data = await import('../data/mockPapers.json')
    return data.default as Paper[]
  }
  const res = await fetch('/api/silver-papers')
  return res.json()
}

export async function getRelationships(): Promise<Relationship[]> {
  if (USE_MOCK) {
    const data = await import('../data/mockRelationships.json')
    return data.default as Relationship[]
  }
  const res = await fetch('/api/gold-relationships')
  return res.json()
}
