import { apiGet } from './apiClient'

export type SearchPaperResponse = {
  title: string
  authors: string[]
  year?: number | null
  citation_count?: number | null
  arxiv_id?: string | null
  external_url?: string | null
}

export async function searchPapers(query: string, limit = 10): Promise<SearchPaperResponse[]> {
  const params = new URLSearchParams({ query, limit: String(limit) })
  return apiGet<SearchPaperResponse[]>(`/search/papers?${params}`)
}
