import { apiGet, apiPost } from './apiClient'

export type IngestionCreateResponse = {
  job_id: string
  arxiv_id: string
  status: 'processing' | 'skipped'
  stage: string
  bronze_status: string
}

export type IngestionStatusResponse = {
  job_id: string
  status: 'processing' | 'done' | 'failed'
  result?: Record<string, unknown> | null
  error?: string | null
}

export function normalizeArxivId(raw: string): string {
  const trimmed = raw.trim()
  const urlMatch = trimmed.match(/arxiv\.org\/(?:abs|pdf)\/([0-9]{4}\.[0-9]+)/)
  if (urlMatch) return urlMatch[1]
  const idMatch = trimmed.match(/^([0-9]{4}\.[0-9]+)(?:v\d+)?$/)
  if (idMatch) return idMatch[1]
  throw new Error(`Could not parse an ArXiv ID from: "${raw}"`)
}

export async function createIngestion(arxivId: string): Promise<IngestionCreateResponse> {
  return apiPost<IngestionCreateResponse>('/ingestions', { arxiv_id: normalizeArxivId(arxivId) })
}

export async function pollIngestionStatus(jobId: string): Promise<IngestionStatusResponse> {
  return apiGet<IngestionStatusResponse>(`/ingestions/${encodeURIComponent(jobId)}`)
}
