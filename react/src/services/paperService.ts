import { normalizeArxivId } from './ingestionService'

export type { IngestionCreateResponse, IngestionStatusResponse } from './ingestionService'

function baseUrl(): string {
  return (import.meta.env.VITE_API_URL ?? '').replace(/\/$/, '')
}

export async function ingestPaper(arxivInput: string): Promise<{ status: string; job_id?: string; error?: string }> {
  const arxiv_id = normalizeArxivId(arxivInput)
  const res = await fetch(`${baseUrl()}/papers/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ arxiv_id }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text)
  }
  return res.json()
}

export async function getPaperStatus(jobId: string): Promise<{ status: string; error?: string }> {
  const res = await fetch(`${baseUrl()}/papers/${encodeURIComponent(jobId)}/status`)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text)
  }
  return res.json()
}
