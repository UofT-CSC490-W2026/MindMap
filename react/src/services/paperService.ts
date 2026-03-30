const USE_MOCK = false  // ← flip to false when backend is ready
const API_BASE = import.meta.env.VITE_API_URL ?? 'https://notsakura--mindmap-pipeline-fastapi-app.modal.run'

export async function ingestPaper(arxivId: string): Promise<{
  job_id?: string
  status: 'processing' | 'failed'
  stage?: string
  bronze_status?: 'ok' | 'skipped' | 'failed'
  error?: string
}> {
  const normalizedArxivId = normalizeArxivId(arxivId)
  const res = await fetch(`${API_BASE}/papers/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ arxiv_id: normalizedArxivId }),
  })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

let mockCallCount = 0

export async function getPaperStatus(
  job_id: string,
): Promise<{ status: 'pending' | 'processing' | 'done' | 'failed'; error?: string }> {
  if (USE_MOCK) {
    await delay(400)
    mockCallCount++
    if (mockCallCount === 1) return { status: 'pending' }
    if (mockCallCount === 2) return { status: 'processing' }
    mockCallCount = 0
    return { status: 'done' }
  }
  const res = await fetch(`${API_BASE}/papers/${job_id}/status`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function normalizeArxivId(raw: string): string {
  const trimmed = raw.trim()
  const urlMatch = trimmed.match(/arxiv\.org\/(?:abs|pdf)\/([0-9]{4}\.[0-9]+)/)
  if (urlMatch) return urlMatch[1]
  const idMatch = trimmed.match(/^([0-9]{4}\.[0-9]+)(?:v\d+)?$/)
  if (idMatch) return idMatch[1]
  throw new Error(`Could not parse an ArXiv ID from: "${raw}"`)
}
