import { apiGet, apiPost } from './apiClient'

export type PaperDetailResponse = {
  paper_id: number
  title: string
  authors: string[]
  year?: number | null
  citations?: number | null
  arxiv_id?: string | null
  abstract?: string | null
}

export type PaperSummaryResponse = {
  paper_id: number
  research_question?: string | null
  methods: string[]
  main_claims: string[]
  key_findings: string[]
  limitations: string[]
  conclusion?: string | null
}

export type PaperChatResponse = {
  paper_id: number
  session_id: string
  answer: string
  cited_chunk_ids: number[]
  rewritten_query?: string | null
}

export async function getPaperDetail(paperId: string): Promise<PaperDetailResponse> {
  return apiGet<PaperDetailResponse>(`/papers/${encodeURIComponent(paperId)}`)
}

export async function getPaperSummary(paperId: string): Promise<PaperSummaryResponse> {
  return apiGet<PaperSummaryResponse>(`/papers/${encodeURIComponent(paperId)}/summary`)
}

export async function chatWithPaper(
  paperId: string,
  question: string,
  sessionId?: string,
): Promise<PaperChatResponse> {
  return apiPost<PaperChatResponse>(`/papers/${encodeURIComponent(paperId)}/chat`, {
    question,
    session_id: sessionId ?? null,
  })
}
