import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  getPaperDetail,
  getPaperSummary,
  chatWithPaper,
} from '../../../react/src/services/paperDetailService'

const mockFetch = vi.fn()

describe('paperDetailService', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    mockFetch.mockReset()
  })

  it('getPaperDetail fetches correct url and returns parsed json', async () => {
    const payload = { paper_id: 42, title: 'Test', authors: ['Alice'], year: 2023 }
    mockFetch.mockResolvedValue({ ok: true, json: async () => payload })
    vi.stubGlobal('fetch', mockFetch)

    const result = await getPaperDetail('42')
    expect(result).toEqual(payload)
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/papers/42')
  })

  it('getPaperDetail encodes special characters in paperId', async () => {
    mockFetch.mockResolvedValue({ ok: true, json: async () => ({}) })
    vi.stubGlobal('fetch', mockFetch)

    await getPaperDetail('a/b')
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/papers/a%2Fb')
  })

  it('getPaperDetail throws ApiError on non-ok response', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 404,
      json: async () => ({ detail: 'not found' }),
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(getPaperDetail('99')).rejects.toMatchObject({ status: 404 })
  })

  it('getPaperSummary fetches correct url and returns parsed json', async () => {
    const payload = { paper_id: 1, methods: [], main_claims: [], key_findings: [], limitations: [] }
    mockFetch.mockResolvedValue({ ok: true, json: async () => payload })
    vi.stubGlobal('fetch', mockFetch)

    const result = await getPaperSummary('1')
    expect(result).toEqual(payload)
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/papers/1/summary')
  })

  it('getPaperSummary throws on non-ok response', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      json: async () => 'error',
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(getPaperSummary('1')).rejects.toMatchObject({ status: 500 })
  })

  it('chatWithPaper sends POST with question and session_id', async () => {
    const payload = { paper_id: 1, session_id: 'sess-1', answer: 'Yes', cited_chunk_ids: [3] }
    mockFetch.mockResolvedValue({ ok: true, json: async () => payload })
    vi.stubGlobal('fetch', mockFetch)

    const result = await chatWithPaper('1', 'What is this?', 'sess-1')
    expect(result).toEqual(payload)
    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/papers/1/chat')
    expect(opts.method).toBe('POST')
    expect(JSON.parse(opts.body)).toEqual({ question: 'What is this?', session_id: 'sess-1' })
  })

  it('chatWithPaper sends null session_id when not provided', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ paper_id: 1, session_id: 'new', answer: 'Hi', cited_chunk_ids: [] }),
    })
    vi.stubGlobal('fetch', mockFetch)

    await chatWithPaper('1', 'Hello?')
    const [, opts] = mockFetch.mock.calls[0]
    expect(JSON.parse(opts.body).session_id).toBeNull()
  })

  it('chatWithPaper throws on non-ok response', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 422,
      json: async () => ({ detail: 'bad input' }),
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(chatWithPaper('1', 'q')).rejects.toMatchObject({ status: 422 })
  })
})
