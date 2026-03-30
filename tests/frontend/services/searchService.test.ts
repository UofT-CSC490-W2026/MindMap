import { afterEach, describe, expect, it, vi } from 'vitest'
import { searchPapers } from '../../../react/src/services/searchService'

const mockFetch = vi.fn()

describe('searchService', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    mockFetch.mockReset()
  })

  it('searchPapers returns parsed json on success', async () => {
    const payload = [{ title: 'Paper A', authors: ['Alice'], year: 2022, arxiv_id: '2201.00001' }]
    mockFetch.mockResolvedValue({ ok: true, json: async () => payload })
    vi.stubGlobal('fetch', mockFetch)

    const result = await searchPapers('transformers')
    expect(result).toEqual(payload)
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/search/papers')
    expect(String(url)).toContain('query=transformers')
    expect(String(url)).toContain('limit=10')
  })

  it('searchPapers uses custom limit', async () => {
    mockFetch.mockResolvedValue({ ok: true, json: async () => [] })
    vi.stubGlobal('fetch', mockFetch)

    await searchPapers('llm', 5)
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('limit=5')
  })

  it('searchPapers throws ApiError on non-ok response', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 503,
      json: async () => ({ detail: 'unavailable' }),
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(searchPapers('query')).rejects.toMatchObject({ status: 503 })
  })
})
