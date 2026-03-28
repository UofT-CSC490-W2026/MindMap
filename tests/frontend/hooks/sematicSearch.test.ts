import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { fetchWithFallback, useSemanticSearch } from '../../../react/src/hooks/sematicSearch'

const fetchMock = vi.fn()

describe('useSemanticSearch', () => {
  afterEach(() => {
    fetchMock.mockReset()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('returns empty results immediately for short query', () => {
    vi.stubGlobal('fetch', fetchMock)
    const { result } = renderHook(() => useSemanticSearch('a'))
    expect(result.current.results).toEqual([])
    expect(result.current.loading).toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('loads and filters results by ArXiv id', async () => {
    vi.stubGlobal('fetch', fetchMock)
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        data: [
          { paperId: '1', title: 'A', authors: [], year: 2020, citationCount: 1, externalIds: { ArXiv: '1706.03762' } },
          { paperId: '2', title: 'B', authors: [], year: 2020, citationCount: 1, externalIds: {} },
        ],
      }),
    })

    const { result } = renderHook(() => useSemanticSearch('transformer'))
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450))
    })

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.results).toHaveLength(1)
    expect(result.current.results[0].paperId).toBe('1')
  })

  it('handles missing data field by returning empty result list', async () => {
    vi.stubGlobal('fetch', fetchMock)
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({}),
    })
    const { result } = renderHook(() => useSemanticSearch('retrieval'))
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450))
    })
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.results).toEqual([])
  })

  it('falls back to next API base when first one fails', async () => {
    vi.stubGlobal('fetch', fetchMock)
    fetchMock
      .mockResolvedValueOnce({ ok: false, status: 500 })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [{ paperId: 'x', title: 'ok', authors: [], year: null, citationCount: 0, externalIds: { ArXiv: '2201.00001' } }] }),
      })

    const { result } = renderHook(() => useSemanticSearch('quantization'))
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450))
    })

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(result.current.results[0].paperId).toBe('x')
  })

  it('clears results when non-abort fetch error occurs', async () => {
    vi.stubGlobal('fetch', fetchMock)
    fetchMock.mockRejectedValue(new Error('network'))
    const { result, rerender } = renderHook(({ q }) => useSemanticSearch(q), {
      initialProps: { q: 'llm' },
    })

    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450))
    })
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.results).toEqual([])

    // Ensure cleanup branch runs as query changes
    rerender({ q: 'new query' })
  })

  it('does not overwrite state on AbortError', async () => {
    vi.stubGlobal('fetch', fetchMock)
    const abortErr = new Error('aborted')
    abortErr.name = 'AbortError'
    fetchMock.mockRejectedValue(abortErr)

    const { result } = renderHook(() => useSemanticSearch('transformers'))
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450))
    })
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.results).toEqual([])
  })

  it('fetchWithFallback throws default error when no urls are provided', async () => {
    await expect(fetchWithFallback([], new AbortController().signal)).rejects.toThrow(
      'Search failed on all configured API URLs.',
    )
  })
})
