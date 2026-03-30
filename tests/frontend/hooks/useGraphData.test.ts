import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useGraphData } from '../../../react/src/hooks/useGraphData'

const getPapersMock = vi.fn()
const getRelationshipsMock = vi.fn()
const buildGraphMock = vi.fn()
const queryGraphMock = vi.fn()

vi.mock('../../../react/src/services/graphService', () => ({
  getPapers: (...args: unknown[]) => getPapersMock(...args),
  getRelationships: (...args: unknown[]) => getRelationshipsMock(...args),
  queryGraph: (...args: unknown[]) => queryGraphMock(...args),
}))

vi.mock('../../../react/src/utils/graphUtils', () => ({
  buildGraph: (...args: unknown[]) => buildGraphMock(...args),
}))

describe('useGraphData', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    getPapersMock.mockReset()
    getRelationshipsMock.mockReset()
    buildGraphMock.mockReset()
    queryGraphMock.mockReset()
  })

  it('starts with empty data and loading false (no auto-fetch on mount)', () => {
    const { result } = renderHook(() => useGraphData())
    expect(result.current.loading).toBe(false)
    expect(result.current.data).toEqual({ nodes: [], links: [] })
    expect(getPapersMock).not.toHaveBeenCalled()
  })

  it('search calls queryGraph and updates data', async () => {
    queryGraphMock.mockResolvedValue({
      nodes: [{ id: 1, title: 'A', authors: '', cluster_name: 'ML', cluster_id: 0 }],
      links: [{ source: 1, target: 2, kind: 'CITES', strength: 0.8 }],
    })

    const { result } = renderHook(() => useGraphData())

    await act(async () => {
      await result.current.search('transformers')
    })

    expect(result.current.loading).toBe(false)
    expect(result.current.data.nodes).toHaveLength(1)
    expect(result.current.data.links).toEqual([{ source: 1, target: 2, relationship_type: 'CITES', strength: 0.8, reason: undefined }])
  })

  it('search sets empty data on failure', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    queryGraphMock.mockRejectedValue(new Error('boom'))

    const { result } = renderHook(() => useGraphData())
    await act(async () => {
      await result.current.search('fail query')
    })

    expect(result.current.data).toEqual({ nodes: [], links: [] })
    expect(errSpy).toHaveBeenCalled()
  })

  it('reload calls getPapers and getRelationships', async () => {
    getPapersMock.mockResolvedValue([{ id: 1 }])
    getRelationshipsMock.mockResolvedValue([{ source_paper_id: 1, target_paper_id: 2 }])
    buildGraphMock.mockReturnValue({ nodes: [{ id: 1 }], links: [{ source: 1, target: 2 }] })

    const { result } = renderHook(() => useGraphData())

    await act(async () => {
      await result.current.reload()
    })

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data.nodes).toEqual([{ id: 1 }])
  })

  it('sets empty data on reload failure', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    getPapersMock.mockRejectedValue(new Error('boom'))
    getRelationshipsMock.mockResolvedValue([])

    const { result } = renderHook(() => useGraphData())
    await act(async () => {
      await result.current.reload()
    })

    expect(result.current.data).toEqual({ nodes: [], links: [] })
    expect(errSpy).toHaveBeenCalled()
  })
})
