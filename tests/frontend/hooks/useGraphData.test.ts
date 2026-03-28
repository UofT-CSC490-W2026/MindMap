import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useGraphData } from '../../../react/src/hooks/useGraphData'

const getPapersMock = vi.fn()
const getRelationshipsMock = vi.fn()
const buildGraphMock = vi.fn()

vi.mock('../../../react/src/services/graphService', () => ({
  getPapers: (...args: unknown[]) => getPapersMock(...args),
  getRelationships: (...args: unknown[]) => getRelationshipsMock(...args),
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
  })

  it('loads graph data on mount', async () => {
    getPapersMock.mockResolvedValue([{ id: 1 }])
    getRelationshipsMock.mockResolvedValue([{ source_paper_id: 1, target_paper_id: 2 }])
    buildGraphMock.mockReturnValue({ nodes: [{ id: 1 }], links: [{ source: 1, target: 2 }] })

    const { result } = renderHook(() => useGraphData())
    expect(result.current.loading).toBe(true)

    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data.nodes).toEqual([{ id: 1 }])
    expect(result.current.data.links).toEqual([{ source: 1, target: 2 }])
  })

  it('sets empty data on load failure', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    getPapersMock.mockRejectedValue(new Error('boom'))
    getRelationshipsMock.mockResolvedValue([])

    const { result } = renderHook(() => useGraphData())
    await waitFor(() => expect(result.current.loading).toBe(false))

    expect(result.current.data).toEqual({ nodes: [], links: [] })
    expect(errSpy).toHaveBeenCalled()
  })

  it('reload refreshes data', async () => {
    getPapersMock
      .mockResolvedValueOnce([{ id: 1 }])
      .mockResolvedValueOnce([{ id: 3 }])
    getRelationshipsMock
      .mockResolvedValueOnce([{ source_paper_id: 1, target_paper_id: 2 }])
      .mockResolvedValueOnce([{ source_paper_id: 3, target_paper_id: 4 }])
    buildGraphMock
      .mockReturnValueOnce({ nodes: [{ id: 1 }], links: [{ source: 1, target: 2 }] })
      .mockReturnValueOnce({ nodes: [{ id: 3 }], links: [{ source: 3, target: 4 }] })

    const { result } = renderHook(() => useGraphData())
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.data.nodes).toEqual([{ id: 1 }])

    await act(async () => {
      await result.current.reload()
    })

    expect(result.current.data.nodes).toEqual([{ id: 3 }])
  })
})
