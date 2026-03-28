import { afterEach, describe, expect, it, vi } from 'vitest'
import { getPapers, getRelationships, rebuildClusters } from '../../../react/src/services/graphService'

const mockFetch = vi.fn()

describe('graphService', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    mockFetch.mockReset()
    vi.unstubAllGlobals()
  })

  it('getPapers returns parsed json on success', async () => {
    const payload = [{ id: 1, title: 'Paper' }]
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => payload,
    })
    vi.stubGlobal('fetch', mockFetch)

    const result = await getPapers()
    expect(result).toEqual(payload)
  })

  it('getPapers throws when response is not ok', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'boom',
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(getPapers()).rejects.toThrow('GET /papers failed: 500 boom')
  })

  it('getRelationships returns parsed json on success', async () => {
    const payload = [{ source_paper_id: 1, target_paper_id: 2, relationship_type: 'CITES', strength: 1 }]
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => payload,
    })
    vi.stubGlobal('fetch', mockFetch)

    const result = await getRelationships()
    expect(result).toEqual(payload)
  })

  it('getRelationships throws when response is not ok', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 503,
      text: async () => 'down',
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(getRelationships()).rejects.toThrow('GET /relationships failed: 503 down')
  })

  it('rebuildClusters sends POST and returns parsed json', async () => {
    const payload = { status: 'ok', database: 'MINDMAP_DEV' }
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => payload,
    })
    vi.stubGlobal('fetch', mockFetch)

    const result = await rebuildClusters(7)

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url, options] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/clusters/rebuild?n_clusters=7')
    expect(options).toMatchObject({ method: 'POST' })
    expect(result).toEqual(payload)
  })

  it('rebuildClusters throws when response is not ok', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'oops',
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(rebuildClusters()).rejects.toThrow('POST /clusters/rebuild failed: 500 oops')
  })
})
