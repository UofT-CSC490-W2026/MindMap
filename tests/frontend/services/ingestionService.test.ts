import { afterEach, describe, expect, it, vi } from 'vitest'
import { createIngestion, pollIngestionStatus } from '../../../react/src/services/ingestionService'

const mockFetch = vi.fn()

describe('ingestionService', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    mockFetch.mockReset()
  })

  it('createIngestion posts normalized arxiv id and returns response', async () => {
    const payload = { job_id: 'j1', arxiv_id: '1706.03762', status: 'processing', stage: 'bronze', bronze_status: 'pending' }
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => payload })

    const result = await createIngestion('1706.03762')
    expect(result).toEqual(payload)
    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/ingestions')
    expect(opts.method).toBe('POST')
    expect(JSON.parse(opts.body)).toEqual({ arxiv_id: '1706.03762' })
  })

  it('createIngestion normalizes arxiv URL before posting', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => ({}) })

    await createIngestion('https://arxiv.org/abs/2301.00001v2')
    const [, opts] = mockFetch.mock.calls[0]
    expect(JSON.parse(opts.body)).toEqual({ arxiv_id: '2301.00001' })
  })

  it('pollIngestionStatus fetches correct url and returns status', async () => {
    const payload = { job_id: 'j1', status: 'done' }
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => payload })

    const result = await pollIngestionStatus('j1')
    expect(result).toEqual(payload)
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/ingestions/j1')
  })

  it('pollIngestionStatus encodes job id in url', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => ({}) })

    await pollIngestionStatus('job/with/slashes')
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('job%2Fwith%2Fslashes')
  })
})
