import { afterEach, describe, expect, it, vi } from 'vitest'
import { getPaperStatus, ingestPaper } from '../../../react/src/services/paperService'

const mockFetch = vi.fn()

describe('paperService', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    mockFetch.mockReset()
    vi.unstubAllGlobals()
  })

  it('ingestPaper normalizes arxiv URL and posts normalized id', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'processing', job_id: 'abc' }),
    })
    vi.stubGlobal('fetch', mockFetch)

    const result = await ingestPaper('https://arxiv.org/abs/1706.03762v5')

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url, options] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/papers/ingest')
    expect(options).toMatchObject({ method: 'POST' })
    expect(JSON.parse(options.body as string)).toEqual({ arxiv_id: '1706.03762' })
    expect(result.status).toBe('processing')
  })

  it('ingestPaper throws when no arxiv id can be parsed', async () => {
    vi.stubGlobal('fetch', mockFetch)
    await expect(ingestPaper('not-an-arxiv-id')).rejects.toThrow('Could not parse an ArXiv ID')
    expect(mockFetch).not.toHaveBeenCalled()
  })

  it('ingestPaper accepts plain arxiv id format', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'processing', job_id: 'plain' }),
    })
    vi.stubGlobal('fetch', mockFetch)

    await ingestPaper('1706.03762')
    const [, options] = mockFetch.mock.calls[0]
    expect(JSON.parse(options.body as string)).toEqual({ arxiv_id: '1706.03762' })
  })

  it('ingestPaper throws when backend response is not ok', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      text: async () => 'ingest failed',
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(ingestPaper('1706.03762')).rejects.toThrow('ingest failed')
  })

  it('getPaperStatus returns parsed status', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'done' }),
    })
    vi.stubGlobal('fetch', mockFetch)

    const result = await getPaperStatus('job-1')
    expect(result).toEqual({ status: 'done' })
    expect(String(mockFetch.mock.calls[0][0])).toContain('/papers/job-1/status')
  })

  it('getPaperStatus throws when backend fails', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      text: async () => 'bad status',
    })
    vi.stubGlobal('fetch', mockFetch)

    await expect(getPaperStatus('job-2')).rejects.toThrow('bad status')
  })
})
