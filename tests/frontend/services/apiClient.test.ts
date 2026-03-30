import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, apiFetch, apiGet, apiPost } from '../../../react/src/services/apiClient'

const mockFetch = vi.fn()

describe('apiClient', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    mockFetch.mockReset()
  })

  it('ApiError has correct name, status, and body', () => {
    const err = new ApiError(404, { detail: 'not found' }, 'GET /foo failed: 404')
    expect(err.name).toBe('ApiError')
    expect(err.status).toBe(404)
    expect(err.body).toEqual({ detail: 'not found' })
    expect(err.message).toBe('GET /foo failed: 404')
    expect(err).toBeInstanceOf(Error)
  })

  it('apiFetch returns parsed json on success', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => ({ id: 1 }) })

    const result = await apiFetch('/test')
    expect(result).toEqual({ id: 1 })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/test')
    expect(opts.headers['Content-Type']).toBe('application/json')
  })

  it('apiFetch throws ApiError with json body when response is not ok', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({
      ok: false,
      status: 422,
      json: async () => ({ detail: 'validation error' }),
    })

    await expect(apiFetch('/bad')).rejects.toMatchObject({
      name: 'ApiError',
      status: 422,
      body: { detail: 'validation error' },
    })
  })

  it('apiFetch falls back to text body when json parse fails on error response', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({
      ok: false,
      status: 500,
      json: async () => { throw new Error('not json') },
      text: async () => 'internal server error',
    })

    await expect(apiFetch('/fail')).rejects.toMatchObject({
      status: 500,
      body: 'internal server error',
    })
  })

  it('apiFetch throws when VITE_API_URL is not set', async () => {
    const original = import.meta.env.VITE_API_URL
    import.meta.env.VITE_API_URL = ''
    await expect(apiFetch('/x')).rejects.toThrow('VITE_API_URL is not set')
    import.meta.env.VITE_API_URL = original
  })

  it('apiFetch merges custom headers', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => ({}) })

    await apiFetch('/headers', { headers: { Authorization: 'Bearer tok' } })
    const [, opts] = mockFetch.mock.calls[0]
    expect(opts.headers['Authorization']).toBe('Bearer tok')
    expect(opts.headers['Content-Type']).toBe('application/json')
  })

  it('apiGet calls apiFetch with no method override', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => [1, 2] })

    const result = await apiGet('/items')
    expect(result).toEqual([1, 2])
    const [url] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/items')
  })

  it('apiPost sends POST with json body', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({ ok: true, json: async () => ({ created: true }) })

    const result = await apiPost('/create', { name: 'test' })
    expect(result).toEqual({ created: true })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(String(url)).toContain('/create')
    expect(opts.method).toBe('POST')
    expect(JSON.parse(opts.body)).toEqual({ name: 'test' })
  })

  it('apiFetch error message uses method from options', async () => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockResolvedValue({
      ok: false,
      status: 403,
      json: async () => 'forbidden',
    })

    await expect(apiFetch('/secure', { method: 'POST' })).rejects.toThrow('POST /secure failed: 403')
  })
})
