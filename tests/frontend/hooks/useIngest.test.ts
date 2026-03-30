import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useIngest } from '../../../react/src/hooks/useIngest'

const ingestPaperMock = vi.fn()
const getPaperStatusMock = vi.fn()

vi.mock('../../../react/src/services/paperService', () => ({
  ingestPaper: (...args: unknown[]) => ingestPaperMock(...args),
  getPaperStatus: (...args: unknown[]) => getPaperStatusMock(...args),
}))

describe('useIngest', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    ingestPaperMock.mockReset()
    getPaperStatusMock.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('transitions to done after polling succeeds', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'processing', job_id: 'job-1' })
    getPaperStatusMock.mockResolvedValue({ status: 'done' })

    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    expect(result.current.status).toBe('pending')

    await act(async () => {
      vi.advanceTimersByTime(4000)
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(result.current.status).toBe('done')
    expect(result.current.error).toBeNull()
  })

  it('fails when ingest response has no job_id', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'processing' })

    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toContain('Missing job id')
  })

  it('fails when ingest returns failed status', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'failed', error: 'Bronze failed' })
    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toContain('Bronze failed')
  })

  it('uses fallback message when ingest failed status has no error', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'failed' })
    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toBe('Bronze ingestion failed')
  })

  it('fails when status polling throws', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'processing', job_id: 'job-2' })
    getPaperStatusMock.mockRejectedValue(new Error('network'))

    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    await act(async () => {
      vi.advanceTimersByTime(4000)
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toContain('Lost connection')
  })

  it('handles processing status before finishing', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'processing', job_id: 'job-3' })
    getPaperStatusMock
      .mockResolvedValueOnce({ status: 'processing' })
      .mockResolvedValueOnce({ status: 'done' })

    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    await act(async () => {
      vi.advanceTimersByTime(4000)
      await Promise.resolve()
    })
    expect(result.current.status).toBe('processing')

    await act(async () => {
      vi.advanceTimersByTime(4000)
      await Promise.resolve()
    })
    expect(result.current.status).toBe('done')
  })

  it('handles failed status polling with fallback message', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'processing', job_id: 'job-4' })
    getPaperStatusMock.mockResolvedValue({ status: 'failed' })

    const { result } = renderHook(() => useIngest())
    await act(async () => {
      await result.current.submit('1706.03762')
    })

    await act(async () => {
      vi.advanceTimersByTime(4000)
      await Promise.resolve()
    })
    expect(result.current.status).toBe('failed')
    expect(result.current.error).toBe('Ingestion failed')
  })

  it('reset returns hook state to idle and clears error', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'failed', error: 'Boom' })
    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })
    expect(result.current.status).toBe('failed')

    act(() => {
      result.current.reset()
    })
    expect(result.current.status).toBe('idle')
    expect(result.current.error).toBeNull()
  })

  it('uses generic error message when submit throws non-Error value', async () => {
    ingestPaperMock.mockRejectedValue('bad')
    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toBe('Something went wrong')
  })

  it('uses error.message when submit throws an Error object', async () => {
    ingestPaperMock.mockRejectedValue(new Error('backend down'))
    const { result } = renderHook(() => useIngest())

    await act(async () => {
      await result.current.submit('1706.03762')
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toBe('backend down')
  })

  it('uses provided error from failed polling status', async () => {
    ingestPaperMock.mockResolvedValue({ status: 'processing', job_id: 'job-5' })
    getPaperStatusMock.mockResolvedValue({ status: 'failed', error: 'worker failed' })

    const { result } = renderHook(() => useIngest())
    await act(async () => {
      await result.current.submit('1706.03762')
    })
    await act(async () => {
      vi.advanceTimersByTime(4000)
      await Promise.resolve()
    })

    expect(result.current.status).toBe('failed')
    expect(result.current.error).toBe('worker failed')
  })
})
