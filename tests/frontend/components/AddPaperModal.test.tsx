import { fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import AddPaperModal from '../../../react/src/components/AddPaperModal'

const mockSubmit = vi.fn()
const mockReset = vi.fn()

const useIngestState = {
  status: 'idle' as 'idle' | 'submitting' | 'pending' | 'processing' | 'done' | 'failed',
  error: null as string | null,
  submit: (...args: unknown[]) => mockSubmit(...args),
  reset: () => mockReset(),
}

vi.mock('../../../react/src/hooks/useIngest', () => ({
  useIngest: () => useIngestState,
}))

describe('AddPaperModal', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    mockSubmit.mockReset()
    mockReset.mockReset()
    useIngestState.status = 'idle'
    useIngestState.error = null
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('submits input when clicking CTA', () => {
    render(<AddPaperModal onClose={() => {}} onSuccess={() => {}} />)

    const input = screen.getByPlaceholderText(/e\.g\. 2310\.06825/i)
    fireEvent.change(input, { target: { value: '1706.03762' } })
    fireEvent.click(screen.getByRole('button', { name: /add to mindmap/i }))

    expect(mockSubmit).toHaveBeenCalledWith('1706.03762')
  })

  it('submits on Enter key', () => {
    render(<AddPaperModal onClose={() => {}} onSuccess={() => {}} />)
    const input = screen.getByPlaceholderText(/e\.g\. 2310\.06825/i)
    fireEvent.change(input, { target: { value: '1706.03762' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(mockSubmit).toHaveBeenCalledWith('1706.03762')
  })

  it('calls onSuccess and onClose after done state timeout', () => {
    useIngestState.status = 'done'
    const onClose = vi.fn()
    const onSuccess = vi.fn()
    render(<AddPaperModal onClose={onClose} onSuccess={onSuccess} />)

    vi.advanceTimersByTime(1500)
    expect(onSuccess).toHaveBeenCalledTimes(1)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('shows error and reset action when failed', () => {
    useIngestState.status = 'failed'
    useIngestState.error = 'Boom'
    render(<AddPaperModal onClose={() => {}} onSuccess={() => {}} />)

    expect(screen.getByText('Boom')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /try again/i }))
    expect(mockReset).toHaveBeenCalledTimes(1)
  })

  it('shows processing label and disables CTA while busy', () => {
    useIngestState.status = 'processing'
    render(<AddPaperModal onClose={() => {}} onSuccess={() => {}} />)
    expect(screen.getAllByText(/processing paper/i).length).toBeGreaterThan(0)
    const cta = screen.getByRole('button', { name: /processing paper/i })
    expect(cta).toBeDisabled()
  })

  it('clicking close icon triggers onClose', () => {
    const onClose = vi.fn()
    render(<AddPaperModal onClose={onClose} onSuccess={() => {}} />)
    fireEvent.click(screen.getByRole('button', { name: '✕' }))
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('clicking backdrop closes modal', () => {
    const onClose = vi.fn()
    const { container } = render(<AddPaperModal onClose={onClose} onSuccess={() => {}} />)
    const backdrop = container.firstChild as HTMLElement
    fireEvent.click(backdrop)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('pressing Enter with empty input does not submit', () => {
    render(<AddPaperModal onClose={() => {}} onSuccess={() => {}} />)
    const input = screen.getByPlaceholderText(/arxiv\.org\/abs/i)
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(mockSubmit).not.toHaveBeenCalled()
  })
})
