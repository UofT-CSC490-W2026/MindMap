import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import PaperPanel from '../../../react/src/components/PaperPanel'
import type { GraphNode } from '../../../react/src/types/graph'

const paper: GraphNode = {
  id: 999,
  title: 'Test Paper',
  shortTitle: 'Test',
  authors: 'Author One',
  year: 2024,
  citations: 12,
  primaryTopic: 'Transformers',
  searchText: 'Test Paper Author One',
}

describe('PaperPanel', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('renders fallback summary for unknown paper id', () => {
    render(<PaperPanel paper={paper} lightMode={false} onClose={() => {}} />)

    expect(screen.getByText(/paper summary/i)).toBeInTheDocument()
    expect(screen.getByText(/How can Transformers techniques be applied/i)).toBeInTheDocument()
  })

  it('calls onClose when clicking backdrop', () => {
    const onClose = vi.fn()
    const { container } = render(<PaperPanel paper={paper} lightMode={false} onClose={onClose} />)
    const backdrop = container.firstChild as HTMLElement
    fireEvent.click(backdrop)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('sends chat message and renders assistant response', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      json: async () => ({ reply: 'Assistant reply' }),
    })
    vi.stubGlobal('fetch', fetchMock)

    render(<PaperPanel paper={paper} lightMode={false} onClose={() => {}} />)
    const input = screen.getByPlaceholderText(/ask about this paper/i)
    fireEvent.change(input, { target: { value: 'What is the contribution?' } })
    fireEvent.click(screen.getByRole('button', { name: /send/i }))

    await waitFor(() => expect(screen.getByText('Assistant reply')).toBeInTheDocument())
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('shows fallback assistant error message when chat request fails', async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error('network'))
    vi.stubGlobal('fetch', fetchMock)

    render(<PaperPanel paper={paper} lightMode={false} onClose={() => {}} />)
    const input = screen.getByPlaceholderText(/ask about this paper/i)
    fireEvent.change(input, { target: { value: 'Hello?' } })
    fireEvent.click(screen.getByRole('button', { name: /send/i }))

    await waitFor(() =>
      expect(screen.getByText(/Something went wrong\. Please try again\./i)).toBeInTheDocument(),
    )
  })

  it('sends chat message on Enter key', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      json: async () => ({ reply: 'Enter reply' }),
    })
    vi.stubGlobal('fetch', fetchMock)

    render(<PaperPanel paper={paper} lightMode={false} onClose={() => {}} />)
    const input = screen.getByPlaceholderText(/ask about this paper/i)
    fireEvent.change(input, { target: { value: 'Enter question' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    await waitFor(() => expect(screen.getByText('Enter reply')).toBeInTheDocument())
  })

  it('shows Thinking state while request is in flight and disables send button', async () => {
    let resolveFetch: ((value: unknown) => void) | null = null
    const fetchPromise = new Promise((resolve) => {
      resolveFetch = resolve
    })
    const fetchMock = vi.fn().mockReturnValue(fetchPromise)
    vi.stubGlobal('fetch', fetchMock)

    render(<PaperPanel paper={paper} lightMode={true} onClose={() => {}} />)
    const input = screen.getByPlaceholderText(/ask about this paper/i)
    fireEvent.change(input, { target: { value: 'Pending question' } })
    fireEvent.click(screen.getByRole('button', { name: /send/i }))

    expect(screen.getByText(/thinking/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /send/i })).toBeDisabled()

    resolveFetch?.({
      json: async () => ({ reply: 'done' }),
    })
    await waitFor(() => expect(screen.getByText('done')).toBeInTheDocument())
  })

  it('does not send when input is blank', () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    render(<PaperPanel paper={paper} lightMode={false} onClose={() => {}} />)
    fireEvent.click(screen.getByRole('button', { name: /send/i }))
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('does not send a second message while loading is true', async () => {
    let resolveFetch: ((value: unknown) => void) | null = null
    const fetchMock = vi.fn().mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveFetch = resolve
        }),
    )
    vi.stubGlobal('fetch', fetchMock)

    render(<PaperPanel paper={paper} lightMode={false} onClose={() => {}} />)
    const input = screen.getByPlaceholderText(/ask about this paper/i)
    fireEvent.change(input, { target: { value: 'First' } })
    fireEvent.click(screen.getByRole('button', { name: /send/i }))
    expect(fetchMock).toHaveBeenCalledTimes(1)

    fireEvent.change(input, { target: { value: 'Second' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(fetchMock).toHaveBeenCalledTimes(1)

    resolveFetch?.({ json: async () => ({ reply: 'done' }) })
    await waitFor(() => expect(screen.getByText('done')).toBeInTheDocument())
  })
})
