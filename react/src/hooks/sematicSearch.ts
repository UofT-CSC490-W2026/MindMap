import { useState, useEffect, useRef } from 'react'
const API_BASES = [
  import.meta.env.VITE_SEARCH_API_URL,
  import.meta.env.VITE_API_URL,
].filter(Boolean) as string[]

export interface SearchResult {
  paperId: string
  title: string
  authors: { name: string }[]
  year: number | null
  citationCount: number
  externalIds?: { ArXiv?: string }
}

export function useSemanticSearch(query: string) {
  const [results, setResults] = useState<SearchResult[]>([])
  const [loading, setLoading] = useState(false)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const q = query.trim()

    if (!q || q.length < 2) {
      setResults([])
      return
    }

    // Debounce: wait 400ms after user stops typing
    const timer = setTimeout(async () => {
      abortRef.current?.abort()
      const controller = new AbortController()
      abortRef.current = controller

      setLoading(true)
      try {
        const params = new URLSearchParams({
          query: q,
          limit: '10',                         // top 10 results
          fields: 'title,authors,year,citationCount,externalIds',
        })
        const res = await fetchWithFallback(
          API_BASES.map((base) => `${base}/papers/search?${params}`),
          controller.signal,
        )

        const json = await res.json()
        setResults((json.data ?? []).filter((p: SearchResult) => !!p.externalIds?.ArXiv))
      } catch (err) {
        if ((err as Error).name !== 'AbortError') setResults([])
      } finally {
        setLoading(false)
      }
    }, 400)

    return () => {
      clearTimeout(timer)
      abortRef.current?.abort()
    }
  }, [query])

  return { results, loading }
}

export async function fetchWithFallback(urls: string[], signal: AbortSignal): Promise<Response> {
  let lastError: unknown = null

  for (const url of urls) {
    try {
      const res = await fetch(url, { signal })
      if (res.ok) return res
      lastError = new Error(`Search failed on ${url}: ${res.status}`)
    } catch (err) {
      lastError = err
      // If the caller aborted, stop trying
      if ((err as Error).name === 'AbortError') throw err
    }
  }

  throw lastError ?? new Error('Search failed on all configured API URLs.')
}
