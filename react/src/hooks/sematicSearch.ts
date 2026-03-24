import { useState, useEffect, useRef } from 'react'

export interface SearchResult {
  paperId: string
  title: string
  authors: { name: string }[]
  year: number | null
  citationCount: number
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
          limit: '3',                          // top 3 results
          fields: 'title,authors,year,citationCount',
        })

        const res = await fetch(
          `https://api.semanticscholar.org/graph/v1/paper/search?${params}`,
          { signal: controller.signal }
        )

        const json = await res.json()
        setResults(json.data ?? [])
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