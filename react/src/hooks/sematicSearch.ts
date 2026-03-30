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

type BackendPaper = {
  title?: string
  authors?: string[] | { name?: string }[]
  year?: number | null
  citation_count?: number | null
  citationCount?: number | null
  arxiv_id?: string | null
  external_url?: string | null
  url?: string | null
  externalIds?: { ArXiv?: string }
}

function extractArxivId(value: string | null | undefined): string | undefined {
  if (!value) return undefined
  const m = value.match(/arxiv\.org\/(?:abs|pdf)\/([0-9]{4}\.[0-9]{4,5})(?:v\d+)?/i)
  return m?.[1]
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
          API_BASES.map((base) => `${base}/search/papers?${params}`),
          controller.signal,
        )

        const json = await res.json()
        const rows: BackendPaper[] = Array.isArray(json)
          ? json
          : Array.isArray(json?.data)
            ? json.data
            : []

        const normalized: SearchResult[] = rows.map((p) => {
          const arxivId =
            p.externalIds?.ArXiv ??
            p.arxiv_id ??
            extractArxivId(p.external_url) ??
            extractArxivId(p.url) ??
            undefined

          const authors = Array.isArray(p.authors)
            ? p.authors
                .map((a) => (typeof a === 'string' ? { name: a } : { name: a?.name ?? '' }))
                .filter((a) => !!a.name)
            : []

          const citationCount = typeof p.citationCount === 'number'
            ? p.citationCount
            : typeof p.citation_count === 'number'
              ? p.citation_count
              : 0

          return {
            paperId: arxivId ?? (p.title || 'unknown'),
            title: p.title || 'Untitled',
            authors,
            year: typeof p.year === 'number' ? p.year : null,
            citationCount,
            externalIds: arxivId ? { ArXiv: arxivId } : undefined,
          }
        })

        setResults(normalized.filter((p) => !!p.externalIds?.ArXiv))
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
