import { useState, useRef } from 'react'
import { ingestPaper, getPaperStatus } from '../services/paperService'

type Status = 'idle' | 'submitting' | 'pending' | 'processing' | 'done' | 'failed'

export function useIngest() {
  const [status, setStatus] = useState<Status>('idle')
  const [error, setError] = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  function stopPolling() {
    if (pollRef.current) clearInterval(pollRef.current)
  }

  async function submit(arxivInput: string) {
    setError(null)
    setStatus('submitting')
    try {
      const { job_id } = await ingestPaper(arxivInput)
      setStatus('pending')
      pollRef.current = setInterval(async () => {
        try {
          const res = await getPaperStatus(job_id)
          setStatus(res.status)
          if (res.status === 'done' || res.status === 'failed') {
            stopPolling()
            if (res.status === 'failed') setError(res.error ?? 'Ingestion failed')
          }
        } catch {
          stopPolling()
          setStatus('failed')
          setError('Lost connection while checking status')
        }
      }, 4000)
    } catch (e) {
      setStatus('failed')
      setError(e instanceof Error ? e.message : 'Something went wrong')
    }
  }

  function reset() {
    stopPolling()
    setStatus('idle')
    setError(null)
  }

  return { status, error, submit, reset }
}
