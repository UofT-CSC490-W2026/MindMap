import { useState, useRef } from 'react'
import { createIngestion, pollIngestionStatus } from '../services/ingestionService'

type Status = 'idle' | 'submitting' | 'processing' | 'done' | 'failed'

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
      const ingest = await createIngestion(arxivInput)
      const jobId = ingest.job_id
      setStatus('processing')
      pollRef.current = setInterval(async () => {
        try {
          const res = await pollIngestionStatus(jobId)
          if (res.status === 'done') {
            setStatus('done')
            stopPolling()
          } else if (res.status === 'failed') {
            setStatus('failed')
            setError(res.error ?? 'Ingestion failed')
            stopPolling()
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
