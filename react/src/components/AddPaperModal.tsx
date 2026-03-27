import { useState, useEffect, useRef } from 'react'
import { useIngest } from '../hooks/useIngest'

type Props = { onClose: () => void; onSuccess: () => void }

const LABELS: Record<string, string> = {
  idle:       'Add to MindMap',
  submitting: 'Submitting…',
  pending:    'Queued — waiting for pipeline…',
  processing: 'Processing paper…',
  done:       'Paper added!',
  failed:     'Failed',
}

export default function AddPaperModal({ onClose, onSuccess }: Props) {
  console.log('AddPaperModal rendering')
  const [input, setInput] = useState('')
  const { status, error, submit, reset } = useIngest()
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => { inputRef.current?.focus() }, [])

  // Auto-close on success after a beat
  useEffect(() => {
    if (status === 'done') {
      const t = setTimeout(() => { onSuccess(); onClose() }, 1500)
      return () => clearTimeout(t)
    }
  }, [status, onClose, onSuccess])

  const busy = ['submitting', 'pending', 'processing'].includes(status)
  const done = status === 'done'

  return (
    <div
      onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
      style={{
        position: 'fixed',
        top: 0, left: 0, right: 0, bottom: 0,
        zIndex: 2000,
        background: 'rgba(10,25,47,0.85)',
        backdropFilter: 'blur(4px)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        minHeight: '100vh',
      }}
    >
      <div style={{
        background: '#0d2137',
        border: '1px solid rgba(100,255,218,0.18)',
        borderRadius: 14,
        padding: '32px 28px',
        width: '100%', maxWidth: 480,
        boxShadow: '0 24px 64px rgba(0,0,0,0.6)',
      }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
          <div>
            <div style={{ color: '#e6f0ff', fontWeight: 600, fontSize: 16 }}>Add paper</div>
            <div style={{ color: '#8892b0', fontSize: 12, marginTop: 3 }}>
              Paste an ArXiv URL or ID
            </div>
          </div>
          <button onClick={onClose} type="button" style={{
            background: 'none', border: 'none', color: '#8892b0',
            cursor: 'pointer', fontSize: 20, lineHeight: 1, padding: 4,
          }}>✕</button>
        </div>

        {/* Input */}
        <input
          ref={inputRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter' && !busy && input.trim()) submit(input) }}
          placeholder="e.g. 2310.06825 or arxiv.org/abs/2310.06825"
          disabled={busy || done}
          style={{
            width: '100%', boxSizing: 'border-box',
            background: 'rgba(255,255,255,0.04)',
            border: `1px solid ${error ? 'rgba(255,100,100,0.4)' : 'rgba(100,255,218,0.18)'}`,
            borderRadius: 8, padding: '10px 14px',
            color: '#ccd6f6', fontSize: 14,
            outline: 'none', marginBottom: 12,
          }}
        />

        {/* Status / error */}
        {(busy || done) && !error && (
          <div style={{
            fontSize: 12, color: done ? '#64ffda' : 'rgba(100,255,218,0.6)',
            marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8,
          }}>
            {!done && <Spinner />}
            {LABELS[status]}
          </div>
        )}
        {error && (
          <div style={{ fontSize: 12, color: '#ff6b6b', marginBottom: 12 }}>
            {error}{' '}
            <button onClick={reset} type="button"
              style={{ color: '#64ffda', background: 'none', border: 'none', cursor: 'pointer', fontSize: 12 }}>
              Try again
            </button>
          </div>
        )}

        {/* CTA */}
        <button
          type="button"
          disabled={busy || done || !input.trim()}
          onClick={() => submit(input)}
          style={{
            width: '100%', padding: '10px 0',
            background: busy || done ? 'rgba(100,255,218,0.08)' : 'rgba(100,255,218,0.12)',
            border: '1px solid rgba(100,255,218,0.35)',
            borderRadius: 8, color: '#64ffda',
            fontWeight: 600, fontSize: 14, cursor: busy || done ? 'not-allowed' : 'pointer',
            transition: 'background 0.15s',
          }}
        >
          {LABELS[status]}
        </button>
      </div>
    </div>
  )
}

function Spinner() {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" style={{ animation: 'spin 1s linear infinite' }}>
      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
      <circle cx="6" cy="6" r="4.5" fill="none" stroke="rgba(100,255,218,0.3)" strokeWidth="1.5"/>
      <path d="M6 1.5A4.5 4.5 0 0 1 10.5 6" fill="none" stroke="#64ffda" strokeWidth="1.5" strokeLinecap="round"/>
    </svg>
  )
}
