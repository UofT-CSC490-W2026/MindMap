import { useState, useRef, useEffect } from 'react'
import type { GraphNode } from '../types/graph'

const API_BASE = import.meta.env.VITE_API_URL ?? 'https://notsakura--mindmap-pipeline-fastapi-app.modal.run'

interface Message {
  role: 'user' | 'assistant'
  content: string
}

interface PaperSummary {
  research_question?: string
  methods?: string | string[]
  main_claims?: string | string[]
  key_findings?: string | string[]
  limitations?: string | string[]
  conclusion?: string
}

interface Props {
  paper: GraphNode
  lightMode: boolean
  onClose: () => void
}

function formatField(value: string | string[] | undefined): string {
  if (!value) return '—'
  if (Array.isArray(value)) return value.length > 0 ? value.join(' • ') : '—'
  return value
}

export default function PaperPanel({ paper, lightMode, onClose }: Props) {
  const accent = lightMode ? '#0070f3' : '#64ffda'
  const bg = lightMode ? '#ffffff' : '#0d1b2e'
  const border = lightMode ? 'rgba(0,0,0,0.08)' : 'rgba(100,255,218,0.12)'
  const textPrimary = lightMode ? '#1a202c' : '#e6f0ff'
  const textSecondary = lightMode ? '#4a5568' : '#8892b0'
  const inputBg = lightMode ? '#f7f9fc' : '#112240'

  const [summary, setSummary] = useState<PaperSummary | null>(null)
  const [summaryLoading, setSummaryLoading] = useState(true)
  const [summaryError, setSummaryError] = useState(false)

  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [sessionId, setSessionId] = useState<string | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    setMessages([])
    setInput('')
    setSessionId(null)
    setSummary(null)
    setSummaryError(false)
    setSummaryLoading(true)

    fetch(`${API_BASE}/papers/summary/${paper.id}`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(data => {
        if (data && data.found === false) {
          setSummaryError(true)
        } else {
          setSummary(data)
        }
      })
      .catch(err => { console.error('Summary fetch failed:', err); setSummaryError(true) })
      .finally(() => setSummaryLoading(false))
  }, [paper.id])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  async function sendMessage() {
    const text = input.trim()
    if (!text || loading) return
    const userMsg: Message = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)
    try {
      const res = await fetch(`${API_BASE}/api/paper-chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          paper_id: paper.id,
          question: text,
          session_id: sessionId,
        }),
      })
      const data = await res.json()
      if (data.session_id) setSessionId(data.session_id)
      const reply = data.answer ?? data.reply ?? 'No response.'
      setMessages(prev => [...prev, { role: 'assistant', content: reply }])
    } catch {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Something went wrong. Please try again.' }])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div
      style={{ position: 'fixed', inset: 0, zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'rgba(0,0,0,0.55)', backdropFilter: 'blur(4px)' }}
      onClick={onClose}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{ width: 580, maxWidth: '95vw', height: '85vh', background: bg, borderRadius: 16, border: `1px solid ${border}`, boxShadow: '0 24px 64px rgba(0,0,0,0.4)', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}
      >
        {/* Header */}
        <div style={{ padding: '18px 20px 14px', borderBottom: `1px solid ${border}`, flexShrink: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12 }}>
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 15, fontWeight: 700, color: textPrimary, lineHeight: 1.4, marginBottom: 4 }}>{paper.title}</div>
              <div style={{ fontSize: 12, color: textSecondary }}>{paper.authors} · {paper.year} · {paper.citations.toLocaleString()} citations</div>
            </div>
            <button type="button" onClick={onClose} style={{ background: 'none', border: 'none', color: textSecondary, cursor: 'pointer', fontSize: 20, lineHeight: 1, flexShrink: 0, padding: 2 }}>×</button>
          </div>
        </div>

        {/* Scrollable body */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px 20px 0' }}>

          {/* Summary section */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginBottom: 28 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Paper Summary</div>

            {summaryLoading && (
              <div style={{ fontSize: 13, color: textSecondary, fontStyle: 'italic', padding: '12px 0' }}>Loading summary…</div>
            )}

            {summaryError && !summaryLoading && (
              <div style={{ fontSize: 13, color: textSecondary, fontStyle: 'italic', padding: '12px 0' }}>
                No summary available yet for this paper.
              </div>
            )}

            {summary && !summaryLoading && (
              <>
                {(
                  [
                    { label: 'Research Question', key: 'research_question' as const, emoji: '🔍' },
                    { label: 'Methods', key: 'methods' as const, emoji: '🔬' },
                    { label: 'Key Findings', key: 'key_findings' as const, emoji: '💡' },
                    { label: 'Conclusion', key: 'conclusion' as const, emoji: '✅' },
                  ]
                ).map(({ label, key, emoji }) => (
                  <div key={key} style={{ padding: '12px 14px', background: lightMode ? 'rgba(0,112,243,0.04)' : 'rgba(100,255,218,0.04)', borderRadius: 10, borderLeft: `3px solid ${accent}` }}>
                    <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>{emoji} {label}</div>
                    <p style={{ fontSize: 13, color: textPrimary, lineHeight: 1.7, margin: 0 }}>{formatField(summary[key])}</p>
                  </div>
                ))}
              </>
            )}
          </div>

          {/* Divider */}
          <div style={{ borderTop: `1px solid ${border}`, marginBottom: 20 }} />

          {/* Chat section */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginBottom: 8 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 10 }}>
              💬 Chat with GPT-4o
            </div>

            {messages.length === 0 && (
              <div style={{ fontSize: 13, color: textSecondary, fontStyle: 'italic', textAlign: 'center', padding: '16px 0' }}>
                Ask anything about this paper based on the summary above.
              </div>
            )}

            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {messages.map((m, i) => (
                <div key={i} style={{ alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start', maxWidth: '85%', padding: '10px 14px', borderRadius: m.role === 'user' ? '16px 16px 4px 16px' : '16px 16px 16px 4px', background: m.role === 'user' ? accent : (lightMode ? '#f0f4f8' : '#112240'), color: m.role === 'user' ? (lightMode ? '#fff' : '#0a192f') : textPrimary, fontSize: 13, lineHeight: 1.6 }}>
                  {m.content}
                </div>
              ))}
              {loading && (
                <div style={{ alignSelf: 'flex-start', padding: '10px 14px', borderRadius: '16px 16px 16px 4px', background: lightMode ? '#f0f4f8' : '#112240', fontSize: 13, color: textSecondary }}>
                  Thinking…
                </div>
              )}
            </div>
            <div ref={bottomRef} />
          </div>

          <div style={{ height: 80 }} />
        </div>

        {/* Chat input */}
        <div style={{ padding: '12px 16px', borderTop: `1px solid ${border}`, flexShrink: 0, display: 'flex', gap: 8, background: bg }}>
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendMessage()}
            placeholder="Ask about this paper…"
            style={{ flex: 1, padding: '9px 14px', borderRadius: 8, border: `1px solid ${border}`, background: inputBg, color: textPrimary, fontSize: 13, outline: 'none' }}
          />
          <button type="button" onClick={sendMessage} disabled={loading || !input.trim()}
            style={{ padding: '9px 16px', borderRadius: 8, border: 'none', background: accent, color: lightMode ? '#fff' : '#0a192f', fontWeight: 600, fontSize: 13, cursor: loading ? 'not-allowed' : 'pointer', opacity: loading || !input.trim() ? 0.5 : 1 }}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  )
}
