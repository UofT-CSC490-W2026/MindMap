import { useState, useRef, useEffect } from 'react'
import type { GraphNode } from '../types/graph'
import {
  getPaperDetail,
  getPaperSummary,
  chatWithPaper,
  type PaperDetailResponse,
  type PaperSummaryResponse,
} from '../services/paperDetailService'

interface Message {
  role: 'user' | 'assistant'
  content: string
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

  const [detail, setDetail] = useState<PaperDetailResponse | null>(null)
  const [summary, setSummary] = useState<PaperSummaryResponse | null>(null)
  const [summaryLoading, setSummaryLoading] = useState(false)
  const [summaryError, setSummaryError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'summary' | 'chat'>('summary')
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [chatLoading, setChatLoading] = useState(false)
  const [sessionId, setSessionId] = useState<string | undefined>(undefined)

  const bottomRef = useRef<HTMLDivElement>(null)

  // Load paper detail on mount / paper change
  useEffect(() => {
    setDetail(null)
    setSummary(null)
    setSummaryError(null)
    setMessages([])
    setInput('')
    setSessionId(undefined)

    void getPaperDetail(String(paper.id))
      .then(setDetail)
      .catch(() => {/* detail is optional enhancement */})
  }, [paper.id])

  // Load summary when summary tab is opened
  useEffect(() => {
    if (activeTab !== 'summary' || summary || summaryLoading) return
    setSummaryLoading(true)
    setSummaryError(null)
    void getPaperSummary(String(paper.id))
      .then(setSummary)
      .catch(() => setSummaryError('Summary not available for this paper.'))
      .finally(() => setSummaryLoading(false))
  }, [activeTab, paper.id, summary, summaryLoading])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  async function sendMessage() {
    const text = input.trim()
    if (!text || chatLoading) return
    const userMsg: Message = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setChatLoading(true)
    try {
      const res = await chatWithPaper(String(paper.id), text, sessionId)
      setSessionId(res.session_id)
      setMessages(prev => [...prev, { role: 'assistant', content: res.answer }])
    } catch {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Something went wrong. Please try again.' }])
    } finally {
      setChatLoading(false)
    }
  }

  const displayDetail = detail ?? {
    title: paper.title,
    authors: paper.authors ? [paper.authors] : [],
    year: paper.year,
    citations: paper.citations,
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
              <div style={{ fontSize: 15, fontWeight: 700, color: textPrimary, lineHeight: 1.4, marginBottom: 4 }}>{displayDetail.title}</div>
              <div style={{ fontSize: 12, color: textSecondary }}>
                {Array.isArray(displayDetail.authors) ? displayDetail.authors.slice(0, 3).join(', ') : displayDetail.authors}
                {displayDetail.year ? ` · ${displayDetail.year}` : ''}
                {displayDetail.citations != null ? ` · ${displayDetail.citations.toLocaleString()} citations` : ''}
              </div>
              {detail?.abstract && (
                <p style={{ fontSize: 12, color: textSecondary, lineHeight: 1.6, marginTop: 8, marginBottom: 0 }}>{detail.abstract}</p>
              )}
            </div>
            <button type="button" onClick={onClose} style={{ background: 'none', border: 'none', color: textSecondary, cursor: 'pointer', fontSize: 20, lineHeight: 1, flexShrink: 0, padding: 2 }}>×</button>
          </div>

          {/* Tabs */}
          <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
            {(['summary', 'chat'] as const).map(tab => (
              <button
                key={tab}
                type="button"
                onClick={() => setActiveTab(tab)}
                style={{
                  padding: '5px 14px', borderRadius: 6, border: `1px solid ${activeTab === tab ? accent : border}`,
                  background: activeTab === tab ? `${accent}18` : 'transparent',
                  color: activeTab === tab ? accent : textSecondary,
                  fontSize: 12, fontWeight: 600, cursor: 'pointer', textTransform: 'capitalize',
                }}
              >
                {tab === 'chat' ? '💬 Chat' : '📄 Summary'}
              </button>
            ))}
          </div>
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px 20px 0' }}>
          {activeTab === 'summary' && (
            <>
              {summaryLoading && (
                <div style={{ color: textSecondary, fontSize: 13, fontStyle: 'italic' }}>Loading summary…</div>
              )}
              {summaryError && (
                <div style={{ color: textSecondary, fontSize: 13, fontStyle: 'italic' }}>{summaryError}</div>
              )}
              {summary && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Paper Summary</div>
                  {([
                    { label: 'Research Question', key: 'research_question' as const, emoji: '🔍' },
                    { label: 'Methods', key: 'methods' as const, emoji: '🔬' },
                    { label: 'Key Findings', key: 'key_findings' as const, emoji: '💡' },
                    { label: 'Conclusion', key: 'conclusion' as const, emoji: '✅' },
                  ]).filter(s => summary[s.key]).map(({ label, key, emoji }) => (
                    <div key={key} style={{ padding: '12px 14px', background: lightMode ? 'rgba(0,112,243,0.04)' : 'rgba(100,255,218,0.04)', borderRadius: 10, borderLeft: `3px solid ${accent}` }}>
                      <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>{emoji} {label}</div>
                      <p style={{ fontSize: 13, color: textPrimary, lineHeight: 1.7, margin: 0 }}>{formatField(summary[key] ?? undefined)}</p>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {activeTab === 'chat' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 10 }}>
                💬 Chat about this paper
              </div>
              {messages.length === 0 && (
                <div style={{ fontSize: 13, color: textSecondary, fontStyle: 'italic', textAlign: 'center', padding: '16px 0' }}>
                  Ask anything about this paper.
                </div>
              )}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                {messages.map((m, i) => (
                  <div key={i} style={{ alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start', maxWidth: '85%', padding: '10px 14px', borderRadius: m.role === 'user' ? '16px 16px 4px 16px' : '16px 16px 16px 4px', background: m.role === 'user' ? accent : (lightMode ? '#f0f4f8' : '#112240'), color: m.role === 'user' ? (lightMode ? '#fff' : '#0a192f') : textPrimary, fontSize: 13, lineHeight: 1.6 }}>
                    {m.content}
                  </div>
                ))}
                {chatLoading && (
                  <div style={{ alignSelf: 'flex-start', padding: '10px 14px', borderRadius: '16px 16px 16px 4px', background: lightMode ? '#f0f4f8' : '#112240', fontSize: 13, color: textSecondary }}>
                    Thinking…
                  </div>
                )}
              </div>
              <div ref={bottomRef} />
              <div style={{ height: 80 }} />
            </div>
          )}
        </div>

        {/* Chat input — only shown in chat tab */}
        {activeTab === 'chat' && (
          <div style={{ padding: '12px 16px', borderTop: `1px solid ${border}`, flexShrink: 0, display: 'flex', gap: 8, background: bg }}>
            <input
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && !e.shiftKey && void sendMessage()}
              placeholder="Ask about this paper…"
              style={{ flex: 1, padding: '9px 14px', borderRadius: 8, border: `1px solid ${border}`, background: inputBg, color: textPrimary, fontSize: 13, outline: 'none' }}
            />
            <button type="button" onClick={() => void sendMessage()} disabled={chatLoading || !input.trim()}
              style={{ padding: '9px 16px', borderRadius: 8, border: 'none', background: accent, color: lightMode ? '#fff' : '#0a192f', fontWeight: 600, fontSize: 13, cursor: chatLoading ? 'not-allowed' : 'pointer', opacity: chatLoading || !input.trim() ? 0.5 : 1 }}
            >
              Send
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
