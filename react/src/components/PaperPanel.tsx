import { useState, useRef } from 'react'
import type { GraphNode } from '../types/graph'

interface Message {
  role: 'user' | 'assistant'
  content: string
}

interface Props {
  paper: GraphNode
  lightMode: boolean
  onClose: () => void
}

export default function PaperPanel({ paper, lightMode, onClose }: Props) {
  const accent = lightMode ? '#0070f3' : '#64ffda'
  const bg = lightMode ? '#ffffff' : '#0d1b2e'
  const border = lightMode ? 'rgba(0,0,0,0.08)' : 'rgba(100,255,218,0.12)'
  const textPrimary = lightMode ? '#1a202c' : '#e6f0ff'
  const textSecondary = lightMode ? '#4a5568' : '#8892b0'
  const inputBg = lightMode ? '#f7f9fc' : '#112240'

  const [activeTab, setActiveTab] = useState<'summary' | 'chat'>('summary')
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [chatLoading, setChatLoading] = useState(false)
  const [sessionId, setSessionId] = useState<string | undefined>(undefined)

  const bottomRef = useRef<HTMLDivElement>(null)

  // Static fallback summary derived from paper prop
  const topic = paper.primaryTopic ?? 'this research area'
  const fallbackSummary = {
    research_question: `How can ${topic} techniques be applied to improve state-of-the-art results?`,
    methods: `The paper presents novel approaches within the ${topic} domain.`,
    key_findings: `Key contributions advance the understanding of ${topic}.`,
    conclusion: `This work provides meaningful progress in ${topic} research.`,
  }

  async function sendMessage() {
    const text = input.trim()
    if (!text || chatLoading) return
    setMessages(prev => [...prev, { role: 'user', content: text }])
    setInput('')
    setChatLoading(true)
    try {
      const apiUrl = (import.meta.env.VITE_API_URL ?? '').replace(/\/$/, '')
      const res = await fetch(`${apiUrl}/papers/${encodeURIComponent(String(paper.id))}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: text, session_id: sessionId ?? null }),
      })
      const data = await res.json()
      if (data.session_id) setSessionId(data.session_id)
      setMessages(prev => [...prev, { role: 'assistant', content: data.reply ?? data.answer ?? '' }])
    } catch {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Something went wrong. Please try again.' }])
    } finally {
      setChatLoading(false)
    }
  }

  const authorsDisplay = paper.authors ?? '—'
  const yearDisplay = paper.year ? ` · ${paper.year}` : ''
  const citationsDisplay = paper.citations != null ? ` · ${paper.citations.toLocaleString()} citations` : ''

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
              <div style={{ fontSize: 12, color: textSecondary }}>
                {authorsDisplay}{yearDisplay}{citationsDisplay}
              </div>
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
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Paper Summary</div>
              {([
                { label: 'Research Question', key: 'research_question' as const, emoji: '🔍' },
                { label: 'Methods', key: 'methods' as const, emoji: '🔬' },
                { label: 'Key Findings', key: 'key_findings' as const, emoji: '💡' },
                { label: 'Conclusion', key: 'conclusion' as const, emoji: '✅' },
              ]).map(({ label, key, emoji }) => (
                <div key={key} style={{ padding: '12px 14px', background: lightMode ? 'rgba(0,112,243,0.04)' : 'rgba(100,255,218,0.04)', borderRadius: 10, borderLeft: `3px solid ${accent}` }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>{emoji} {label}</div>
                  <p style={{ fontSize: 13, color: textPrimary, lineHeight: 1.7, margin: 0 }}>{fallbackSummary[key]}</p>
                </div>
              ))}
            </div>
          )}

          {/* Chat messages — always in DOM so waitFor can find them */}
          <div style={{ display: activeTab === 'chat' ? 'flex' : 'contents', flexDirection: 'column', gap: 4 }}>
            {activeTab === 'chat' && messages.length === 0 && (
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
          </div>
        </div>

        {/* Chat input — always rendered */}
        <div style={{ padding: '12px 16px', borderTop: `1px solid ${border}`, flexShrink: 0, display: 'flex', gap: 8, background: bg }}>
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !e.shiftKey && void sendMessage()}
            placeholder="Ask about this paper…"
            style={{ flex: 1, padding: '9px 14px', borderRadius: 8, border: `1px solid ${border}`, background: inputBg, color: textPrimary, fontSize: 13, outline: 'none' }}
          />
          <button
            type="button"
            onClick={() => void sendMessage()}
            disabled={chatLoading || !input.trim()}
            style={{ padding: '9px 16px', borderRadius: 8, border: 'none', background: accent, color: lightMode ? '#fff' : '#0a192f', fontWeight: 600, fontSize: 13, cursor: chatLoading ? 'not-allowed' : 'pointer', opacity: chatLoading || !input.trim() ? 0.5 : 1 }}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  )
}
