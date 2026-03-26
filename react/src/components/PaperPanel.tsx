import { useState, useRef, useEffect } from 'react'
import type { GraphNode } from '../types/graph'

interface Message {
  role: 'user' | 'assistant'
  content: string
}

interface PaperSummary {
  research_question: string
  methods: string
  key_findings: string
  conclusion: string
}

interface Props {
  paper: GraphNode
  lightMode: boolean
  onClose: () => void
}

const SUMMARIES: Record<number, PaperSummary> = {
  2: {
    research_question: 'How can model quantization techniques be systematically surveyed and categorized to guide practitioners deploying deep neural networks in image classification?',
    methods: 'The authors perform a comprehensive literature review of quantization methods, categorizing them by bit-width, granularity (layer-wise vs. channel-wise), and training strategy (QAT vs. PTQ). They analyze trade-offs across hardware targets including GPUs, FPGAs, and edge accelerators.',
    key_findings: 'Post-training quantization to 8-bit incurs minimal accuracy loss on most CNN architectures. Sub-4-bit quantization requires quantization-aware training to remain competitive. Hardware-aware methods that co-design quantization with target accelerators yield the best efficiency gains.',
    conclusion: 'Quantization is a mature and practical compression technique. The survey provides a structured taxonomy and identifies open challenges in ultra-low-bit and mixed-precision quantization for emerging architectures.',
  },
  101: {
    research_question: 'Can large generative transformer models be accurately quantized to 4-bit weights post-training without retraining, at billion-parameter scale?',
    methods: 'GPTQ uses a layer-wise second-order quantization approach based on the Optimal Brain Quantization (OBQ) framework. It quantizes weights one-by-one, updating remaining weights to compensate for quantization error using approximate inverse Hessians computed from a small calibration set.',
    key_findings: 'GPTQ achieves near-lossless 4-bit quantization of GPT models up to 175B parameters in a few GPU-hours. It enables running OPT-175B and BLOOM-176B on a single GPU for the first time, with less than 1% perplexity degradation versus FP16.',
    conclusion: 'Second-order weight quantization scales to the largest publicly available LLMs and makes 4-bit inference practical, opening the door to consumer-grade deployment of frontier models.',
  },
  102: {
    research_question: 'How can 8-bit matrix multiplication be applied to transformers at scale without degrading model quality, given the presence of large-magnitude outlier features?',
    methods: 'The paper introduces a mixed-precision decomposition: outlier features (identified via a threshold) are kept in 16-bit while the remaining 99.9% of values are quantized to Int8. A vector-wise quantization scheme normalizes each row and column independently before multiplication.',
    key_findings: 'LLM.int8() reduces memory by ~2× for large models with no measurable perplexity degradation. Outlier features emerge predictably at scale (≥6.7B parameters) and must be handled explicitly — naive Int8 quantization fails above this threshold.',
    conclusion: 'Emergent outlier features are a fundamental property of large transformers. Handling them with mixed-precision decomposition makes 8-bit inference viable for production LLMs without any fine-tuning.',
  },
  201: {
    research_question: 'How can both weights and activations of LLMs be quantized to 8-bit without accuracy loss, given that activation outliers make direct quantization difficult?',
    methods: 'SmoothQuant migrates quantization difficulty from activations to weights by applying a mathematically equivalent per-channel scaling transformation. A smoothing factor derived from activation statistics is absorbed into the preceding linear layer, making activations easier to quantize.',
    key_findings: 'SmoothQuant achieves W8A8 quantization of LLMs up to 530B parameters with accuracy matching FP16. It delivers up to 1.56× speedup and 2× memory reduction on real hardware, and is the first method to enable practical W8A8 inference at this scale.',
    conclusion: 'Migrating quantization difficulty from activations to weights via a simple scaling transform is a highly effective and hardware-friendly strategy for deploying large transformers at 8-bit precision.',
  },
  202: {
    research_question: 'Which weight channels are most salient for LLM performance, and can activation-aware analysis guide more accurate weight-only quantization?',
    methods: 'AWQ observes that only ~1% of weight channels are salient (corresponding to large activation magnitudes). It protects these channels by applying per-channel scaling before quantization, searching for optimal scales that minimize quantization error on a small calibration set without backpropagation.',
    key_findings: 'AWQ outperforms GPTQ on instruction-tuned and multi-modal LLMs while being simpler and faster to apply. It generalizes well across model families (LLaMA, OPT, BLOOM) and achieves near-lossless 4-bit compression with hardware-efficient kernels.',
    conclusion: 'Activation-aware weight protection is a lightweight yet powerful principle for LLM quantization, enabling accurate low-bit deployment without gradient-based optimization.',
  },
  301: {
    research_question: 'How can post-training quantization be applied to diffusion models, which have unique sensitivity to numerical precision due to their iterative denoising process?',
    methods: 'Q-Diffusion analyzes the time-step-dependent activation distributions in diffusion U-Nets and proposes time-step-aware calibration. It uses a shortcut-splitting quantization scheme to handle residual connections and calibrates using a small set of generated samples.',
    key_findings: 'Q-Diffusion achieves 4-bit weight and 8-bit activation quantization of latent diffusion models with FID scores close to full-precision baselines. Standard PTQ methods fail on diffusion models without time-step-aware calibration.',
    conclusion: 'Diffusion models require specialized quantization strategies that account for their iterative, time-conditioned nature. Q-Diffusion establishes a practical baseline for compressed generative image synthesis.',
  },
  302: {
    research_question: 'Can post-training quantization be made efficient and accurate for diffusion probabilistic models used in image generation?',
    methods: "The paper proposes a PTQ pipeline tailored to diffusion models, using a calibration set drawn from the model's own generative process. It applies mixed-precision quantization guided by sensitivity analysis of each layer's contribution to final sample quality.",
    key_findings: 'The method achieves competitive FID scores at 8-bit precision with minimal calibration overhead. Sensitivity-guided mixed precision outperforms uniform quantization, particularly for attention layers.',
    conclusion: 'Efficient PTQ for diffusion models is achievable with careful calibration data selection and sensitivity-aware bit allocation, enabling deployment of high-quality generative models on resource-constrained hardware.',
  },
  401: {
    research_question: 'How can post-training quantization be made both accurate and hardware-efficient for large-scale transformer models without expensive retraining?',
    methods: 'ZeroQuant introduces a fine-grained hardware-aware quantization scheme with group-wise quantization for weights and token-wise quantization for activations. It uses a layer-by-layer knowledge distillation step (LKD) to recover accuracy lost during quantization.',
    key_findings: 'ZeroQuant achieves W8A8 quantization of GPT-3-scale models with less than 1% accuracy drop. The custom CUDA kernels deliver up to 5.19× speedup over FP16 on A100 GPUs. LKD is critical for maintaining quality at INT4.',
    conclusion: 'Combining hardware-aware quantization granularity with lightweight distillation makes ZeroQuant a practical and scalable solution for deploying large transformers at reduced precision.',
  },
  402: {
    research_question: 'How can quantization research be made more reproducible and practically deployable across diverse hardware backends?',
    methods: 'MQBench introduces a unified benchmarking framework that decouples the quantization algorithm from hardware deployment. It implements a hardware-aware simulation mode that accurately models the quantization behavior of specific backends (TensorRT, SNPE, etc.) during training.',
    key_findings: 'Existing quantization methods show highly inconsistent rankings across hardware backends — a method that wins on one backend may lose on another. Hardware-aware simulation significantly closes the gap between simulated and real deployed accuracy.',
    conclusion: 'Reproducible and deployable quantization research requires hardware-faithful simulation. MQBench provides the infrastructure to fairly compare methods and predict real-world performance before deployment.',
  },
  403: {
    research_question: 'Can model quantization be exploited to enhance the transferability of adversarial attacks across different model precisions?',
    methods: 'The paper proposes Quantization Aware Attack (QAA), which crafts adversarial examples by simulating quantization during the attack optimization process. This exposes the attack to quantization-induced gradient noise, improving transferability to quantized victim models.',
    key_findings: 'QAA significantly improves adversarial transferability to quantized models compared to attacks crafted on full-precision models. The method reveals a previously overlooked security vulnerability introduced by model compression.',
    conclusion: 'Quantization is not just a compression tool — it changes the adversarial robustness landscape. Security evaluations of deployed models must account for the quantization pipeline used in production.',
  },
}

function getFallbackSummary(paper: GraphNode): PaperSummary {
  return {
    research_question: `How can ${paper.primaryTopic} techniques be applied to improve model performance and efficiency?`,
    methods: 'The authors conduct a systematic study combining theoretical analysis with empirical evaluation across multiple benchmarks, proposing a novel framework that addresses key limitations in prior work.',
    key_findings: 'The paper demonstrates significant improvements over baselines, achieving state-of-the-art results on standard benchmarks. Key insights include trade-offs between accuracy and computational cost.',
    conclusion: `This work advances the field of ${paper.primaryTopic} by providing both theoretical grounding and practical techniques that can be adopted by practitioners.`,
  }
}

export default function PaperPanel({ paper, lightMode, onClose }: Props) {
  const accent = lightMode ? '#0070f3' : '#64ffda'
  const bg = lightMode ? '#ffffff' : '#0d1b2e'
  const border = lightMode ? 'rgba(0,0,0,0.08)' : 'rgba(100,255,218,0.12)'
  const textPrimary = lightMode ? '#1a202c' : '#e6f0ff'
  const textSecondary = lightMode ? '#4a5568' : '#8892b0'
  const inputBg = lightMode ? '#f7f9fc' : '#112240'

  const summary = SUMMARIES[paper.id] ?? getFallbackSummary(paper)

  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    setMessages([])
    setInput('')
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
      const res = await fetch('/api/paper-chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paper_title: paper.title, summary, messages: [...messages, userMsg] }),
      })
      const data = await res.json()
      setMessages(prev => [...prev, { role: 'assistant', content: data.reply }])
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
        {/* Header — fixed */}
        <div style={{ padding: '18px 20px 14px', borderBottom: `1px solid ${border}`, flexShrink: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12 }}>
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 15, fontWeight: 700, color: textPrimary, lineHeight: 1.4, marginBottom: 4 }}>{paper.title}</div>
              <div style={{ fontSize: 12, color: textSecondary }}>{paper.authors} · {paper.year} · {paper.citations.toLocaleString()} citations</div>
            </div>
            <button type="button" onClick={onClose} style={{ background: 'none', border: 'none', color: textSecondary, cursor: 'pointer', fontSize: 20, lineHeight: 1, flexShrink: 0, padding: 2 }}>×</button>
          </div>
        </div>

        {/* Scrollable body — summary + chat stacked */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px 20px 0' }}>

          {/* Summary section */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginBottom: 28 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Paper Summary</div>
            {(
              [
                { label: 'Research Question', key: 'research_question', emoji: '🔍' },
                { label: 'Methods', key: 'methods', emoji: '🔬' },
                { label: 'Key Findings', key: 'key_findings', emoji: '💡' },
                { label: 'Conclusion', key: 'conclusion', emoji: '✅' },
              ] as const
            ).map(({ label, key, emoji }) => (
              <div key={key} style={{ padding: '12px 14px', background: lightMode ? 'rgba(0,112,243,0.04)' : 'rgba(100,255,218,0.04)', borderRadius: 10, borderLeft: `3px solid ${accent}` }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: accent, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>{emoji} {label}</div>
                <p style={{ fontSize: 13, color: textPrimary, lineHeight: 1.7, margin: 0 }}>{summary[key]}</p>
              </div>
            ))}
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

          {/* Spacer so last message isn't hidden behind input */}
          <div style={{ height: 80 }} />
        </div>

        {/* Chat input — fixed at bottom */}
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
