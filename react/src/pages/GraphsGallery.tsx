import { useNavigate } from 'react-router-dom'

const GRAPHS = [
  {
    id: 'model-quantization',
    query: 'model quantization',
    title: 'Model Quantization',
    description: 'Explore research on compressing neural networks through quantization techniques.',
    dotColor: 'var(--research-accent)',
    tag: 'Efficiency',
    tagColor: 'var(--research-accent)',
    preview: [
      { x: 50, y: 38, r: 10, color: '#64ffda' },
      { x: 24, y: 62, r: 7, color: '#60a5fa' },
      { x: 76, y: 62, r: 7, color: '#60a5fa' },
      { x: 14, y: 38, r: 5, color: '#a855f7' },
      { x: 86, y: 38, r: 5, color: '#a855f7' },
      { x: 50, y: 80, r: 5, color: '#64ffda' },
    ],
    lines: [
      [50, 38, 24, 62], [50, 38, 76, 62], [24, 62, 14, 38],
      [76, 62, 86, 38], [24, 62, 50, 80], [76, 62, 50, 80],
    ],
  },
  {
    id: 'nuclear-physics',
    query: 'nuclear physics',
    title: 'Nuclear Physics',
    description: 'Explore research on nuclear reactions, particle interactions, and atomic structure.',
    dotColor: '#f59e0b',
    tag: 'Physics',
    tagColor: '#f59e0b',
    preview: [
      { x: 50, y: 45, r: 10, color: '#f59e0b' },
      { x: 28, y: 30, r: 6, color: '#64ffda' },
      { x: 72, y: 30, r: 6, color: '#64ffda' },
      { x: 20, y: 62, r: 5, color: '#60a5fa' },
      { x: 80, y: 62, r: 5, color: '#60a5fa' },
      { x: 50, y: 75, r: 5, color: '#f59e0b' },
    ],
    lines: [
      [50, 45, 28, 30], [50, 45, 72, 30], [50, 45, 20, 62],
      [50, 45, 80, 62], [50, 45, 50, 75],
    ],
  },
  {
    id: 'astronomy',
    query: 'astronomy',
    title: 'Astronomy',
    description: 'Map the research landscape around stars, galaxies, cosmology, and space observation.',
    dotColor: '#60a5fa',
    tag: 'Astronomy',
    tagColor: '#60a5fa',
    preview: [
      { x: 50, y: 50, r: 9, color: '#60a5fa' },
      { x: 22, y: 35, r: 6, color: '#a855f7' },
      { x: 78, y: 35, r: 6, color: '#a855f7' },
      { x: 22, y: 65, r: 5, color: '#64ffda' },
      { x: 78, y: 65, r: 5, color: '#64ffda' },
      { x: 50, y: 22, r: 5, color: '#60a5fa' },
    ],
    lines: [
      [50, 50, 22, 35], [50, 50, 78, 35], [50, 50, 22, 65],
      [50, 50, 78, 65], [50, 50, 50, 22],
    ],
  },
  {
    id: 'natural-language-processing',
    query: 'natural language processing',
    title: 'Natural Language Processing',
    description: 'Discover connections across NLP research from parsing to large language models.',
    dotColor: '#64ffda',
    tag: 'NLP',
    tagColor: '#64ffda',
    preview: [
      { x: 50, y: 40, r: 9, color: '#64ffda' },
      { x: 25, y: 60, r: 7, color: '#60a5fa' },
      { x: 75, y: 60, r: 7, color: '#60a5fa' },
      { x: 12, y: 38, r: 5, color: '#a855f7' },
      { x: 88, y: 38, r: 5, color: '#a855f7' },
      { x: 50, y: 78, r: 5, color: '#64ffda' },
    ],
    lines: [
      [50, 40, 25, 60], [50, 40, 75, 60], [25, 60, 12, 38],
      [75, 60, 88, 38], [25, 60, 50, 78], [75, 60, 50, 78],
    ],
  },
  {
    id: 'attention-is-all-you-need',
    query: 'Attention Is All You Need',
    title: 'Attention Is All You Need',
    description: 'Trace the citation landscape around the landmark Transformer paper.',
    dotColor: '#a855f7',
    tag: 'Transformers',
    tagColor: '#a855f7',
    preview: [
      { x: 50, y: 50, r: 11, color: '#a855f7' },
      { x: 25, y: 28, r: 6, color: '#64ffda' },
      { x: 75, y: 28, r: 6, color: '#64ffda' },
      { x: 18, y: 65, r: 5, color: '#60a5fa' },
      { x: 82, y: 65, r: 5, color: '#60a5fa' },
      { x: 50, y: 78, r: 5, color: '#f59e0b' },
    ],
    lines: [
      [50, 50, 25, 28], [50, 50, 75, 28], [50, 50, 18, 65],
      [50, 50, 82, 65], [50, 50, 50, 78],
    ],
  },
]

export default function GraphsGallery() {
  const navigate = useNavigate()

  return (
    <main className="app" style={{ overflow: 'auto' }}>
      <header className="topbar glass">
        <div className="brand">
          <div className="brandMark" aria-hidden="true">M</div>
          <div className="brandText">
            <div className="brandTitle">Mind<span className="accent">Map</span></div>
            <div className="brandSub">Paper topics &bull; citations &bull; relationships</div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ fontSize: 13, color: 'var(--research-text)', letterSpacing: '0.04em' }}>
            Graph Explorer
          </span>
        </div>
        <div className="topbarRight">
          <button className="ghostBtn" type="button" onClick={() => navigate('/')}>
            Graph View
          </button>
          <div className="avatar" aria-hidden="true" />
        </div>
      </header>

      <div style={{ padding: '28px 32px', overflowY: 'auto' }}>
        <div style={{ marginBottom: 28 }}>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 800, color: 'var(--research-light)' }}>
            Available <span className="accent">Graphs</span>
          </h1>
          <p style={{ margin: '6px 0 0', fontSize: 13, color: 'var(--research-text)' }}>
            Click any graph to explore it in the interactive viewer.
          </p>
        </div>

        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
          gap: 20,
        }}>
          {GRAPHS.map((g) => (
            <button
              key={g.id}
              type="button"
              onClick={() => navigate(`/?q=${encodeURIComponent(g.query)}`)}
              style={{
                textAlign: 'left',
                background: 'var(--panel-bg)',
                border: '1px solid var(--research-stroke)',
                borderRadius: 16,
                padding: 0,
                cursor: 'pointer',
                color: 'var(--research-light)',
                backdropFilter: 'blur(10px)',
                transition: 'transform 140ms ease, border-color 140ms ease, box-shadow 140ms ease',
                overflow: 'hidden',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'translateY(-3px)'
                e.currentTarget.style.borderColor = `${g.dotColor}55`
                e.currentTarget.style.boxShadow = `0 12px 40px rgba(0,0,0,0.4), 0 0 0 1px ${g.dotColor}22`
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translateY(0)'
                e.currentTarget.style.borderColor = 'var(--research-stroke)'
                e.currentTarget.style.boxShadow = 'none'
              }}
            >
              {/* SVG preview */}
              <div style={{
                background: '#0a192f',
                borderBottom: '1px solid var(--research-stroke)',
                height: 140,
                position: 'relative',
                overflow: 'hidden',
              }}>
                <svg width="100%" height="100%" viewBox="0 0 100 100" preserveAspectRatio="xMidYMid meet">
                  {/* grid dots */}
                  {Array.from({ length: 5 }, (_, row) =>
                    Array.from({ length: 8 }, (_, col) => (
                      <circle
                        key={`${row}-${col}`}
                        cx={col * 14 + 7}
                        cy={row * 22 + 11}
                        r={0.5}
                        fill="rgba(100,255,218,0.08)"
                      />
                    ))
                  )}
                  {/* edges */}
                  {g.lines.map(([x1, y1, x2, y2], i) => (
                    <line
                      key={i}
                      x1={x1} y1={y1} x2={x2} y2={y2}
                      stroke={g.dotColor}
                      strokeWidth={0.6}
                      strokeOpacity={0.3}
                    />
                  ))}
                  {/* nodes */}
                  {g.preview.map((n, i) => (
                    <g key={i}>
                      <circle cx={n.x} cy={n.y} r={n.r + 3} fill={n.color} fillOpacity={0.08} />
                      <circle cx={n.x} cy={n.y} r={n.r} fill={n.color} fillOpacity={0.85} />
                    </g>
                  ))}
                </svg>
              </div>

              {/* Card body */}
              <div style={{ padding: '14px 16px 16px' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span style={{
                    fontSize: 10,
                    fontWeight: 800,
                    letterSpacing: '0.1em',
                    textTransform: 'uppercase',
                    color: g.tagColor,
                    background: `${g.tagColor}18`,
                    padding: '2px 8px',
                    borderRadius: 999,
                  }}>
                    {g.tag}
                  </span>
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--research-text)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M5 12h14M12 5l7 7-7 7" />
                  </svg>
                </div>

                <div style={{ fontWeight: 800, fontSize: 15, marginBottom: 6, lineHeight: 1.2 }}>
                  {g.title}
                </div>
                <div style={{ fontSize: 12, color: 'var(--research-text)', lineHeight: 1.5 }}>
                  {g.description}
                </div>
              </div>
            </button>
          ))}
        </div>
      </div>
    </main>
  )
}
