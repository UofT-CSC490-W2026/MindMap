import { useNavigate } from 'react-router-dom'

const GRAPHS = [
  {
    id: 'citation-network',
    title: 'Citation Network',
    description: 'Explore how papers reference each other across NLP, Graphs, and IR research.',
    nodes: 16,
    edges: 24,
    clusters: 3,
    dotColor: 'var(--research-accent)',
    tag: 'NLP',
    tagColor: 'var(--research-accent)',
    preview: [
      { x: 50, y: 40, r: 10, color: '#64ffda' },
      { x: 25, y: 65, r: 7, color: '#60a5fa' },
      { x: 75, y: 65, r: 7, color: '#60a5fa' },
      { x: 15, y: 40, r: 5, color: '#a855f7' },
      { x: 85, y: 40, r: 5, color: '#a855f7' },
      { x: 50, y: 80, r: 5, color: '#64ffda' },
    ],
    lines: [
      [50, 40, 25, 65], [50, 40, 75, 65], [25, 65, 15, 40],
      [75, 65, 85, 40], [25, 65, 50, 80], [75, 65, 50, 80],
    ],
  },
  {
    id: 'topic-clusters',
    title: 'Topic Clusters',
    description: 'Visualize how research topics like Transformers, Embeddings, and GNNs interconnect.',
    nodes: 12,
    edges: 18,
    clusters: 4,
    dotColor: '#60a5fa',
    tag: 'Topics',
    tagColor: '#60a5fa',
    preview: [
      { x: 30, y: 35, r: 9, color: '#60a5fa' },
      { x: 70, y: 35, r: 9, color: '#a855f7' },
      { x: 30, y: 70, r: 9, color: '#64ffda' },
      { x: 70, y: 70, r: 9, color: '#f59e0b' },
      { x: 50, y: 52, r: 6, color: '#8892b0' },
    ],
    lines: [
      [30, 35, 50, 52], [70, 35, 50, 52], [30, 70, 50, 52], [70, 70, 50, 52],
      [30, 35, 70, 35], [30, 70, 70, 70],
    ],
  },
  {
    id: 'author-connections',
    title: 'Author Connections',
    description: 'Map co-authorship and collaboration patterns across landmark ML papers.',
    nodes: 10,
    edges: 14,
    clusters: 2,
    dotColor: '#a855f7',
    tag: 'Authors',
    tagColor: '#a855f7',
    preview: [
      { x: 20, y: 50, r: 8, color: '#a855f7' },
      { x: 45, y: 30, r: 6, color: '#a855f7' },
      { x: 45, y: 70, r: 6, color: '#a855f7' },
      { x: 70, y: 50, r: 8, color: '#64ffda' },
      { x: 85, y: 30, r: 5, color: '#64ffda' },
      { x: 85, y: 70, r: 5, color: '#64ffda' },
    ],
    lines: [
      [20, 50, 45, 30], [20, 50, 45, 70], [45, 30, 70, 50],
      [45, 70, 70, 50], [70, 50, 85, 30], [70, 50, 85, 70],
    ],
  },
  {
    id: 'embedding-space',
    title: 'Embedding Space',
    description: 'See how word2vec, BERT, and GNN embeddings cluster in semantic space.',
    nodes: 20,
    edges: 30,
    clusters: 5,
    dotColor: '#f59e0b',
    tag: 'Embeddings',
    tagColor: '#f59e0b',
    preview: [
      { x: 25, y: 25, r: 7, color: '#f59e0b' },
      { x: 55, y: 20, r: 5, color: '#f59e0b' },
      { x: 75, y: 35, r: 6, color: '#64ffda' },
      { x: 20, y: 65, r: 6, color: '#60a5fa' },
      { x: 50, y: 60, r: 8, color: '#a855f7' },
      { x: 78, y: 68, r: 5, color: '#64ffda' },
    ],
    lines: [
      [25, 25, 55, 20], [55, 20, 75, 35], [20, 65, 50, 60],
      [50, 60, 78, 68], [25, 25, 20, 65], [75, 35, 78, 68],
    ],
  },
  {
    id: 'rag-pipeline',
    title: 'RAG Pipeline',
    description: 'Trace retrieval-augmented generation flows from query to grounded response.',
    nodes: 8,
    edges: 10,
    clusters: 2,
    dotColor: '#64ffda',
    tag: 'IR',
    tagColor: '#64ffda',
    preview: [
      { x: 15, y: 50, r: 7, color: '#64ffda' },
      { x: 38, y: 30, r: 5, color: '#60a5fa' },
      { x: 38, y: 70, r: 5, color: '#60a5fa' },
      { x: 62, y: 50, r: 7, color: '#64ffda' },
      { x: 85, y: 50, r: 6, color: '#a855f7' },
    ],
    lines: [
      [15, 50, 38, 30], [15, 50, 38, 70],
      [38, 30, 62, 50], [38, 70, 62, 50],
      [62, 50, 85, 50],
    ],
  },
  {
    id: 'knowledge-graph',
    title: 'Knowledge Graph',
    description: 'Browse entity relationships extracted from paper abstracts and metadata.',
    nodes: 25,
    edges: 40,
    clusters: 6,
    dotColor: '#60a5fa',
    tag: 'Graphs',
    tagColor: '#60a5fa',
    preview: [
      { x: 50, y: 50, r: 10, color: '#60a5fa' },
      { x: 25, y: 30, r: 6, color: '#64ffda' },
      { x: 75, y: 30, r: 6, color: '#64ffda' },
      { x: 20, y: 65, r: 5, color: '#a855f7' },
      { x: 80, y: 65, r: 5, color: '#a855f7' },
      { x: 50, y: 78, r: 5, color: '#f59e0b' },
    ],
    lines: [
      [50, 50, 25, 30], [50, 50, 75, 30], [50, 50, 20, 65],
      [50, 50, 80, 65], [50, 50, 50, 78],
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
              onClick={() => navigate('/')}
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
                <div style={{ fontSize: 12, color: 'var(--research-text)', lineHeight: 1.5, marginBottom: 14 }}>
                  {g.description}
                </div>

                <div style={{ display: 'flex', gap: 16 }}>
                  {[
                    { label: 'Nodes', value: g.nodes, color: 'var(--research-accent)' },
                    { label: 'Edges', value: g.edges, color: '#60a5fa' },
                    { label: 'Clusters', value: g.clusters, color: '#a855f7' },
                  ].map((m) => (
                    <div key={m.label}>
                      <div style={{ fontSize: 10, color: 'var(--research-text)', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 2 }}>
                        {m.label}
                      </div>
                      <div style={{ fontFamily: 'ui-monospace, monospace', fontWeight: 800, fontSize: 14, color: m.color }}>
                        {m.value}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </button>
          ))}
        </div>
      </div>
    </main>
  )
}
