import { useMemo, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { useGraphData } from './hooks/useGraphData'
import type { GraphNode, GraphLink } from './types/graph'

function asNodeId(v: GraphLink['source'] | GraphLink['target']) {
  return typeof v === 'object' ? (v as GraphNode).id : v
}

export default function App() {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fgRef = useRef<any>(undefined)
  const { data: graphData, loading } = useGraphData()

  const [query, setQuery] = useState('')
  const [selectedId, setSelectedId] = useState<number | null>(null)

  const idToNode = useMemo(() => {
    const map = new Map<number, GraphNode>()
    for (const n of graphData.nodes) map.set(n.id, n)
    return map
  }, [graphData.nodes])

  const neighborIds = useMemo(() => {
    const s = new Set<number>()
    if (selectedId == null) return s
    s.add(selectedId)
    for (const l of graphData.links) {
      const a = asNodeId(l.source)
      const b = asNodeId(l.target)
      if (a === selectedId) s.add(b)
      if (b === selectedId) s.add(a)
    }
    return s
  }, [selectedId, graphData.links])

  const filteredPaperNodes = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return graphData.nodes
    return graphData.nodes.filter((p) => p.searchText.toLowerCase().includes(q))
  }, [graphData.nodes, query])

  const selectedPaper = selectedId != null ? idToNode.get(selectedId) : undefined

  if (loading) {
    return (
      <main className="app" style={{ display: 'grid', placeItems: 'center' }}>
        <div style={{ color: 'var(--research-text)' }}>Loading graph...</div>
      </main>
    )
  }

  return (
    <main className="app">
      <header className="topbar glass">
        <div className="brand">
          <div className="brandMark" aria-hidden="true">
            M
          </div>
          <div className="brandText">
            <div className="brandTitle">
              Mind<span className="accent">Map</span>
            </div>
            <div className="brandSub">Paper topics &bull; citations &bull; relationships</div>
          </div>
        </div>

        <div className="searchWrap">
          <div className="search">
            <svg
              className="searchIcon"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search papers, authors, or topics..."
              className="searchInput"
              spellCheck={false}
            />
          </div>
        </div>

        <div className="topbarRight">
          <button className="ghostBtn" type="button">
            Library
          </button>
          <div className="avatar" aria-hidden="true" />
        </div>
      </header>

      <section className="content">
        <aside className="left glass panel">
          <div className="panelHeader">Graph Controls</div>
          <nav className="nav">
            <button className="navItem navItemActive" type="button">
              <span className="navDot" />
              Citation Network
            </button>
            <button className="navItem" type="button">
              <span className="navDot navDotBlue" />
              Topic Clusters
            </button>
            <button className="navItem" type="button">
              <span className="navDot navDotPurple" />
              Author Connections
            </button>
          </nav>

          <div className="divider" />

          <div className="control">
            <div className="controlLabel">Edge Density</div>
            <input className="range" type="range" min={0} max={100} defaultValue={65} />
          </div>

          <div className="miniActions" aria-hidden="true">
            <div className="miniAction" />
            <div className="miniAction" />
            <div className="miniAction" />
          </div>

          <div className="tierNote">
            <div className="tierTitle">3-tier data model</div>
            <ul className="tierList">
              <li>
                <span className="tierTag">Bronze</span> basic paper metadata
              </li>
              <li>
                <span className="tierTag">Silver</span> parsed entities &amp; topics
              </li>
              <li>
                <span className="tierTag">Gold</span> relationships between papers
              </li>
            </ul>
          </div>
        </aside>

        <section className="center">
          <div className="graphWrap">
            <ForceGraph2D<GraphNode, GraphLink>
              ref={fgRef}
              graphData={graphData}
              backgroundColor="#0a192f"
              nodeRelSize={4}
              nodeId="id"
              nodeLabel={(n) => `${n.title}\n${n.authors} (${n.year})`}
              linkColor={(l) => {
                const link = l as GraphLink
                return link.relationship_type === 'CITES'
                  ? 'rgba(100, 255, 218, 0.15)'
                  : 'rgba(96, 165, 250, 0.15)'
              }}
              linkWidth={(l) => {
                const link = l as GraphLink
                return link.relationship_type === 'CITES' ? 1.2 * link.strength : 0.8 * link.strength
              }}
              linkDirectionalParticles={(l) => {
                const link = l as GraphLink
                return link.relationship_type === 'CITES' ? 2 : 0
              }}
              linkDirectionalParticleWidth={1.2}
              linkDirectionalParticleSpeed={(l) => {
                const link = l as GraphLink
                return link.relationship_type === 'CITES' ? 0.008 : 0.004
              }}
              linkLineDash={(l) => {
                const link = l as GraphLink
                return link.relationship_type === 'SIMILAR' ? [4, 2] : []
              }}
              nodeColor={(n) => {
                if (n.id === selectedId) return '#64ffda'
                if (neighborIds.has(n.id)) return '#233554'
                return '#112240'
              }}
              nodeCanvasObject={(node, ctx, globalScale) => {
                const isSelected = node.id === selectedId
                const isNeighbor = neighborIds.has(node.id)
                const baseR = 6
                const r = isSelected ? baseR * 1.6 : isNeighbor ? baseR * 1.2 : baseR

                ctx.beginPath()
                ctx.arc(node.x ?? 0, node.y ?? 0, r, 0, Math.PI * 2)
                ctx.fillStyle =
                  node.id === selectedId
                    ? '#64ffda'
                    : isNeighbor
                      ? '#233554'
                      : '#112240'
                ctx.shadowBlur = isSelected ? 18 : 0
                ctx.shadowColor = isSelected ? '#64ffda' : 'transparent'
                ctx.fill()

                ctx.shadowBlur = 0
                ctx.lineWidth = 1 / globalScale
                ctx.strokeStyle = isSelected ? 'rgba(100,255,218,0.9)' : 'rgba(35,53,84,0.9)'
                ctx.stroke()

                if (globalScale > 2.1 && (isSelected || isNeighbor)) {
                  const label = node.shortTitle
                  const fontSize = 12 / globalScale
                  ctx.font = `${fontSize}px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif`
                  ctx.textAlign = 'left'
                  ctx.textBaseline = 'middle'
                  ctx.fillStyle = 'rgba(204, 214, 246, 0.95)'
                  ctx.fillText(label, (node.x ?? 0) + (r + 3) / globalScale, node.y ?? 0)
                }
              }}
              onNodeClick={(n) => {
                setSelectedId(n.id)
                fgRef.current?.centerAt(n.x ?? 0, n.y ?? 0, 600)
                fgRef.current?.zoom(3.2, 600)
              }}
            />
          </div>

          <div className="stats">
            <div className="stat">
              <span className="statDot statDotAccent" />
              Nodes: {graphData.nodes.length}
            </div>
            <div className="stat">
              <span className="statDot statDotBlue" />
              Edges: {graphData.links.length}
            </div>
            <div className="stat">
              <span className="statDot statDotPurple" />
              Clusters: 3
            </div>
          </div>
        </section>

        <aside className="right glass panel">
          <div className="panelHeaderRow">
            <div className="panelHeader">Papers</div>
            <div className="panelMeta">{filteredPaperNodes.length} shown</div>
          </div>

          <div className="paperList">
            {filteredPaperNodes.map((p) => {
              const isActive = p.id === selectedId
              return (
                <button
                  key={p.id}
                  className={`paperItem ${isActive ? 'paperItemActive' : ''}`}
                  type="button"
                  onClick={() => {
                    setSelectedId(p.id)
                    const n = idToNode.get(p.id)
                    if (n) {
                      fgRef.current?.centerAt(n.x ?? 0, n.y ?? 0, 600)
                      fgRef.current?.zoom(3.2, 600)
                    }
                  }}
                >
                  <div className="paperTop">
                    <span className="chip">{p.primaryTopic}</span>
                    <span className="paperYear">{p.year}</span>
                  </div>
                  <div className="paperTitle">{p.title}</div>
                  <div className="paperAuthors">{p.authors}</div>
                </button>
              )
            })}
          </div>

          <div className="divider" />

          <div className="preview">
            <div className="previewTitle">Selected</div>
            {selectedPaper ? (
              <>
                <div className="previewHeading">{selectedPaper.title}</div>
                <div className="previewAuthors">{selectedPaper.authors}</div>
                <div className="previewGrid">
                  <div className="metric">
                    <div className="metricLabel">Citations</div>
                    <div className="metricValue">{selectedPaper.citations.toLocaleString()}</div>
                  </div>
                  <div className="metric">
                    <div className="metricLabel">Connections</div>
                    <div className="metricValue">{neighborIds.size - 1}</div>
                  </div>
                </div>
                <button className="cta" type="button">
                  Explore Abstract
                </button>
              </>
            ) : (
              <div className="previewEmpty">Click a paper node or pick one from the list.</div>
            )}
          </div>
        </aside>
      </section>
    </main>
  )
}
