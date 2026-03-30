import { useMemo, useRef, useState, useEffect } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import ForceGraph2D from 'react-force-graph-2d'
import { useGraphData } from './hooks/useGraphData'
import { useSemanticSearch } from './hooks/sematicSearch'
import { rebuildClusters } from './services/graphService'
import { createIngestion, pollIngestionStatus } from './services/ingestionService'
import type { GraphNode, GraphLink } from './types/graph'
import { clusterColor, CLUSTER_COLORS } from './utils/graphUtils'
import PaperPanel from './components/PaperPanel'

// Right sidebar width — used to offset centerAt so nodes land in the visual center of the graph area
const SIDEBAR_OFFSET_X = -90

function asNodeId(v: GraphLink['source'] | GraphLink['target']) {
  return typeof v === 'object' ? (v as GraphNode).id : v
}

type ViewMode = 'papers' | 'clusters' | 'semantic'

type ClusterCenter = { x: number; y: number }

function buildClusterCenters(nodes: GraphNode[], radius = 260): Map<number, ClusterCenter> {
  const clusterIds = Array.from(
    new Set(nodes.map((n) => n.clusterId).filter((id): id is number => id != null)),
  ).sort((a, b) => a - b)

  const centers = new Map<number, ClusterCenter>()
  if (clusterIds.length === 0) return centers
  if (clusterIds.length === 1) {
    centers.set(clusterIds[0], { x: 0, y: 0 })
    return centers
  }

  clusterIds.forEach((clusterId, i) => {
    const theta = (2 * Math.PI * i) / clusterIds.length
    centers.set(clusterId, {
      x: Math.cos(theta) * radius,
      y: Math.sin(theta) * radius,
    })
  })

  return centers
}

function makeClusterForce(
  centers: Map<number, ClusterCenter>,
  strength: number,
) {
  let nodes: Array<GraphNode & { vx?: number; vy?: number; x?: number; y?: number }> = []

  const force = (alpha: number) => {
    if (!nodes.length) return
    for (const n of nodes) {
      if (n.clusterId == null) continue
      const center = centers.get(n.clusterId)
      if (!center) continue
      const x = n.x ?? 0
      const y = n.y ?? 0
      n.vx = (n.vx ?? 0) + (center.x - x) * strength * alpha
      n.vy = (n.vy ?? 0) + (center.y - y) * strength * alpha
    }
  }

  // d3-force calls initialize(nodes) when force is registered.
  ;(force as unknown as { initialize: (nextNodes: GraphNode[]) => void }).initialize = (nextNodes: GraphNode[]) => {
    nodes = nextNodes as Array<GraphNode & { vx?: number; vy?: number; x?: number; y?: number }>
  }

  return force
}

// Relationship type styling — colors adapt to light/dark mode
function getRelMeta(lm: boolean): Record<string, { label: string; color: string; emoji: string }> {
  return {
    CITES:      { label: 'Cites',       color: lm ? 'rgba(0,112,243,0.55)'   : 'rgba(100,255,218,0.7)',  emoji: '📎' },
    SIMILAR:    { label: 'Similar',     color: lm ? 'rgba(99,102,241,0.55)'  : 'rgba(96,165,250,0.7)',   emoji: '🔗' },
    SUPPORT:    { label: 'Supports',    color: lm ? 'rgba(22,163,74,0.85)'   : 'rgba(129,199,132,0.9)',  emoji: '✅' },
    CONTRADICT: { label: 'Contradicts', color: lm ? 'rgba(220,38,38,0.85)'   : 'rgba(229,115,115,0.9)', emoji: '⚡' },
    NEUTRAL:    { label: 'Neutral',     color: lm ? 'rgba(100,116,139,0.55)' : 'rgba(144,164,174,0.7)', emoji: '➖' },
  }
}

export default function App() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fgRef = useRef<any>(undefined)
  const progressTimerRef = useRef<number | null>(null)
  const hideProgressTimerRef = useRef<number | null>(null)
  const rebuildStartedAtRef = useRef<number | null>(null)
  const { data: graphData, loading, search: searchGraph, reload: reloadGraph } = useGraphData()
  const optimisticIdRef = useRef(-1)

  // Auto-load graph from ?q= param (e.g. navigated from gallery)
  useEffect(() => {
    const q = searchParams.get('q')
    if (q) {
      setQuery(q)
      void searchGraph(q)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  const [optimisticNodes, setOptimisticNodes] = useState<GraphNode[]>([])
  const [query, setQuery] = useState('')
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [selectedLink, setSelectedLink] = useState<GraphLink | null>(null)
  const [viewMode, setViewMode] = useState<ViewMode>('papers')
  const [selectedClusterId, setSelectedClusterId] = useState<number | null>(null)
  const [highlightRelType, setHighlightRelType] = useState<string | null>(null)
  const [lightMode, setLightMode] = useState(false)
  const [panelPaper, setPanelPaper] = useState<GraphNode | null>(null)

  // Apply theme to document root
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', lightMode ? 'light' : 'dark')
  }, [lightMode])

  const REL_META = getRelMeta(lightMode)

  // Dropdown state
  const [dropdownOpen, setDropdownOpen] = useState(false)
  const searchWrapRef = useRef<HTMLDivElement>(null)
  const { results: searchResults, loading: searchLoading } = useSemanticSearch(query)
  const [activeRebuildJobs, setActiveRebuildJobs] = useState(0)
  const [rebuildProgress, setRebuildProgress] = useState(0)
  const rebuildingGraph = activeRebuildJobs > 0
  const [pendingAutoRecluster, setPendingAutoRecluster] = useState(false)

  // Adding ids
  const [addingIds, setAddingIds] = useState<Set<string>>(new Set())
  const [addedIds, setAddedIds] = useState<Set<string>>(new Set())
  const [clustering, setClustering] = useState(false)
  const effectiveGraphData = useMemo(
    () => ({
      nodes: [...graphData.nodes, ...optimisticNodes],
      links: graphData.links,
    }),
    [graphData.links, graphData.nodes, optimisticNodes],
  )

  // Load existing graph data on initial page load.
  useEffect(() => {
    void reloadGraph()
  }, [reloadGraph])

  async function waitForIngestJob(jobId: string, timeoutMs = 120000) {
    const start = Date.now()
    while (Date.now() - start < timeoutMs) {
      let status: { status: 'processing' | 'done' | 'failed'; error?: string | null } | null = null
      try {
        status = await pollIngestionStatus(jobId)
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 2000))
        continue
      }
      if (status.status === 'done') return
      if (status.status === 'failed') {
        throw new Error(status.error ?? 'Ingestion failed')
      }
      await new Promise((resolve) => setTimeout(resolve, 2000))
    }
    console.warn(`Timed out waiting for ingestion job ${jobId}`)
  }

  useEffect(() => {
    if (rebuildingGraph) {
      if (rebuildStartedAtRef.current == null) {
        rebuildStartedAtRef.current = Date.now()
      }
      if (hideProgressTimerRef.current != null) {
        window.clearTimeout(hideProgressTimerRef.current)
        hideProgressTimerRef.current = null
      }
      if (progressTimerRef.current == null) {
        progressTimerRef.current = window.setInterval(() => {
          const startedAt = rebuildStartedAtRef.current ?? Date.now()
          const elapsedSec = (Date.now() - startedAt) / 1000
          setRebuildProgress((prev) => {
            // Move quickly at first, then slower, but keep moving during long model phases.
            if (elapsedSec < 10) return Math.min(45, prev + 4)
            if (elapsedSec < 30) return Math.min(70, prev + 1.8)
            if (elapsedSec < 90) return Math.min(88, prev + 0.7)
            if (elapsedSec < 180) return Math.min(95, prev + 0.35)
            return Math.min(98, prev + 0.15)
          })
        }, 600)
      }
      setRebuildProgress((prev) => (prev < 8 ? 8 : prev))
      return
    }

    if (progressTimerRef.current != null) {
      window.clearInterval(progressTimerRef.current)
      progressTimerRef.current = null
    }
    if (rebuildProgress > 0) {
      rebuildStartedAtRef.current = null
      setRebuildProgress(100)
      hideProgressTimerRef.current = window.setTimeout(() => {
        setRebuildProgress(0)
        hideProgressTimerRef.current = null
      }, 700)
    }

    return () => {
      if (progressTimerRef.current != null) {
        window.clearInterval(progressTimerRef.current)
        progressTimerRef.current = null
      }
      if (hideProgressTimerRef.current != null) {
        window.clearTimeout(hideProgressTimerRef.current)
        hideProgressTimerRef.current = null
      }
    }
  }, [rebuildProgress, rebuildingGraph])

  useEffect(() => {
    if (!pendingAutoRecluster || activeRebuildJobs > 0 || clustering) return

    let cancelled = false
    void (async () => {
      setClustering(true)
      try {
        await rebuildClusters(5)
        if (!cancelled) {
          await reloadGraph()
        }
      } catch (e) {
        console.error('Auto recluster failed:', e)
      } finally {
        if (!cancelled) {
          setClustering(false)
          setPendingAutoRecluster(false)
        }
      }
    })()

    return () => {
      cancelled = true
    }
  }, [activeRebuildJobs, clustering, pendingAutoRecluster, reloadGraph])
  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (searchWrapRef.current && !searchWrapRef.current.contains(e.target as Node)) {
        setDropdownOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const idToNode = useMemo(() => {
    const map = new Map<number, GraphNode>()
    for (const n of effectiveGraphData.nodes) map.set(n.id, n)
    return map
  }, [effectiveGraphData.nodes])

  const neighborIds = useMemo(() => {
    const s = new Set<number>()
    if (selectedId == null) return s
    s.add(selectedId)
    for (const l of effectiveGraphData.links) {
      const a = asNodeId(l.source)
      const b = asNodeId(l.target)
      if (a === selectedId) s.add(b)
      if (b === selectedId) s.add(a)
    }
    return s
  }, [selectedId, effectiveGraphData.links])

  // Derive unique clusters from nodes
  const clusters = useMemo(() => {
    const seen = new Map<number, { id: number; name: string; description: string; count: number }>()
    for (const n of effectiveGraphData.nodes) {
      if (n.clusterId == null) continue
      if (!seen.has(n.clusterId)) {
        seen.set(n.clusterId, {
          id: n.clusterId,
          name: n.clusterName ?? `Cluster ${n.clusterId}`,
          description: n.clusterDescription ?? '',
          count: 0,
        })
      }
      seen.get(n.clusterId)!.count++
    }
    return Array.from(seen.values()).sort((a, b) => a.id - b.id)
  }, [effectiveGraphData.nodes])

  // Filter links by view mode
  const visibleGraphData = useMemo(() => {
    if (viewMode === 'semantic') {
      return {
        nodes: effectiveGraphData.nodes,
        links: effectiveGraphData.links.filter(l => l.relationship_type === 'SIMILAR'),
      }
    }
    return {
      nodes: effectiveGraphData.nodes,
      links: effectiveGraphData.links.filter(l => l.relationship_type === 'CITES'),
    }
  }, [effectiveGraphData, viewMode])

  // Nodes/links involved in the highlighted relationship type
  const highlightedLinks = useMemo(() => {
    if (!highlightRelType || viewMode !== 'semantic') return new Set<GraphLink>()
    return new Set(visibleGraphData.links.filter(l => {
      if (highlightRelType === 'NEUTRAL') return l.relationship_type === 'NEUTRAL' || l.relationship_type === 'CITES'
      return l.relationship_type === highlightRelType
    }))
  }, [highlightRelType, visibleGraphData.links, viewMode])

  const highlightedNodeIds = useMemo(() => {
    const ids = new Set<number>()
    highlightedLinks.forEach(l => {
      ids.add(asNodeId(l.source))
      ids.add(asNodeId(l.target))
    })
    return ids
  }, [highlightedLinks])
  const filteredPaperNodes = useMemo(() => effectiveGraphData.nodes, [effectiveGraphData.nodes])

  const selectedPaper = selectedId != null ? idToNode.get(selectedId) : undefined
  const selectedLinkNodes = useMemo(() => {
    if (!selectedLink) return null
    const src = idToNode.get(asNodeId(selectedLink.source))
    const tgt = idToNode.get(asNodeId(selectedLink.target))
    return { src, tgt }
  }, [selectedLink, idToNode])
  const selectedLinkReason = useMemo(() => {
    if (!selectedLink) return null
    if (selectedLink.reason) return selectedLink.reason
    if (selectedLink.relationship_type !== 'SIMILAR') return null

    const srcId = asNodeId(selectedLink.source)
    const tgtId = asNodeId(selectedLink.target)
    const semanticEdge = effectiveGraphData.links.find((l) => {
      const ls = asNodeId(l.source)
      const lt = asNodeId(l.target)
      return (
        ls === srcId &&
        lt === tgtId &&
        ['SUPPORT', 'CONTRADICT', 'NEUTRAL'].includes(l.relationship_type) &&
        !!l.reason
      )
    })
    return semanticEdge?.reason ?? null
  }, [selectedLink, effectiveGraphData.links])

  useEffect(() => {
    const graph = fgRef.current
    if (!graph) return

    const centers = buildClusterCenters(
      visibleGraphData.nodes,
      viewMode === 'clusters' ? 320 : viewMode === 'papers' ? 260 : 220,
    )
    const clusterStrength =
      viewMode === 'clusters' ? 0.2
        : viewMode === 'papers' ? 0.02
          : 0.08

    graph.d3Force('cluster', makeClusterForce(centers, clusterStrength))

    const linkForce = graph.d3Force('link')
    if (linkForce && typeof linkForce.distance === 'function') {
      linkForce.distance((link: GraphLink) => {
        const sourceId = asNodeId(link.source)
        const targetId = asNodeId(link.target)
        const sourceNode = visibleGraphData.nodes.find((n) => n.id === sourceId)
        const targetNode = visibleGraphData.nodes.find((n) => n.id === targetId)
        const sameCluster =
          sourceNode?.clusterId != null &&
          targetNode?.clusterId != null &&
          sourceNode.clusterId === targetNode.clusterId

        if (viewMode === 'papers') {
          if (sameCluster) return 130
          if (link.relationship_type === 'CITES') return 185
          return 220
        }

        if (sameCluster) return viewMode === 'clusters' ? 45 : 60
        if (link.relationship_type === 'CITES') return 95
        return 120
      })
    }

    const chargeForce = graph.d3Force('charge')
    if (chargeForce && typeof chargeForce.strength === 'function') {
      chargeForce.strength(
        viewMode === 'clusters' ? -65
          : viewMode === 'papers' ? -180
            : -95,
      )
    }

    const collideForce = graph.d3Force('collide')
    if (collideForce && typeof collideForce.radius === 'function') {
      collideForce
        .radius(() => (viewMode === 'papers' ? 16 : 10))
        .strength(viewMode === 'papers' ? 0.9 : 0.6)
    }

    graph.d3ReheatSimulation()
  }, [viewMode, visibleGraphData.links, visibleGraphData.nodes])

  const hasGraph = effectiveGraphData.nodes.length > 0

  return (
    <main className="app">
      <header className="topbar glass">
        <div className="brand" onClick={() => navigate('/graphs')} style={{ cursor: 'pointer' }}>
          <div className="brandMark" aria-hidden="true">M</div>
          <div className="brandText">
            <div className="brandTitle">Mind<span className="accent">Map</span></div>
            <div className="brandSub">structurally reasoning over scientific literature</div>
          </div>
        </div>

        {/* Search */}
        <div className="searchWrap" ref={searchWrapRef} style={{ position: 'relative' }}>
          <div className="search">
            <svg className="searchIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true">
              <path d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            <input
              value={query}
              onChange={(e) => { setQuery(e.target.value); setDropdownOpen(true) }}
              onFocus={() => { if (query.trim().length >= 2) setDropdownOpen(true) }}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && query.trim().length >= 2) {
                  setDropdownOpen(false)
                  void searchGraph(query.trim())
                }
              }}
              placeholder="Search papers, authors, or topics..."
              className="searchInput"
              spellCheck={false}
            />
            {searchLoading && (
              <span style={{ color: lightMode ? '#0070f3' : '#64ffda', fontSize: 11, paddingRight: 12, opacity: 0.7, whiteSpace: 'nowrap' }}>
                searching…
              </span>
            )}
          </div>

          {dropdownOpen && searchResults.length > 0 && (
            <div style={{ position: 'absolute', top: 'calc(100% + 6px)', left: 0, right: 0, background: '#0d2137', border: `1px solid ${lightMode ? 'rgba(0,112,243,0.18)' : 'rgba(100,255,218,0.18)'}`, borderRadius: 10, zIndex: 1000, overflow: 'hidden', boxShadow: '0 16px 48px rgba(0,0,0,0.55)' }}>
              <div style={{ padding: '8px 16px', fontSize: 10, letterSpacing: '0.08em', textTransform: 'uppercase', color: lightMode ? '#0070f3' : '#64ffda', borderBottom: `1px solid ${lightMode ? 'rgba(0,112,243,0.08)' : 'rgba(100,255,218,0.08)'}`, background: lightMode ? 'rgba(0,112,243,0.04)' : 'rgba(100,255,218,0.04)' }}>
                Top results from Semantic Scholar
              </div>
              {searchResults.map((r, i) => (
                <div
                  key={r.paperId}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 12,
                    width: '100%',
                    padding: '12px 16px',
                    borderBottom: i < searchResults.length - 1
                      ? '1px solid rgba(255,255,255,0.05)'
                      : 'none',
                    color: '#ccd6f6',
                    boxSizing: 'border-box',
                  }}
                >
                  {/* Index badge */}
                  <span
                    style={{
                      flexShrink: 0,
                      width: 20, height: 20,
                      borderRadius: '50%',
                      background: 'rgba(100,255,218,0.12)',
                      border: '1px solid rgba(100,255,218,0.25)',
                      color: '#64ffda',
                      fontSize: 10, fontWeight: 700,
                      display: 'grid', placeItems: 'center',
                    }}
                  >
                    {i + 1}
                  </span>

                  {/* Text — clicking sets query */}
                  <button
                    type="button"
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => { setQuery(r.title); setDropdownOpen(false) }}
                    style={{
                      flex: 1, minWidth: 0,
                      background: 'none', border: 'none',
                      cursor: 'pointer', textAlign: 'left', padding: 0,
                    }}
                  >
                    <div style={{
                      fontWeight: 500, fontSize: 13, marginBottom: 4,
                      lineHeight: 1.4, color: '#e6f0ff',
                      overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    }}>
                      {r.title}
                    </div>
                    <div style={{ fontSize: 11, color: '#8892b0', display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                      {r.authors.length > 0 && (
                        <span>
                          {r.authors.slice(0, 3).map((a) => a.name).join(', ')}
                          {r.authors.length > 3 ? ' et al.' : ''}
                        </span>
                      )}
                      {r.year && <span style={{ color: 'rgba(100,255,218,0.5)' }}>{r.year}</span>}
                      {r.citationCount > 0 && (
                        <span style={{ color: 'rgba(100,255,218,0.5)' }}>
                          {r.citationCount.toLocaleString()} citations
                        </span>
                      )}
                    </div>
                  </button>

                  {/* Add button */}
                  <button
                    type="button"
                    onMouseDown={(e) => e.preventDefault()}
                    disabled={addingIds.has(r.paperId) || addedIds.has(r.paperId)}
                    onClick={async () => {
                      console.log('externalIds:', JSON.stringify(r.externalIds))
                      const arxivId = r.externalIds?.ArXiv
                      if (!arxivId) return
                      setAddingIds((prev) => {
                        const next = new Set(prev)
                        next.add(r.paperId)
                        return next
                      })
                      try {
                        const ingest = await createIngestion(arxivId)
                        if (ingest.status !== 'processing' && ingest.status !== 'skipped') {
                          throw new Error('Bronze ingestion failed')
                        }

                        // Mark as added immediately after Bronze is confirmed.
                        if (ingest.bronze_status === 'ok' || ingest.bronze_status === 'skipped') {
                          setAddedIds((prev) => new Set(prev).add(r.paperId))
                          setOptimisticNodes((prev) => {
                            const normalizedTitle = r.title.trim().toLowerCase()
                            const alreadyPresent =
                              effectiveGraphData.nodes.some((n) => n.title.trim().toLowerCase() === normalizedTitle) ||
                              prev.some((n) => n.title.trim().toLowerCase() === normalizedTitle)
                            if (alreadyPresent) return prev

                            const optimisticId = optimisticIdRef.current
                            optimisticIdRef.current -= 1
                            const authors =
                              r.authors.length > 0
                                ? `${r.authors.slice(0, 3).map((a) => a.name).join(', ')}${r.authors.length > 3 ? ' et al.' : ''}`
                                : 'Unknown'
                            return [
                              ...prev,
                              {
                                id: optimisticId,
                                title: r.title,
                                shortTitle: r.title.slice(0, 20),
                                authors,
                                year: r.year ?? 0,
                                citations: r.citationCount ?? 0,
                                primaryTopic: 'ML',
                                searchText: r.title,
                              },
                            ]
                          })
                        }

                        const jobId = ingest.job_id
                        // Continue the rest of the pipeline in background and refresh graph on completion.
                        if (jobId) {
                          setActiveRebuildJobs((n) => n + 1)
                          void waitForIngestJob(jobId)
                            .then(async () => {
                              await reloadGraph()
                              setPendingAutoRecluster(true)
                              setOptimisticNodes((prev) =>
                                prev.filter(
                                  (n) => n.title.trim().toLowerCase() !== r.title.trim().toLowerCase(),
                                ),
                              )
                            })
                            .catch((e) => console.error('Background pipeline failed:', e))
                            .finally(() => {
                              setActiveRebuildJobs((n) => Math.max(0, n - 1))
                            })
                        }
                      } catch (e) {
                        console.error('Add paper failed:', e)
                      } finally {
                        setAddingIds((prev) => {
                          const next = new Set(prev)
                          next.delete(r.paperId)
                          return next
                        })
                      }
                    }}
                    style={{
                      flexShrink: 0,
                      padding: '4px 10px',
                      borderRadius: 6,
                      border: '1px solid rgba(100,255,218,0.3)',
                      background: addedIds.has(r.paperId)
                        ? 'rgba(100,255,218,0.12)'
                        : 'transparent',
                      color: addedIds.has(r.paperId) ? '#64ffda' : 'rgba(100,255,218,0.7)',
                      fontSize: 11, fontWeight: 600,
                      cursor: addingIds.has(r.paperId) || addedIds.has(r.paperId)
                        ? 'not-allowed' : 'pointer',
                      whiteSpace: 'nowrap',
                      transition: 'all 0.15s',
                    }}
                  >
                    {addingIds.has(r.paperId) ? '…' : addedIds.has(r.paperId) ? '✓ Added' : '+ Add'}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="topbarRight">
          <button
            className="ghostBtn"
            type="button"
            onClick={() => setLightMode(m => !m)}
            title={lightMode ? 'Switch to dark mode' : 'Switch to light mode'}
            style={{ fontSize: 18, padding: '6px 10px' }}
          >
            {lightMode ? '🌙' : '💡'}
          </button>
          <button className="ghostBtn" type="button" onClick={() => navigate('/graphs')}>
            Graphs
          </button>
          <div className="avatar" aria-hidden="true" />
        </div>
      </header>
      <section className="content">
        {!hasGraph ? (
          <div style={{ flex: 1, display: 'grid', placeItems: 'center', color: 'var(--research-text)', textAlign: 'center', gap: 12 }}>
            <div>
              <div style={{ fontSize: 18, marginBottom: 8, opacity: 0.7 }}>
                {loading ? 'Loading graph…' : 'Enter a topic above and press Enter to explore a research landscape'}
              </div>
              {!loading && (
                <div style={{ fontSize: 13, opacity: 0.4 }}>e.g. "attention mechanisms", "model quantization", "diffusion models"</div>
              )}
            </div>
          </div>
        ) : (
        <>
        {/* ── Left sidebar ── */}
        <aside className="left glass panel">
          <div className="panelHeader">Graph Controls</div>
          <nav className="nav">
            <button
              className={`navItem ${viewMode === 'papers' ? 'navItemActive' : ''}`}
              type="button"
              onClick={() => { setViewMode('papers'); setSelectedClusterId(null); setSelectedLink(null); setHighlightRelType(null) }}
            >
              <span className="navDot" />
              Citation Network
            </button>
            <button
              className={`navItem ${viewMode === 'semantic' ? 'navItemActive' : ''}`}
              type="button"
              onClick={() => { setViewMode(viewMode === 'semantic' ? 'papers' : 'semantic'); setSelectedClusterId(null); setSelectedLink(null); setHighlightRelType(null) }}
            >
              <span className="navDot navDotBlue" />
              Semantic Network
            </button>
            <button
              className={`navItem ${viewMode === 'clusters' ? 'navItemActive' : ''}`}
              type="button"
              onClick={() => { setViewMode(viewMode === 'clusters' ? 'papers' : 'clusters'); setSelectedLink(null) }}
            >
              <span className="navDot navDotPurple" />
              Topic Clusters
            </button>
          </nav>
        </aside>

        {/* ── Graph ── */}
        <section className="center">
          <div className="graphWrap">
            <ForceGraph2D<GraphNode, GraphLink>
              ref={fgRef}
              graphData={visibleGraphData}
              backgroundColor={lightMode ? '#e8edf2' : '#0a192f'}
              nodeRelSize={4}
              nodeId="id"
              nodeLabel={(n) => `${n.title}\n${n.authors} (${n.year})`}
              linkColor={(l) => {
                const link = l as GraphLink
                // In semantic mode, CITES renders as neutral gray
                const relType = (viewMode === 'semantic' && link.relationship_type === 'CITES')
                  ? 'NEUTRAL' : link.relationship_type
                const meta = REL_META[relType]
                const baseColor = meta?.color ?? 'rgba(100,255,218,0.15)'
                if (highlightRelType && viewMode === 'semantic') {
                  return highlightedLinks.has(link) ? baseColor : 'rgba(255,255,255,0.04)'
                }
                return baseColor
              }}
              linkWidth={(l) => {
                const link = l as GraphLink
                if (highlightRelType && viewMode === 'semantic' && !highlightedLinks.has(link)) return 0.5
                if (link.relationship_type === 'SUPPORT' || link.relationship_type === 'CONTRADICT') return 2.5
                if (link.relationship_type === 'NEUTRAL') return 1.5
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
                if (link.relationship_type === 'SIMILAR') return [4, 2]
                if (link.relationship_type === 'NEUTRAL') return [3, 3]
                return []
              }}
              nodeColor={(n) => {
                if (viewMode === 'clusters' && n.clusterId != null) return clusterColor(n.clusterId)
                if (n.id === selectedId) return lightMode ? '#0070f3' : '#64ffda'
                if (neighborIds.has(n.id)) return lightMode ? '#93c5fd' : '#233554'
                return lightMode ? '#94a3b8' : '#112240'
              }}
              nodeCanvasObject={(node, ctx, globalScale) => {
                const isSelected = node.id === selectedId
                const isNeighbor = neighborIds.has(node.id)
                const isHighlighted = highlightedNodeIds.has(node.id)
                const dimmed = highlightRelType && viewMode === 'semantic' && !isHighlighted
                const baseR = 6
                const r = isSelected ? baseR * 1.6 : isHighlighted ? baseR * 1.3 : isNeighbor ? baseR * 1.2 : baseR

                const nodeDefault  = lightMode ? '#94a3b8' : '#112240'
                const nodeNeighbor = lightMode ? '#93c5fd' : '#233554'
                const nodeSelected = lightMode ? '#0070f3' : '#64ffda'
                const nodeDimmed   = lightMode ? 'rgba(0,0,0,0.08)' : 'rgba(255,255,255,0.05)'

                let fill: string
                if (viewMode === 'clusters' && node.clusterId != null) {
                  fill = dimmed ? nodeDimmed : clusterColor(node.clusterId)
                } else if (dimmed) {
                  fill = nodeDimmed
                } else {
                  fill = isSelected ? nodeSelected : isHighlighted ? (lightMode ? '#1d4ed8' : '#e6f0ff') : isNeighbor ? nodeNeighbor : nodeDefault
                }

                ctx.beginPath()
                ctx.arc(node.x ?? 0, node.y ?? 0, r, 0, Math.PI * 2)
                ctx.fillStyle = fill
                ctx.shadowBlur = isSelected ? 18 : isHighlighted ? 10 : 0
                ctx.shadowColor = isSelected ? nodeSelected : isHighlighted ? nodeSelected : 'transparent'
                ctx.fill()
                ctx.shadowBlur = 0
                ctx.lineWidth = 1.5 / globalScale
                ctx.strokeStyle = isSelected
                  ? (lightMode ? '#0070f3' : 'rgba(100,255,218,0.9)')
                  : lightMode ? 'rgba(100,116,139,0.25)' : 'rgba(35,53,84,0.9)'
                ctx.stroke()

                if (globalScale > 2.1 && (isSelected || isNeighbor || isHighlighted)) {
                  const label = node.shortTitle
                  const fontSize = 12 / globalScale
                  ctx.font = `${fontSize}px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif`
                  ctx.textAlign = 'left'
                  ctx.textBaseline = 'middle'
                  ctx.fillStyle = lightMode ? '#1a202c' : 'rgba(204,214,246,0.95)'
                  ctx.fillText(label, (node.x ?? 0) + (r + 3) / globalScale, node.y ?? 0)
                }
              }}
              onNodeClick={(n) => {
                setSelectedId(n.id)
                setSelectedLink(null)
                setSelectedClusterId(null)
                setHighlightRelType(null)
                fgRef.current?.centerAt((n.x ?? 0) - SIDEBAR_OFFSET_X, n.y ?? 0, 600)
                fgRef.current?.zoom(3.2, 600)
                setPanelPaper(n)
              }}
              onLinkClick={(l) => {
                const link = l as GraphLink
                setSelectedLink(link)
                setSelectedId(null)
                setSelectedClusterId(null)
                // zoom to midpoint of the edge
                const src = link.source as GraphNode
                const tgt = link.target as GraphNode
                if (src.x != null && tgt.x != null) {
                  fgRef.current?.centerAt((src.x + tgt.x) / 2 - SIDEBAR_OFFSET_X, (src.y! + tgt.y!) / 2, 600)
                  fgRef.current?.zoom(3.5, 600)
                }
              }}
            />
          </div>

          <div className="stats">
            <div className="stat"><span className="statDot statDotAccent" />Nodes: {effectiveGraphData.nodes.length}</div>
            <div className="stat"><span className="statDot statDotBlue" />Edges: {effectiveGraphData.links.length}</div>
            <div className="stat"><span className="statDot statDotPurple" />Clusters: {clusters.length}</div>
          </div>
        </section>

        {/* ── Right sidebar ── */}
        <aside className="right glass panel">

          {/* ── Cluster view ── */}
          {viewMode === 'clusters' ? (
            <>
              <div className="panelHeaderRow">
                <div className="panelHeader">Topic Clusters</div>
                <div className="panelMeta">{clusters.length} clusters</div>
              </div>
              <div style={{ padding: '0 16px 10px' }}>
                <button
                  className="ghostBtn"
                  type="button"
                  disabled={clustering}
                  onClick={async () => {
                    setClustering(true)
                    try {
                      await rebuildClusters(5)
                      await reloadGraph()
                    } catch (e) {
                      console.error('Recluster failed:', e)
                    } finally {
                      setClustering(false)
                    }
                  }}
                  style={{
                    width: '100%',
                    opacity: clustering ? 0.7 : 1,
                    cursor: clustering ? 'not-allowed' : 'pointer',
                  }}
                  title="Rebuild topic clusters from current graph"
                >
                  {clustering ? 'Clustering…' : 'Recluster This Graph'}
                </button>
              </div>

              {selectedClusterId != null ? (() => {
                const c = clusters.find(cl => cl.id === selectedClusterId)!
                const color = CLUSTER_COLORS[c.id % CLUSTER_COLORS.length]
                const papers = filteredPaperNodes.filter(n => n.clusterId === c.id)
                return (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: '0 16px 16px' }}>
                    <button
                      type="button"
                      onClick={() => setSelectedClusterId(null)}
                      style={{ background: 'none', border: 'none', color: lightMode ? '#0070f3' : '#64ffda', cursor: 'pointer', textAlign: 'left', fontSize: 12, padding: 0 }}
                    >
                      ← Back to clusters
                    </button>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ width: 12, height: 12, borderRadius: '50%', background: color, flexShrink: 0, display: 'inline-block' }} />
                      <span style={{ fontWeight: 600, color: lightMode ? '#1a202c' : '#e6f0ff', fontSize: 14 }}>{c.name}</span>
                    </div>
                    {c.description && (
                      <p style={{ fontSize: 12, color: lightMode ? '#4a5568' : '#8892b0', lineHeight: 1.6, margin: 0 }}>{c.description}</p>
                    )}
                    <div style={{ fontSize: 11, color: lightMode ? '#0070f3' : '#64ffda' }}>{papers.length} papers</div>
                    <div className="paperList">
                      {papers.map(p => (
                        <button key={p.id} className="paperItem" type="button"
                          onClick={() => {
                            setSelectedId(p.id)
                            const n = idToNode.get(p.id)
                            if (n) { fgRef.current?.centerAt((n.x ?? 0) - SIDEBAR_OFFSET_X, n.y ?? 0, 600); fgRef.current?.zoom(3.2, 600) }
                          }}
                        >
                          <div className="paperTop">
                            <span className="chip">{p.primaryTopic}</span>
                            <span className="paperYear">{p.year}</span>
                          </div>
                          <div className="paperTitle">{p.title}</div>
                          <div className="paperAuthors">{p.authors}</div>
                        </button>
                      ))}
                    </div>
                  </div>
                )
              })() : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {clusters.map(c => {
                    const color = CLUSTER_COLORS[c.id % CLUSTER_COLORS.length]
                    return (
                      <button key={c.id} type="button"
                        onClick={() => {
                          setSelectedClusterId(c.id)
                          // highlight cluster nodes
                          const clusterNodes = effectiveGraphData.nodes.filter(n => n.clusterId === c.id)
                          if (clusterNodes.length > 0) {
                            const cx = clusterNodes.reduce((s, n) => s + (n.x ?? 0), 0) / clusterNodes.length
                            const cy = clusterNodes.reduce((s, n) => s + (n.y ?? 0), 0) / clusterNodes.length
                            fgRef.current?.centerAt(cx - SIDEBAR_OFFSET_X, cy, 600)
                            fgRef.current?.zoom(2.5, 600)
                          }
                        }}
                        style={{ display: 'flex', alignItems: 'flex-start', gap: 10, padding: '10px 12px', background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.07)', borderRadius: 8, cursor: 'pointer', textAlign: 'left', transition: 'background 0.15s' }}
                        onMouseEnter={(e) => (e.currentTarget.style.background = lightMode ? 'rgba(0,112,243,0.06)' : 'rgba(100,255,218,0.06)')}
                        onMouseLeave={(e) => (e.currentTarget.style.background = 'rgba(255,255,255,0.03)')}
                      >
                        <span style={{ width: 12, height: 12, borderRadius: '50%', background: color, flexShrink: 0, marginTop: 3 }} />
                        <div>
                          <div style={{ fontWeight: 600, fontSize: 13, color: lightMode ? '#1a202c' : '#e6f0ff', marginBottom: 3 }}>{c.name}</div>
                          <div style={{ fontSize: 11, color: lightMode ? '#4a5568' : '#8892b0', lineHeight: 1.5 }}>{c.description.slice(0, 90)}{c.description.length > 90 ? '…' : ''}</div>
                          <div style={{ fontSize: 10, color: lightMode ? '#0070f3' : '#64ffda', marginTop: 4 }}>{c.count} papers</div>
                        </div>
                      </button>
                    )
                  })}
                </div>
              )}
            </>
          ) : selectedLink ? (
            /* ── Edge detail view ── */
            <>
              <div className="panelHeaderRow">
                <div className="panelHeader">Relationship</div>
                <button type="button" onClick={() => setSelectedLink(null)}
                  style={{ background: 'none', border: 'none', color: lightMode ? '#4a5568' : '#8892b0', cursor: 'pointer', fontSize: 18, lineHeight: 1 }}>×</button>
              </div>

              {(() => {
                const meta = REL_META[selectedLink.relationship_type]
                const { src, tgt } = selectedLinkNodes ?? {}
                return (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 20, padding: '4px' }}>
                    {/* Relationship type badge */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', background: 'rgba(255,255,255,0.04)', borderRadius: 8, border: `1px solid ${meta.color}33` }}>
                      <span style={{ fontSize: 22 }}>{meta.emoji}</span>
                      <div>
                        <div style={{ fontWeight: 700, fontSize: 15, color: meta.color }}>{meta.label}</div>
                        <div style={{ fontSize: 11, color: lightMode ? '#4a5568' : '#8892b0', marginTop: 2 }}>Semantic relationship</div>
                      </div>
                    </div>

                    {/* Source */}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, padding: '12px 14px', background: 'rgba(255,255,255,0.03)', borderRadius: 8 }}>
                      <div style={{ fontSize: 10, color: lightMode ? '#0070f3' : '#64ffda', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>From</div>
                      <div style={{ fontSize: 13, color: lightMode ? '#1a202c' : '#e6f0ff', fontWeight: 500, lineHeight: 1.5 }}>{src?.title ?? '—'}</div>
                      <div style={{ fontSize: 11, color: lightMode ? '#4a5568' : '#8892b0' }}>{src?.authors} · {src?.year}</div>
                    </div>

                    {/* Arrow */}
                    <div style={{ textAlign: 'center', color: meta.color, fontSize: 18 }}>↓</div>

                    {/* Target */}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, padding: '12px 14px', background: 'rgba(255,255,255,0.03)', borderRadius: 8 }}>
                      <div style={{ fontSize: 10, color: lightMode ? '#0070f3' : '#64ffda', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>To</div>
                      <div style={{ fontSize: 13, color: lightMode ? '#1a202c' : '#e6f0ff', fontWeight: 500, lineHeight: 1.5 }}>{tgt?.title ?? '—'}</div>
                      <div style={{ fontSize: 11, color: lightMode ? '#4a5568' : '#8892b0' }}>{tgt?.authors} · {tgt?.year}</div>
                    </div>

                    {/* Reason */}
                    {selectedLinkReason ? (
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                        <div style={{ fontSize: 10, color: lightMode ? '#0070f3' : '#64ffda', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>Reasoning</div>
                        <p style={{ fontSize: 12, color: lightMode ? '#1a202c' : '#ccd6f6', lineHeight: 1.75, margin: 0, padding: '12px 14px', background: lightMode ? 'rgba(0,112,243,0.04)' : 'rgba(100,255,218,0.04)', borderLeft: `3px solid ${meta.color}`, borderRadius: '0 8px 8px 0' }}>
                          {selectedLinkReason}
                        </p>
                      </div>
                    ) : (
                      <div style={{ fontSize: 12, color: lightMode ? '#4a5568' : '#8892b0', fontStyle: 'italic', padding: '0 2px' }}>No reasoning available for this edge.</div>
                    )}

                    {/* Strength bar */}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '12px 14px', background: 'rgba(255,255,255,0.03)', borderRadius: 8 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div style={{ fontSize: 10, color: lightMode ? '#0070f3' : '#64ffda', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>Confidence</div>
                        <div style={{ fontSize: 12, color: lightMode ? '#1a202c' : '#ccd6f6', fontWeight: 600 }}>{(selectedLink.strength * 100).toFixed(0)}%</div>
                      </div>
                      <div style={{ height: 6, background: 'rgba(255,255,255,0.08)', borderRadius: 3, overflow: 'hidden' }}>
                        <div style={{ height: '100%', width: `${selectedLink.strength * 100}%`, background: meta.color, borderRadius: 3, transition: 'width 0.3s ease' }} />
                      </div>
                    </div>
                  </div>
                )
              })()}
            </>
          ) : (
            /* ── Default papers / semantic empty state ── */
            <>
              {viewMode === 'semantic' && !selectedLink ? (
                <>
                  <div className="panelHeaderRow">
                    <div className="panelHeader">Semantic Network</div>
                  </div>
                  <div style={{ padding: '16px', display: 'flex', flexDirection: 'column', gap: 16 }}>
                    <p style={{ fontSize: 12, color: lightMode ? '#4a5568' : '#8892b0', lineHeight: 1.7, margin: 0 }}>
                      This view shows AI-inferred semantic relationships between papers — whether findings <span style={{ color: REL_META.SUPPORT.color }}>support</span>, <span style={{ color: REL_META.CONTRADICT.color }}>contradict</span>, or are <span style={{ color: REL_META.NEUTRAL.color }}>neutral</span> relative to each other.
                    </p>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                      {(['SUPPORT', 'CONTRADICT', 'NEUTRAL'] as const).map(rel => {
                        const meta = REL_META[rel]
                        const count = rel === 'NEUTRAL'
                          ? effectiveGraphData.links.filter(l => l.relationship_type === 'NEUTRAL' || l.relationship_type === 'CITES').length
                          : effectiveGraphData.links.filter(l => l.relationship_type === rel).length
                        const isActive = highlightRelType === rel
                        return (
                          <button key={rel} type="button"
                            onClick={() => setHighlightRelType(isActive ? null : rel)}
                            style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', background: isActive ? `${meta.color}18` : 'rgba(255,255,255,0.03)', borderRadius: 8, border: `1px solid ${isActive ? meta.color : `${meta.color}22`}`, cursor: 'pointer', textAlign: 'left', transition: 'all 0.15s', width: '100%' }}
                            onMouseEnter={(e) => { if (!isActive) e.currentTarget.style.background = `${meta.color}0d` }}
                            onMouseLeave={(e) => { if (!isActive) e.currentTarget.style.background = 'rgba(255,255,255,0.03)' }}
                          >
                            <span style={{ fontSize: 16 }}>{meta.emoji}</span>
                            <div style={{ flex: 1 }}>
                              <span style={{ fontSize: 13, color: meta.color, fontWeight: 600 }}>{meta.label}</span>
                            </div>
                            <span style={{ fontSize: 12, color: isActive ? meta.color : (lightMode ? '#4a5568' : '#8892b0'), fontWeight: isActive ? 600 : 400 }}>{count} edges</span>
                          </button>
                        )
                      })}
                    </div>
                    <p style={{ fontSize: 11, color: lightMode ? '#4a5568' : '#8892b0', lineHeight: 1.6, margin: 0, fontStyle: 'italic' }}>
                      Click an edge in the graph to see the full reasoning.
                    </p>
                  </div>
                </>
              ) : (
              <>
              <div className="panelHeaderRow">
                <div className="panelHeader">Papers</div>
                <div className="panelMeta">{filteredPaperNodes.length} shown</div>
              </div>

              <div className="paperList">
                {filteredPaperNodes.map((p) => {
                  const isActive = p.id === selectedId
                  return (
                    <button key={p.id} className={`paperItem ${isActive ? 'paperItemActive' : ''}`} type="button"
                      onClick={() => {
                        setSelectedId(p.id)
                        setSelectedLink(null)
                        const n = idToNode.get(p.id)
                        if (n) { fgRef.current?.centerAt((n.x ?? 0) - SIDEBAR_OFFSET_X, n.y ?? 0, 600); fgRef.current?.zoom(3.2, 600) }
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
                    <button
                      className="cta"
                      type="button"
                      onClick={() => setPanelPaper(selectedPaper)}
                    >
                      Explore Abstract
                    </button>
                  </>
                ) : (
                  <div className="previewEmpty">Click a paper node or pick one from the list.</div>
                )}
              </div>
              </>
              )}
            </>
          )}
        </aside>
        </>
        )}
      </section>
      {rebuildProgress > 0 && (
        <div
          style={{
            position: 'fixed',
            left: 20,
            right: 20,
            bottom: 16,
            zIndex: 1200,
            borderRadius: 10,
            border: '1px solid rgba(100,255,218,0.28)',
            background: 'rgba(10,25,47,0.92)',
            boxShadow: '0 10px 28px rgba(0,0,0,0.35)',
            padding: '10px 12px',
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              fontSize: 12,
              color: '#ccd6f6',
              marginBottom: 8,
            }}
          >
            <span>{rebuildingGraph ? 'Rebuilding graph…' : 'Graph rebuilt'}</span>
            <span style={{ color: '#64ffda' }}>{Math.round(rebuildProgress)}%</span>
          </div>
          <div
            style={{
              width: '100%',
              height: 8,
              borderRadius: 999,
              background: 'rgba(255,255,255,0.08)',
              overflow: 'hidden',
            }}
          >
            <div
              style={{
                width: `${rebuildProgress}%`,
                height: '100%',
                background: 'linear-gradient(90deg, #64ffda 0%, #4dd0e1 100%)',
                transition: 'width 260ms ease',
              }}
            />
          </div>
        </div>
      )}

      {panelPaper && (
        <PaperPanel
          paper={panelPaper}
          lightMode={lightMode}
          onClose={() => setPanelPaper(null)}
        />
      )}
    </main>
  )
}
