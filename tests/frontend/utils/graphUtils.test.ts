import { describe, expect, it } from 'vitest'
import { buildGraph, clusterColor, CLUSTER_COLORS } from '../../../react/src/utils/graphUtils'
import type { Paper, Relationship } from '../types/paper'

describe('graphUtils', () => {
  it('buildGraph maps papers and relationships to graph nodes/links', () => {
    const papers: Paper[] = [
      {
        id: 1,
        title: 'Attention Is All You Need',
        shortTitle: 'Attention',
        authors: 'Vaswani et al.',
        year: 2017,
        citations: 1000,
        primaryTopic: 'Transformers',
        clusterId: 2,
        clusterName: 'NLP',
        clusterDescription: 'Natural language processing',
      },
    ]
    const relationships: Relationship[] = [
      {
        source_paper_id: 1,
        target_paper_id: 2,
        relationship_type: 'CITES',
        strength: 1.0,
        reason: 'Referenced in related work',
      },
    ]

    const { nodes, links } = buildGraph(papers, relationships)

    expect(nodes).toHaveLength(1)
    expect(nodes[0].id).toBe(1)
    expect(nodes[0].searchText).toContain('Attention Is All You Need')
    expect(links).toEqual([
      {
        source: 1,
        target: 2,
        relationship_type: 'CITES',
        strength: 1.0,
        reason: 'Referenced in related work',
      },
    ])
  })

  it('clusterColor returns fallback color when cluster id is undefined/null', () => {
    expect(clusterColor(undefined)).toBe('#112240')
    expect(clusterColor(null as unknown as number)).toBe('#112240')
  })

  it('clusterColor cycles through CLUSTER_COLORS by modulo', () => {
    expect(clusterColor(0)).toBe(CLUSTER_COLORS[0])
    expect(clusterColor(11)).toBe(CLUSTER_COLORS[11 % CLUSTER_COLORS.length])
  })
})

