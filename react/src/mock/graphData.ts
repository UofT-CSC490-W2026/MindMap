export type NodeKind = 'paper' | 'topic'
export type LinkKind = 'cites' | 'about'

export type GraphNode = {
  id: string
  kind: NodeKind
  // shared
  label: string
  // paper fields
  title: string
  shortTitle: string
  authors: string
  year: number
  citations: number
  primaryTopic: string
  searchText: string
}

export type GraphLink = {
  source: string
  target: string
  kind: LinkKind
}

const papers = [
  {
    id: 'paper:attention',
    title: 'Attention Is All You Need',
    shortTitle: 'Attention Is All You Need',
    authors: 'Vaswani et al.',
    year: 2017,
    citations: 14203,
    primaryTopic: 'NLP',
    topics: ['Transformers', 'Sequence Modeling', 'Self-Attention'],
    cites: ['paper:bert', 'paper:word2vec'],
  },
  {
    id: 'paper:bert',
    title: 'BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding',
    shortTitle: 'BERT',
    authors: 'Devlin et al.',
    year: 2018,
    citations: 85321,
    primaryTopic: 'NLP',
    topics: ['Transformers', 'Pretraining', 'Embeddings'],
    cites: ['paper:word2vec'],
  },
  {
    id: 'paper:word2vec',
    title: 'Distributed Representations of Words and Phrases and their Compositionality',
    shortTitle: 'word2vec',
    authors: 'Mikolov et al.',
    year: 2013,
    citations: 119500,
    primaryTopic: 'Embeddings',
    topics: ['Embeddings', 'Representation Learning'],
    cites: [],
  },
  {
    id: 'paper:gnn',
    title: 'A Gentle Introduction to Graph Neural Networks',
    shortTitle: 'Graph Neural Networks',
    authors: 'Hamilton',
    year: 2020,
    citations: 9800,
    primaryTopic: 'Graphs',
    topics: ['Graph Neural Networks', 'Representation Learning'],
    cites: ['paper:deepwalk'],
  },
  {
    id: 'paper:deepwalk',
    title: 'DeepWalk: Online Learning of Social Representations',
    shortTitle: 'DeepWalk',
    authors: 'Perozzi et al.',
    year: 2014,
    citations: 15300,
    primaryTopic: 'Graphs',
    topics: ['Graph Embeddings', 'Random Walks', 'Embeddings'],
    cites: ['paper:word2vec'],
  },
  {
    id: 'paper:lda',
    title: 'Latent Dirichlet Allocation',
    shortTitle: 'LDA',
    authors: 'Blei, Ng, Jordan',
    year: 2003,
    citations: 89500,
    primaryTopic: 'Topics',
    topics: ['Topic Modeling', 'Probabilistic Models'],
    cites: [],
  },
  {
    id: 'paper:tfidf',
    title: 'A Statistical Interpretation of Term Specificity and its Application in Retrieval',
    shortTitle: 'TF-IDF',
    authors: 'Jones',
    year: 1972,
    citations: 22500,
    primaryTopic: 'IR',
    topics: ['Information Retrieval', 'TF-IDF'],
    cites: [],
  },
  {
    id: 'paper:rag',
    title: 'Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks',
    shortTitle: 'RAG',
    authors: 'Lewis et al.',
    year: 2020,
    citations: 9100,
    primaryTopic: 'NLP',
    topics: ['Information Retrieval', 'Transformers'],
    cites: ['paper:bert', 'paper:tfidf'],
  },
] as const

const allTopics = Array.from(
  new Set(papers.flatMap((p) => p.topics).map((t) => t.trim()).filter(Boolean)),
)

const topicId = (t: string) => `topic:${t.toLowerCase().replaceAll(/\s+/g, '-')}`

const topicNodes: GraphNode[] = allTopics.map((t) => ({
  id: topicId(t),
  kind: 'topic',
  label: t,
  title: t,
  shortTitle: t,
  authors: '',
  year: 0,
  citations: 0,
  primaryTopic: 'Topic',
  searchText: t,
}))

const paperNodes: GraphNode[] = papers.map((p) => ({
  id: p.id,
  kind: 'paper',
  label: p.title,
  title: p.title,
  shortTitle: p.shortTitle,
  authors: p.authors,
  year: p.year,
  citations: p.citations,
  primaryTopic: p.primaryTopic,
  searchText: `${p.title} ${p.shortTitle} ${p.authors} ${p.year} ${p.primaryTopic} ${p.topics.join(' ')}`,
}))

const links: GraphLink[] = [
  // paper -> topic (silver-esque)
  ...papers.flatMap((p) =>
    p.topics.map((t) => ({
      source: p.id,
      target: topicId(t),
      kind: 'about' as const,
    })),
  ),
  // paper citations (gold-esque)
  ...papers.flatMap((p) =>
    p.cites.map((tgt) => ({
      source: p.id,
      target: tgt,
      kind: 'cites' as const,
    })),
  ),
]

export const graphData = {
  nodes: [...paperNodes, ...topicNodes],
  links,
} as const

