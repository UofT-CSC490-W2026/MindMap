-- =========================================================
-- MindMap: Bronze / Silver / Gold schema + tables
-- =========================================================

-- ---------- 0) Database + Warehouse ----------
CREATE DATABASE IF NOT EXISTS MINDMAP_DB;

CREATE WAREHOUSE IF NOT EXISTS MINDMAP_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Use them
USE WAREHOUSE MINDMAP_WH;
USE DATABASE MINDMAP_DB;

-- ---------- 1) Schemas ----------
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- =========================================================
-- BRONZE: Raw data lake (unedited JSON)
-- =========================================================
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS PAPERS_RAW (
  ingestion_id STRING DEFAULT UUID_STRING(),
  raw_payload VARIANT,
  ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source STRING DEFAULT 'arxiv'
);

-- Helpful for debugging: record the arXiv entry_id if present
-- (not required; raw_payload is the source of truth)
CREATE VIEW IF NOT EXISTS PAPERS_RAW_FLATTENED AS
SELECT
  ingestion_id,
  ingested_at,
  source,
  raw_payload:"entry_id"::STRING AS entry_id,
  raw_payload:"title"::STRING AS title,
  raw_payload:"primary_category"::STRING AS primary_category,
  raw_payload:"published"::STRING AS published
FROM PAPERS_RAW;

-- =========================================================
-- SILVER: Structured warehouse (clean metadata + links)
-- =========================================================
USE SCHEMA SILVER;

-- Core paper metadata
CREATE TABLE IF NOT EXISTS PAPERS (
  paper_id STRING DEFAULT UUID_STRING(),         -- internal ID
  arxiv_id STRING UNIQUE,                        -- e.g., "2307.09288"
  ss_id STRING,                                  -- Semantic Scholar paperId (optional)
  doi STRING,                                    -- optional
  title STRING,
  abstract STRING,
  conclusion STRING,
  published_date DATE,
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (paper_id)
);

-- References/citations edges stored as rows (recommended design)
-- link_type: 'REFERENCE' (this paper cites other), 'CITATION' (other cites this paper)
CREATE TABLE IF NOT EXISTS PAPER_LINKS (
  source_paper_id STRING NOT NULL,               -- paper_id in SILVER.PAPERS
  target_external_id STRING,                     -- e.g., arxiv_id or ss_id of the target (if not ingested yet)
  target_paper_id STRING,                        -- if target exists in SILVER.PAPERS, store its paper_id
  link_type STRING NOT NULL,                     -- 'REFERENCE' or 'CITATION'
  link_rank INT,                                 -- 1..10 (your pipeline enforces top 10)
  link_text STRING,                              -- raw reference string (useful when parsing PDFs)
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT chk_link_type CHECK (link_type IN ('REFERENCE', 'CITATION'))
);

-- Embeddings table (kept separate to allow multiple models/versions cleanly)
-- (This matches your current vector approach.)
CREATE TABLE IF NOT EXISTS PAPER_EMBEDDINGS (
  paper_id STRING NOT NULL,
  model_name STRING NOT NULL,
  embedding VECTOR(FLOAT, 384),
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (paper_id, model_name),
  CONSTRAINT fk_emb_paper FOREIGN KEY (paper_id) REFERENCES PAPERS(paper_id)
);

-- =========================================================
-- GOLD: Insights / Knowledge graph relationships
-- =========================================================
USE SCHEMA GOLD;

CREATE TABLE IF NOT EXISTS PAPER_RELATIONSHIPS (
  source_paper_id STRING NOT NULL,               -- paper being analyzed
  target_paper_id STRING NOT NULL,               -- related paper
  relationship_type STRING NOT NULL,             -- SUPPORTS/CONTRADICTS/MENTIONS
  evidence_snippet STRING,                       -- short text justification from LLM
  semantic_similarity_score FLOAT,               -- from embedding similarity
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT chk_rel_type CHECK (relationship_type IN ('SUPPORTS', 'CONTRADICTS', 'MENTIONS'))
);

-- Optional: prevent duplicate edges of same type
-- (If you want multiple snippets per pair, remove this.)
CREATE UNIQUE INDEX IF NOT EXISTS UX_REL
  ON PAPER_RELATIONSHIPS (source_paper_id, target_paper_id, relationship_type);

-- =========================================================
-- Sanity check
-- =========================================================
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();

SHOW TABLES IN SCHEMA BRONZE;
SHOW TABLES IN SCHEMA SILVER;
SHOW TABLES IN SCHEMA GOLD;
