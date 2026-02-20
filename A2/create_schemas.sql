
-- Initial Setup
CREATE DATABASE IF NOT EXISTS MINDMAP_DB;
CREATE WAREHOUSE IF NOT EXISTS MINDMAP_WH WAREHOUSE_SIZE = 'XSMALL';

-- BRONZE LAYER: Raw Data Lake (Stores unedited JSON)

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.BRONZE_PAPERS (
    ingestion_id UUID DEFAULT UUID_STRING(),
    raw_payload VARIANT, -- Native JSON storage
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPERS (
    id INT IDENTITY(1,1) PRIMARY KEY, -- Starts at 1, increments by 1
    arxiv_id STRING UNIQUE,
    ss_id STRING UNIQUE,
    title STRING,
    abstract STRING, 
    conclusion TEXT,
    reference_list VARIANT,
    citation_list VARIANT,
    embedding VECTOR(FLOAT, 384),
    similar_embeddings_ids VARIANT, -- List of paper IDs with similar embeddings (optional, can be populated later
);

-- GOLD LAYER: Insights (Knowledge Graph Edges)
CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.GOLD_CONNECTIONS (
    source_id STRING,
    target_id STRING,
    relationship STRING, -- 'SUPPORTS', 'CONTRADICTS', or 'CITES'
    confidence_score FLOAT
);
