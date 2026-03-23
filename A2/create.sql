-- Initial Setup
CREATE DATABASE IF NOT EXISTS MINDMAP_DB;
CREATE WAREHOUSE IF NOT EXISTS MINDMAP_WH WAREHOUSE_SIZE = 'XSMALL';

-- BRONZE LAYER: Raw Data Lake (Stores unedited JSON)
CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.BRONZE_PAPERS (
    ingestion_id UUID DEFAULT UUID_STRING(),
    raw_payload VARIANT, -- Native JSON storage
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SILVER LAYER: Structured Warehouse (Cleaned Metadata)
CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPERS (
    paper_id UUID DEFAULT UUID_STRING(),
    arxiv_id STRING UNIQUE,
    title STRING,
    abstract STRING,
    published_date DATE
);

-- GOLD LAYER: Insights (Knowledge Graph Edges)
CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.GOLD_CONNECTIONS (
    source_id STRING,
    target_id STRING,
    relationship STRING, -- 'SUPPORTS', 'CONTRADICTS', or 'CITES'
    confidence_score FLOAT
);