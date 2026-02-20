
-- Initial Setup
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.GOLD_PAPER_RELATIONSHIPS;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPERS;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.BRONZE_PAPERS;

CREATE DATABASE IF NOT EXISTS MINDMAP_DB;
CREATE WAREHOUSE IF NOT EXISTS MINDMAP_WH WAREHOUSE_SIZE = 'XSMALL';

USE DATABASE MINDMAP_DB;
USE SCHEMA PUBLIC;

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
    similar_embeddings_ids VARIANT -- List of paper IDs with similar embeddings (optional, can be populated later)
);

-- GOLD LAYER: Paper Relationships
CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.GOLD_PAPER_RELATIONSHIPS (
    source_paper_id INT,
    target_paper_id INT,
    relationship_type VARCHAR(50), -- 'CITES', 'SIMILAR'
    strength FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (source_paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id),
    FOREIGN KEY (target_paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id),
    PRIMARY KEY (source_paper_id, target_paper_id, relationship_type)
);