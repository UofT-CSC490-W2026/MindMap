
-- Initial Setup
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.APP_QA_LOGS;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.GOLD_SUMMARY_EVIDENCE;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.GOLD_PAPER_SUMMARIES;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.GOLD_PAPER_RELATIONSHIPS;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPER_CHUNKS;
DROP TABLE IF EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPER_SECTIONS;
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
    full_text TEXT,
    full_text_source STRING,
    full_text_extracted_at TIMESTAMP_NTZ,
    reference_list VARIANT,
    citation_list VARIANT,
    embedding VECTOR(FLOAT, 384),
    similar_embeddings_ids VARIANT, -- List of paper IDs with similar embeddings (optional, can be populated later)
    tldr STRING
);

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPER_SECTIONS (
    section_id INT IDENTITY(1,1) PRIMARY KEY,
    paper_id INT NOT NULL,
    section_name STRING,              -- abstract, introduction, methods, results, conclusion
    section_order INT,
    content TEXT,
    token_estimate INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id)
);

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.SILVER_PAPER_CHUNKS (
    chunk_id INT IDENTITY(1,1) PRIMARY KEY,
    paper_id INT NOT NULL,
    section_id INT,
    chunk_index INT,
    chunk_text TEXT,
    token_estimate INT,
    embedding VECTOR(FLOAT, 384),
    chunk_type STRING,                -- body, abstract, conclusion, reference_context
    metadata VARIANT,                 -- page range, char offsets, parser info, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id),
    FOREIGN KEY (section_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPER_SECTIONS(section_id)
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

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.GOLD_PAPER_SUMMARIES (
    paper_id INT PRIMARY KEY,
    summary_json VARIANT,             -- methods, claims, findings, limitations, conclusion
    model_name STRING,
    prompt_version STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id)
);

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.GOLD_SUMMARY_EVIDENCE (
    paper_id INT,
    summary_field STRING,             -- methods, claim_1, conclusion, etc.
    chunk_id INT,
    evidence_rank INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (paper_id, summary_field, chunk_id),
    FOREIGN KEY (paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id),
    FOREIGN KEY (chunk_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPER_CHUNKS(chunk_id)
);

CREATE TABLE IF NOT EXISTS MINDMAP_DB.PUBLIC.APP_QA_LOGS (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    session_id STRING NOT NULL,
    paper_id INT NOT NULL,
    role STRING NOT NULL,
    message TEXT NOT NULL,
    rewritten_query STRING,
    cited_chunk_ids VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (paper_id) REFERENCES MINDMAP_DB.PUBLIC.SILVER_PAPERS(id)
);
