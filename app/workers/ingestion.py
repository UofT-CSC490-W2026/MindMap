import arxiv
import json
import re
import threading
import time
from config import app, image, snowflake_secret, semantic_scholar_secret, DATABASE, SCHEMA, qualify_table
from utils import connect_to_snowflake
import os


_SS_MIN_INTERVAL_SECONDS = 1.05
_ss_lock = threading.Lock()
_ss_last_request_ts = 0.0


def _bronze_papers_table(database: str = DATABASE, schema: str = SCHEMA) -> str:
    return qualify_table("BRONZE_PAPERS", database=database, schema=schema)


def _extract_arxiv_id(external_ids: dict) -> str | None:
    if not isinstance(external_ids, dict):
        return None
    raw = external_ids.get("ArXiv")
    if not raw:
        return None
    m = re.search(r"(\d{4}\.\d{4,5})(?:v\d+)?", str(raw))
    return m.group(1) if m else None


def _ss_get_json(url: str, params: dict, timeout: float = 30.0) -> dict:
    import httpx

    global _ss_last_request_ts
    with _ss_lock:
        now = time.time()
        wait = _SS_MIN_INTERVAL_SECONDS - (now - _ss_last_request_ts)
        if wait > 0:
            time.sleep(wait)
        _ss_last_request_ts = time.time()

    api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
    headers = {"x-api-key": api_key} if api_key else None
    response = httpx.get(url, params=params, timeout=timeout, headers=headers)
    if response.status_code == 429:
        # Backoff once while still preserving the strict lower-bound interval.
        time.sleep(2.0)
        with _ss_lock:
            now = time.time()
            wait = _SS_MIN_INTERVAL_SECONDS - (now - _ss_last_request_ts)
            if wait > 0:
                time.sleep(wait)
            _ss_last_request_ts = time.time()
        response = httpx.get(url, params=params, timeout=timeout, headers=headers)

    response.raise_for_status()
    return response.json()


# Credentials should be stored in a Modal Secret
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret])
def ingest_from_arxiv(query: str, max_results: int = 5, database: str = DATABASE, schema: str = SCHEMA):
    """
    Step 1: Ingestion to Bronze Layer (Idempotent)
    Fulfills Use Case 2: User types a general topic.
    Only inserts papers that don't already exist by entry_id.
    """
    search = arxiv.Search(query=query, max_results=max_results)
    
    conn = connect_to_snowflake(database=database, schema=schema)
    cur = conn.cursor()
    
    bronze_table = _bronze_papers_table(database=database, schema=schema)
    ingested_count = 0
    skipped_count = 0

    for result in search.results():
        # 1. Check if this paper already exists (idempotency)
        cur.execute(
            f'SELECT 1 FROM {bronze_table} WHERE raw_payload:entry_id::STRING = %s LIMIT 1',
            (result.entry_id,)
        )
        if cur.fetchone():
            skipped_count += 1
            continue
        
        # 2. Convert the ArXiv object to a serializable dictionary
        raw_data = {
            "entry_id": result.entry_id,
            "updated": str(result.updated),
            "published": str(result.published),
            "title": result.title,
            "authors": [author.name for author in result.authors],
            "summary": result.summary,
            "comment": result.comment,
            "journal_ref": result.journal_ref,
            "doi": result.doi,
            "primary_category": result.primary_category,
            "categories": result.categories,
            "links": [link.href for link in result.links],
            "pdf_url": result.pdf_url
        }
        
        # 3. Convert dictionary to a valid JSON string
        json_payload = json.dumps(raw_data)
        
        # 4. Insert into the Bronze Table
        cur.execute(
            f'INSERT INTO {bronze_table} (raw_payload) SELECT PARSE_JSON(%s)',
            (json_payload,)
        )
        ingested_count += 1
    
    conn.commit()
    if skipped_count > 0:
        print(f"Ingested {ingested_count} papers (skipped {skipped_count} duplicates) into Bronze layer.")
    else:
        print(f"Ingested {ingested_count} papers into Bronze layer.")


@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], timeout=60 * 5, max_containers=1)
def ingest_from_semantic_scholar(
    query: str,
    max_results: int = 25,
    database: str = DATABASE,
    schema: str = SCHEMA,
):
    """
    Step 1: Ingestion to Bronze Layer using Semantic Scholar search.
    Idempotent and normalized to the same Bronze payload shape used downstream.

    Notes:
    - We keep Bronze as raw landing storage for lineage/reprocessing.
    - We only ingest rows that have an ArXiv id, because downstream transformation
      expects arXiv ids for PDF/conclusion extraction.
    """
    bronze_table = _bronze_papers_table(database=database, schema=schema)
    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": query,
        "limit": int(max_results),
        "fields": (
            "paperId,title,abstract,authors,externalIds,year,url,openAccessPdf,"
            "publicationDate,journal,citationCount,referenceCount"
        ),
    }

    conn = connect_to_snowflake(database=database, schema=schema)
    cur = conn.cursor()

    inserted = 0
    skipped_dupe = 0
    skipped_no_arxiv = 0

    try:
        data = _ss_get_json(url=url, params=params, timeout=30.0).get("data", [])

        for paper in data:
            external_ids = paper.get("externalIds") or {}
            arxiv_id = _extract_arxiv_id(external_ids)
            if not arxiv_id:
                skipped_no_arxiv += 1
                continue

            entry_id = f"https://arxiv.org/abs/{arxiv_id}"
            ss_paper_id = paper.get("paperId")

            cur.execute(
                f"""
                SELECT 1
                FROM {bronze_table}
                WHERE raw_payload:entry_id::STRING = %s
                   OR raw_payload:ss_paper_id::STRING = %s
                LIMIT 1
                """,
                (entry_id, str(ss_paper_id) if ss_paper_id else None),
            )
            if cur.fetchone():
                skipped_dupe += 1
                continue

            open_pdf = paper.get("openAccessPdf") or {}
            authors = [a.get("name") for a in (paper.get("authors") or []) if a.get("name")]

            raw_data = {
                "source": "semantic_scholar",
                "entry_id": entry_id,
                "ss_paper_id": ss_paper_id,
                "updated": str(paper.get("publicationDate") or ""),
                "published": str(paper.get("publicationDate") or ""),
                "title": paper.get("title"),
                "authors": authors,
                "summary": paper.get("abstract"),
                "comment": None,
                "journal_ref": (paper.get("journal") or {}).get("name"),
                "doi": external_ids.get("DOI"),
                "primary_category": None,
                "categories": [],
                "links": [paper.get("url")] if paper.get("url") else [],
                "pdf_url": open_pdf.get("url"),
                "external_ids": external_ids,
            }

            cur.execute(
                f"INSERT INTO {bronze_table} (raw_payload) SELECT PARSE_JSON(%s)",
                (json.dumps(raw_data),),
            )
            inserted += 1

        conn.commit()
        print(
            "Semantic Scholar ingest complete: "
            f"inserted={inserted}, skipped_duplicates={skipped_dupe}, skipped_no_arxiv={skipped_no_arxiv}"
        )
    finally:
        cur.close()
        conn.close()

# testing function to see content of ingested bronze papers
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret])
def peek_bronze(limit: int = 3, database: str = DATABASE, schema: str = SCHEMA):
    """
    Inspects the raw JSON in Bronze, specifically focusing on the Abstract (summary).
    """
    import json
    import textwrap

    conn = connect_to_snowflake(database=database, schema=schema)
    cur = conn.cursor()

    try:
        # Pull the raw_payload from Snowflake
        cur.execute(f'SELECT raw_payload FROM {_bronze_papers_table(database=database, schema=schema)} LIMIT %s', (limit,))
        rows = cur.fetchall()

        print(f"\n--- BRONZE LAYER PEEK: {len(rows)} PAPERS ---\n")

        for (raw_json_str,) in rows:
            data = json.loads(raw_json_str)
            
            # Extract fields
            title = data.get("title", "No Title")
            arxiv_id = data.get("entry_id", "No ID")
            abstract = data.get("summary", "No Abstract Found")

            print(f"TITLE: {title}")
            print(f"ID:    {arxiv_id}")
            print(f"ABSTRACT:")
            
            # Wrap text to 80 characters for terminal readability
            wrapped_abstract = textwrap.fill(abstract, width=80)
            print(wrapped_abstract)
            
            print("\n" + "="*80 + "\n")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()


# ADD THIS - Required for CLI to work!
@app.local_entrypoint()
def main(query: str, max_results: int = 5, source: str = "semantic_scholar"):
    """CLI entry point for running ingestion from terminal."""
    if source == "semantic_scholar":
        result = ingest_from_semantic_scholar.remote(query=query, max_results=max_results)
    else:
        result = ingest_from_arxiv.remote(query=query, max_results=max_results)
    return result
