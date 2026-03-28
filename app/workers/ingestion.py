import requests
import json
import re
import threading
import time
import os

from app.config import app, image, snowflake_secret, semantic_scholar_secret, DATABASE, qualify_table
from app.utils import connect_to_snowflake


_SS_MIN_INTERVAL_SECONDS = 1.05
_ss_lock = threading.Lock()
_ss_last_request_ts = 0.0


def _bronze_papers_table(database: str = DATABASE) -> str:
    return qualify_table("BRONZE_PAPERS", database=database)


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _resolve_bronze_payload_column(cur, bronze_table: str) -> str:
    """Return the exact SQL identifier for the Bronze payload column."""
    cur.execute(f"DESC TABLE {bronze_table}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]

    for name in columns:
        if str(name).lower() == "raw_payload":
            return _quote_ident(str(name))

    raise RuntimeError(
        f"Could not find raw payload column in {bronze_table}. "
        f"Columns found: {columns}"
    )


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

    def _request(req_headers):
        return httpx.get(url, params=params, timeout=timeout, headers=req_headers)

    response = _request(headers)
    # If provided key is invalid/forbidden, retry once without auth to use public access.
    if headers and response.status_code in (401, 403):
        print("Semantic Scholar key rejected; retrying request without API key.")
        headers = None
        response = _request(headers)

    if response.status_code == 429:
        # Backoff once while still preserving the strict lower-bound interval.
        time.sleep(2.0)
        with _ss_lock:
            now = time.time()
            wait = _SS_MIN_INTERVAL_SECONDS - (now - _ss_last_request_ts)
            if wait > 0:
                time.sleep(wait)
            _ss_last_request_ts = time.time()
        response = _request(headers)

        # If rate-limited key-auth also resolves to unauthorized, final fallback to public access.
        if headers and response.status_code in (401, 403):
            print("Semantic Scholar key rejected after retry; falling back to unauthenticated access.")
            response = _request(None)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        # Raise a plain exception so Modal can always deserialize it locally.
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = ""
        if exc.response is not None:
            body = (exc.response.text or "")[:400]
        raise RuntimeError(
            f"Semantic Scholar request failed (status={status}) for url={url} params={params}. "
            f"Response body: {body}"
        ) from None

    return response.json()


# Credentials should be stored in a Modal Secret

@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret])
def ingest_from_openalex(query: str, max_results: int = 25, database: str = DATABASE):
    """
    Step 1: Ingestion to Bronze Layer using OpenAlex search.
    Only inserts papers that don't already exist by entry_id.
    """
    bronze_table = _bronze_papers_table(database=database)
    print("Using bronze_table:", bronze_table)
    url = "https://api.openalex.org/works"
    params = {
        "search": query,
        "per-page": int(max_results),
        "mailto": "your-email@example.com"  # Replace with your email for OpenAlex API etiquette
    }

    conn = connect_to_snowflake(database=database, schema="BRONZE")
    cur = conn.cursor()

    inserted = 0
    skipped_dupe = 0
    skipped_no_id = 0

    try:
        response = requests.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        data = response.json().get("results", [])

        # Print all possible properties (keys) for each paper
        # print("\n--- OpenAlex Paper Properties (keys) ---")
        # for idx, paper in enumerate(data):
        #     print(f"Paper {idx+1} keys: {sorted(list(paper.keys()))}")
        # print("--- End of Paper Properties ---\n")

        for paper in data:
            openalex_id = paper.get("id")
            if not openalex_id:
                skipped_no_id += 1
                continue

            # Use OpenAlex ID as entry_id
            entry_id = openalex_id

            cur.execute(
                f'SELECT 1 FROM {bronze_table} WHERE "raw_payload":entry_id::STRING = %s LIMIT 1',
                (entry_id,)
            )
            if cur.fetchone():
                skipped_dupe += 1
                continue

            authors = [a.get("author", {}).get("display_name") for a in (paper.get("authorships") or []) if a.get("author", {}).get("display_name")]

            raw_data = {
                "source": "openalex",
                "entry_id": entry_id,
                "openalex_id": openalex_id,
                "title": paper.get("title"),
                "authors": authors,
                "summary": paper.get("abstract_inverted_index"),
                "publication_date": paper.get("publication_date"),
                "doi": paper.get("doi"),
                "primary_location": (paper.get("primary_location") or {}).get("source", {}).get("display_name"),
                "pdf_url": (paper.get("primary_location") or {}).get("url"),
                "external_ids": paper.get("ids"),
                "cited_by_count": paper.get("cited_by_count"),
                "referenced_works": paper.get("referenced_works"),
                "related_works": paper.get("related_works"),
            }

            print(f"Ingested paper: {raw_data['title']} ({raw_data['publication_date']}), Authors: {raw_data['authors']}")
            print(f"ids: {raw_data['external_ids']}")

            cur.execute(
                f'INSERT INTO {bronze_table} ("raw_payload") SELECT PARSE_JSON(%s)',
                (json.dumps(raw_data),)
            )
            inserted += 1

        conn.commit()
        print(
            "OpenAlex ingest complete: "
            f"inserted={inserted}, skipped_duplicates={skipped_dupe}, skipped_no_id={skipped_no_id}"
        )
    finally:
        cur.close()
        conn.close()


@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret])
def ingest_from_arxiv(query: str, max_results: int = 5, database: str = DATABASE):
    """
    Step 1: Ingestion to Bronze Layer (Idempotent)
    Fulfills Use Case 2: User types a general topic.
    Only inserts papers that don't already exist by entry_id.
    """
    import arxiv

    search = arxiv.Search(query=query, max_results=max_results)
    
    conn = connect_to_snowflake(database=database, schema="BRONZE")
    cur = conn.cursor()
    
    bronze_table = _bronze_papers_table(database=database)
    payload_col = _resolve_bronze_payload_column(cur, bronze_table)

    ingested_count = 0
    skipped_count = 0

    for result in search.results():
        # 1. Check if this paper already exists (idempotency)
        cur.execute(
            f'SELECT 1 FROM {bronze_table} WHERE {payload_col}:entry_id::STRING = %s LIMIT 1',
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
            f'INSERT INTO {bronze_table} ({payload_col}) SELECT PARSE_JSON(%s)',
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
):
    """
    Step 1: Ingestion to Bronze Layer using Semantic Scholar search.
    Idempotent and normalized to the same Bronze payload shape used downstream.

    Notes:
    - We keep Bronze as raw landing storage for lineage/reprocessing.
    - We only ingest rows that have an ArXiv id, because downstream transformation
      expects arXiv ids for PDF/conclusion extraction.
    """
    safe_query = (query or "").strip()
    if not safe_query:
        raise ValueError(
            "Semantic Scholar ingestion requires a non-empty query. "
            "Pass --query \"your topic\" (for example: --query \"transformers\")."
        )

    bronze_table = _bronze_papers_table(database=database)
    print("Using bronze_table:", bronze_table)
    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    # Prepare API query parameters
    params = {
        "query": safe_query,
        "limit": int(max_results),
        "fields": (
            "paperId,title,abstract,authors,externalIds,year,url,openAccessPdf,"
            "publicationDate,journal,citationCount,referenceCount"
        ),
    }

    # Connect to Snowflake Bronze layer
    conn = connect_to_snowflake(database=database, schema="BRONZE")
    cur = conn.cursor()
    payload_col = _resolve_bronze_payload_column(cur, bronze_table)

    inserted = 0
    skipped_dupe = 0
    skipped_no_arxiv = 0

    # Profiled because: _ss_get_json makes a rate-limited HTTP call, then for
    # each paper we do a Snowflake duplicate-check SELECT + INSERT — the
    # per-paper round-trips to Snowflake compound quickly at large max_results.

    try:
        # Fetch papers from Semantic Scholar API
        data = _ss_get_json(url=url, params=params, timeout=30.0).get("data", [])

        for paper in data:
            # Extract arXiv id from external IDs
            external_ids = paper.get("externalIds") or {}
            arxiv_id = _extract_arxiv_id(external_ids)
            if not arxiv_id:
                print(f"Skipping paper without arXiv ID: {paper.get('title')}")
                # Skip if no arXiv id (required for downstream processing)
                skipped_no_arxiv += 1
                continue

            # Compose unique entry_id and get Semantic Scholar paperId
            entry_id = f"https://arxiv.org/abs/{arxiv_id}"
            ss_paper_id = paper.get("paperId")

            # Check for duplicates by entry_id or ss_paper_id
            cur.execute(
                f"""
                SELECT 1
                FROM {bronze_table}
                     WHERE {payload_col}:entry_id::STRING = %s
                         OR {payload_col}:ss_paper_id::STRING = %s
                LIMIT 1
                """,
                (entry_id, str(ss_paper_id) if ss_paper_id else None),
            )
            if cur.fetchone():
                # Skip duplicate
                skipped_dupe += 1
                continue

            # Extract authors and PDF info
            open_pdf = paper.get("openAccessPdf") or {}
            authors = [a.get("name") for a in (paper.get("authors") or []) if a.get("name")]

            # Build the raw data dictionary for Bronze
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

            print(f"Ingesting paper: {raw_data['title']} ({raw_data['published']}), Authors: {raw_data['authors']}")

            # Insert the paper into the Bronze table
            cur.execute(
                f"INSERT INTO {bronze_table} ({payload_col}) SELECT PARSE_JSON(%s)",
                (json.dumps(raw_data),),
            )
            inserted += 1

        # Commit all inserts
        conn.commit()
        print(
            "Semantic Scholar ingest complete: "
            f"inserted={inserted}, skipped_duplicates={skipped_dupe}, skipped_no_arxiv={skipped_no_arxiv}"
        )
    finally:
        # Clean up DB connection
        cur.close()
        conn.close()

# testing function to see content of ingested bronze papers
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret])
def peek_bronze(limit: int = 3, database: str = DATABASE):
    """
    Inspects the raw JSON in Bronze, specifically focusing on the Abstract (summary).
    """
    import json
    import textwrap

    conn = connect_to_snowflake(database=database, schema="BRONZE")
    cur = conn.cursor()
    bronze_table = _bronze_papers_table(database=database)
    payload_col = _resolve_bronze_payload_column(cur, bronze_table)

    try:
        # Pull the raw_payload from Snowflake
        cur.execute(f'SELECT {payload_col} FROM {bronze_table} LIMIT %s', (limit,))
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


# ingest a single paper: for use case where user searches and selects a paper from the dropdown
# to add to their MindMap
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], timeout=60 * 5)
def ingest_single_paper(arxiv_id: str, database: str = DATABASE):
    """Ingest one specific paper by arXiv ID. Used when user clicks '+ Add' on a search result."""
    bronze_table = _bronze_papers_table(database=database)
    entry_id = f"https://arxiv.org/abs/{arxiv_id}"
    print(f"[ingest_single_paper] start arxiv_id={arxiv_id} database={database} table={bronze_table}", flush=True)

    print("[ingest_single_paper] connecting to Snowflake (BRONZE schema)", flush=True)
    conn = connect_to_snowflake(database=database, schema="BRONZE")
    cur = conn.cursor()
    try:
        print(f"[ingest_single_paper] duplicate check entry_id={entry_id}", flush=True)
        cur.execute(
            f'SELECT 1 FROM {bronze_table} WHERE "raw_payload":entry_id::STRING = %s LIMIT 1',
            (entry_id,)
        )
        if cur.fetchone():
            print(f"Paper {arxiv_id} already exists in Bronze, skipping.")
            return {"status": "skipped", "arxiv_id": arxiv_id}

        url = f"https://api.semanticscholar.org/graph/v1/paper/ArXiv:{arxiv_id}"
        params = {
            "fields": (
                "paperId,title,abstract,authors,externalIds,year,url,openAccessPdf,"
                "publicationDate,journal,citationCount,referenceCount"
            )
        }
        print(f"[ingest_single_paper] fetching Semantic Scholar url={url}", flush=True)
        paper = _ss_get_json(url=url, params=params, timeout=30.0)
        print("[ingest_single_paper] Semantic Scholar response received", flush=True)
        external_ids = paper.get("externalIds") or {}
        open_pdf = paper.get("openAccessPdf") or {}
        authors = [a.get("name") for a in (paper.get("authors") or []) if a.get("name")]

        raw_data = {
            "source": "semantic_scholar",
            "entry_id": entry_id,
            "ss_paper_id": paper.get("paperId"),
            "year": paper.get("year"),
            "citationCount": paper.get("citationCount"),
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

        print("[ingest_single_paper] inserting row into BRONZE_PAPERS", flush=True)
        cur.execute(
            f'INSERT INTO {bronze_table} ("raw_payload") SELECT PARSE_JSON(%s)',
            (json.dumps(raw_data),)
        )
        print("[ingest_single_paper] committing transaction", flush=True)
        conn.commit()
        print(f"Ingested single paper: {arxiv_id}")
        return {"status": "ok", "arxiv_id": arxiv_id}
    finally:
        print("[ingest_single_paper] closing Snowflake cursor/connection", flush=True)
        cur.close()
        conn.close()


# ADD THIS - Required for CLI to work!
@app.local_entrypoint()
def main(query: str, max_results: int = 5, source: str = "semantic_scholar"):
    """CLI entry point for running ingestion from terminal."""
    if source == "semantic_scholar":
        result = ingest_from_semantic_scholar.remote(query=query, max_results=max_results)
    elif source == "openalex":
        result = ingest_from_openalex(query=query, max_results=max_results)
    else:
        result = ingest_from_arxiv.remote(query=query, max_results=max_results)
    return result
