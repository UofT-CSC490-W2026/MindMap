import re
import threading
import time
import os
from typing import Any, Dict, List, Optional
from config import app, image, snowflake_secret, semantic_scholar_secret, DATABASE, qualify_table
from utils import connect_to_snowflake


_SS_MIN_INTERVAL_SECONDS = 1.05
_ss_lock = threading.Lock()
_ss_last_request_ts = 0.0


def _ss_get_json(url: str, params: dict | None = None, timeout: float = 20.0):
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
    if headers and response.status_code in (401, 403):
        print("Semantic Scholar key rejected; retrying request without API key.")
        headers = None
        response = _request(headers)

    if response.status_code == 429:
        # One retry with additional backoff to keep request pace compliant.
        time.sleep(2.0)
        with _ss_lock:
            now = time.time()
            wait = _SS_MIN_INTERVAL_SECONDS - (now - _ss_last_request_ts)
            if wait > 0:
                time.sleep(wait)
            _ss_last_request_ts = time.time()
        response = _request(headers)

        if headers and response.status_code in (401, 403):
            print("Semantic Scholar key rejected after retry; falling back to unauthenticated access.")
            response = _request(None)

    response.raise_for_status()
    return response.json()


def _ss_post_json(url: str, payload: dict, params: dict | None = None, timeout: float = 30.0):
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
        return httpx.post(url, json=payload, params=params, timeout=timeout, headers=req_headers)

    response = _request(headers)
    if headers and response.status_code in (401, 403):
        print("Semantic Scholar key rejected; retrying request without API key.")
        headers = None
        response = _request(headers)

    if response.status_code == 429:
        time.sleep(2.0)
        with _ss_lock:
            now = time.time()
            wait = _SS_MIN_INTERVAL_SECONDS - (now - _ss_last_request_ts)
            if wait > 0:
                time.sleep(wait)
            _ss_last_request_ts = time.time()
        response = _request(headers)

        if headers and response.status_code in (401, 403):
            print("Semantic Scholar key rejected after retry; falling back to unauthenticated access.")
            response = _request(None)

    response.raise_for_status()
    return response.json()


def _chunks(seq, n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _extract_arxiv_id_from_external_ids(external_ids: dict) -> Optional[str]:
    if not isinstance(external_ids, dict):
        return None
    raw = external_ids.get("ArXiv")
    if not raw:
        return None
    m = re.search(r"(\d{4}\.\d{4,5})(?:v\d+)?", str(raw))
    return m.group(1) if m else None


def _normalize_connection_entry(node: dict) -> Optional[dict]:
    if not isinstance(node, dict):
        return None
    external_ids = node.get("externalIds") or {}
    return {
        "title": node.get("title"),
        "year": node.get("year"),
        "arxiv_id": _extract_arxiv_id_from_external_ids(external_ids),
        "doi": external_ids.get("DOI"),
        "ss_paper_id": node.get("paperId"),
    }


def _extract_connections(items: list, relation_key: str, limit: int = 10) -> List[dict]:
    results: List[dict] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        # Batch payloads may be either direct paper nodes or wrapped nodes.
        node = item.get(relation_key) if relation_key in item else item
        normalized = _normalize_connection_entry(node)
        if normalized:
            results.append(normalized)
        if len(results) >= int(limit):
            break
    return results


def _fetch_ss_batch_metadata(arxiv_ids: List[str], batch_size: int = 100, relation_limit: int = 10) -> Dict[str, Dict[str, Any]]:
    """
    Fetch ss_id + references + citations using Semantic Scholar batch endpoint.
    Returns a mapping keyed by arxiv_id.
    """
    if not arxiv_ids:
        return {}

    batch_url = "https://api.semanticscholar.org/graph/v1/paper/batch"
    params = {
        "fields": (
            "paperId,externalIds,tldr,"
            "references.paperId,references.title,references.year,references.externalIds,"
            "citations.paperId,citations.title,citations.year,citations.externalIds"
        )
    }

    output: Dict[str, Dict[str, Any]] = {}
    for batch in _chunks(arxiv_ids, max(1, int(batch_size))):
        payload = {"ids": [f"ARXIV:{aid}" for aid in batch]}
        try:
            rows = _ss_post_json(url=batch_url, payload=payload, params=params, timeout=30.0)
        except Exception as e:
            print(f"Batch metadata fetch failed ({len(batch)} ids): {e}")
            rows = [None] * len(batch)

        for aid, row in zip(batch, rows):
            if not isinstance(row, dict):
                continue
            tldr = row.get("tldr") or {}
            output[aid] = {
                "ss_id": row.get("paperId"),
                "tldr": tldr.get("text") if isinstance(tldr, dict) else None,
                "references": _extract_connections(row.get("references", []), relation_key="citedPaper", limit=relation_limit),
                "citations": _extract_connections(row.get("citations", []), relation_key="citingPaper", limit=relation_limit),
            }

    return output


def _fetch_ss_batch_tldr(arxiv_ids: List[str], batch_size: int = 100) -> Dict[str, Dict[str, Any]]:
    """
    Fetch only TLDR + ids via Semantic Scholar batch endpoint.
    Returns mapping keyed by arxiv_id.
    """
    if not arxiv_ids:
        return {}

    batch_url = "https://api.semanticscholar.org/graph/v1/paper/batch"
    params = {"fields": "paperId,externalIds,tldr"}

    output: Dict[str, Dict[str, Any]] = {}
    for batch in _chunks(arxiv_ids, max(1, int(batch_size))):
        payload = {"ids": [f"ARXIV:{aid}" for aid in batch]}
        try:
            rows = _ss_post_json(url=batch_url, payload=payload, params=params, timeout=30.0)
        except Exception as e:
            print(f"Batch TLDR fetch failed ({len(batch)} ids): {e}")
            rows = [None] * len(batch)

        for aid, row in zip(batch, rows):
            if not isinstance(row, dict):
                continue
            tldr = row.get("tldr") or {}
            output[aid] = {
                "ss_id": row.get("paperId"),
                "tldr": tldr.get("text") if isinstance(tldr, dict) else None,
            }

    return output


def _bronze_papers_table(database: str = DATABASE) -> str:
    return qualify_table("BRONZE_PAPERS", database=database)


def _silver_papers_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)



# parse PDF to search for conclusion
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60*2)
def extract_conclusion(arxiv_id: str):
    import httpx
    import pymupdf # PyMuPDF

    try:
        # 1. Download
        pdf_url = f"https://export.arxiv.org/pdf/{arxiv_id}.pdf"
        print(f"Downloading: {pdf_url}")
        response = httpx.get(pdf_url, follow_redirects=True, timeout=30.0)
        
        # 2. Open and Extract Text
        try:
            doc = pymupdf.open(stream=response.content, filetype="pdf")
        except Exception as e:
            print(f"Warning: Could not parse PDF for {arxiv_id}: {e}")
            return ""
        
        full_text = ""
        for page in doc:
            full_text += page.get_text()

        # 3. Robust Section Detection
        # We look for Conclusion but also identify common "Stop" sections
        conclusion_patterns = [
            'Conclusion',
            r'\n(?:[0-9.]*\s*)?Conclusion',
            r'\n(?:[0-9.]*\s*)?Concluding Remarks',
            r'\n(?:[0-9.]*\s*)?Summary and Discussion'
        ]
        
        # Sections that typically follow a conclusion
        stop_patterns = [
            r'\n(?:[0-9.]*\s*)?References',
            r'\n(?:[0-9.]*\s*)?Bibliography',
            r'\n(?:[0-9.]*\s*)?Appendix',
            r'\n(?:[0-9.]*\s*)?Acknowledgments',
            r'\n(?:[0-9.]*\s*)?Supplementary Material'
        ]

        # Find the start of the conclusion
        start_idx = -1
        for pattern in conclusion_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                start_idx = match.start()
                break

        if start_idx == -1:
            return ""

        # Find the earliest occurrence of any 'stop' section AFTER the conclusion starts
        text_after_start = full_text[start_idx:]
        end_idx = len(text_after_start)

        for pattern in stop_patterns:
            match = re.search(pattern, text_after_start, re.IGNORECASE)
            if match and match.start() > 50: # Ensure it's not matching the header itself
                if match.start() < end_idx:
                    end_idx = match.start()

        conclusion_raw = text_after_start[:end_idx]

        # 4. Clean formatting
        # Removes LaTeX inline math, fix newlines, and strip double spaces
        clean_text = re.sub(r'\$.*?\$', '', conclusion_raw) 
        clean_text = clean_text.replace('\n', ' ')
        clean_text = " ".join(clean_text.split())

        return clean_text
    
    except Exception as e:
        print(f"Error extracting conclusion for {arxiv_id}: {e}")
        return ""

# Use the Semantic Scholar API to extract connections
# mode == 0 (default): use the API to fetch references (papers this arxiv_id cites)
# mode == 1: use the API to fetch citations (papers that cite this arxiv_id)
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60*2)
def fetch_connections_ss(arxiv_id: str, mode=0):

    if mode == 0: type = "references"
    else: type = "citations"
    # Semantic Scholar expects the prefix 'ARXIV:'
    ss_url = f"https://api.semanticscholar.org/graph/v1/paper/ARXIV:{arxiv_id}/{type}"
    
    # We ask for specific fields to keep the payload clean
    params = {
        "fields": "title,authors,year,externalIds",
        "limit": 10
    }

    print(f"Querying Semantic Scholar for: {arxiv_id}")
    
    try:
        data = _ss_get_json(url=ss_url, params=params, timeout=20.0)
        citations = data.get("data", [])

        print(f"\nFOUND {len(citations)} STRUCTURED CITATIONS\n")

        # Process and print for verification
        structured_list = []
        for item in citations:
            if item is None: continue
            if mode == 0: paper = item.get("citedPaper")
            else: paper = item.get("citingPaper")
            if paper is None:
                continue
    
            ids = paper.get("externalIds", {})
            if ids is None: continue

            ref_arxiv_id = ids.get("ArXiv") # Often None for new papers
            doi = ids.get("DOI")

            # If ArXiv ID is missing, we still want the metadata for the Silver Layer
            structured_list.append({
                "title": paper.get("title"),
                "year": paper.get("year"),
                "arxiv_id": ref_arxiv_id,
                "doi": doi,
                "ss_paper_id": paper.get("paperId") # Use this as a unique key in Snowflake
            })

        return structured_list

    except Exception as e:
        print(f"API Error: {e}")
        return None
    
# Use PDF parsing to extract references
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=4, timeout=60*2)
def extract_references_pdf(arxiv_id: str):
    import httpx
    import fitz 
    import re

    pdf_url = f"https://export.arxiv.org/pdf/{arxiv_id}.pdf"
    
    try:
        response = httpx.get(pdf_url, follow_redirects=True, timeout=30.0)
        response.raise_for_status()
        
        try:
            with fitz.open(stream=response.content, filetype="pdf") as doc:
                full_text = ""
                # Only look at the very end (last 5 pages)
                start_page = max(0, len(doc) - 5)
                for i in range(start_page, len(doc)):
                    full_text += doc[i].get_text()
        except Exception as pdf_err:
            print(f"Warning: Could not parse PDF for {arxiv_id}: {pdf_err}")
            return []

        # 1. Find the actual start of the list
        # We look for the first instance of "[1]" that appears after the word "References"
        ref_match = re.search(r'References', full_text, re.IGNORECASE)
        start_search = ref_match.start() if ref_match else 0
        
        # 2. Extract and clean the text from that point on
        content = full_text[start_search:]
        # Join lines to fix broken citations
        clean_content = " ".join(content.replace('\n', ' ').split())

        # 3. SPLIT INTO THE LIST
        # This regex looks for [1], [2], etc. and captures them
        parts = re.split(r'(\[\d+\])', clean_content)
        
        # 4. Reconstruct the list (combine the bracket with its following text)
        citations_list = []
        for i in range(1, len(parts), 2):
            if i + 1 < len(parts):
                citation_entry = f"{parts[i]} {parts[i+1].strip()}"
                # Filter out short fragments that are usually page numbers or footers
                if len(citation_entry) > 15:
                    citations_list.append(citation_entry)

        return citations_list

    except Exception as e:
        print(f"Error extracting references for {arxiv_id}: {e}")
        return []
    

# get citations for a given paper with arxiv_id
# first attempts via the Semantic Scholar, then tries parsing pdf if that fails
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=5)
def get_references(arxiv_id: str):

    # 1. Try Semantic Scholar (The "Clean" Way)
    api_results = fetch_connections_ss.remote(arxiv_id, mode=0)

    if api_results and len(api_results) > 0:
        return {"source": "api", "data": api_results}

    # 2. Fallback to PDF Parsing
    print(f"API returned 0 results. Falling back to PDF parsing for {arxiv_id}...")

    # This now returns a LIST of strings
    citations_list = extract_references_pdf.remote(arxiv_id)

    if citations_list and isinstance(citations_list, list):
        return {"source": "pdf_parsed_list", "data": citations_list}

    return {"source": "none", "data": []}
     
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60*5)
def transform_to_silver(
    arxiv_id: str,
    ss_prefetched: Optional[Dict[str, Any]] = None,
    database: str = DATABASE,
):
    import json
    
    try:
        # 1. Primary conclusion strategy: Semantic Scholar TLDR (more reliable than PDF parsing).
        conclusion = ""
        refs_data: List[dict] = []
        cites_data: List[dict] = []
        ss_id = None

        if ss_prefetched:
            conclusion = (ss_prefetched.get("tldr") or "").strip()
            refs_data = ss_prefetched.get("references", []) or []
            cites_data = ss_prefetched.get("citations", []) or []
            ss_id = ss_prefetched.get("ss_id")
            if not conclusion:
                # Fallback for older/partial SS records where TLDR is unavailable.
                conclusion = extract_conclusion.local(arxiv_id)
        else:
            conclusion = extract_conclusion.local(arxiv_id)
            refs_task = get_references.local(arxiv_id)
            cites_task = fetch_connections_ss.local(arxiv_id, mode=1)

            refs_data = refs_task.get("data", []) if refs_task else []
            cites_data = cites_task if cites_task else []

            # 2. Get the SS_ID for the primary paper
            # We query SS directly for the seed paper's ID to ensure we have it for the check
            try:
                ss_data = _ss_get_json(
                    url=f"https://api.semanticscholar.org/graph/v1/paper/ARXIV:{arxiv_id}",
                    params={"fields": "paperId"},
                    timeout=20.0,
                )
                ss_id = ss_data.get("paperId")
            except Exception:
                pass

        conn = connect_to_snowflake(database=database, schema="SILVER")
        cur = conn.cursor()

        try:
            # 3. Dual-Key MERGE Logic
            # This matches if EITHER the arxiv_id OR the ss_paper_id exists.
            cur.execute("""
                MERGE INTO {silver_papers} target
                USING (
                    SELECT 
                        %s as "arxiv_id",
                        %s as "ss_id",
                        "raw_payload":title::STRING as "title",
                        "raw_payload":summary::STRING as "abstract",
                        %s as "conclusion",
                        PARSE_JSON(%s) as "reference_list",
                        PARSE_JSON(%s) as "citation_list"
                    FROM {bronze_papers}
                    WHERE "raw_payload":entry_id::STRING LIKE %s
                    LIMIT 1
                ) source
                ON target."arxiv_id" = source."arxiv_id" OR (target."ss_id" = source."ss_id" AND source."ss_id" IS NOT NULL)
                WHEN MATCHED THEN
                    UPDATE SET 
                        target."arxiv_id" = COALESCE(target."arxiv_id", source."arxiv_id"),
                        target."ss_id" = COALESCE(target."ss_id", source."ss_id"),
                        target."conclusion" = source."conclusion",
                        target."reference_list" = source."reference_list",
                        target."citation_list" = source."citation_list"
                WHEN NOT MATCHED THEN
                    INSERT ("arxiv_id", "ss_id", "title", "abstract", "conclusion", "reference_list", "citation_list")
                    VALUES (source."arxiv_id", source."ss_id", source."title", source."abstract", source."conclusion", source."reference_list", source."citation_list");
            """.format(
                silver_papers=_silver_papers_table(database=database),
                bronze_papers=_bronze_papers_table(database=database),
            ), (
                arxiv_id,
                ss_id, 
                conclusion,
                json.dumps(refs_data), 
                json.dumps(cites_data), 
                f"%{arxiv_id}%",
            ))

            conn.commit()
            print(f"Processed {arxiv_id} (SS_ID: {ss_id}) into Silver.")

        except Exception as e:
            print(f"Database Error for {arxiv_id}: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    except Exception as e:
        print(f"Error processing {arxiv_id}: {e}")


# Provides a list of arxiv_ids in the bronze layer to be processed into silver
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=5)
def get_bronze_worklist(database: str = DATABASE):
    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    
    # Get all IDs in Bronze
    cur.execute(f'SELECT "raw_payload":entry_id::STRING FROM {_bronze_papers_table(database=database)}')
    rows = cur.fetchall()
    
    # Optional: Filter out papers already in Silver to avoid redundant work
    cur.execute(f'SELECT "arxiv_id" FROM {_silver_papers_table(database=database)}')
    existing_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    arxiv_ids = []
    for row in rows:
        match = re.search(r"(\d{4}\.\d{4,5})", row[0])
        if match:
            aid = match.group(1)
            if aid not in existing_ids: # Only process new papers
                arxiv_ids.append(aid)
    return arxiv_ids

@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60*30)
def main(parallel=1, database: str = DATABASE):
    ids_to_process = get_bronze_worklist.remote(database=database)
    print(f"DEBUG: Found {len(ids_to_process)} papers to process.")

    if not ids_to_process:
        print("No new papers to process.")
        return

    print("Prefetching Semantic Scholar metadata in batch...")
    ss_prefetch = _fetch_ss_batch_metadata(ids_to_process, batch_size=100, relation_limit=10)
    print(f"Prefetched SS metadata for {len(ss_prefetch)} papers.")

    if(parallel == 1):
        print(f"Parallel processing {len(ids_to_process)} papers...")
        for entry in ids_to_process:
            transform_to_silver.remote(
                entry,
                ss_prefetched=ss_prefetch.get(entry),
                database=database,
            )
        
        print("Done!")
    else:
        for i, entry in enumerate(ids_to_process):
            try:
                print(f"[{i+1}/{len(ids_to_process)}] Processing {entry}...")
                # Use .remote() so it runs in the cloud
                transform_to_silver.remote(
                    entry,
                    ss_prefetched=ss_prefetch.get(entry),
                    database=database,
                )
                
            except Exception as e:
                print(f"CRITICAL FAILURE on {entry}: {e}")
                # This 'continue' ensures we try the next paper instead of stopping
                continue


@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60 * 20)
def backfill_missing_ss_ids(
    limit: int = 1000,
    batch_size: int = 100,
    database: str = DATABASE,
):
    """
    Backfill missing ss_id values in SILVER_PAPERS using Semantic Scholar batch endpoint.

    This is idempotent and only updates rows where ss_id IS NULL and arxiv_id IS NOT NULL.
    """
    silver = _silver_papers_table(database=database)

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
                        SELECT "id", "arxiv_id"
                        FROM {silver}
                        WHERE "ss_id" IS NULL
                            AND "arxiv_id" IS NOT NULL
                        LIMIT {int(limit)}
            """
        )
        rows = [(int(r[0]), str(r[1])) for r in cur.fetchall() if r[1]]
        if not rows:
            return {"status": "ok", "updated": 0, "note": "No rows with missing ss_id."}

        by_arxiv_id = {arxiv_id: pid for pid, arxiv_id in rows}
        arxiv_ids = list(by_arxiv_id.keys())

        ss_map: dict[str, str] = {}
        batch_url = "https://api.semanticscholar.org/graph/v1/paper/batch"
        params = {"fields": "paperId,externalIds"}

        for batch in _chunks(arxiv_ids, max(1, int(batch_size))):
            batch_ids = [f"ARXIV:{aid}" for aid in batch]
            payload = {"ids": batch_ids}
            try:
                data = _ss_post_json(url=batch_url, payload=payload, params=params, timeout=30.0)
            except Exception as e:
                print(f"Batch lookup failed ({len(batch)} ids): {e}")
                data = [None] * len(batch_ids)

            for req_id, item in zip(batch_ids, data):
                if not item or not isinstance(item, dict):
                    continue
                ss_id = item.get("paperId")
                if not ss_id:
                    continue
                aid = req_id.split(":", 1)[1]
                ss_map[aid] = str(ss_id)

        updates = []
        for aid, ss_id in ss_map.items():
            pid = by_arxiv_id.get(aid)
            if pid is not None:
                updates.append((ss_id, int(pid)))

        if updates:
            cur.executemany(
                f"""
                UPDATE {silver}
                SET ss_id = %s
                WHERE id = %s
                  AND ss_id IS NULL
                """,
                updates,
            )
            conn.commit()

        return {
            "status": "ok",
            "candidates": len(rows),
            "resolved": len(ss_map),
            "updated": len(updates),
            "database": database,
        }
    finally:
        cur.close()
        conn.close()


@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60 * 20)
def backfill_conclusions_from_tldr(
    limit: int = 5000,
    batch_size: int = 100,
    overwrite_existing: bool = False,
    database: str = DATABASE,
):
    """
    Fill SILVER_PAPERS.conclusion from Semantic Scholar TLDR.

    Default behavior only fills missing/blank conclusions. If overwrite_existing=True,
    existing conclusions are replaced by TLDR when available.
    """
    silver = _silver_papers_table(database=database)

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        if overwrite_existing:
            cur.execute(
                f"""
                SELECT "id", "arxiv_id"
                FROM {silver}
                WHERE "arxiv_id" IS NOT NULL
                LIMIT {int(limit)}
                """
            )
        else:
            cur.execute(
                f"""
                SELECT "id", "arxiv_id"
                FROM {silver}
                WHERE "arxiv_id" IS NOT NULL
                  AND ("conclusion" IS NULL OR LENGTH(TRIM("conclusion")) = 0)
                LIMIT {int(limit)}
                """
            )

        rows = [(int(r[0]), str(r[1])) for r in cur.fetchall() if r[1]]
        if not rows:
            return {
                "status": "ok",
                "updated": 0,
                "note": "No eligible rows for conclusion backfill.",
            }

        by_arxiv_id = {arxiv_id: pid for pid, arxiv_id in rows}
        arxiv_ids = list(by_arxiv_id.keys())

        batch_data = _fetch_ss_batch_tldr(arxiv_ids=arxiv_ids, batch_size=batch_size)

        updates = []
        ss_id_updates = []
        for aid, pid in by_arxiv_id.items():
            item = batch_data.get(aid) or {}
            tldr_text = (item.get("tldr") or "").strip()
            if tldr_text:
                updates.append((tldr_text, int(pid)))
            ss_id = item.get("ss_id")
            if ss_id:
                ss_id_updates.append((str(ss_id), int(pid)))

        if updates:
            if overwrite_existing:
                cur.executemany(
                    f"""
                    UPDATE {silver}
                    SET conclusion = %s
                    WHERE id = %s
                    """,
                    updates,
                )
            else:
                cur.executemany(
                    f"""
                    UPDATE {silver}
                    SET conclusion = %s
                    WHERE id = %s
                      AND (conclusion IS NULL OR LENGTH(TRIM(conclusion)) = 0)
                    """,
                    updates,
                )

        if ss_id_updates:
            cur.executemany(
                f"""
                UPDATE {silver}
                SET ss_id = COALESCE(ss_id, %s)
                WHERE id = %s
                """,
                ss_id_updates,
            )

        conn.commit()
        return {
            "status": "ok",
            "candidates": len(rows),
            "tldr_found": len(updates),
            "updated": len(updates),
            "ss_id_updated": len(ss_id_updates),
            "database": database,
        }
    finally:
        cur.close()
        conn.close()
