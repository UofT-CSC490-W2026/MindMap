import re
import threading
import time
import os
import random
from typing import Any, Dict, List, Optional
from config import app, image, snowflake_secret, semantic_scholar_secret, DATABASE, qualify_table
from utils import connect_to_snowflake


_SS_MIN_INTERVAL_SECONDS = 1.05
_ss_lock = threading.Lock()
_ss_last_request_ts = 0.0
_ARXIV_MIN_INTERVAL_SECONDS = 1.25
_arxiv_lock = threading.Lock()
_arxiv_last_request_ts = 0.0
MAX_FULL_TEXT_CHARS = 350000
FULL_TEXT_PAGE_LIMIT = 80


def _retry_delay_from_response(response, attempt: int, base: float = 1.5, cap: float = 20.0) -> float:
    retry_after = None
    if response is not None:
        retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(cap, max(0.0, float(retry_after)))
        except (TypeError, ValueError):
            pass
    return min(cap, base * (2 ** attempt) + random.uniform(0.0, 0.5))


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

    max_attempts = 4
    response = None

    for attempt in range(max_attempts):
        response = _request(headers)

        if headers and response.status_code in (401, 403):
            print("Semantic Scholar key rejected; retrying request without API key.")
            headers = None
            continue

        if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
            delay = _retry_delay_from_response(response, attempt, base=2.0, cap=30.0)
            print(f"Semantic Scholar transient error {response.status_code}; retrying in {delay:.2f}s...")
            time.sleep(delay)
            continue

        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            body = ""
            if exc.response is not None:
                body = (exc.response.text or "")[:400]
            raise RuntimeError(
                f"Semantic Scholar GET failed (status={status}) for url={url} params={params}. "
                f"Response body: {body}"
            ) from None

    status = response.status_code if response is not None else "unknown"
    raise RuntimeError(f"Semantic Scholar GET failed after retries (status={status}) for url={url}")


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

    max_attempts = 4
    response = None

    for attempt in range(max_attempts):
        response = _request(headers)

        if headers and response.status_code in (401, 403):
            print("Semantic Scholar key rejected; retrying request without API key.")
            headers = None
            continue

        if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
            delay = _retry_delay_from_response(response, attempt, base=2.0, cap=30.0)
            print(f"Semantic Scholar transient error {response.status_code}; retrying in {delay:.2f}s...")
            time.sleep(delay)
            continue

        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            body = ""
            if exc.response is not None:
                body = (exc.response.text or "")[:400]
            raise RuntimeError(
                f"Semantic Scholar POST failed (status={status}) for url={url}. "
                f"Response body: {body}"
            ) from None

    status = response.status_code if response is not None else "unknown"
    raise RuntimeError(f"Semantic Scholar POST failed after retries (status={status}) for url={url}")


def _arxiv_get_pdf_bytes(arxiv_id: str, timeout: float = 45.0, max_attempts: int = 5) -> bytes:
    import httpx

    global _arxiv_last_request_ts
    pdf_url = f"https://export.arxiv.org/pdf/{arxiv_id}.pdf"
    response = None

    for attempt in range(max(1, int(max_attempts))):
        with _arxiv_lock:
            now = time.time()
            wait = _ARXIV_MIN_INTERVAL_SECONDS - (now - _arxiv_last_request_ts)
            if wait > 0:
                time.sleep(wait)
            _arxiv_last_request_ts = time.time()

        try:
            response = httpx.get(pdf_url, follow_redirects=True, timeout=timeout)
        except httpx.HTTPError as exc:
            if attempt < max_attempts - 1:
                delay = min(20.0, 1.5 * (2 ** attempt) + random.uniform(0.0, 0.5))
                print(f"arXiv PDF request error for {arxiv_id}: {exc}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
                continue
            raise RuntimeError(f"arXiv PDF request failed for {arxiv_id}: {exc}") from None

        if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
            delay = _retry_delay_from_response(response, attempt, base=2.0, cap=30.0)
            print(f"arXiv PDF transient error {response.status_code} for {arxiv_id}; retrying in {delay:.2f}s...")
            time.sleep(delay)
            continue

        try:
            response.raise_for_status()
            return response.content
        except httpx.HTTPStatusError:
            break

    status = response.status_code if response is not None else "unknown"
    body = (response.text[:300] if response is not None and response.text else "")
    raise RuntimeError(f"arXiv PDF download failed for {arxiv_id} (status={status}). Body: {body}")


def _chunks(seq, n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _fetch_ss_batch_rows_resilient(batch_url: str, ids: List[str], params: dict, timeout: float = 30.0):
    payload = {"ids": ids}
    try:
        rows = _ss_post_json(url=batch_url, payload=payload, params=params, timeout=timeout)
        if isinstance(rows, list):
            return rows
        return [None] * len(ids)
    except Exception as e:
        if len(ids) <= 1:
            print(f"Batch metadata fetch failed for single id {ids[0] if ids else '<none>'}: {e}")
            return [None] * len(ids)

        mid = len(ids) // 2
        left_ids = ids[:mid]
        right_ids = ids[mid:]
        print(
            f"Batch metadata fetch failed ({len(ids)} ids): {e}. "
            f"Retrying as smaller batches: {len(left_ids)} + {len(right_ids)}"
        )
        left_rows = _fetch_ss_batch_rows_resilient(batch_url=batch_url, ids=left_ids, params=params, timeout=timeout)
        right_rows = _fetch_ss_batch_rows_resilient(batch_url=batch_url, ids=right_ids, params=params, timeout=timeout)
        return left_rows + right_rows


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
        batch_ids = [f"ARXIV:{aid}" for aid in batch]
        rows = _fetch_ss_batch_rows_resilient(
            batch_url=batch_url,
            ids=batch_ids,
            params=params,
            timeout=30.0,
        )

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
        batch_ids = [f"ARXIV:{aid}" for aid in batch]
        rows = _fetch_ss_batch_rows_resilient(
            batch_url=batch_url,
            ids=batch_ids,
            params=params,
            timeout=30.0,
        )

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


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _resolve_bronze_payload_column(cur, database: str = DATABASE) -> str:
    bronze_table = _bronze_papers_table(database=database)
    cur.execute(f"DESC TABLE {bronze_table}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]

    for name in columns:
        if str(name).lower() == "raw_payload":
            return _quote_ident(str(name))

    raise RuntimeError(
        f"Could not find raw payload column in {bronze_table}. Columns found: {columns}"
    )


def _resolve_table_columns(cur, table_name: str) -> dict[str, str]:
    cur.execute(f"DESC TABLE {table_name}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]
    return {str(name).lower(): _quote_ident(str(name)) for name in columns}


def _require_columns(column_map: dict[str, str], required: list[str], table_name: str) -> dict[str, str]:
    missing = [name for name in required if name not in column_map]
    if missing:
        raise RuntimeError(f"Missing required columns in {table_name}: {missing}")
    return {name: column_map[name] for name in required}


def _clean_extracted_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _truncate_text(text: str, max_chars: int = MAX_FULL_TEXT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0].strip()


def _extract_conclusion_from_text(full_text: str) -> str:
    if not full_text:
        return ""

    conclusion_patterns = [
        r"\n(?:[0-9.]*\s*)?Conclusion\b",
        r"\n(?:[0-9.]*\s*)?Concluding Remarks\b",
        r"\n(?:[0-9.]*\s*)?Summary and Discussion\b",
    ]
    stop_patterns = [
        r"\n(?:[0-9.]*\s*)?References\b",
        r"\n(?:[0-9.]*\s*)?Bibliography\b",
        r"\n(?:[0-9.]*\s*)?Appendix\b",
        r"\n(?:[0-9.]*\s*)?Acknowledg?ments?\b",
        r"\n(?:[0-9.]*\s*)?Supplementary Material\b",
    ]

    start_idx = -1
    for pattern in conclusion_patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            start_idx = match.start()
            break

    if start_idx == -1:
        return ""

    text_after_start = full_text[start_idx:]
    end_idx = len(text_after_start)

    for pattern in stop_patterns:
        match = re.search(pattern, text_after_start, re.IGNORECASE)
        if match and match.start() > 50 and match.start() < end_idx:
            end_idx = match.start()

    conclusion_raw = text_after_start[:end_idx]
    clean_text = re.sub(r"\$.*?\$", "", conclusion_raw)
    clean_text = clean_text.replace("\n", " ")
    return " ".join(clean_text.split())


@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60 * 4)
def extract_full_text_pdf(arxiv_id: str) -> Dict[str, Any]:
    import pymupdf

    try:
        pdf_bytes = _arxiv_get_pdf_bytes(arxiv_id=arxiv_id, timeout=45.0, max_attempts=5)
    except Exception as e:
        print(f"Error downloading PDF for {arxiv_id}: {e}")
        return {"full_text": "", "source": "unavailable", "truncated": False, "pages_processed": 0}

    try:
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        print(f"Warning: Could not parse PDF for {arxiv_id}: {e}")
        return {"full_text": "", "source": "parse_failed", "truncated": False, "pages_processed": 0}

    page_texts: List[str] = []
    char_count = 0
    pages_processed = 0
    truncated = False

    for page_idx in range(min(len(doc), FULL_TEXT_PAGE_LIMIT)):
        page = doc[page_idx]
        page_text = _clean_extracted_text(page.get_text())
        if not page_text:
            pages_processed += 1
            continue

        if char_count + len(page_text) + 2 > MAX_FULL_TEXT_CHARS:
            remaining = max(0, MAX_FULL_TEXT_CHARS - char_count)
            if remaining > 200:
                page_texts.append(_truncate_text(page_text, max_chars=remaining))
            truncated = True
            break

        page_texts.append(page_text)
        char_count += len(page_text) + 2
        pages_processed += 1

    full_text = "\n\n".join(part for part in page_texts if part).strip()
    return {
        "full_text": full_text,
        "source": "pdf" if full_text else "empty_pdf",
        "truncated": truncated,
        "pages_processed": pages_processed,
    }



# parse PDF to search for conclusion
@app.function(image=image, secrets=[snowflake_secret, semantic_scholar_secret], max_containers=1, timeout=60*2)
def extract_conclusion(arxiv_id: str):
    try:
        full_text_result = extract_full_text_pdf.local(arxiv_id)
        return _extract_conclusion_from_text(full_text_result.get("full_text") or "")
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
    import fitz 
    import re
    
    try:
        pdf_bytes = _arxiv_get_pdf_bytes(arxiv_id=arxiv_id, timeout=30.0, max_attempts=5)
        
        try:
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
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
        # 1. Primary TLDR strategy: Semantic Scholar TLDR (more reliable than PDF parsing).
        tldr_text = ""
        conclusion_text = ""
        full_text = ""
        full_text_source = "unavailable"
        refs_data: List[dict] = []
        cites_data: List[dict] = []
        ss_id = None

        full_text_result = extract_full_text_pdf.local(arxiv_id)
        full_text = (full_text_result.get("full_text") or "").strip()
        full_text_source = str(full_text_result.get("source") or "unavailable")
        if full_text:
            conclusion_text = _extract_conclusion_from_text(full_text)

        if ss_prefetched:
            tldr_text = (ss_prefetched.get("tldr") or "").strip()
            refs_data = ss_prefetched.get("references", []) or []
            cites_data = ss_prefetched.get("citations", []) or []
            ss_id = ss_prefetched.get("ss_id")
        else:
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

        if not tldr_text:
            tldr_text = conclusion_text or extract_conclusion.local(arxiv_id)

        conn = connect_to_snowflake(database=database, schema="SILVER")
        cur = conn.cursor()
        payload_col = _resolve_bronze_payload_column(cur, database=database)
        silver_table = _silver_papers_table(database=database)
        silver_cols = _require_columns(
            _resolve_table_columns(cur, silver_table),
            [
                "arxiv_id",
                "ss_id",
                "conclusion",
                "full_text",
                "full_text_source",
                "full_text_extracted_at",
                "tldr",
                "reference_list",
                "citation_list",
                "title",
                "abstract",
            ],
            silver_table,
        )

        try:
            # 3. Dual-Key MERGE Logic
            # This matches if EITHER the arxiv_id OR the ss_paper_id exists.
            cur.execute("""
                MERGE INTO {silver_papers} target
                USING (
                    SELECT 
                        %s as arxiv_id,
                        %s as ss_id,
                        {payload_col}:title::STRING as title,
                        {payload_col}:summary::STRING as abstract,
                        %s as conclusion,
                        %s as full_text,
                        %s as full_text_source,
                        %s as tldr,
                        PARSE_JSON(%s) as reference_list,
                        PARSE_JSON(%s) as citation_list
                    FROM {bronze_papers}
                    WHERE {payload_col}:entry_id::STRING LIKE %s
                    LIMIT 1
                ) source
                ON target.{arxiv_id_col} = source.arxiv_id OR (target.{ss_id_col} = source.ss_id AND source.ss_id IS NOT NULL)
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.{arxiv_id_col} = COALESCE(target.{arxiv_id_col}, source.arxiv_id),
                        target.{ss_id_col} = COALESCE(target.{ss_id_col}, source.ss_id),
                        target.{conclusion_col} = COALESCE(source.conclusion, target.{conclusion_col}),
                        target.{full_text_col} = COALESCE(NULLIF(source.full_text, ''), target.{full_text_col}),
                        target.{full_text_source_col} = CASE
                            WHEN source.full_text IS NOT NULL AND LENGTH(TRIM(source.full_text)) > 0 THEN source.full_text_source
                            ELSE target.{full_text_source_col}
                        END,
                        target.{full_text_extracted_at_col} = CASE
                            WHEN source.full_text IS NOT NULL AND LENGTH(TRIM(source.full_text)) > 0 THEN CURRENT_TIMESTAMP()
                            ELSE target.{full_text_extracted_at_col}
                        END,
                        target.{tldr_col} = source.tldr,
                        target.{reference_list_col} = source.reference_list,
                        target.{citation_list_col} = source.citation_list
                WHEN NOT MATCHED THEN
                    INSERT ({arxiv_id_col}, {ss_id_col}, {title_col}, {abstract_col}, {conclusion_col}, {full_text_col}, {full_text_source_col}, {full_text_extracted_at_col}, {tldr_col}, {reference_list_col}, {citation_list_col})
                    VALUES (source.arxiv_id, source.ss_id, source.title, source.abstract, source.conclusion, source.full_text, source.full_text_source, CURRENT_TIMESTAMP(), source.tldr, source.reference_list, source.citation_list);
            """.format(
                silver_papers=silver_table,
                bronze_papers=_bronze_papers_table(database=database),
                payload_col=payload_col,
                arxiv_id_col=silver_cols["arxiv_id"],
                ss_id_col=silver_cols["ss_id"],
                title_col=silver_cols["title"],
                abstract_col=silver_cols["abstract"],
                conclusion_col=silver_cols["conclusion"],
                full_text_col=silver_cols["full_text"],
                full_text_source_col=silver_cols["full_text_source"],
                full_text_extracted_at_col=silver_cols["full_text_extracted_at"],
                tldr_col=silver_cols["tldr"],
                reference_list_col=silver_cols["reference_list"],
                citation_list_col=silver_cols["citation_list"],
            ), (
                arxiv_id,
                ss_id, 
                conclusion_text,
                full_text,
                full_text_source,
                tldr_text,
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
    payload_col = _resolve_bronze_payload_column(cur, database=database)
    silver_table = _silver_papers_table(database=database)
    silver_cols = _require_columns(
        _resolve_table_columns(cur, silver_table),
        ["arxiv_id"],
        silver_table,
    )
    
    # Get all IDs in Bronze
    cur.execute(f'SELECT {payload_col}:entry_id::STRING FROM {_bronze_papers_table(database=database)}')
    rows = cur.fetchall()
    
    # Optional: Filter out papers already in Silver to avoid redundant work
    cur.execute(f'SELECT {silver_cols["arxiv_id"]} FROM {silver_table}')
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
    silver_cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "arxiv_id", "ss_id"],
        silver,
    )
    try:
        cur.execute(
            f"""
                        SELECT {silver_cols["id"]}, {silver_cols["arxiv_id"]}
                        FROM {silver}
                        WHERE {silver_cols["ss_id"]} IS NULL
                            AND {silver_cols["arxiv_id"]} IS NOT NULL
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
                                SET {silver_cols["ss_id"]} = %s
                                WHERE {silver_cols["id"]} = %s
                                    AND {silver_cols["ss_id"]} IS NULL
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
    Fill SILVER_PAPERS.tldr from Semantic Scholar TLDR.

    Default behavior only fills missing/blank TLDR values. If overwrite_existing=True,
    existing TLDR values are replaced when available.
    """
    silver = _silver_papers_table(database=database)

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    silver_cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "arxiv_id", "tldr", "ss_id"],
        silver,
    )
    try:
        if overwrite_existing:
            cur.execute(
                f"""
                SELECT {silver_cols["id"]}, {silver_cols["arxiv_id"]}
                FROM {silver}
                WHERE {silver_cols["arxiv_id"]} IS NOT NULL
                LIMIT {int(limit)}
                """
            )
        else:
            cur.execute(
                f"""
                SELECT {silver_cols["id"]}, {silver_cols["arxiv_id"]}
                FROM {silver}
                WHERE {silver_cols["arxiv_id"]} IS NOT NULL
                                    AND ({silver_cols["tldr"]} IS NULL OR LENGTH(TRIM({silver_cols["tldr"]})) = 0)
                LIMIT {int(limit)}
                """
            )

        rows = [(int(r[0]), str(r[1])) for r in cur.fetchall() if r[1]]
        if not rows:
            return {
                "status": "ok",
                "updated": 0,
                "note": "No eligible rows for TLDR backfill.",
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
                                        SET {silver_cols["tldr"]} = %s
                    WHERE {silver_cols["id"]} = %s
                    """,
                    updates,
                )
            else:
                cur.executemany(
                    f"""
                    UPDATE {silver}
                                        SET {silver_cols["tldr"]} = %s
                    WHERE {silver_cols["id"]} = %s
                                            AND ({silver_cols["tldr"]} IS NULL OR LENGTH(TRIM({silver_cols["tldr"]})) = 0)
                    """,
                    updates,
                )

        if ss_id_updates:
            cur.executemany(
                f"""
                UPDATE {silver}
                SET {silver_cols["ss_id"]} = COALESCE({silver_cols["ss_id"]}, %s)
                WHERE {silver_cols["id"]} = %s
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
