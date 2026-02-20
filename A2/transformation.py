
import modal
import snowflake.connector
import os
import re

# Infrastructure Definition (Part 4)
app = modal.App("mindmap-pipeline")
image = modal.Image.debian_slim().pip_install("arxiv", "snowflake-connector-python", "httpx", "pymupdf")


# some ideas:
# --- def get_conclusion: parse the PDF, read, get conclusion
# --- def get_embedding: how to use embedding model on Modal, generate embedding for the abstract, conclusion (so we can use semantic search later on)
# --- def get_citations: get the papers that were cited
# --- def get_related_papers: use the embedding to find related papers in the Silver layer, return top 5 related papers (title, abstract, link to PDF)
# --- def make_graph: fetch related / citations papers for query paper

def _connect_to_snowflake():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database='MINDMAP_DB', warehouse='MINDMAP_WH',
        schema='PUBLIC'
    )

# parse PDF to search for conclusion
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")], max_containers=1)
def extract_conclusion(arxiv_id: str):
    import httpx
    import pymupdf # PyMuPDF

    # 1. Download
    pdf_url = f"https://export.arxiv.org/pdf/{arxiv_id}.pdf"
    print(f"Downloading: {pdf_url}")
    response = httpx.get(pdf_url, follow_redirects=True, timeout=30.0)
    
    # 2. Open and Extract Text
    doc = pymupdf.open(stream=response.content, filetype="pdf")
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
        return f"Could not find a Conclusion header for {arxiv_id}"

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

# Use the Semantic Scholar API to extract connections
# mode == 0 (default): use the API to fetch references (papers this arxiv_id cites)
# mode == 1: use the API to fetch citations (papers that cite this arxiv_id)
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")], max_containers=5)
def fetch_connections_ss(arxiv_id: str, mode=0):
    import httpx
    import time
    
    time.sleep(1)
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
        response = httpx.get(ss_url, params=params, timeout=20.0)
        
        if response.status_code == 429:
            print("Rate limited! Semantic Scholar wants you to slow down.")
            return None
            
        response.raise_for_status()
        data = response.json()
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
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")], max_containers=4)
def extract_references_pdf(arxiv_id: str):
    import httpx
    import fitz 
    import re

    pdf_url = f"https://export.arxiv.org/pdf/{arxiv_id}.pdf"
    
    try:
        response = httpx.get(pdf_url, follow_redirects=True, timeout=30.0)
        response.raise_for_status()
        
        with fitz.open(stream=response.content, filetype="pdf") as doc:
            full_text = ""
            # Only look at the very end (last 5 pages)
            start_page = max(0, len(doc) - 5)
            for i in range(start_page, len(doc)):
                full_text += doc[i].get_text()

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
        return [f"Error: {str(e)}"]
    

# get citations for a given paper with arxiv_id
# first attempts via the Semantic Scholar, then tries parsing pdf if that fails
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")], max_containers=5)
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
     
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")], max_containers=5)
def transform_to_silver(arxiv_id: str):
    import json
    
    # 1. Parallel Extractions
    conclusion = extract_conclusion.local(arxiv_id)
    refs_task = get_references.local(arxiv_id)
    cites_task = fetch_connections_ss.local(arxiv_id, mode=1)

    refs_data = refs_task.get("data", []) if refs_task else []
    cites_data = cites_task if cites_task else []

    # 2. Get the SS_ID for the primary paper
    # We query SS directly for the seed paper's ID to ensure we have it for the check
    import httpx
    ss_id = None
    try:
        ss_res = httpx.get(f"https://api.semanticscholar.org/graph/v1/paper/ARXIV:{arxiv_id}?fields=paperId")
        if ss_res.status_code == 200:
            ss_id = ss_res.json().get("paperId")
    except:
        pass

    conn = _connect_to_snowflake()
    cur = conn.cursor()

    try:
        # 3. Dual-Key MERGE Logic
        # This matches if EITHER the arxiv_id OR the ss_paper_id exists.
        cur.execute("""
            MERGE INTO SILVER_PAPERS target
            USING (
                SELECT 
                    %s as arxiv_id,
                    %s as ss_id,
                    raw_payload:title::STRING as title,
                    raw_payload:summary::STRING as abstract,
                    %s as conclusion,
                    PARSE_JSON(%s) as reference_list,
                    PARSE_JSON(%s) as citation_list
                FROM BRONZE_PAPERS
                WHERE raw_payload:entry_id::STRING LIKE %s
                LIMIT 1
            ) source
            ON target.arxiv_id = source.arxiv_id OR (target.ss_id = source.ss_id AND source.ss_id IS NOT NULL)
            WHEN MATCHED THEN
                UPDATE SET 
                    target.arxiv_id = COALESCE(target.arxiv_id, source.arxiv_id),
                    target.ss_id = COALESCE(target.ss_id, source.ss_id),
                    target.conclusion = source.conclusion,
                    target.reference_list = source.reference_list,
                    target.citation_list = source.citation_list
            WHEN NOT MATCHED THEN
                INSERT (arxiv_id, ss_id, title, abstract, conclusion, reference_list, citation_list)
                VALUES (source.arxiv_id, source.ss_id, source.title, source.abstract, source.conclusion, source.reference_list, source.citation_list);
        """, (
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
        print(f"Database Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


# Provides a list of arxiv_ids in the bronze layer to be processed into silver
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")], max_containers=5)
def get_bronze_worklist():
    import re
    conn = _connect_to_snowflake()
    cur = conn.cursor()
    
    # Get all IDs in Bronze
    cur.execute("SELECT raw_payload:entry_id::STRING FROM BRONZE_PAPERS")
    rows = cur.fetchall()
    
    # Optional: Filter out papers already in Silver to avoid redundant work
    cur.execute("SELECT arxiv_id FROM SILVER_PAPERS")
    existing_ids = {row[0] for row in cur.fetchall()}
    conn.close()

    arxiv_ids = []
    for row in rows:
        match = re.search(r"(\d{4}\.\d{4,5})", row[0])
        if match:
            aid = match.group(1)
            if aid not in existing_ids: # Only process new papers
                arxiv_ids.append(aid)
    return arxiv_ids

@app.local_entrypoint()
def main(parallel=1):
    ids_to_process = get_bronze_worklist.remote()
    print(f"DEBUG: Found {len(ids_to_process)} papers to process.")

    if not ids_to_process:
        print("No new papers to process.")
        return

    if(parallel == 1):
        print(f"Parallel processing {len(ids_to_process)} papers...")
        
        # .map() returns a generator. wrapping in list() forces execution.
        # concurrency_limit=3 is the 'sweet spot' for free-tier APIs.
        list(transform_to_silver.map(ids_to_process))
        
        print("Done!")
    else:
        for i, entry in enumerate(ids_to_process):
            try:
                print(f"[{i+1}/{len(ids_to_process)}] Processing {entry}...")
                # Use .remote() so it runs in the cloud
                transform_to_silver.remote(entry)
                
            except Exception as e:
                print(f"CRITICAL FAILURE on {entry}: {e}")
                # This 'continue' ensures we try the next paper instead of stopping
                continue