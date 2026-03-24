import json

from app.utils.snowflake_utils import connect_snowflake
from app.utils.modal_config import app_ingestion, image_ingestion, secret_mindmap

@app_ingestion.function(image=image_ingestion, secrets=[secret_mindmap])
def ingest_from_arxiv(query: str, max_results: int = 5):
    """
    Step 1: Ingestion to Bronze Layer
    Fulfills Use Case 2: User types a general topic.
    """
    import arxiv
    search = arxiv.Search(query=query, max_results=max_results)
    
    conn = connect_snowflake()
    cur = conn.cursor()

    for result in search.results():
        # 1. Convert the ArXiv object to a serializable dictionary
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
        
        # 2. Convert dictionary to a valid JSON string
        json_payload = json.dumps(raw_data)
        
        # 3. Insert into the Bronze Table
        cur.execute(
            "INSERT INTO BRONZE_PAPERS (raw_payload) SELECT PARSE_JSON(%s)", 
            (json_payload,)
        )
    
    conn.commit()
    print(f"Ingested {max_results} papers into Bronze layer.")