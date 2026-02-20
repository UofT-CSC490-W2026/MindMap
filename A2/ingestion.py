import modal
import arxiv
import snowflake.connector
import os
import json
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
        database='MINDMAP_DEV', warehouse='MINDMAP_WH',
        schema='BRONZE'
    )


# Credentials should be stored in a Modal Secret
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")])
def ingest_from_arxiv(query: str, max_results: int = 5):
    """
    Step 1: Ingestion to Bronze Layer
    Fulfills Use Case 2: User types a general topic.
    """
    search = arxiv.Search(query=query, max_results=max_results)
    
    conn = _connect_to_snowflake()
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
            'INSERT INTO "MINDMAP_DEV"."BRONZE"."BRONZE_PAPERS" ("raw_payload") SELECT PARSE_JSON(%s)',
            (json_payload,)
        )

    
    conn.commit()
    print(f"Ingested {max_results} papers into Bronze layer.")

# testing function to see content of ingested bronze papers
@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")])
def peek_bronze(limit: int = 3):
    """
    Inspects the raw JSON in Bronze, specifically focusing on the Abstract (summary).
    """
    import json
    import textwrap

    conn = _connect_to_snowflake()
    cur = conn.cursor()

    try:
        # Pull the raw_payload from Snowflake
        cur.execute('SELECT "raw_payload" FROM "MINDMAP_DEV"."BRONZE"."BRONZE_PAPERS" LIMIT %s', (limit,))
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
