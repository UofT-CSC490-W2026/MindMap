from modal import Image
import modal

import snowflake.connector
from app.utils.snowflake_utils import connect_snowflake

app = modal.App("transformation-worker")
image = Image.debian_slim().pip_install("snowflake-connector-python")


@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")])
def transform_to_silver():
    """
    Step 2: Cleaning and Transformation
    Extracts metadata from Bronze and populates the Silver table.
    """
    conn = connect_snowflake()
    cur = conn.cursor()

    # Logic to parse Bronze VARIANT data into Silver columns (Requirement 16)
    cur.execute("""
        INSERT INTO SILVER_PAPERS (arxiv_id, title, abstract)
        SELECT 
            raw_payload:id::STRING,
            raw_payload:title::STRING,
            raw_payload:summary::STRING
        FROM BRONZE_PAPERS
        WHERE NOT EXISTS (SELECT 1 FROM SILVER_PAPERS WHERE arxiv_id = raw_payload:id::STRING)
    """)
    conn.commit()
