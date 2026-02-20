# test_ingest.py
import modal

app = modal.App("mindmap-pipeline")
image = modal.Image.debian_slim().pip_install("snowflake-connector-python")

@app.function(image=image, secrets=[modal.Secret.from_name("snowflake-creds")])
def test_ingest():
    import os, snowflake.connector, json

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse='MINDMAP_WH'
    )
    cur = conn.cursor()
    cur.execute('USE DATABASE MINDMAP_DEV')
    cur.execute('USE SCHEMA BRONZE')

    dummy_data = {
        "entry_id": "dummy123",
        "title": "Dummy Paper",
        "summary": "This is a test abstract for the dummy paper.",
        "authors": ["Jane Doe", "John Doe"]
    }
    json_payload = json.dumps(dummy_data)

    cur.execute(
        'INSERT INTO "BRONZE_PAPERS" ("raw_payload") SELECT PARSE_JSON(%s)',
        (json_payload,)
    )

    conn.commit()

    cur.execute('SELECT "raw_payload" FROM "BRONZE_PAPERS" WHERE PARSE_JSON("raw_payload"):entry_id = %s', ('dummy123',))
    rows = cur.fetchall()
    for row in rows:
        print("Fetched row:", row[0])

    cur.close()
    conn.close()
