import modal

app = modal.App("arxiv-ingestion")

image = modal.Image.debian_slim().pip_install(
    "requests",
    "feedparser",
    "snowflake-connector-python"
)

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("snowflake-creds")]
)
def ingest_arxiv(query: str, max_results: int = 5):
    import requests
    import feedparser
    import json
    import datetime
    import snowflake.connector
    import os

    url = "http://export.arxiv.org/api/query"
    params = {
        "search_query": f"all:{query}",
        "start": 0,
        "max_results": max_results
    }

    response = requests.get(url, params=params, timeout=20)
    feed = feedparser.parse(response.text)

    ctx = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="COMPUTE_WH",
        database="MINDMAP_PROD",  ## might chnage thsi from PROd TO DEV
        schema="BRONZE"
    )

    cs = ctx.cursor()

    for entry in feed.entries:
        record = {
            "arxiv_id": entry.id.split("/")[-1],
            "title": entry.title.replace("\n", " ").strip(),
            "abstract": entry.summary.replace("\n", " ").strip(),
            "published": entry.published,
            "raw": dict(entry)
        }

        cs.execute(
            """
            INSERT INTO PAPERS_RAW ("raw_json", "ingested_at")
            SELECT PARSE_JSON(%s), %s
            """,
            (json.dumps(record), datetime.datetime.utcnow())
        )


    cs.close()
    ctx.close()


@app.local_entrypoint()
def main():
    ingest_arxiv.remote("transformer", max_results=3)
