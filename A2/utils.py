import snowflake.connector
import os

def _connect_to_snowflake():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database='MINDMAP_DB', warehouse='MINDMAP_WH',
        schema='PUBLIC'
    )