import os
import snowflake.connector

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MINDMAP_DB")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "MINDMAP_WH")

def connect_to_snowflake(schema: str, database: str = DATABASE, warehouse: str = WAREHOUSE):
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=database,
        warehouse=warehouse,
        schema=schema,
    )