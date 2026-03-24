import os

import snowflake.connector
from config import DATABASE, SCHEMA


def connect_to_snowflake(database: str = DATABASE, schema: str = SCHEMA):
    connection_args = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": database,
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "MINDMAP_WH"),
        "schema": schema,
    }
    return snowflake.connector.connect(**connection_args)
