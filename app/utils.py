import os

import snowflake.connector
from config import DATABASE, WAREHOUSE


def connect_to_snowflake(schema: str, database: str = DATABASE, warehouse: str = WAREHOUSE):
    connection_args = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": database,
        "warehouse": warehouse,
        "schema": schema,
    }
    return snowflake.connector.connect(**connection_args)
