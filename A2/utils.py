import os
from pathlib import Path
import snowflake.connector

def connect_to_snowflake():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database='MINDMAP_DB', warehouse='MINDMAP_WH',
        schema='PUBLIC'
    )

def load_env_file() -> None:
    """Load key=value pairs from .env if present."""
    env_path = Path("/Users/huayinluo/Documents/code/MindMap/A2/.env")
    if not env_path.exists():
        print(".env file not found, relying on environment variables")
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())