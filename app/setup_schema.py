"""
One-time setup script to initialize Snowflake schema.
Run this once before starting the pipeline.
"""
from config import DATABASE, SCHEMA
from utils import connect_to_snowflake
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
SCHEMA_SQL = APP_DIR / "create_schemas.sql"

def setup_schema():
    """Read and execute create_schemas.sql"""
    conn = connect_to_snowflake(database=DATABASE, schema=SCHEMA)
    cur = conn.cursor()
    
    try:
        sql_script = SCHEMA_SQL.read_text()
        
        # Execute each statement
        for statement in sql_script.split(";"):
            statement = statement.strip()
            if statement:
                print(f"Executing: {statement[:60]}...")
                cur.execute(statement)
        
        conn.commit()
        print("✓ Schema created successfully!")
        
    except Exception as e:
        print(f"✗ Schema creation failed: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    setup_schema()
