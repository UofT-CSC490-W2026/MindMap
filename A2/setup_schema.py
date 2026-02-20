"""
One-time setup script to initialize Snowflake schema.
Run this once before starting the pipeline.
"""
import os
from utils import connect_to_snowflake, load_env_file

def setup_schema():
    """Read and execute create_schemas.sql"""
    load_env_file()
    conn = connect_to_snowflake()
    cur = conn.cursor()
    
    try:
        # Read the SQL file
        with open("A2/create_schemas.sql", "r") as f:
            sql_script = f.read()
        
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