from utils import connect_to_snowflake, load_env_file

DDL = """
USE DATABASE MINDMAP_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE FUNCTION vector_cosine_similarity(a ARRAY, b ARRAY)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
WITH computations AS (
    SELECT
        SUM(A.VALUE::FLOAT * B.VALUE::FLOAT) AS dot_product,
        SQRT(SUM(A.VALUE::FLOAT * A.VALUE::FLOAT)) AS norm_a,
        SQRT(SUM(B.VALUE::FLOAT * B.VALUE::FLOAT)) AS norm_b
    FROM TABLE(FLATTEN(input => a)) A
    JOIN TABLE(FLATTEN(input => b)) B
      ON A.INDEX = B.INDEX
)
SELECT dot_product / NULLIF((norm_a * norm_b), 0) FROM computations
$$;
"""

def main():
    load_env_file()
    conn = connect_to_snowflake()
    cur = conn.cursor()
    try:
        for stmt in DDL.strip().split(";"):
            if stmt.strip():
                cur.execute(stmt)
        conn.commit()
        print("✓ UDF created")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()