# how to set up

1) Make a Snowflake account
    - click '+' to create SQL, copy paste create.sql and run it w. cmd + shift + enter
    - check your snowflake account identifier in lower left corner by clicking profile -> account -> account details
2) Make a Modal account
    - follow their quickstart: `pip install modal`
    - add a Secret -> Custom:
        - name it 'snowflake-creds'
        - SNOWFLAKE_ACCOUNT: your account identifier for snowflake
        - SNOWFLAKE_USER: your chosen username to login to snowflake
        - SNOWFLAKE_PASSWORD: your chosen password to login to snowflake

run `modal run main.py::ingest_from_arxiv --query "Transformers"`