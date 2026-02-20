locals {
  env = terraform.workspace
}

resource "snowflake_database" "mindmap" {
  name = "MINDMAP_${upper(local.env)}"
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.mindmap.name
  name     = "BRONZE"
}

resource "snowflake_table" "papers_raw" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.bronze.name
  name     = "PAPERS_RAW"

  column {
    name = "raw_json"
    type = "VARIANT"
  }

  column {
    name = "ingested_at"
    type = "TIMESTAMP_NTZ"
  }
}
