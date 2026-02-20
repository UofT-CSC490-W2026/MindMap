locals {
  env = terraform.workspace
}

resource "snowflake_database" "mindmap" {
  name = "MINDMAP_${upper(local.env)}"
}

# BRONZE LAYER: Raw Data Lake
resource "snowflake_schema" "bronze" {
  database = snowflake_database.mindmap.name
  name     = "BRONZE"
}

resource "snowflake_table" "bronze_papers" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.bronze.name
  name     = "BRONZE_PAPERS"

  column {
    name = "ingestion_id"
    type = "VARCHAR"
    default {
      expression = "UUID_STRING()"
    }
  }

  column {
    name = "raw_payload"
    type = "VARIANT"
  }

  column {
    name = "ingested_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

# SILVER LAYER: Cleaned & Enriched Data
resource "snowflake_schema" "silver" {
  database = snowflake_database.mindmap.name
  name     = "SILVER"
}

resource "snowflake_table" "silver_papers" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.silver.name
  name     = "SILVER_PAPERS"

  column {
    name = "id"
    type = "INTEGER"
  }

  column {
    name = "arxiv_id"
    type = "VARCHAR"
  }

  column {
    name = "ss_id"
    type = "VARCHAR"
  }

  column {
    name = "title"
    type = "VARCHAR"
  }

  column {
    name = "abstract"
    type = "VARCHAR"
  }

  column {
    name = "conclusion"
    type = "VARCHAR"
  }

  column {
    name = "reference_list"
    type = "VARIANT"
  }

  column {
    name = "citation_list"
    type = "VARIANT"
  }

  column {
    name = "embedding"
    type = "VECTOR(FLOAT, 384)"
  }

  column {
    name = "similar_embeddings_ids"
    type = "VARIANT"
  }
}

# GOLD LAYER: Insights & Knowledge Graph
resource "snowflake_schema" "gold" {
  database = snowflake_database.mindmap.name
  name     = "GOLD"
}

resource "snowflake_table" "gold_connections" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.gold.name
  name     = "GOLD_CONNECTIONS"

  column {
    name = "source_id"
    type = "VARCHAR"
  }

  column {
    name = "target_id"
    type = "VARCHAR"
  }

  column {
    name = "relationship"
    type = "VARCHAR"
  }

  column {
    name = "confidence_score"
    type = "FLOAT"
  }
}


# Compute Warehouse
resource "snowflake_warehouse" "mindmap_wh" {
  name               = "MINDMAP_WH"
  warehouse_size     = "SMALL"       # can be XSMALL, SMALL, MEDIUM, etc.
  auto_suspend       = 300           # seconds to suspend after inactivity
  auto_resume        = true
  initially_suspended = true         # start suspended
}
