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
    type = "NUMBER"
    identity {
      start_num = 1
      step_num  = 1
    }
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
    name = "source_paper_id"
    type = "NUMBER"
  }

  column {
    name = "target_paper_id"
    type = "NUMBER"
  }

  column {
    name = "relationship_type"
    type = "VARCHAR"
  }

  column {
    name = "strength"
    type = "FLOAT"
  }

  column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  primary_key {
    keys = ["source_paper_id", "target_paper_id", "relationship_type"]
  }

}
resource "snowflake_table_constraint" "fk_source_paper" {
  name     = "fk_gold_source_paper"
  type     = "FOREIGN KEY"
  # Use fully_qualified_name instead of .id
  table_id = snowflake_table.gold_connections.fully_qualified_name
  columns  = ["source_paper_id"]
  
  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table_constraint" "fk_target_paper" {
  name     = "fk_gold_target_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.gold_connections.fully_qualified_name
  columns  = ["target_paper_id"]
  
  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

# Compute Warehouse
resource "snowflake_warehouse" "mindmap_wh" {
  name               = "MINDMAP_${upper(local.env)}_WH"  
  warehouse_size     = "SMALL"
  auto_suspend       = 300
  auto_resume        = true
  initially_suspended = true
}
