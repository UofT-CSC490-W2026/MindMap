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
    name = "full_text"
    type = "VARCHAR"
  }

  column {
    name = "full_text_source"
    type = "VARCHAR"
  }

  column {
    name = "full_text_extracted_at"
    type = "TIMESTAMP_NTZ"
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

  column {
    name = "tldr"
    type = "VARCHAR"
  }
}

# GOLD LAYER: Insights & Knowledge Graph
resource "snowflake_schema" "gold" {
  database = snowflake_database.mindmap.name
  name     = "GOLD"
}

resource "snowflake_table" "gold_paper_relationships" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.gold.name
  name     = "GOLD_PAPER_RELATIONSHIPS"

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
    name = "reason"
    type = "VARCHAR"
  }

  column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

}

resource "snowflake_table" "gold_paper_clusters" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.gold.name
  name     = "GOLD_PAPER_CLUSTERS"

  column {
    name = "paper_id"
    type = "NUMBER"
  }

  column {
    name = "cluster_id"
    type = "NUMBER"
  }

  column {
    name = "cluster_label"
    type = "VARCHAR"
  }

  column {
    name = "cluster_name"
    type = "VARCHAR"
  }

  column {
    name = "cluster_description"
    type = "VARCHAR"
  }

  column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table_constraint" "pk_gold_paper_clusters" {
  name     = "pk_gold_paper_clusters"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.gold_paper_clusters.fully_qualified_name
  columns  = ["paper_id"]
}

resource "snowflake_table_constraint" "fk_gold_clusters_paper" {
  name     = "fk_gold_clusters_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.gold_paper_clusters.fully_qualified_name
  columns  = ["paper_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table" "gold_paper_summaries" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.gold.name
  name     = "GOLD_PAPER_SUMMARIES"

  column {
    name = "paper_id"
    type = "NUMBER"
  }

  column {
    name = "summary_json"
    type = "VARIANT"
  }

  column {
    name = "model_name"
    type = "VARCHAR"
  }

  column {
    name = "prompt_version"
    type = "VARCHAR"
  }

  column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  column {
    name = "updated_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

}

resource "snowflake_table" "gold_summary_evidence" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.gold.name
  name     = "GOLD_SUMMARY_EVIDENCE"

  column {
    name = "paper_id"
    type = "NUMBER"
  }

  column {
    name = "summary_field"
    type = "VARCHAR"
  }

  column {
    name = "chunk_id"
    type = "NUMBER"
  }

  column {
    name = "evidence_rank"
    type = "NUMBER"
  }

  column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

}

resource "snowflake_table_constraint" "pk_gold_paper_relationships" {
  name     = "pk_gold_paper_relationships"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.gold_paper_relationships.fully_qualified_name
  columns  = ["source_paper_id", "target_paper_id", "relationship_type"]
}

resource "snowflake_table_constraint" "pk_gold_paper_summaries" {
  name     = "pk_gold_paper_summaries"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.gold_paper_summaries.fully_qualified_name
  columns  = ["paper_id"]
}

resource "snowflake_table_constraint" "pk_gold_summary_evidence" {
  name     = "pk_gold_summary_evidence"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.gold_summary_evidence.fully_qualified_name
  columns  = ["paper_id", "summary_field", "chunk_id"]
}
resource "snowflake_table_constraint" "fk_source_paper" {
  name     = "fk_gold_source_paper"
  type     = "FOREIGN KEY"
  # Use fully_qualified_name instead of .id
  table_id = snowflake_table.gold_paper_relationships.fully_qualified_name
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
  table_id = snowflake_table.gold_paper_relationships.fully_qualified_name
  columns  = ["target_paper_id"]
  
  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table_constraint" "fk_gold_summary_paper" {
  name     = "fk_gold_summary_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.gold_paper_summaries.fully_qualified_name
  columns  = ["paper_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table_constraint" "fk_gold_evidence_paper" {
  name     = "fk_gold_evidence_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.gold_summary_evidence.fully_qualified_name
  columns  = ["paper_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table_constraint" "fk_gold_evidence_chunk" {
  name     = "fk_gold_evidence_chunk"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.gold_summary_evidence.fully_qualified_name
  columns  = ["chunk_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_paper_chunks.fully_qualified_name
      columns  = ["chunk_id"]
    }
  }
}

  # SILVER_PAPER_SECTIONS table
  resource "snowflake_table" "silver_paper_sections" {
    database = snowflake_database.mindmap.name
    schema   = snowflake_schema.silver.name
    name     = "SILVER_PAPER_SECTIONS"

    column {
      name = "section_id"
      type = "NUMBER"
      identity {
        start_num = 1
        step_num  = 1
      }
    }

    column {
      name = "paper_id"
      type = "NUMBER"
    }

    column {
      name = "section_name"
      type = "VARCHAR"
    }

    column {
      name = "section_order"
      type = "NUMBER"
    }

    column {
      name = "content"
      type = "VARCHAR"
    }

    column {
      name = "token_estimate"
      type = "NUMBER"
    }

    column {
      name = "created_at"
      type = "TIMESTAMP_NTZ"
      default {
        expression = "CURRENT_TIMESTAMP()"
      }
    }

  }

  # SILVER_PAPER_CHUNKS table
  resource "snowflake_table" "silver_paper_chunks" {
    database = snowflake_database.mindmap.name
    schema   = snowflake_schema.silver.name
    name     = "SILVER_PAPER_CHUNKS"

    column {
      name = "chunk_id"
      type = "NUMBER"
      identity {
        start_num = 1
        step_num  = 1
      }
    }

    column {
      name = "paper_id"
      type = "NUMBER"
    }

    column {
      name = "section_id"
      type = "NUMBER"
    }

    column {
      name = "chunk_index"
      type = "NUMBER"
    }

    column {
      name = "chunk_text"
      type = "VARCHAR"
    }

    column {
      name = "token_estimate"
      type = "NUMBER"
    }

    column {
      name = "chunk_type"
      type = "VARCHAR"
    }

    column {
      name = "embedding"
      type = "VECTOR(FLOAT, 384)"
    }

    column {
      name = "metadata"
      type = "VARIANT"
    }

    column {
      name = "created_at"
      type = "TIMESTAMP_NTZ"
      default {
        expression = "CURRENT_TIMESTAMP()"
      }
    }

  }

resource "snowflake_table_constraint" "pk_silver_paper_sections" {
  name     = "pk_silver_paper_sections"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.silver_paper_sections.fully_qualified_name
  columns  = ["section_id"]
}

resource "snowflake_table_constraint" "fk_silver_sections_paper" {
  name     = "fk_silver_sections_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.silver_paper_sections.fully_qualified_name
  columns  = ["paper_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table_constraint" "pk_silver_paper_chunks" {
  name     = "pk_silver_paper_chunks"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.silver_paper_chunks.fully_qualified_name
  columns  = ["chunk_id"]
}

resource "snowflake_table_constraint" "fk_silver_chunks_paper" {
  name     = "fk_silver_chunks_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.silver_paper_chunks.fully_qualified_name
  columns  = ["paper_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_papers.fully_qualified_name
      columns  = ["id"]
    }
  }
}

resource "snowflake_table_constraint" "fk_silver_chunks_section" {
  name     = "fk_silver_chunks_section"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.silver_paper_chunks.fully_qualified_name
  columns  = ["section_id"]

  foreign_key_properties {
    references {
      table_id = snowflake_table.silver_paper_sections.fully_qualified_name
      columns  = ["section_id"]
    }
  }
}

# APP schema for conversational QA logs
resource "snowflake_schema" "app" {
  database = snowflake_database.mindmap.name
  name     = "APP"
}

resource "snowflake_table" "app_qa_logs" {
  database = snowflake_database.mindmap.name
  schema   = snowflake_schema.app.name
  name     = "APP_QA_LOGS"

  column {
    name = "log_id"
    type = "NUMBER"
    identity {
      start_num = 1
      step_num  = 1
    }
  }

  column {
    name = "session_id"
    type = "VARCHAR"
  }

  column {
    name = "paper_id"
    type = "NUMBER"
  }

  column {
    name = "role"
    type = "VARCHAR"
  }

  column {
    name = "message"
    type = "VARCHAR"
  }

  column {
    name = "rewritten_query"
    type = "VARCHAR"
  }

  column {
    name = "cited_chunk_ids"
    type = "VARIANT"
  }

  column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

}

resource "snowflake_table_constraint" "pk_app_qa_logs" {
  name     = "pk_app_qa_logs"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.app_qa_logs.fully_qualified_name
  columns  = ["log_id"]
}

resource "snowflake_table_constraint" "fk_app_qa_logs_paper" {
  name     = "fk_app_qa_logs_paper"
  type     = "FOREIGN KEY"
  table_id = snowflake_table.app_qa_logs.fully_qualified_name
  columns  = ["paper_id"]

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
