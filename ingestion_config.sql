CREATE TABLE IF NOT EXISTS `{{project_id}}.{{config_dataset}}.ingestion_config` (
  table_name              STRING NOT NULL,      -- Logical table name (ODP_ACCOUNTS)
  source_system           STRING NOT NULL,      -- Source identifier
  gcs_bucket              STRING NOT NULL,      -- Source bucket
  file_name               STRING NOT NULL,      -- Object path in bucket
  file_type               STRING NOT NULL,      -- CSV / TXT / JSON / AVRO
  load_type               STRING NOT NULL,      -- RELOAD / INCREMENTAL
  sql_query               STRING,               -- Optional custom SQL
  is_active               BOOL NOT NULL,        -- Controls multi-table execution
  created_by              STRING,
  created_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_ts              TIMESTAMP
)
PARTITION BY DATE(created_ts);
