-- DDL
CREATE TABLE IF NOT EXISTS `<Project_ID>.<DataSet>.ingestion_config` (
  table_name        STRING NOT NULL,     -- Logical table name (ODP_ACCOUNTS)
  category          STRING,              -- Optional grouping
  source_system     STRING NOT NULL,     -- Source identifier
  gcs_bucket        STRING NOT NULL,     -- Source bucket
  file_name         STRING NOT NULL,     -- Object path in bucket
  file_type         STRING NOT NULL,     -- CSV / TXT / JSON / AVRO
  load_type         STRING NOT NULL,     -- RELOAD / INCREMENTAL
  schema_json       JSON,                -- Optional schema override
  sql_query         STRING,              -- Optional custom SQL
  is_active         BOOL NOT NULL,       -- Multi-table execution flag
  created_by        STRING,
  created_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY table_name, load_type;

--- Basic INSERT (RELOAD example)

INSERT INTO `ap-bld-01.odp_dataset.ingestion_config`
(
  table_name,
  category,
  source_system,
  gcs_bucket,
  file_name,
  file_type,
  load_type,
  schema_json,
  sql_query,
  is_active,
  created_by
)
VALUES
(
  'ODP_ACCOUNTS',
  'FINANCE',
  'ODP',
  'ap-bld-01-source-bucket',
  'odp/accounts/accounts_2025_01_01.csv',
  'CSV',
  'RELOAD',
  NULL,
  NULL,
  TRUE,
  'airflow'
);

--- INSERT for INCREMENTAL (SCD Type-2)

INSERT INTO `ap-bld-01.odp_dataset.ingestion_config`
(
  table_name,
  category,
  source_system,
  gcs_bucket,
  file_name,
  file_type,
  load_type,
  schema_json,
  sql_query,
  is_active,
  created_by
)
VALUES
(
  'ODP_TRANSACTIONS',
  'FINANCE',
  'ODP',
  'ap-bld-01-source-bucket',
  'odp/transactions/transactions_delta_2025_01_01.csv',
  'CSV',
  'INCREMENTAL',
  NULL,
  NULL,
  TRUE,
  'airflow'
);

--- INSERT with custom SQL override (advanced)
--- Use this only if you want to override default load behavior.

INSERT INTO `ap-bld-01.odp_dataset.ingestion_config`
(
  table_name,
  category,
  source_system,
  gcs_bucket,
  file_name,
  file_type,
  load_type,
  schema_json,
  sql_query,
  is_active,
  created_by
)
VALUES
(
  'ODP_CUSTOM_TABLE',
  'CUSTOM',
  'ODP',
  'ap-bld-01-source-bucket',
  'odp/custom/custom_data.csv',
  'CSV',
  'RELOAD',
  NULL,
  '''
  SELECT
    account_id,
    balance,
    CURRENT_TIMESTAMP() AS rec_load_ts
  FROM `{project_id}.ODP_STAGING.ODP_CUSTOM_TABLE_STG`
  ''',
  TRUE,
  'airflow'
);

--- Insert multiple tables at once (recommended)

INSERT INTO `ap-bld-01.odp_dataset.ingestion_config`
(
  table_name,
  category,
  source_system,
  gcs_bucket,
  file_name,
  file_type,
  load_type,
  schema_json,
  sql_query,
  is_active,
  created_by
)
VALUES
('ODP_ACCOUNTS', 'FINANCE', 'ODP',
 'ap-bld-01-source-bucket',
 'odp/accounts/accounts.csv',
 'CSV', 'RELOAD',
 NULL, NULL, TRUE, 'airflow'),

('ODP_TRANSACTIONS', 'FINANCE', 'ODP',
 'ap-bld-01-source-bucket',
 'odp/transactions/transactions_delta.csv',
 'CSV', 'INCREMENTAL',
 NULL, NULL, TRUE, 'airflow');

--- Important validation checks (before running DAG)
--- Run these once after insert:

-- Check active tables
SELECT table_name, load_type
FROM `ap-bld-01.odp_dataset.ingestion_config`
WHERE is_active = TRUE;

-- Check file paths
SELECT table_name, gcs_bucket, file_name
FROM `ap-bld-01.odp_dataset.ingestion_config`
WHERE file_name IS NULL;
