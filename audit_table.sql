CREATE TABLE IF NOT EXISTS `{{project_id}}.{{audit_dataset}}.ingestion_audit_log` (
  dag_name               STRING,
  run_id                 STRING,
  table_name             STRING,
  load_type              STRING,

  job_start_time         TIMESTAMP,
  job_end_time           TIMESTAMP,

  job_status             STRING,    -- SUCCESS / FAILED
  audit_status           STRING,    -- PASS / FAIL

  source_count           INT64,
  target_count           INT64,
  mismatch               INT64,

  new_records            INT64,
  unchanged_records      INT64,
  changed_records        INT64,

  error_message          STRING,

  created_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_timestamp)
CLUSTER BY table_name, load_type;
