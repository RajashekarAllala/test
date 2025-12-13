CREATE TABLE IF NOT EXISTS `{{project_id}}.{{audit_dataset}}.ingestion_audit_log` (
  dag_name               STRING NOT NULL,
  run_id                 STRING NOT NULL,
  table_name             STRING NOT NULL,
  load_type              STRING NOT NULL,

  task_name              STRING NOT NULL,      -- which task
  task_status            STRING NOT NULL,      -- SUCCESS / FAILED

  job_start_time         TIMESTAMP,
  job_end_time           TIMESTAMP,

  job_status             STRING,               -- SUCCESS / FAILED
  audit_status           STRING,               -- PASS / FAIL

  source_count           INT64,
  target_count           INT64,
  mismatch               INT64,

  new_records            INT64,
  unchanged_records      INT64,
  changed_records        INT64,

  error_message          STRING,               -- exception text
  error_type             STRING,               -- FOOTER / DQ / LOAD / MERGE

  created_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_timestamp)
CLUSTER BY table_name, load_type;
