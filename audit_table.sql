CREATE TABLE IF NOT EXISTS `{{project_id}}.{{audit_dataset}}.ingestion_audit_log` (
  dag_name STRING,
  table_name STRING,
  task_id STRING,
  task_name STRING,
  task_start_datetime TIMESTAMP,
  task_end_datetime TIMESTAMP,
  duration_seconds INT64,
  task_status STRING,
  audit_status STRING,
  source_count INT64,
  target_count INT64,
  error_message STRING
)
PARTITION BY DATE(task_start_datetime)
CLUSTER BY table_name, task_name;
