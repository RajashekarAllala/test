CREATE TABLE IF NOT EXISTS `{{qa_project}}.{{qa_dataset}}.qa_execution_log` (
  dag_name              STRING NOT NULL,     -- Ingestion DAG name
  run_id                STRING NOT NULL,     -- DAG run id

  table_name            STRING NOT NULL,     -- Logical table name
  load_type             STRING NOT NULL,     -- RELOAD / INCREMENTAL

  qa_status             STRING NOT NULL,     -- PASS / FAIL

  failed_task_name      STRING,              -- Task that caused QA failure
  failure_reason        STRING,              -- Human-readable reason

  job_start_time        TIMESTAMP,           -- From ingestion DAG
  job_end_time          TIMESTAMP,

  source_count          INT64,
  target_count          INT64,

  new_records           INT64,
  unchanged_records     INT64,
  changed_records       INT64,

  created_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_timestamp)
CLUSTER BY table_name, qa_status;
