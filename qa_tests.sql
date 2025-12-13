CREATE TABLE IF NOT EXISTS `{{qa_project}}.{{qa_dataset}}.qa_tests` (
  dag_name            STRING,
  run_id              STRING,
  overall_status      STRING,
  total_tables        INT64,
  passed_tables       INT64,
  failed_tables       INT64,
  created_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_timestamp);
