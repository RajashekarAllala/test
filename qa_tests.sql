CREATE TABLE IF NOT EXISTS `<Project_ID>.<DataSet>.qa_test_log` (
  dag_name             STRING,
  dag_run_datetime     TIMESTAMP,
  table_name           STRING,
  load_type            STRING,
  task_name            STRING,
  task_status          STRING,
  qa_status            STRING,
  failed_task_name     STRING,
  failure_reason       STRING
)
PARTITION BY DATE(dag_run_datetime)
CLUSTER BY table_name, qa_status;
