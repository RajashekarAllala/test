{
  /* ================================
     GCP / Composer
     ================================ */
  "gcp_conn_id": "google_cloud_default",
  "impersonation_chain": "data-access-sa@data-project.iam.gserviceaccount.com",
  "gcp_project_id": "ap-bld-01",

  /* ================================
     Config Table (Control Plane)
     ================================ */
  "config_dataset_name": "odp_dataset",
  "config_table_name": "odp_ingestion_config",

  /* ================================
     Staging & Target Datasets
     ================================ */
  "staging_dataset_name": "ODP_STAGING",
  "target_reload_dataset": "ODP_TARGET_RELOAD",
  "target_cdc_dataset": "ODP_TARGET_CDC",

  /* ================================
     DQ Configuration
     ================================ */
  "dq_config_bucket": "ap-bld-01-dq-config",
  "dq_config_object": "dq_rules/odp_dq_rules.json",

  /* ================================
     Archive Configuration
     ================================ */
  "archival_bucket": "ap-bld-01-archive",
  "archival_path": "odp/archive",

  /* ================================
     Audit Configuration
     ================================ */
  "audit_dataset": "qa_audit",
  "audit_table": "ingestion_audit_log",

  /* ================================
     QA Configuration
     ================================ */
  "qa_dag_id": "config_qa_tests_with_exceptions",
  "qa_bq_project": "ap-bld-01",
  "qa_bq_dataset": "qa_audit",
  "qa_bq_table": "qa_execution_log",

  /* ================================
     QA Report Output (GCS)
     ================================ */
  "test_report_bucket": "ap-bld-01-stb-euwe2-loans",
  "test_report_folder": "test_report_folder"
}

1️⃣ Ingestion DAG (odp_multi_table_ingestion.py)

------------------------------------------------------------------
| Variable                | Used for                             |
| ----------------------- | ------------------------------------ |
| `gcp_conn_id`           | Airflow → GCP connection             |
| `impersonation_chain`   | Cross-project service account access |
| `gcp_project_id`        | BigQuery project ID                  |
| `config_dataset_name`   | Config table dataset                 |
| `config_table_name`     | Config table name                    |
| `staging_dataset_name`  | Staging tables dataset               |
| `target_reload_dataset` | RELOAD target dataset                |
| `target_cdc_dataset`    | SCD-Type-2 target dataset            |
| `dq_config_bucket`      | DQ rules JSON GCS bucket             |
| `dq_config_object`      | DQ rules JSON object path            |
| `archival_bucket`       | Archive GCS bucket                   |
| `archival_path`         | Archive folder path                  |
| `qa_dag_id`             | QA DAG trigger ID                    |
------------------------------------------------------------------

2️⃣ QA DAG (config_qa_tests_with_exceptions.py)

----------------------------------------------------------------
| Variable              | Used for                             |
| --------------------- | ------------------------------------ |
| `gcp_conn_id`         | BigQuery and GCS access              |
| `impersonation_chain` | Cross-project service account access |
| `gcp_project_id`      | Audit and config BigQuery project    |
| `audit_dataset`       | Audit dataset                        |
| `audit_table`         | Audit table                          |
| `config_dataset_name` | Config dataset                       |
| `config_table_name`   | Config table                         |
| `qa_bq_project`       | QA BigQuery project                  |
| `qa_bq_dataset`       | QA dataset                           |
| `qa_bq_table`         | QA results table                     |
| `test_report_bucket`  | QA report GCS bucket                 |
| `test_report_folder`  | QA report folder path                |
----------------------------------------------------------------

3️⃣ myAudit.py

---------------------------------------------------------
| Variable              | Used for                      |
| --------------------- | ----------------------------- |
| `gcp_conn_id`         | BigQuery write access         |
| `impersonation_chain` | Service account impersonation |
| `gcp_project_id`      | Audit table project           |
| `audit_dataset`       | Audit dataset                 |
| `audit_table`         | Audit table                   |
---------------------------------------------------------
