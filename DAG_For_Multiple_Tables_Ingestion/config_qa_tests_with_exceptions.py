from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import json
import csv
import io

# =====================================================
# Airflow Variables
# =====================================================
GCP_CONN_ID = Variable.get("gcp_conn_id")
IMPERSONATION_CHAIN = Variable.get("impersonation_chain")

PROJECT_ID = Variable.get("gcp_project_id")

AUDIT_DATASET = Variable.get("audit_dataset")
AUDIT_TABLE = Variable.get("audit_table")

CONFIG_DATASET = Variable.get("config_dataset_name")
CONFIG_TABLE = Variable.get("config_table_name")

QA_PROJECT = Variable.get("qa_bq_project")
QA_DATASET = Variable.get("qa_bq_dataset")
QA_TABLE = Variable.get("qa_bq_table")

REPORT_BUCKET = Variable.get("test_report_bucket")
REPORT_FOLDER = Variable.get("test_report_folder")

# =====================================================
# QA Logic
# =====================================================
def run_qa(**context):
    dag_run_ts = context["dag_run"].execution_date

    bq = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN
    )

    gcs = GCSHook(
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN
    )

    # -------------------------------------------------
    # QA SQL with explicit SCD-2 validation
    # -------------------------------------------------
    qa_sql = f"""
    WITH audit_base AS (
      SELECT
        a.dag_name,
        a.table_name,
        a.task_name,
        a.task_status,
        a.audit_status,
        a.source_count,
        a.target_count,
        a.error_message,
        a.task_start_datetime
      FROM `{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE}` a
    ),

    cfg AS (
      SELECT
        table_name,
        UPPER(load_type) AS load_type
      FROM `{PROJECT_ID}.{CONFIG_DATASET}.{CONFIG_TABLE}`
      WHERE is_active = TRUE
    ),

    joined AS (
      SELECT
        a.*,
        c.load_type
      FROM audit_base a
      JOIN cfg c
        ON a.table_name = c.table_name
    ),

    -- any failed task
    first_failure AS (
      SELECT
        table_name,
        MIN(task_start_datetime) AS first_fail_ts
      FROM joined
      WHERE audit_status = 'FAIL'
      GROUP BY table_name
    ),

    -- explicit SCD-2 validation
    scd2_validation AS (
      SELECT
        table_name,
        MIN(
          CASE
            WHEN source_count = target_count THEN 1
            ELSE 0
          END
        ) AS scd2_ok
      FROM joined
      WHERE load_type = 'INCREMENTAL'
        AND task_name = 'load_to_target'
      GROUP BY table_name
    )

    SELECT
      j.dag_name,
      TIMESTAMP('{dag_run_ts}') AS dag_run_datetime,
      j.table_name,
      j.load_type,

      CASE
        WHEN f.table_name IS NOT NULL THEN 'FAILED'
        WHEN j.load_type = 'INCREMENTAL' AND s.scd2_ok = 0 THEN 'FAILED'
        ELSE 'SUCCESS'
      END AS task_status,

      CASE
        WHEN f.table_name IS NOT NULL THEN 'FAIL'
        WHEN j.load_type = 'INCREMENTAL' AND s.scd2_ok = 0 THEN 'FAIL'
        ELSE 'PASS'
      END AS qa_status,

      CASE
        WHEN f.table_name IS NOT NULL THEN (
          SELECT task_name
          FROM joined jf
          WHERE jf.table_name = j.table_name
            AND jf.audit_status = 'FAIL'
          ORDER BY jf.task_start_datetime
          LIMIT 1
        )
        WHEN j.load_type = 'INCREMENTAL' AND s.scd2_ok = 0
          THEN 'scd2_record_check_ts_reconciliation'
        ELSE NULL
      END AS failed_task_name,

      CASE
        WHEN f.table_name IS NOT NULL THEN (
          SELECT error_message
          FROM joined jf
          WHERE jf.table_name = j.table_name
            AND jf.audit_status = 'FAIL'
          ORDER BY jf.task_start_datetime
          LIMIT 1
        )
        WHEN j.load_type = 'INCREMENTAL' AND s.scd2_ok = 0
          THEN 'SCD2 touched rows do not match staging count'
        ELSE NULL
      END AS failure_reason

    FROM joined j
    LEFT JOIN first_failure f
      ON j.table_name = f.table_name
    LEFT JOIN scd2_validation s
      ON j.table_name = s.table_name
    GROUP BY
      j.dag_name,
      j.table_name,
      j.load_type,
      f.table_name,
      s.scd2_ok
    """

    qa_rows = bq.get_records(qa_sql)

    if not qa_rows:
        raise RuntimeError("QA query returned no rows")

    # -------------------------------------------------
    # Insert QA results into BigQuery
    # -------------------------------------------------
    qa_dict_rows = []
    for r in qa_rows:
        qa_dict_rows.append({
            "dag_name": r[0],
            "dag_run_datetime": r[1],
            "table_name": r[2],
            "load_type": r[3],
            "task_name": "ALL_TASKS",
            "task_status": r[4],
            "qa_status": r[5],
            "failed_task_name": r[6],
            "failure_reason": r[7]
        })

    bq.insert_rows(
        table=f"{QA_PROJECT}.{QA_DATASET}.{QA_TABLE}",
        rows=qa_dict_rows
    )

    # -------------------------------------------------
    # Write QA report to GCS (JSON + CSV)
    # -------------------------------------------------
    json_path = f"{REPORT_FOLDER}/qa_report_{dag_run_ts}.json"
    csv_path = f"{REPORT_FOLDER}/qa_report_{dag_run_ts}.csv"

    gcs.upload(
        REPORT_BUCKET,
        json_path,
        json.dumps(qa_dict_rows, default=str, indent=2)
    )

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=qa_dict_rows[0].keys())
    writer.writeheader()
    writer.writerows(qa_dict_rows)

    gcs.upload(
        REPORT_BUCKET,
        csv_path,
        buffer.getvalue()
    )

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="config_qa_tests_with_exceptions",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["qa", "audit"]
) as dag:

    PythonOperator(
        task_id="run_qa",
        python_callable=run_qa
    )
