"""
FINAL QA DAG
============
• Validates dag_run.conf
• Writes JSON + CSV + latest summary to GCS
• Inserts audit rows into BigQuery
• NEVER fails
"""

from datetime import datetime
import json, csv, io
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

DEFAULT_ARGS = {"owner": "qa", "retries": 0, "depends_on_past": False}

def write_report(**context):
    conf = context["dag_run"].conf or {}

    bucket = Variable.get("test_report_bucket")
    folder = Variable.get("test_report_folder")

    bq_project = Variable.get("qa_bq_project")
    bq_dataset = Variable.get("qa_bq_dataset")
    bq_table = Variable.get("qa_bq_table")

    child_runs = conf.get("child_runs", [])
    failed = any(c.get("state") != "success" for c in child_runs)

    status = "FAIL" if failed else "PASS"
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    detailed = {
        "run_ts": ts,
        "session_id": conf.get("session_id"),
        "status": status,
        "child_run_statuses": child_runs
    }

    summary = {
        "run_ts": ts,
        "status": status,
        "dag_count": len(child_runs),
        "failed_dags": [c["dag_id"] for c in child_runs if c["state"] != "success"]
    }

    # ---------- GCS ----------
    gcs = GCSHook()
    gcs.upload(bucket, f"{folder}/config_qa_report_{ts}.json", json.dumps(detailed, indent=2))
    gcs.upload(bucket, f"{folder}/latest_qa_summary.json", json.dumps(summary, indent=2))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["run_ts", "status", "child_runs"])
    writer.writerow([ts, status, json.dumps(child_runs)])
    gcs.upload(bucket, f"{folder}/config_qa_report_{ts}.csv", buf.getvalue())

    # ---------- BigQuery audit ----------
    try:
        bq = BigQueryHook()
        rows = []
        for c in child_runs:
            rows.append({
                "run_ts": ts,
                "session_id": conf.get("session_id"),
                "status": status,
                "dag_id": c["dag_id"],
                "dag_state": c["state"],
                "failed_task_count": len(c.get("failed_tasks", [])),
                "exception_count": len(c.get("exception_details", []))
            })
        bq.insert_all(
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_id=bq_table,
            rows=rows
        )
    except Exception as e:
        # QA DAG must never fail
        print(f"BQ insert skipped: {e}")

with DAG(
    dag_id=Variable.get("qa_dag_id"),
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS
) as dag:

    PythonOperator(task_id="write_report", python_callable=write_report)
