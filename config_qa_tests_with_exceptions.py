"""
QA DAG
------
• Reads dag_run.conf
• Writes JSON, CSV, latest summary
• Uses Airflow Variables only
• NEVER fails
"""

from datetime import datetime
import json, csv, io
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

DEFAULT_ARGS = {"owner": "qa", "retries": 0, "depends_on_past": False}


def write_report(**context):
    conf = context["dag_run"].conf or {}

    bucket = Variable.get("test_report_bucket")
    folder = Variable.get("test_report_folder")

    child_runs = conf.get("child_runs", [])
    failed = any(c.get("state") != "success" for c in child_runs)

    status = "FAIL" if failed else "PASS"
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    detailed = {
        "run_ts": ts,
        "status": status,
        "child_run_statuses": child_runs
    }

    summary = {
        "run_ts": ts,
        "status": status,
        "dags": [
            {"dag_id": c["dag_id"], "state": c["state"]}
            for c in child_runs
        ]
    }

    gcs = GCSHook()

    gcs.upload(bucket, f"{folder}/config_qa_report_{ts}.json", json.dumps(detailed, indent=2))
    gcs.upload(bucket, f"{folder}/latest_qa_summary.json", json.dumps(summary, indent=2))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["run_ts", "status", "child_runs"])
    writer.writerow([ts, status, json.dumps(child_runs)])
    gcs.upload(bucket, f"{folder}/config_qa_report_{ts}.csv", buf.getvalue())


with DAG(
    dag_id=Variable.get("qa_dag_id"),
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS
) as dag:

    PythonOperator(
        task_id="write_report",
        python_callable=write_report
    )
