"""
QA DAG
======

- Reads dag_run.conf
- Validates structure
- Writes:
  - detailed JSON
  - CSV
  - latest_qa_summary.json
- NEVER fails
"""

from datetime import datetime
import json, csv, io

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

DEFAULT_ARGS = {
    "owner": "qa",
    "depends_on_past": False,
    "retries": 0
}


def validate_conf(conf):
    errors = []
    if not isinstance(conf, dict):
        return ["dag_run.conf missing or invalid"], []

    child_runs = conf.get("child_runs")
    if not isinstance(child_runs, list):
        return ["child_runs missing or invalid"], []

    for i, cr in enumerate(child_runs):
        if "dag_id" not in cr:
            errors.append(f"child_runs[{i}] missing dag_id")
        if "state" not in cr:
            errors.append(f"child_runs[{i}] missing state")

    return errors, child_runs


def write_report(**context):
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    errors, child_runs = validate_conf(conf)

    bucket = "ap-bld-01-stb-euwe2-loans"
    folder = Variable.get("test_report_folder")

    failed = bool(errors)
    for cr in child_runs:
        if cr.get("state", "").lower() != "success":
            failed = True

    status = "FAIL" if failed else "PASS"
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    detailed = {
        "run_ts": ts,
        "status": status,
        "dq_conf_errors": errors,
        "child_run_statuses": child_runs
    }

    summary = {
        "run_ts": ts,
        "status": status,
        "dags": [
            {
                "dag_id": c.get("dag_id"),
                "state": c.get("state"),
                "failed_tasks": len(c.get("failed_tasks", []))
            } for c in child_runs
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
    dag_id="config_qa_tests_with_exceptions",
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
