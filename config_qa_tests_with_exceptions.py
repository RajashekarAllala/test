"""
config_qa_tests_with_exceptions.py

QA DAG that reads conf["child_runs"] (list of child run dicts) or falls back to monitored_dags.
Writes JSON + CSV reports to gs://ap-bld-01-stb-euwe2-loans/<test_report_folder>/.
Includes any exception_details provided by the parent (from log extraction).
Always succeeds (Option 3 behavior).
Also writes a compact human-friendly summary file latest_qa_summary.json for quick lookup.
"""

from datetime import datetime
import json, io, csv, logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from reusable_templates import utils  # your existing utils
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger("airflow.task")

DEFAULT_ARGS = {"owner": "qa", "depends_on_past": False, "retries": 0}


def _var(name, default=None):
    try:
        return Variable.get(name, default_var=default)
    except Exception:
        return default


def _hooks():
    return BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False), GCSHook(gcp_conn_id="google_cloud_default")


def fetch_config_rows(**context):
    ti = context["ti"]
    # fetch config table rows if needed for validations (optional)
    project = _var("gcp_project_id", getattr(utils, "project_id", None))
    dataset = _var("audit_dataset", getattr(utils, "audit_dataset", None))
    table = _var("config_table", default=f"{project}.{dataset}.config")
    bq, _ = _hooks()
    try:
        df = bq.get_pandas_df(f"SELECT * FROM `{table}`")
        rows = json.loads(df.to_json(orient="records"))
    except Exception:
        rows = []
    ti.xcom_push("config_rows", rows)
    ti.xcom_push("errors", [])
    ti.xcom_push("config_table", table)
    return f"Fetched {len(rows)} config rows"


def capture_conf_child_runs(**context):
    """
    Read dag_run.conf['child_runs'] if present (set by parent) otherwise fallback to empty list.
    The parent pushes detailed child run statuses (including exception_details) into conf.
    """
    ti = context["ti"]
    dr = context.get("dag_run")
    conf = {}
    if dr and getattr(dr, "conf", None):
        conf = dr.conf
    child_runs = conf.get("child_runs") if isinstance(conf, dict) else None
    if not child_runs:
        # fallback: try reading monitored_dags variable and get latest runs (older behavior)
        monitored = _var("monitored_dags", default="[]")
        try:
            child_runs = json.loads(monitored) if isinstance(monitored, str) else monitored
            # normalize to dicts if they are just DAG ids
            child_runs = [{"dag_id": d, "run_id": None} if isinstance(d, str) else d for d in child_runs]
        except Exception:
            child_runs = []
    ti.xcom_push("child_run_statuses", child_runs)
    return f"Captured {len(child_runs)} child runs from conf"


def write_report(**context):
    ti = context["ti"]
    bucket = "ap-bld-01-stb-euwe2-loans"
    folder = _var("test_report_folder")
    if not folder:
        return "test_report_folder not set"

    rows = ti.xcom_pull("config_rows") or []
    errors = ti.xcom_pull("errors") or []
    child_statuses = ti.xcom_pull("child_run_statuses") or []

    # Determine PASS/FAIL: if any child run has state != success OR there are config errors
    failed = False
    for cr in child_statuses:
        if cr.get("state") and str(cr.get("state")).lower() != "success":
            failed = True
            break
    if errors:
        failed = True

    status = "FAIL" if failed else "PASS"

    # Full detailed summary
    summary = {
        "run_ts": datetime.utcnow().isoformat() + "Z",
        "status": status,
        "num_config_rows": len(rows),
        "num_errors": len(errors),
        "errors": errors,
        "child_run_statuses": child_statuses,
        "config_table": ti.xcom_pull("config_table"),
    }

    # Build a compact human-friendly summary
    # include top-level fields and compact child summaries (no full tracebacks)
    child_compact = []
    for cr in child_statuses:
        child_compact.append({
            "dag_id": cr.get("dag_id"),
            "run_id": cr.get("run_id"),
            "state": cr.get("state"),
            "failed_tasks_count": len(cr.get("failed_tasks") or []),
            # optionally include short exception messages if present
            "exceptions_present": bool(cr.get("exception_details"))
        })

    latest_summary = {
        "run_ts": summary["run_ts"],
        "status": summary["status"],
        "num_config_rows": summary["num_config_rows"],
        "num_errors": summary["num_errors"],
        "child_runs": child_compact,
        # small human message for dashboards
        "message": "All good" if status == "PASS" else "One or more failures found. See detailed report."
    }

    # Upload to GCS
    _, gcs = _hooks()
    client = gcs.get_conn().get_bucket(bucket)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    json_path = f"{folder}/config_qa_report_{ts}.json"
    csv_path = f"{folder}/config_qa_report_{ts}.csv"
    summary_path = f"{folder}/latest_qa_summary.json"

    # Upload full JSON report
    client.blob(json_path).upload_from_string(json.dumps(summary, indent=2, default=str), content_type="application/json")

    # CSV single row with child_statuses JSON embedded
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["run_ts", "status", "num_config_rows", "num_errors", "child_statuses_json"])
    writer.writerow([summary["run_ts"], summary["status"], summary["num_config_rows"], summary["num_errors"], json.dumps(child_statuses)])
    client.blob(csv_path).upload_from_string(buf.getvalue(), content_type="text/csv")

    # Write the human-friendly summary (overwritten on each run)
    client.blob(summary_path).upload_from_string(json.dumps(latest_summary, indent=2, default=str), content_type="application/json")

    # Optionally push a tiny xcom so parent can read QA status easily
    ti.xcom_push(key="qa_result", value={"status": status, "json_path": json_path, "csv_path": csv_path, "summary_path": summary_path})
    return f"Wrote report to gs://{bucket}/{folder}/ (detailed JSON, CSV, short summary)"

# DAG definition
with DAG(
    dag_id="config_qa_tests_with_exceptions",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["qa", "validation", "with-exceptions"]
) as dag:

    t_fetch = PythonOperator(task_id="fetch_config_rows", python_callable=fetch_config_rows)
    t_capture = PythonOperator(task_id="capture_conf_child_runs", python_callable=capture_conf_child_runs, provide_context=True)
    t_report = PythonOperator(task_id="write_report", python_callable=write_report, provide_context=True)

    t_fetch >> t_capture >> t_report
