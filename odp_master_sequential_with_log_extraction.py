"""
odp_master_sequential_with_log_extraction.py

Parent DAG: sequential child DAGs with QA after each stage if failure or a final QA at end.
Monitor step extracts failed task exceptions from Composer GCS logs and includes them in child run XComs.

Behavior:
- trigger stage1 -> monitor -> if failed -> prepare QA conf (includes stage1 + stage2 if present) -> run QA -> fail parent
- if stage1 success -> continue stage2...
- final QA at end if all stages success

Config via Airflow Variables:
 - composer_log_bucket (optional): GCS bucket where Airflow/Composer logs are stored.
 - test_report_folder: folder inside ap-bld-01-stb-euwe2-loans for reports.
"""

import time
import json
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
from airflow.utils.state import State

# GCS client
try:
    from google.cloud import storage
except Exception:
    storage = None  # we'll handle missing lib gracefully in the code

default_args = {"owner": "qa", "depends_on_past": False, "retries": 0}
DEFAULT_TIMEOUT_MINUTES = 60


def _var(name, default=None):
    try:
        return Variable.get(name, default_var=default)
    except Exception:
        return default


# -----------------------------
# Log extraction helper
# -----------------------------
def _get_log_tail_from_gcs(bucket_name, candidate_paths, tail_lines=120):
    """
    Try a list of candidate GCS paths in order and return the last `tail_lines`
    of the matched log if any found. Returns None if nothing found or if GCS client missing.
    """
    if storage is None:
        return None, "google-cloud-storage not available in environment"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for p in candidate_paths:
        blob = bucket.blob(p)
        if blob.exists():
            try:
                text = blob.download_as_text(errors="ignore")
                lines = text.splitlines()
                tail = "\n".join(lines[-tail_lines:]) if lines else ""
                return tail, f"found:{p}"
            except Exception as e:
                return None, f"error_reading:{p}:{str(e)}"
    return None, "no_log_found"


def _guess_log_paths(dag_id, task_id, run_id, try_number, execution_date_iso):
    """
    Return a list of likely GCS log paths for Composer/Cloud Composer variations.
    We keep patterns broad; monitor will try them in order.
    """
    # normalize execution_date for path usage (replace ":" which are not allowed in some systems)
    ed = execution_date_iso.replace(":", "").replace("+", "")
    patterns = [
        # Cloud Composer typical path: airflow-logs/<dag_id>/<task_id>/<execution_date>/<try_number>.log
        f"airflow-logs/{dag_id}/{task_id}/{execution_date_iso}/{try_number}.log",
        f"airflow-logs/{dag_id}/{task_id}/{ed}/{try_number}.log",
        # older/alternate: logs/<dag_id>/<task_id>/<execution_date>/<try_number>.log
        f"logs/{dag_id}/{task_id}/{execution_date_iso}/{try_number}.log",
        f"logs/{dag_id}/{task_id}/{ed}/{try_number}.log",
        # sometimes run_id is used in path
        f"airflow-logs/{dag_id}/{task_id}/{run_id}/{try_number}.log",
        f"logs/{dag_id}/{task_id}/{run_id}/{try_number}.log",
    ]
    return patterns


# -----------------------------
# Monitor child DAG run + extract exceptions into XCom
# -----------------------------
@provide_session
def monitor_child_run(trigger_task_id: str, child_dag_id: str,
                      timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES,
                      poke_interval_seconds: int = 15,
                      session=None, **context):
    ti = context["ti"]

    # 1) extract run_id from TriggerDagRunOperator XCom if available
    trig = ti.xcom_pull(task_ids=trigger_task_id, key="trigger_dag_run")
    run_id = None
    if isinstance(trig, dict):
        run_id = trig.get("run_id") or trig.get("dag_run_id")
    elif hasattr(trig, "run_id"):
        run_id = getattr(trig, "run_id")

    # 2) fallback: latest recent DagRun
    if not run_id:
        lookback = datetime.utcnow() - timedelta(hours=3)
        dr = session.query(DagRun).filter(
            DagRun.dag_id == child_dag_id,
            DagRun.start_date >= lookback
        ).order_by(DagRun.execution_date.desc()).first()
        if dr:
            run_id = dr.run_id

    if not run_id:
        result = {"dag_id": child_dag_id, "run_id": None, "state": "NOT_FOUND", "failed_tasks": [], "exception_details": []}
        ti.xcom_push(key=f"{child_dag_id}_run_status", value=result)
        return result

    # 3) Poll until terminal state (success/failed/etc.)
    deadline = datetime.utcnow() + timedelta(minutes=timeout_minutes)
    dr_obj = None
    while datetime.utcnow() < deadline:
        dr_obj = session.query(DagRun).filter(DagRun.dag_id == child_dag_id, DagRun.run_id == run_id).first()
        if dr_obj and dr_obj.state in (State.SUCCESS, State.FAILED, State.UP_FOR_RETRY, State.SKIPPED):
            break
        time.sleep(poke_interval_seconds)

    if not dr_obj:
        result = {"dag_id": child_dag_id, "run_id": run_id, "state": "UNKNOWN_OR_NOT_COMPLETED", "failed_tasks": [], "exception_details": []}
        ti.xcom_push(key=f"{child_dag_id}_run_status", value=result)
        return result

    # 4) collect failed task instances
    failed_tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == child_dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.state == State.FAILED
    ).all()

    failed_tasks = []
    exception_details = []

    # gather basic failed task info
    for f in failed_tis:
        ft = {
            "task_id": f.task_id,
            "try_number": f.try_number,
            "start_date": str(f.start_date),
            "end_date": str(f.end_date),
            "state": f.state
        }
        failed_tasks.append(ft)

    # 5) attempt log extraction from GCS for each failed task (be defensive)
    composer_bucket = _var("composer_log_bucket", default=None)
    # If not provided, try reasonable default bucket names won't be guessed reliably;
    # still attempt using 'airflow-logs' within the same project GCS root if available.
    # We'll build candidate paths and try bucket if provided.
    for f in failed_tasks:
        # choose try number and execution_date for path generation
        try_number = f.get("try_number", 1) or 1
        exec_date = dr_obj.execution_date.isoformat() if dr_obj and dr_obj.execution_date else ""
        candidates = _guess_log_paths(child_dag_id, f["task_id"], run_id, try_number, exec_date)
        log_text = None
        status_meta = None

        if composer_bucket:
            try:
                tail, meta = _get_log_tail_from_gcs(composer_bucket, candidates)
                log_text = tail
                status_meta = f"bucket:{composer_bucket}:{meta}"
            except Exception as e:
                log_text = None
                status_meta = f"error_reading_bucket:{composer_bucket}:{str(e)}"
        else:
            # If no bucket configured, attempt common default names in a short list (best-effort)
            tried = False
            for try_bucket in [f"airflow-logs-{child_dag_id}", "airflow-logs", "logs"]:
                # these are guesses; skip if storage client not available
                if storage is None:
                    status_meta = "gcs_client_not_available"
                    break
                try:
                    tail, meta = _get_log_tail_from_gcs(try_bucket, candidates)
                    if tail:
                        log_text = tail
                        status_meta = f"bucket_guess:{try_bucket}:{meta}"
                        tried = True
                        break
                except Exception:
                    continue
            if not tried and storage is None:
                status_meta = "gcs_client_not_available"

        if log_text:
            # try to extract useful exception traceback snippet (last Traceback section)
            m = re.findall(r"Traceback[\s\S]*?$", log_text, flags=re.MULTILINE)
            tb = m[-1] if m else log_text.splitlines()[-20:]  # fallback to last lines
            exception_details.append({
                "task_id": f["task_id"],
                "try_number": try_number,
                "log_meta": status_meta,
                "traceback_snippet": tb if isinstance(tb, str) else "\n".join(tb)
            })
        else:
            exception_details.append({
                "task_id": f["task_id"],
                "try_number": try_number,
                "log_meta": status_meta or "log_not_found",
                "traceback_snippet": None
            })

    result = {
        "dag_id": child_dag_id,
        "run_id": run_id,
        "state": dr_obj.state,
        "execution_date": str(dr_obj.execution_date),
        "start_date": str(dr_obj.start_date),
        "end_date": str(dr_obj.end_date),
        "failed_tasks": failed_tasks,
        "exception_details": exception_details
    }

    ti.xcom_push(key=f"{child_dag_id}_run_status", value=result)
    return result


# -----------------------------
# prepare QA conf
# -----------------------------
def prepare_qa_conf(collected_keys, **context):
    ti = context["ti"]
    runs = []
    for k in collected_keys:
        r = ti.xcom_pull(key=k)
        if r:
            runs.append(r)
    conf = {
        "child_runs": runs,
        "session_id": f"session_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    }
    ti.xcom_push(key="qa_conf", value=conf)
    return conf


# -----------------------------
# Decide and fail if needed (after QA)
# -----------------------------
def decide_and_fail_if_needed(child_key, **context):
    ti = context["ti"]
    st = ti.xcom_pull(key=child_key) or {}
    state = (st.get("state") or "UNKNOWN").lower()
    if state != "success":
        # include exception details in error message is optional; we let QA have full text in GCS
        raise AirflowFailException(f"Child DAG {st.get('dag_id')} failed (state={st.get('state')}). Parent failing after QA.")
    return {"ok": True}


# -----------------------------
# Parent DAG wiring
# -----------------------------
with DAG(
    dag_id="odp_master_sequential_with_log_extraction",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
    default_args=default_args,
    tags=["orchestration", "qa", "log-extraction"]
) as dag:

    start = EmptyOperator(task_id="start")

    # Stage 1
    trigger1 = TriggerDagRunOperator(task_id="trigger_stage1", trigger_dag_id="odp_temp_dag1", wait_for_completion=False)
    monitor1 = PythonOperator(task_id="monitor_stage1", python_callable=monitor_child_run,
                              op_kwargs={"trigger_task_id": "trigger_stage1", "child_dag_id": "odp_temp_dag1"},
                              provide_context=True)
    prepare_qa1 = PythonOperator(task_id="prepare_qa1", python_callable=prepare_qa_conf,
                                op_kwargs={"collected_keys": ["odp_temp_dag1_run_status"]}, provide_context=True)
    run_qa1 = TriggerDagRunOperator(task_id="run_qa1", trigger_dag_id="config_qa_tests_with_exceptions",
                                    conf="{{ ti.xcom_pull(task_ids='prepare_qa1', key='qa_conf') }}",
                                    wait_for_completion=True)
    decide1 = PythonOperator(task_id="decide_after_stage1", python_callable=decide_and_fail_if_needed,
                             op_kwargs={"child_key": "odp_temp_dag1_run_status"}, provide_context=True)

    # Stage 2
    trigger2 = TriggerDagRunOperator(task_id="trigger_stage2", trigger_dag_id="odp_target_reload_dag", wait_for_completion=False)
    monitor2 = PythonOperator(task_id="monitor_stage2", python_callable=monitor_child_run,
                              op_kwargs={"trigger_task_id": "trigger_stage2", "child_dag_id": "odp_target_reload_dag"},
                              provide_context=True)
    prepare_qa2 = PythonOperator(task_id="prepare_qa2", python_callable=prepare_qa_conf,
                                op_kwargs={"collected_keys": ["odp_temp_dag1_run_status", "odp_target_reload_dag_run_status"]},
                                provide_context=True)
    run_qa2 = TriggerDagRunOperator(task_id="run_qa2", trigger_dag_id="config_qa_tests_with_exceptions",
                                    conf="{{ ti.xcom_pull(task_ids='prepare_qa2', key='qa_conf') }}",
                                    wait_for_completion=True)
    decide2 = PythonOperator(task_id="decide_after_stage2", python_callable=decide_and_fail_if_needed,
                             op_kwargs={"child_key": "odp_target_reload_dag_run_status"}, provide_context=True)

    # Stage 3
    trigger3 = TriggerDagRunOperator(task_id="trigger_stage3", trigger_dag_id="load_ODP_TEST_RELOAD", wait_for_completion=False)
    monitor3 = PythonOperator(task_id="monitor_stage3", python_callable=monitor_child_run,
                              op_kwargs={"trigger_task_id": "trigger_stage3", "child_dag_id": "load_ODP_TEST_RELOAD"},
                              provide_context=True)
    prepare_qa3 = PythonOperator(task_id="prepare_qa3", python_callable=prepare_qa_conf,
                                op_kwargs={"collected_keys": [
                                    "odp_temp_dag1_run_status",
                                    "odp_target_reload_dag_run_status",
                                    "load_ODP_TEST_RELOAD_run_status"
                                ]}, provide_context=True)
    run_qa3 = TriggerDagRunOperator(task_id="run_qa3", trigger_dag_id="config_qa_tests_with_exceptions",
                                    conf="{{ ti.xcom_pull(task_ids='prepare_qa3', key='qa_conf') }}",
                                    wait_for_completion=True)
    decide3 = PythonOperator(task_id="decide_after_stage3", python_callable=decide_and_fail_if_needed,
                             op_kwargs={"child_key": "load_ODP_TEST_RELOAD_run_status"}, provide_context=True)

    end = EmptyOperator(task_id="end")

    # wiring: sequential with QA at failure point or final QA
    start >> trigger1 >> monitor1 >> prepare_qa1 >> run_qa1 >> decide1
    decide1 >> trigger2 >> monitor2 >> prepare_qa2 >> run_qa2 >> decide2
    decide2 >> trigger3 >> monitor3 >> prepare_qa3 >> run_qa3 >> decide3
    decide3 >> end
