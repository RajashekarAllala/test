"""
Parent DAG
==========

Runs ingestion DAGs sequentially:
1) odp_temp_footer_validation_string
2) odp_target_load_reload
3) load_odp_ODP_TEST_RELOAD

Rules:
- Stop immediately on first failure
- Capture failed task exception text from Composer logs
- Build dag_run.conf
- Trigger QA DAG
- Fail parent DAG ONLY AFTER QA finishes
"""

import re
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
from airflow.utils.state import State

from google.cloud import storage

DEFAULT_ARGS = {
    "owner": "qa",
    "depends_on_past": False,
    "retries": 0
}

# --------------------------
# Helpers
# --------------------------
def _var(name):
    try:
        return Variable.get(name)
    except Exception:
        return None


def _log_paths(dag_id, task_id, run_id, try_no, exec_date):
    ed = exec_date.replace(":", "")
    return [
        f"airflow-logs/{dag_id}/{task_id}/{exec_date}/{try_no}.log",
        f"airflow-logs/{dag_id}/{task_id}/{ed}/{try_no}.log",
        f"logs/{dag_id}/{task_id}/{run_id}/{try_no}.log"
    ]


def _read_log_tail(bucket, paths):
    client = storage.Client()
    bkt = client.bucket(bucket)
    for p in paths:
        blob = bkt.blob(p)
        if blob.exists():
            text = blob.download_as_text(errors="ignore")
            return "\n".join(text.splitlines()[-150:])
    return None


# --------------------------
# Monitor child DAG
# --------------------------
@provide_session
def monitor_child_run(trigger_task_id, child_dag_id, session=None, **context):
    ti = context["ti"]

    trig = ti.xcom_pull(task_ids=trigger_task_id, key="trigger_dag_run")
    run_id = trig.get("run_id") if isinstance(trig, dict) else None

    if not run_id:
        dr = session.query(DagRun).filter(
            DagRun.dag_id == child_dag_id
        ).order_by(DagRun.execution_date.desc()).first()
        run_id = dr.run_id if dr else None

    dr = session.query(DagRun).filter(
        DagRun.dag_id == child_dag_id,
        DagRun.run_id == run_id
    ).first()

    failed_tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == child_dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.state == State.FAILED
    ).all()

    failed_tasks = []
    exception_details = []
    log_bucket = _var("composer_log_bucket")

    for ft in failed_tis:
        failed_tasks.append({
            "task_id": ft.task_id,
            "try_number": ft.try_number,
            "state": ft.state
        })

        if log_bucket and dr:
            paths = _log_paths(
                child_dag_id,
                ft.task_id,
                run_id,
                ft.try_number,
                dr.execution_date.isoformat()
            )
            tail = _read_log_tail(log_bucket, paths)
            if tail:
                match = re.findall(r"(AirflowFailException|Exception).*", tail)
                exception_details.append({
                    "task_id": ft.task_id,
                    "traceback_snippet": match[-1] if match else tail[-300:]
                })

    result = {
        "dag_id": child_dag_id,
        "run_id": run_id,
        "state": dr.state if dr else "UNKNOWN",
        "execution_date": str(dr.execution_date) if dr else None,
        "failed_tasks": failed_tasks,
        "exception_details": exception_details
    }

    ti.xcom_push(key=f"{child_dag_id}_run_status", value=result)
    return result


# --------------------------
# Build dag_run.conf
# --------------------------
def prepare_qa_conf(collected_keys, **context):
    ti = context["ti"]
    child_runs = []

    for key in collected_keys:
        val = ti.xcom_pull(key=key)
        if val:
            child_runs.append(val)

    conf = {
        "session_id": f"session_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
        "child_runs": child_runs
    }

    ti.xcom_push(key="qa_conf", value=conf)
    return conf


def fail_parent(child_key, **context):
    st = context["ti"].xcom_pull(key=child_key)
    if st["state"].lower() != "success":
        raise AirflowFailException(f"{st['dag_id']} failed")
    return "OK"


# --------------------------
# DAG definition
# --------------------------
with DAG(
    dag_id="odp_master_sequential_with_log_extraction",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS
) as dag:

    start = EmptyOperator(task_id="start")

    # -------- DAG 1 --------
    t1 = TriggerDagRunOperator(
        task_id="trigger_dag1",
        trigger_dag_id="odp_temp_footer_validation_string",
        wait_for_completion=False
    )

    m1 = PythonOperator(
        task_id="monitor_dag1",
        python_callable=monitor_child_run,
        op_kwargs={
            "trigger_task_id": "trigger_dag1",
            "child_dag_id": "odp_temp_footer_validation_string"
        }
    )

    # -------- DAG 2 --------
    t2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="odp_target_load_reload",
        wait_for_completion=False
    )

    m2 = PythonOperator(
        task_id="monitor_dag2",
        python_callable=monitor_child_run,
        op_kwargs={
            "trigger_task_id": "trigger_dag2",
            "child_dag_id": "odp_target_load_reload"
        }
    )

    # -------- DAG 3 --------
    t3 = TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id="load_odp_ODP_TEST_RELOAD",
        wait_for_completion=False
    )

    m3 = PythonOperator(
        task_id="monitor_dag3",
        python_callable=monitor_child_run,
        op_kwargs={
            "trigger_task_id": "trigger_dag3",
            "child_dag_id": "load_odp_ODP_TEST_RELOAD"
        }
    )

    # -------- QA --------
    prepare_qa = PythonOperator(
        task_id="prepare_qa_conf",
        python_callable=prepare_qa_conf,
        op_kwargs={
            "collected_keys": [
                "odp_temp_footer_validation_string_run_status",
                "odp_target_load_reload_run_status",
                "load_odp_ODP_TEST_RELOAD_run_status"
            ]
        }
    )

    run_qa = TriggerDagRunOperator(
        task_id="run_qa",
        trigger_dag_id="config_qa_tests_with_exceptions",
        conf="{{ ti.xcom_pull(task_ids='prepare_qa_conf', key='qa_conf') }}",
        wait_for_completion=True
    )

    end = EmptyOperator(task_id="end")

    start >> t1 >> m1 >> t2 >> m2 >> t3 >> m3 >> prepare_qa >> run_qa >> end
