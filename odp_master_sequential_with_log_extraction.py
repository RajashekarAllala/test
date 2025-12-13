"""
FINAL PARENT DAG
================
• Sequential ingestion DAG execution (variable-driven)
• Failure short-circuit using BranchPythonOperator
• Retry-aware (only evaluate final state)
• Per-DAG timeout overrides
• Extract Airflow exceptions from Composer logs
• Trigger QA DAG once
• Fail parent ONLY after QA finishes
"""

import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable, DagRun, TaskInstance
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.exceptions import AirflowFailException
from google.cloud import storage

# ---------------- DAG-LEVEL VALIDATION ----------------
REQUIRED_VARS = [
    "ingestion_dag_order", "qa_dag_id",
    "test_report_bucket", "test_report_folder",
    "composer_log_bucket",
    "parent_dag_timeout_minutes",
    "parent_task_timeout_minutes",
    "parent_sla_minutes",
    "per_dag_timeout_minutes"
]

missing = [v for v in REQUIRED_VARS if not Variable.get(v, default_var=None)]
if missing:
    raise RuntimeError(f"Missing Airflow Variables: {missing}")

DAG_ORDER = Variable.get("ingestion_dag_order", deserialize_json=True)
QA_DAG_ID = Variable.get("qa_dag_id")
LOG_BUCKET = Variable.get("composer_log_bucket")

PER_DAG_TIMEOUTS = Variable.get("per_dag_timeout_minutes", deserialize_json=True)

DAG_TIMEOUT = int(Variable.get("parent_dag_timeout_minutes"))
TASK_TIMEOUT = int(Variable.get("parent_task_timeout_minutes"))
SLA_MINUTES = int(Variable.get("parent_sla_minutes"))

DEFAULT_ARGS = {
    "owner": "qa",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=TASK_TIMEOUT),
    "sla": timedelta(minutes=SLA_MINUTES)
}

# ---------------- Helpers ----------------
def _log_paths(dag_id, task_id, run_id, try_no, exec_date):
    ed = exec_date.replace(":", "")
    return [
        f"logs/{dag_id}/{task_id}/{run_id}/{try_no}.log",
        f"airflow-logs/{dag_id}/{task_id}/{exec_date}/{try_no}.log",
        f"airflow-logs/{dag_id}/{task_id}/{ed}/{try_no}.log"
    ]

def _read_log_tail(bucket, paths):
    client = storage.Client()
    bkt = client.bucket(bucket)
    for p in paths:
        blob = bkt.blob(p)
        if blob.exists():
            txt = blob.download_as_text(errors="ignore")
            return "\n".join(txt.splitlines()[-200:])
    return None

# ---------------- Monitor (retry-aware) ----------------
@provide_session
def monitor_child_run(trigger_task_id, child_dag_id, session=None, **context):
    ti = context["ti"]
    trig = ti.xcom_pull(task_ids=trigger_task_id, key="trigger_dag_run")
    run_id = trig.get("run_id") if trig else None

    dr = session.query(DagRun).filter(
        DagRun.dag_id == child_dag_id,
        DagRun.run_id == run_id
    ).first()

    # FINAL state only (retry-aware)
    failed_tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == child_dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.state == State.FAILED
    ).all()

    failed_tasks, exception_details = [], []

    for ft in failed_tis:
        failed_tasks.append({"task_id": ft.task_id})

        paths = _log_paths(
            child_dag_id, ft.task_id, run_id,
            ft.try_number, dr.execution_date.isoformat()
        )
        tail = _read_log_tail(LOG_BUCKET, paths)
        if tail:
            m = re.findall(r"(AirflowFailException|Exception).*", tail)
            exception_details.append({
                "task_id": ft.task_id,
                "traceback": m[-1] if m else tail[-300:]
            })

    result = {
        "dag_id": child_dag_id,
        "run_id": run_id,
        "state": dr.state,
        "failed_tasks": failed_tasks,
        "exception_details": exception_details
    }

    ti.xcom_push(key=f"{child_dag_id}_status", value=result)
    return result

# ---------------- Branch on failure ----------------
def branch_on_failure(child_dag_id, next_task, qa_task, **context):
    st = context["ti"].xcom_pull(key=f"{child_dag_id}_status")
    if st and st["state"] != "success":
        return qa_task
    return next_task

# ---------------- Build QA conf ----------------
def prepare_qa_conf(**context):
    ti = context["ti"]
    child_runs = [
        ti.xcom_pull(key=f"{d}_status")
        for d in DAG_ORDER
        if ti.xcom_pull(key=f"{d}_status")
    ]
    conf = {
        "session_id": f"session_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
        "child_runs": child_runs
    }
    ti.xcom_push("qa_conf", conf)
    return conf

# ---------------- DAG ----------------
with DAG(
    dag_id="odp_master_sequential_with_log_extraction",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=DAG_TIMEOUT),
    default_args=DEFAULT_ARGS
) as dag:

    start = EmptyOperator(task_id="start")

    previous = start
    branches = []

    for idx, dag_id in enumerate(DAG_ORDER):
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=False
        )

        monitor = PythonOperator(
            task_id=f"monitor_{dag_id}",
            python_callable=monitor_child_run,
            execution_timeout=timedelta(
                minutes=PER_DAG_TIMEOUTS.get(dag_id, TASK_TIMEOUT)
            ),
            op_kwargs={
                "trigger_task_id": f"trigger_{dag_id}",
                "child_dag_id": dag_id
            }
        )

        next_task = (
            f"trigger_{DAG_ORDER[idx + 1]}"
            if idx + 1 < len(DAG_ORDER)
            else "prepare_qa_conf"
        )

        branch = BranchPythonOperator(
            task_id=f"branch_after_{dag_id}",
            python_callable=branch_on_failure,
            op_kwargs={
                "child_dag_id": dag_id,
                "next_task": next_task,
                "qa_task": "prepare_qa_conf"
            }
        )

        previous >> trigger >> monitor >> branch
        previous = branch
        branches.append(branch)

    prepare_qa = PythonOperator(
        task_id="prepare_qa_conf",
        python_callable=prepare_qa_conf
    )

    run_qa = TriggerDagRunOperator(
        task_id="run_qa",
        trigger_dag_id=QA_DAG_ID,
        conf="{{ ti.xcom_pull(task_ids='prepare_qa_conf', key='qa_conf') }}",
        wait_for_completion=True
    )

    branches >> prepare_qa >> run_qa
