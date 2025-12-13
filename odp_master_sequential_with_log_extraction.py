"""
Parent DAG
----------
• Sequentially runs ingestion DAGs (dynamic list)
• Stops on first failure
• Extracts Airflow exceptions from Composer logs
• Builds dag_run.conf dynamically
• Triggers QA DAG
"""

import re
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance, Variable
from airflow.utils.state import State
from airflow.exceptions import AirflowFailException
from google.cloud import storage

DEFAULT_ARGS = {"owner": "qa", "retries": 0, "depends_on_past": False}

INGESTION_DAGS = Variable.get("ingestion_dags", deserialize_json=True)
QA_DAG_ID = Variable.get("qa_dag_id")
LOG_BUCKET = Variable.get("composer_log_bucket")


# ---------------- helpers ----------------
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
            return "\n".join(txt.splitlines()[-150:])
    return None


# ---------------- monitor ----------------
@provide_session
def monitor_child_run(trigger_task_id, child_dag_id, session=None, **context):
    ti = context["ti"]

    trig = ti.xcom_pull(task_ids=trigger_task_id, key="trigger_dag_run")
    run_id = trig.get("run_id") if isinstance(trig, dict) else None

    dr = session.query(DagRun).filter(
        DagRun.dag_id == child_dag_id,
        DagRun.run_id == run_id
    ).first()

    failed_tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == child_dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.state == State.FAILED
    ).all()

    failed_tasks, exception_details = [], []

    for ft in failed_tis:
        failed_tasks.append({
            "task_id": ft.task_id,
            "state": ft.state
        })

        paths = _log_paths(
            child_dag_id,
            ft.task_id,
            run_id,
            ft.try_number,
            dr.execution_date.isoformat()
        )
        tail = _read_log_tail(LOG_BUCKET, paths)
        if tail:
            m = re.findall(r"(AirflowFailException|Exception).*", tail)
            exception_details.append({
                "task_id": ft.task_id,
                "traceback_snippet": m[-1] if m else tail[-300:]
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


# ---------------- build dag_run.conf ----------------
def prepare_qa_conf(**context):
    ti = context["ti"]
    child_runs = [
        ti.xcom_pull(key=f"{dag_id}_status")
        for dag_id in INGESTION_DAGS
        if ti.xcom_pull(key=f"{dag_id}_status")
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
    default_args=DEFAULT_ARGS
) as dag:

    start = EmptyOperator(task_id="start")
    previous = start

    for dag_id in INGESTION_DAGS:
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=False
        )

        monitor = PythonOperator(
            task_id=f"monitor_{dag_id}",
            python_callable=monitor_child_run,
            op_kwargs={
                "trigger_task_id": f"trigger_{dag_id}",
                "child_dag_id": dag_id
            }
        )

        previous >> trigger >> monitor
        previous = monitor

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

    previous >> prepare_qa >> run_qa
