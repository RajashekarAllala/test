from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime

# ======================================================
# Airflow Variables
# ======================================================
GCP_CONN_ID = Variable.get("gcp_conn_id")
IMPERSONATION_CHAIN = Variable.get("impersonation_chain")

PROJECT_ID = Variable.get("gcp_project_id")
AUDIT_DATASET = Variable.get("audit_dataset")
AUDIT_TABLE = Variable.get("audit_table")

# ======================================================
# BigQuery Hook (with impersonation)
# ======================================================
def _bq():
    return BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN
    )

# ======================================================
# Audit Writer
# ======================================================
def write_audit(
    dag_name: str,
    table_name: str,
    task_id: str,
    task_name: str,
    task_start_datetime: datetime,
    task_end_datetime: datetime,
    task_status: str,
    source_count: int = None,
    target_count: int = None,
    error_message: str = None
):
    """
    Writes one audit record per logical task execution.

    - Duration is derived, never passed
    - audit_status is derived from task_status
    - Safe for SUCCESS / FAILED paths
    """

    duration_seconds = int(
        (task_end_datetime - task_start_datetime).total_seconds()
    )

    audit_status = "PASS" if task_status == "SUCCESS" else "FAIL"

    row = {
        "dag_name": dag_name,
        "table_name": table_name,
        "task_id": task_id,
        "task_name": task_name,
        "task_start_datetime": task_start_datetime,
        "task_end_datetime": task_end_datetime,
        "duration_seconds": duration_seconds,
        "task_status": task_status,
        "audit_status": audit_status,
        "source_count": source_count,
        "target_count": target_count,
        "error_message": error_message
    }

    _bq().insert_rows(
        table=f"{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE}",
        rows=[row]
    )
